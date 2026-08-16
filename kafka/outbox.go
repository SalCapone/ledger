// Package kafka bridges the ledger's Postgres event log to Kafka via the
// transactional outbox pattern: events are written to Postgres and the
// outbox table in the same transaction (see migrations/0001_init.sql), and
// this package's Publisher polls the outbox for unpublished rows, produces
// them to Kafka, and marks them published — all with at-least-once delivery.
//
// This is deliberately a separate Go module from the core ledger package
// (see ../go.mod vs ./go.mod) so that consumers who only want the
// in-process event-sourcing primitives never have to pull in a Kafka
// client.
package kafka

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// WireEvent is the JSON shape published to Kafka. It's intentionally a
// separate type from ledger.Event (rather than reusing it directly) so the
// wire format can evolve independently of the internal Go struct.
type WireEvent struct {
	ID            string            `json:"id"`
	AggregateID   string            `json:"aggregate_id"`
	AggregateType string            `json:"aggregate_type"`
	Version       int               `json:"version"`
	Type          string            `json:"type"`
	Data          json.RawMessage   `json:"data"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	RecordedAt    time.Time         `json:"recorded_at"`
	GlobalSeq     int64             `json:"global_seq"`
}

// Publisher polls the event_outbox table for rows that haven't been
// published yet, produces one Kafka message per event, and marks each row
// published only after a successful produce — giving at-least-once
// delivery (a crash between produce and mark-published can cause a
// duplicate, which is why downstream consumers should key on ID/GlobalSeq
// and treat them as idempotency tokens).
type Publisher struct {
	db       *sql.DB
	writer   *kafka.Writer
	topic    string
	batch    int
	pollWait time.Duration
}

// NewPublisher builds a Publisher. brokers is a list like
// []string{"localhost:9092"}. topic receives one message per event, keyed
// by AggregateID so Kafka preserves per-aggregate ordering across
// partitions.
func NewPublisher(db *sql.DB, brokers []string, topic string) *Publisher {
	return &Publisher{
		db:    db,
		topic: topic,
		batch: 200,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // key-based partitioning keeps one aggregate's events in order
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 50 * time.Millisecond,
		},
		pollWait: 500 * time.Millisecond,
	}
}

// Close releases the underlying Kafka writer's connections.
func (p *Publisher) Close() error {
	return p.writer.Close()
}

// Run polls indefinitely until ctx is cancelled, publishing unpublished
// outbox rows in commit order. It's safe to run multiple instances
// concurrently (e.g. one per app replica): the `FOR UPDATE SKIP LOCKED`
// below means each row is claimed by exactly one poller at a time, so
// publishers horizontally scale without a distributed lock.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollWait)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := p.publishBatch(ctx)
			if err != nil {
				return fmt.Errorf("ledger/kafka: publish batch: %w", err)
			}
			if n == p.batch {
				// Outbox likely still has a backlog; don't wait a full
				// tick before draining more of it.
				continue
			}
		}
	}
}

// publishBatch claims up to p.batch unpublished rows, produces them, and
// marks them published. Returns the number of rows processed.
func (p *Publisher) publishBatch(ctx context.Context) (int, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck

	rows, err := tx.QueryContext(ctx, `
		SELECT e.global_seq, e.id, e.aggregate_id, e.aggregate_type, e.version,
		       e.type, e.data, e.metadata, e.recorded_at
		FROM event_outbox o
		JOIN events e ON e.global_seq = o.global_seq
		WHERE o.published_at IS NULL
		ORDER BY o.global_seq ASC
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED`, p.batch)
	if err != nil {
		return 0, fmt.Errorf("select unpublished: %w", err)
	}

	var claimed []WireEvent
	for rows.Next() {
		var we WireEvent
		var metaJSON []byte
		if err := rows.Scan(&we.GlobalSeq, &we.ID, &we.AggregateID, &we.AggregateType,
			&we.Version, &we.Type, &we.Data, &metaJSON, &we.RecordedAt); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan outbox row: %w", err)
		}
		if len(metaJSON) > 0 {
			_ = json.Unmarshal(metaJSON, &we.Metadata)
		}
		claimed = append(claimed, we)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(claimed) == 0 {
		return 0, tx.Commit()
	}

	msgs := make([]kafka.Message, len(claimed))
	seqs := make([]int64, len(claimed))
	for i, we := range claimed {
		payload, err := json.Marshal(we)
		if err != nil {
			return 0, fmt.Errorf("marshal wire event: %w", err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(we.AggregateID),
			Value: payload,
			Headers: []kafka.Header{
				{Key: "event-type", Value: []byte(we.Type)},
				{Key: "aggregate-type", Value: []byte(we.AggregateType)},
			},
		}
		seqs[i] = we.GlobalSeq
	}

	// Produce BEFORE marking published: if the process dies here, the rows
	// stay unpublished and get retried by the next poll (at-least-once).
	// Producing after commit instead would risk marking rows published
	// that never actually reached Kafka.
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return 0, fmt.Errorf("write to kafka: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE event_outbox SET published_at = now() WHERE global_seq = ANY($1)`,
		int64SliceToPQArray(seqs)); err != nil {
		return 0, fmt.Errorf("mark published: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(claimed), nil
}

// int64SliceToPQArray renders a Go []int64 as a Postgres array literal
// usable with = ANY($1), avoiding a dependency on lib/pq's pq.Array helper
// so this file has no compile-time tie to a specific driver's array type.
func int64SliceToPQArray(vals []int64) string {
	s := "{"
	for i, v := range vals {
		if i > 0 {
			s += ","
		}
		s += fmt.Sprintf("%d", v)
	}
	return s + "}"
}
