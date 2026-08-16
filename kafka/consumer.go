package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/salribaudo/ledger"
)

// Handler processes one event read off the topic. Handlers should be
// idempotent (safe to call twice with the same event) since the outbox
// publisher guarantees at-least-once delivery, not exactly-once.
type Handler func(ctx context.Context, ev ledger.Event, globalSeq int64) error

// Consumer reads events published by Publisher and hands each one to a
// Handler — typically a read-model projector (e.g. "materialize current
// account balances into a fast lookup table").
type Consumer struct {
	reader  *kafka.Reader
	handler Handler
}

// NewConsumer builds a Consumer in the given consumer group, so multiple
// instances of the same projector can share the topic's partitions instead
// of each seeing every message.
func NewConsumer(brokers []string, topic, groupID string, handler Handler) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		handler: handler,
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

// Run reads and handles messages until ctx is cancelled. The Kafka offset
// is only committed after Handler returns successfully, so a handler that
// errors will see the same message again on restart rather than silently
// losing it.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("ledger/kafka: fetch message: %w", err)
		}

		var we WireEvent
		if err := json.Unmarshal(msg.Value, &we); err != nil {
			return fmt.Errorf("ledger/kafka: decode wire event at offset %d: %w", msg.Offset, err)
		}

		ev := ledger.Event{
			ID:            we.ID,
			AggregateID:   we.AggregateID,
			AggregateType: we.AggregateType,
			Version:       we.Version,
			Type:          we.Type,
			Data:          we.Data,
			Metadata:      we.Metadata,
			RecordedAt:    we.RecordedAt,
		}

		if err := c.handler(ctx, ev, we.GlobalSeq); err != nil {
			return fmt.Errorf("ledger/kafka: handler error at global_seq %d: %w", we.GlobalSeq, err)
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("ledger/kafka: commit offset: %w", err)
		}
	}
}
