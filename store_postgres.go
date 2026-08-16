package ledger

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

// PostgresStore is an EventStore backed by a single append-only Postgres
// table (see migrations/0001_init.sql). It relies on a unique constraint on
// (aggregate_id, version) to implement optimistic concurrency: a conflicting
// append fails at the database with a unique-violation, which Append
// translates into ErrConcurrencyConflict, rather than doing a racy
// check-then-write in application code.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore wraps an already-open *sql.DB. The caller owns the
// connection's lifecycle (including calling Close).
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// Open is a convenience constructor that opens a connection using
// github.com/lib/pq and verifies it with a ping.
func Open(ctx context.Context, connStr string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("ledger: open postgres: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ledger: ping postgres: %w", err)
	}
	return NewPostgresStore(db), nil
}

func (s *PostgresStore) Append(ctx context.Context, aggregateID string, expectedVersion int, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("ledger: begin tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op if committed

	// Lock the aggregate's row range so two concurrent transactions can't
	// both read the same "current max version" and both believe they're
	// clear to write. This turns the unique-constraint race into a clean
	// serialization point instead of relying on the constraint alone.
	var actualVersion int
	err = tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM events WHERE aggregate_id = $1 FOR UPDATE`,
		aggregateID,
	).Scan(&actualVersion)
	if err != nil {
		return fmt.Errorf("ledger: read current version: %w", err)
	}
	if actualVersion != expectedVersion {
		return fmt.Errorf("%w: aggregate %q expected version %d, actual %d",
			ErrConcurrencyConflict, aggregateID, expectedVersion, actualVersion)
	}

	insertEvent := `
		INSERT INTO events (aggregate_id, aggregate_type, version, type, data, metadata)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING global_seq, id, recorded_at`
	insertOutbox := `INSERT INTO event_outbox (global_seq) VALUES ($1)`

	for i := range events {
		ev := events[i]
		ev.AggregateID = aggregateID
		ev.Version = actualVersion + i + 1
		if ev.Metadata == nil {
			ev.Metadata = map[string]string{}
		}
		metaJSON, err := json.Marshal(ev.Metadata)
		if err != nil {
			return fmt.Errorf("ledger: marshal metadata: %w", err)
		}

		var globalSeq int64
		row := tx.QueryRowContext(ctx, insertEvent,
			ev.AggregateID, ev.AggregateType, ev.Version, ev.Type, ev.Data, metaJSON)
		if err := row.Scan(&globalSeq, &ev.ID, &ev.RecordedAt); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w (detected at insert for version %d)", ErrConcurrencyConflict, ev.Version)
			}
			return fmt.Errorf("ledger: insert event: %w", err)
		}
		if _, err := tx.ExecContext(ctx, insertOutbox, globalSeq); err != nil {
			return fmt.Errorf("ledger: insert outbox row: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ledger: commit: %w", err)
	}
	return nil
}

func (s *PostgresStore) Load(ctx context.Context, aggregateID string, afterVersion int) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, aggregate_id, aggregate_type, version, type, data, metadata, recorded_at
		FROM events
		WHERE aggregate_id = $1 AND version > $2
		ORDER BY version ASC`, aggregateID, afterVersion)
	if err != nil {
		return nil, fmt.Errorf("ledger: load stream: %w", err)
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var ev Event
		var metaJSON []byte
		if err := rows.Scan(&ev.ID, &ev.AggregateID, &ev.AggregateType, &ev.Version, &ev.Type, &ev.Data, &metaJSON, &ev.RecordedAt); err != nil {
			return nil, fmt.Errorf("ledger: scan event: %w", err)
		}
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &ev.Metadata); err != nil {
				return nil, fmt.Errorf("ledger: unmarshal metadata: %w", err)
			}
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 && afterVersion == 0 {
		return nil, ErrStreamNotFound
	}
	return out, nil
}

func (s *PostgresStore) LoadAll(ctx context.Context, afterSeq int64, limit int) ([]StoredEvent, error) {
	q := `
		SELECT global_seq, id, aggregate_id, aggregate_type, version, type, data, metadata, recorded_at
		FROM events
		WHERE global_seq > $1
		ORDER BY global_seq ASC`
	args := []any{afterSeq}
	if limit > 0 {
		q += " LIMIT $2"
		args = append(args, limit)
	}

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("ledger: load all: %w", err)
	}
	defer rows.Close()

	var out []StoredEvent
	for rows.Next() {
		var se StoredEvent
		var metaJSON []byte
		if err := rows.Scan(&se.GlobalSeq, &se.ID, &se.AggregateID, &se.AggregateType, &se.Version, &se.Type, &se.Data, &metaJSON, &se.RecordedAt); err != nil {
			return nil, fmt.Errorf("ledger: scan stored event: %w", err)
		}
		if len(metaJSON) > 0 {
			if err := json.Unmarshal(metaJSON, &se.Metadata); err != nil {
				return nil, fmt.Errorf("ledger: unmarshal metadata: %w", err)
			}
		}
		out = append(out, se)
	}
	return out, rows.Err()
}

// isUniqueViolation checks for Postgres SQLSTATE 23505 without importing
// lib/pq's error type directly at every call site.
func isUniqueViolation(err error) bool {
	// lib/pq wraps the error as *pq.Error with a Code field; string-matching
	// the code avoids an extra type assertion import cycle here.
	return err != nil && strings.Contains(err.Error(), "23505")
}
