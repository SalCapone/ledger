// Package ledger is a small, dependency-light event-sourcing toolkit aimed
// at auditable financial systems: every state change is captured as an
// immutable, ordered event, aggregates are rebuilt by replaying their event
// stream, and optimistic concurrency prevents two writers from silently
// clobbering each other's version of the truth.
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Event is a single immutable fact that happened to an aggregate. Events are
// never mutated or deleted once appended; corrections are made by appending
// a new, compensating event.
type Event struct {
	// ID is a globally unique identifier for this event (e.g. a UUID or a
	// Kafka-style "topic-partition-offset" style key). It is assigned by the
	// store on append and is safe to use as an idempotency key downstream.
	ID string

	// AggregateID identifies the entity this event belongs to (e.g. an
	// account ID). All events for one aggregate form one ordered stream.
	AggregateID string

	// AggregateType names the kind of aggregate ("Account", "Invoice", ...).
	// It lets a single store hold streams for many aggregate kinds.
	AggregateType string

	// Version is the 1-indexed position of this event within its
	// aggregate's stream. The store enforces that Version is contiguous and
	// unique per AggregateID, which is what gives us optimistic concurrency:
	// a writer that read version N and tries to append version N+1 will be
	// rejected if someone else already wrote N+1.
	Version int

	// Type is the event's name, e.g. "FundsDeposited". Used to pick the
	// right Go type when deserializing Data.
	Type string

	// Data is the event payload, serialized as JSON. Kept as raw bytes in
	// the core package so the store never needs to know about concrete
	// domain event types.
	Data json.RawMessage

	// Metadata carries cross-cutting, non-domain information: who caused
	// the event, a correlation/causation ID for tracing a chain of commands
	// across services, and so on. Kept separate from Data so audit tooling
	// can inspect it without understanding every event schema.
	Metadata map[string]string

	// RecordedAt is set by the store at append time (server clock), not by
	// the caller, so it can be trusted for audit purposes.
	RecordedAt time.Time
}

// ErrConcurrencyConflict is returned by EventStore.Append when the caller's
// ExpectedVersion no longer matches the aggregate's actual latest version,
// i.e. someone else appended in between the caller's read and its write.
var ErrConcurrencyConflict = errors.New("ledger: concurrency conflict: aggregate was modified by another writer")

// ErrStreamNotFound is returned by EventStore.Load when no events exist for
// the given aggregate ID.
var ErrStreamNotFound = errors.New("ledger: no event stream for aggregate")

// NewEvent is a constructor for pre-append events. Version, ID and
// RecordedAt are filled in by the store, so they're left zero here.
func NewEvent(aggregateType, aggregateID, eventType string, data any, metadata map[string]string) (Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Event{}, err
	}
	return Event{
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		Type:          eventType,
		Data:          raw,
		Metadata:      metadata,
	}, nil
}

// EventStore is the append-only log every aggregate is persisted to. All
// implementations must guarantee:
//  1. Append is atomic across the whole batch: either every event in the
//     batch is durably written or none are.
//  2. Append enforces optimistic concurrency via expectedVersion.
//  3. Load returns events for one aggregate strictly ordered by Version.
type EventStore interface {
	// Append writes events to the given aggregate's stream. expectedVersion
	// is the version the caller believes the stream is currently at (0 for
	// a brand-new aggregate). If the actual current version differs,
	// ErrConcurrencyConflict is returned and nothing is written.
	Append(ctx context.Context, aggregateID string, expectedVersion int, events []Event) error

	// Load returns all events for aggregateID in version order, starting
	// after afterVersion (pass 0 to load the full stream).
	Load(ctx context.Context, aggregateID string, afterVersion int) ([]Event, error)

	// LoadAll streams every event ever recorded, across all aggregates, in
	// global append order starting after the given global sequence number.
	// This is the feed a projection or an outbox publisher tails.
	LoadAll(ctx context.Context, afterSeq int64, limit int) ([]StoredEvent, error)
}

// StoredEvent wraps an Event with the store-assigned global sequence
// number, used by LoadAll for cursoring through the full history in commit
// order regardless of aggregate.
type StoredEvent struct {
	Event
	GlobalSeq int64
}
