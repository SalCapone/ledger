package ledger

import (
	"context"
	"encoding/json"
	"fmt"
)

// Aggregate is anything whose state is derived entirely by replaying an
// ordered sequence of domain events. Implementations should have no other
// source of truth: Apply is the only place state is mutated.
type Aggregate interface {
	// AggregateID returns this instance's identity.
	AggregateID() string

	// AggregateType names the kind of aggregate, used to route/deserialize
	// events (e.g. "Account").
	AggregateType() string

	// Version is the number of events applied so far. A brand-new,
	// never-persisted aggregate has version 0.
	Version() int

	// Apply mutates in-memory state to reflect one event. It must be a pure
	// function of (current state, event) -> new state, with no side
	// effects, since it runs both when a command is first handled and every
	// time the aggregate is rebuilt from history.
	Apply(event Event) error

	// setVersion lets the repository track replay position without every
	// concrete aggregate re-implementing version bookkeeping. Embed
	// BaseAggregate to get this for free.
	setVersion(v int)
}

// BaseAggregate is a small mixin that gives a concrete aggregate type its
// ID, type name and version bookkeeping, so aggregate authors only need to
// write the Apply method's event switch.
type BaseAggregate struct {
	id      string
	aggType string
	version int
}

func NewBaseAggregate(aggregateType, id string) BaseAggregate {
	return BaseAggregate{id: id, aggType: aggregateType}
}

func (b *BaseAggregate) AggregateID() string   { return b.id }
func (b *BaseAggregate) AggregateType() string { return b.aggType }
func (b *BaseAggregate) Version() int          { return b.version }
func (b *BaseAggregate) setVersion(v int)      { b.version = v }

// Repository loads an aggregate by replaying its full event stream and
// saves new events produced by command handlers, enforcing optimistic
// concurrency via the aggregate's in-memory version.
type Repository[A Aggregate] struct {
	store   EventStore
	newFunc func(id string) A
}

// NewRepository builds a Repository for aggregate type A. newFunc must
// return a zero-value, unpersisted instance of A for the given ID, ready to
// have events applied to it.
func NewRepository[A Aggregate](store EventStore, newFunc func(id string) A) *Repository[A] {
	return &Repository[A]{store: store, newFunc: newFunc}
}

// Load rebuilds the aggregate identified by id by replaying every event in
// its stream, in order, through Apply. Returns ErrStreamNotFound if no
// events exist yet for id.
func (r *Repository[A]) Load(ctx context.Context, id string) (A, error) {
	agg := r.newFunc(id)
	events, err := r.store.Load(ctx, id, 0)
	if err != nil {
		return agg, err
	}
	for _, ev := range events {
		if err := agg.Apply(ev); err != nil {
			return agg, fmt.Errorf("ledger: replay event v%d for %s %q: %w", ev.Version, agg.AggregateType(), id, err)
		}
	}
	agg.setVersion(events[len(events)-1].Version)
	return agg, nil
}

// Save appends newEvents to the aggregate's stream (using its current
// in-memory Version as the optimistic-concurrency expected version), then
// applies each event to agg so the in-memory instance reflects what was
// just persisted without a round trip.
func (r *Repository[A]) Save(ctx context.Context, agg A, newEvents []Event) error {
	if len(newEvents) == 0 {
		return nil
	}
	for i := range newEvents {
		newEvents[i].AggregateType = agg.AggregateType()
	}
	if err := r.store.Append(ctx, agg.AggregateID(), agg.Version(), newEvents); err != nil {
		return err
	}
	// Re-load the just-appended events from the store so Apply sees fully
	// populated Event values (server-assigned ID, Version, RecordedAt)
	// rather than the caller's partially-filled ones.
	persisted, err := r.store.Load(ctx, agg.AggregateID(), agg.Version())
	if err != nil {
		return err
	}
	for _, ev := range persisted {
		if err := agg.Apply(ev); err != nil {
			return err
		}
	}
	agg.setVersion(persisted[len(persisted)-1].Version)
	return nil
}

// Decode is a small helper for Apply implementations: unmarshal an event's
// JSON payload into a concrete Go type.
func Decode[T any](ev Event) (T, error) {
	var v T
	err := json.Unmarshal(ev.Data, &v)
	return v, err
}
