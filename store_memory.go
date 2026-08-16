package ledger

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore is a goroutine-safe, in-process EventStore. It has no
// durability guarantees at all — it exists for tests, demos, and for
// exercising the Aggregate/Repository API without standing up Postgres.
type MemoryStore struct {
	mu      sync.Mutex
	byAgg   map[string][]Event // aggregateID -> ordered events
	all     []StoredEvent      // global append order
	nextSeq int64
	nextID  int64
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		byAgg: make(map[string][]Event),
	}
}

func (s *MemoryStore) Append(ctx context.Context, aggregateID string, expectedVersion int, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.byAgg[aggregateID]
	actualVersion := len(current)
	if actualVersion != expectedVersion {
		return fmt.Errorf("%w: aggregate %q expected version %d, actual %d",
			ErrConcurrencyConflict, aggregateID, expectedVersion, actualVersion)
	}

	now := time.Now().UTC()
	for i := range events {
		s.nextID++
		s.nextSeq++
		ev := events[i]
		ev.AggregateID = aggregateID
		ev.Version = actualVersion + i + 1
		ev.ID = fmt.Sprintf("mem-%d", s.nextID)
		ev.RecordedAt = now
		current = append(current, ev)
		s.all = append(s.all, StoredEvent{Event: ev, GlobalSeq: s.nextSeq})
	}
	s.byAgg[aggregateID] = current
	return nil
}

func (s *MemoryStore) Load(ctx context.Context, aggregateID string, afterVersion int) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, ok := s.byAgg[aggregateID]
	if !ok || len(stream) == 0 {
		return nil, ErrStreamNotFound
	}
	// stream is already sorted by version since Append only ever appends.
	idx := sort.Search(len(stream), func(i int) bool { return stream[i].Version > afterVersion })
	out := make([]Event, len(stream)-idx)
	copy(out, stream[idx:])
	return out, nil
}

func (s *MemoryStore) LoadAll(ctx context.Context, afterSeq int64, limit int) ([]StoredEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := sort.Search(len(s.all), func(i int) bool { return s.all[i].GlobalSeq > afterSeq })
	end := len(s.all)
	if limit > 0 && idx+limit < end {
		end = idx + limit
	}
	out := make([]StoredEvent, end-idx)
	copy(out, s.all[idx:end])
	return out, nil
}
