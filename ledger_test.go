package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/salribaudo/ledger"
	"github.com/salribaudo/ledger/examples/account"
)

func TestOpenDepositWithdraw(t *testing.T) {
	ctx := context.Background()
	store := ledger.NewMemoryStore()
	repo := account.NewRepository(store)
	id := "acct-1"
	acc := account.New(id)

	events, err := account.Open(acc, "Ada Lovelace", "USD")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := repo.Save(ctx, acc, events); err != nil {
		t.Fatalf("save open: %v", err)
	}

	events, err = account.Deposit(acc, 10000, "seed")
	if err != nil {
		t.Fatalf("deposit: %v", err)
	}
	if err := repo.Save(ctx, acc, events); err != nil {
		t.Fatalf("save deposit: %v", err)
	}

	if acc.BalanceCents != 10000 {
		t.Fatalf("balance = %d, want 10000", acc.BalanceCents)
	}

	events, err = account.Withdraw(acc, 4000, "rent")
	if err != nil {
		t.Fatalf("withdraw: %v", err)
	}
	if err := repo.Save(ctx, acc, events); err != nil {
		t.Fatalf("save withdraw: %v", err)
	}
	if acc.BalanceCents != 6000 {
		t.Fatalf("balance after withdraw = %d, want 6000", acc.BalanceCents)
	}
	if acc.Version() != 3 {
		t.Fatalf("version = %d, want 3", acc.Version())
	}
}

func TestOverdraftIsRefusedButRecorded(t *testing.T) {
	ctx := context.Background()
	store := ledger.NewMemoryStore()
	repo := account.NewRepository(store)
	acc := account.New("acct-2")

	events, _ := account.Open(acc, "Grace Hopper", "USD")
	_ = repo.Save(ctx, acc, events)
	events, _ = account.Deposit(acc, 1000, "seed")
	_ = repo.Save(ctx, acc, events)

	events, err := account.Withdraw(acc, 5000, "too-much")
	if !errors.Is(err, account.ErrInsufficientFunds) {
		t.Fatalf("expected ErrInsufficientFunds, got %v", err)
	}
	if err := repo.Save(ctx, acc, events); err != nil {
		t.Fatalf("save refusal: %v", err)
	}

	// Balance must be untouched by the refused withdrawal...
	if acc.BalanceCents != 1000 {
		t.Fatalf("balance = %d, want 1000 (refusal must not move funds)", acc.BalanceCents)
	}
	// ...but the refusal itself must be a permanent, replayable event.
	stream, err := store.Load(ctx, "acct-2", 0)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(stream), 3; got != want {
		t.Fatalf("stream length = %d, want %d (open, deposit, refused)", got, want)
	}
	if stream[2].Type != account.EventWithdrawalRefused {
		t.Fatalf("stream[2].Type = %q, want %q", stream[2].Type, account.EventWithdrawalRefused)
	}
}

func TestReplayMatchesLiveState(t *testing.T) {
	ctx := context.Background()
	store := ledger.NewMemoryStore()
	repo := account.NewRepository(store)
	acc := account.New("acct-3")

	events, _ := account.Open(acc, "Katherine Johnson", "USD")
	_ = repo.Save(ctx, acc, events)
	events, _ = account.Deposit(acc, 25000, "seed")
	_ = repo.Save(ctx, acc, events)
	events, _ = account.Withdraw(acc, 5000, "groceries")
	_ = repo.Save(ctx, acc, events)

	replayed := account.MustReplay(ctx, repo, "acct-3")
	if replayed.BalanceCents != acc.BalanceCents {
		t.Fatalf("replayed balance %d != live balance %d", replayed.BalanceCents, acc.BalanceCents)
	}
	if replayed.Version() != acc.Version() {
		t.Fatalf("replayed version %d != live version %d", replayed.Version(), acc.Version())
	}
	if !replayed.Open || replayed.Owner != "Katherine Johnson" {
		t.Fatalf("replayed account missing basic fields: %+v", replayed)
	}
}

func TestConcurrencyConflict(t *testing.T) {
	ctx := context.Background()
	store := ledger.NewMemoryStore()

	ev, err := ledger.NewEvent(account.AggregateType, "acct-4", account.EventOpened,
		account.OpenedData{Owner: "X", Currency: "USD"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Append(ctx, "acct-4", 0, []ledger.Event{ev}); err != nil {
		t.Fatalf("first append: %v", err)
	}

	// A second writer that also thinks the stream is at version 0 (i.e. it
	// never saw the first writer's append) must be rejected.
	err = store.Append(ctx, "acct-4", 0, []ledger.Event{ev})
	if !errors.Is(err, ledger.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestLoadUnknownStream(t *testing.T) {
	store := ledger.NewMemoryStore()
	_, err := store.Load(context.Background(), "does-not-exist", 0)
	if !errors.Is(err, ledger.ErrStreamNotFound) {
		t.Fatalf("expected ErrStreamNotFound, got %v", err)
	}
}

func TestLoadAllGlobalOrdering(t *testing.T) {
	ctx := context.Background()
	store := ledger.NewMemoryStore()
	repo := account.NewRepository(store)

	a := account.New("acct-5")
	b := account.New("acct-6")
	events, _ := account.Open(a, "A", "USD")
	_ = repo.Save(ctx, a, events)
	events, _ = account.Open(b, "B", "USD")
	_ = repo.Save(ctx, b, events)
	events, _ = account.Deposit(a, 100, "x")
	_ = repo.Save(ctx, a, events)

	all, err := store.LoadAll(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("len(all) = %d, want 3", len(all))
	}
	for i := 1; i < len(all); i++ {
		if all[i].GlobalSeq <= all[i-1].GlobalSeq {
			t.Fatalf("global seq not strictly increasing at %d", i)
		}
	}
}
