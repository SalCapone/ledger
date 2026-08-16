// ledger-demo exercises the framework end-to-end against the in-memory
// store: open an account, deposit, attempt an overdraft, withdraw, then
// throw away the in-memory Account and rebuild it purely by replaying its
// event stream, proving the replayed state matches.
//
//	go run ./cmd/ledger-demo
//
// Pass -postgres="postgres://..." to run the same script against a real
// Postgres instance (see migrations/0001_init.sql) instead of memory.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/salribaudo/ledger"
	"github.com/salribaudo/ledger/examples/account"
)

func main() {
	pgConn := flag.String("postgres", "", "Postgres connection string; if empty, uses an in-memory store")
	flag.Parse()

	ctx := context.Background()

	var store ledger.EventStore
	if *pgConn != "" {
		pg, err := ledger.Open(ctx, *pgConn)
		if err != nil {
			log.Fatalf("connect to postgres: %v", err)
		}
		store = pg
		fmt.Println("using Postgres store — make sure migrations/0001_init.sql has been applied")
	} else {
		store = ledger.NewMemoryStore()
		fmt.Println("using in-memory store")
	}

	repo := account.NewRepository(store)
	id := "acct-demo-001"

	acc := account.New(id)

	run(ctx, repo, acc, "open", func() ([]ledger.Event, error) {
		return account.Open(acc, "Sal Ribaudo", "USD")
	})
	run(ctx, repo, acc, "deposit $500.00", func() ([]ledger.Event, error) {
		return account.Deposit(acc, 50000, "initial-funding")
	})
	run(ctx, repo, acc, "withdraw $900.00 (should be refused)", func() ([]ledger.Event, error) {
		return account.Withdraw(acc, 90000, "attempted-overdraft")
	})
	run(ctx, repo, acc, "withdraw $120.00", func() ([]ledger.Event, error) {
		return account.Withdraw(acc, 12000, "rent")
	})

	fmt.Printf("\nlive aggregate:    balance=$%.2f version=%d\n",
		float64(acc.BalanceCents)/100, acc.Version())

	// Now forget the in-memory instance entirely and rebuild it purely from
	// the event log, to demonstrate that Apply is really the only source
	// of truth.
	replayed := account.MustReplay(ctx, repo, id)
	fmt.Printf("replayed aggregate: balance=$%.2f version=%d\n",
		float64(replayed.BalanceCents)/100, replayed.Version())

	if replayed.BalanceCents != acc.BalanceCents || replayed.Version() != acc.Version() {
		log.Fatal("replay mismatch — this should never happen")
	}
	fmt.Println("\nreplay matches live state ✓")

	fmt.Println("\nfull event stream:")
	events, err := store.Load(ctx, id, 0)
	if err != nil {
		log.Fatal(err)
	}
	for _, ev := range events {
		fmt.Printf("  v%-2d %-20s %s\n", ev.Version, ev.Type, string(ev.Data))
	}
}

func run(ctx context.Context, repo *account.Repository, acc *account.Account, label string, cmd func() ([]ledger.Event, error)) {
	events, err := cmd()
	if len(events) > 0 {
		if saveErr := repo.Save(ctx, acc, events); saveErr != nil {
			log.Fatalf("%s: save: %v", label, saveErr)
		}
	}
	if err != nil {
		fmt.Printf("- %-45s -> refused (%v)\n", label, err)
		return
	}
	fmt.Printf("- %-45s -> ok\n", label)
}
