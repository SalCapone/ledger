# Ledger

A small, dependency-light event-sourcing toolkit for building auditable
financial systems in Go. State changes are captured as immutable, ordered
events; aggregates are rebuilt by replaying their stream; optimistic
concurrency stops two writers from silently clobbering each other.

```
go get github.com/salribaudo/ledger
```

## Why event sourcing for financial systems

A ledger that only stores current balances can tell you *what* an account
has. It can't tell you *why* — which deposits, withdrawals, and refused
overdrafts got it there, in what order, or who caused each one. For
anything that needs to survive an audit or a dispute, the event log **is**
the source of truth; a balance is just one projection of it.

## Core ideas

- **`Event`** — an immutable fact (`FundsDeposited`, `AccountOpened`, ...)
  tied to an aggregate ID and a 1-indexed version.
- **`EventStore`** — the append-only log. Ships with `MemoryStore` (tests,
  demos) and `PostgresStore` (production), both implementing the same
  interface.
- **`Aggregate` / `Repository[A]`** — load an aggregate by replaying its
  full history through `Apply`; save new events with optimistic
  concurrency keyed on the aggregate's current version.
- **Commands** — plain functions (`account.Deposit(acc, amount, ref)`) that
  validate business rules against current state and return the events to
  append. They never mutate state directly; `Apply` is the only place that
  happens, which is what makes replay trustworthy.

See [`examples/account`](examples/account) for a complete worked example: a
bank account that opens, accepts deposits, and refuses (but still records!)
withdrawals that would overdraw it.

## Quickstart

```bash
go run ./cmd/ledger-demo
```

This runs entirely against the in-memory store: opens an account, deposits,
attempts (and gets refused for) an overdraft, withdraws, then throws away
the in-memory struct and rebuilds it purely by replaying Postgres-shaped
events, to prove replay and live state agree.

To run the same script against real Postgres:

```bash
docker compose up -d postgres
go run ./cmd/ledger-demo -postgres="postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"
```

## Getting events to Kafka

The [`kafka/`](kafka) directory is a **separate Go module** (so the core
package has zero Kafka dependency) implementing the transactional outbox
pattern:

1. `PostgresStore.Append` writes each event and an `event_outbox` row in
   the *same* database transaction — so "the event was persisted" and "the
   event will eventually reach Kafka" can never disagree.
2. `kafka.Publisher` polls the outbox with `FOR UPDATE SKIP LOCKED` (safe to
   run multiple replicas concurrently), produces to Kafka keyed by
   aggregate ID (preserving per-aggregate order across partitions), and
   only then marks rows published.
3. `kafka.Consumer` decodes messages back into `ledger.Event` for
   projections — e.g. materializing current balances into a read-optimized
   table.

```bash
docker compose up -d
cd kafka && go mod tidy   # fetches kafka-go; needs full internet access
```

## Design notes

- **Why a unique constraint instead of a version column check-then-write?**
  `PostgresStore.Append` takes a row lock (`FOR UPDATE`) on the aggregate's
  current max version *and* relies on a `UNIQUE (aggregate_id, version)`
  constraint as a second line of defense. Belt and suspenders: the lock
  handles the common case cleanly, the constraint makes a conflict
  impossible to miss even under isolation-level surprises.
- **Why record refused commands as events?** `WithdrawalRefused` isn't an
  application error that vanishes after the HTTP response — "someone tried
  to withdraw more than was available, and was refused" is itself a fact
  worth having in the audit trail.
- **Why is the Kafka integration a separate module?** Most consumers of an
  event-sourcing core don't want a Kafka client pulled in transitively.
  Splitting `kafka/` into its own `go.mod` (with a `replace` back to the
  parent) keeps the dependency opt-in.

## Status

This is a portfolio-scale implementation: the core (`EventStore`,
`Repository`, optimistic concurrency, the `Account` example) is fully
implemented and tested (`go test ./...`). The Kafka module is complete,
production-shaped code but — because it depends on `segmentio/kafka-go` —
hasn't been compiled in this sandboxed environment; running `go mod tidy`
inside `kafka/` with normal internet access will build it.

## License

MIT — see [LICENSE](LICENSE).
