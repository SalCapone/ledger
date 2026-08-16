// Package account is a worked example of the ledger framework: a bank
// account whose balance only ever moves through recorded events, never
// through a direct field assignment. It's deliberately small — the point is
// to show the shape of a real aggregate, not to be a complete ledger.
package account

import (
	"context"
	"errors"
	"fmt"

	"github.com/salribaudo/ledger"
)

const AggregateType = "Account"

// Event type names. Kept as constants so typos become compile errors at
// the call site instead of silent no-op events.
const (
	EventOpened            = "AccountOpened"
	EventFundsDeposited    = "FundsDeposited"
	EventFundsWithdrawn    = "FundsWithdrawn"
	EventWithdrawalRefused = "WithdrawalRefused"
)

var (
	ErrAlreadyOpen       = errors.New("account: already open")
	ErrNotOpen           = errors.New("account: not open")
	ErrInsufficientFunds = errors.New("account: insufficient funds")
	ErrNonPositiveAmount = errors.New("account: amount must be positive")
)

// --- Event payloads -------------------------------------------------------

type OpenedData struct {
	Owner    string `json:"owner"`
	Currency string `json:"currency"`
}

type DepositedData struct {
	AmountCents int64  `json:"amount_cents"`
	Reference   string `json:"reference"`
}

type WithdrawnData struct {
	AmountCents int64  `json:"amount_cents"`
	Reference   string `json:"reference"`
}

type WithdrawalRefusedData struct {
	AmountCents int64  `json:"amount_cents"`
	Reference   string `json:"reference"`
	Reason      string `json:"reason"`
}

// --- Aggregate --------------------------------------------------------

// Account is the current, replay-derived state of one bank account. Every
// field here exists because some past event set it — there is no other way
// to get a nonzero BalanceCents than for a FundsDeposited event to have been
// applied.
type Account struct {
	ledger.BaseAggregate

	Open         bool
	Owner        string
	Currency     string
	BalanceCents int64
}

// New constructs an empty, unopened Account ready to have history (or a new
// Open command) applied to it. Matches the ledger.NewRepository newFunc
// signature.
func New(id string) *Account {
	return &Account{BaseAggregate: ledger.NewBaseAggregate(AggregateType, id)}
}

// Apply is the ONLY place Account state changes. Both replay-from-history
// and "just persisted this new event" paths go through here, which is what
// guarantees the in-memory instance always matches what Load() would
// reconstruct from Postgres.
func (a *Account) Apply(ev ledger.Event) error {
	switch ev.Type {
	case EventOpened:
		d, err := ledger.Decode[OpenedData](ev)
		if err != nil {
			return err
		}
		a.Open = true
		a.Owner = d.Owner
		a.Currency = d.Currency

	case EventFundsDeposited:
		d, err := ledger.Decode[DepositedData](ev)
		if err != nil {
			return err
		}
		a.BalanceCents += d.AmountCents

	case EventFundsWithdrawn:
		d, err := ledger.Decode[WithdrawnData](ev)
		if err != nil {
			return err
		}
		a.BalanceCents -= d.AmountCents

	case EventWithdrawalRefused:
		// Deliberately recorded, not just returned as an error to the
		// caller: a refused withdrawal (e.g. attempted overdraft) is
		// itself an auditable fact about the account, and it doesn't
		// change the balance.

	default:
		return fmt.Errorf("account: unknown event type %q", ev.Type)
	}
	return nil
}

// --- Commands -----------------------------------------------------------
//
// Commands are plain functions that validate business rules against the
// CURRENT in-memory state and return the events that should be appended.
// They never mutate state directly — that discipline is what makes Apply
// trustworthy as the single source of truth.

// Open opens a brand-new account. Fails if this ID has already been opened.
func Open(a *Account, owner, currency string) ([]ledger.Event, error) {
	if a.Open {
		return nil, ErrAlreadyOpen
	}
	ev, err := ledger.NewEvent(AggregateType, a.AggregateID(), EventOpened,
		OpenedData{Owner: owner, Currency: currency}, nil)
	if err != nil {
		return nil, err
	}
	return []ledger.Event{ev}, nil
}

// Deposit records a deposit. Deposits always succeed for an open account —
// there's no failure mode to model here beyond validation.
func Deposit(a *Account, amountCents int64, reference string) ([]ledger.Event, error) {
	if !a.Open {
		return nil, ErrNotOpen
	}
	if amountCents <= 0 {
		return nil, ErrNonPositiveAmount
	}
	ev, err := ledger.NewEvent(AggregateType, a.AggregateID(), EventFundsDeposited,
		DepositedData{AmountCents: amountCents, Reference: reference}, nil)
	if err != nil {
		return nil, err
	}
	return []ledger.Event{ev}, nil
}

// Withdraw records a withdrawal, or — if funds are insufficient — records
// the refusal instead of just returning an error, so "someone tried to
// overdraw this account" remains part of the permanent audit trail.
func Withdraw(a *Account, amountCents int64, reference string) ([]ledger.Event, error) {
	if !a.Open {
		return nil, ErrNotOpen
	}
	if amountCents <= 0 {
		return nil, ErrNonPositiveAmount
	}
	if amountCents > a.BalanceCents {
		ev, err := ledger.NewEvent(AggregateType, a.AggregateID(), EventWithdrawalRefused,
			WithdrawalRefusedData{AmountCents: amountCents, Reference: reference, Reason: "insufficient_funds"}, nil)
		if err != nil {
			return nil, err
		}
		return []ledger.Event{ev}, ErrInsufficientFunds
	}
	ev, err := ledger.NewEvent(AggregateType, a.AggregateID(), EventFundsWithdrawn,
		WithdrawnData{AmountCents: amountCents, Reference: reference}, nil)
	if err != nil {
		return nil, err
	}
	return []ledger.Event{ev}, nil
}

// Repository is a convenience alias so callers don't have to spell out the
// generic instantiation themselves.
type Repository = ledger.Repository[*Account]

func NewRepository(store ledger.EventStore) *Repository {
	return ledger.NewRepository(store, New)
}

// --- Sanity-check helper used by the demo & tests ------------------------

// MustReplay loads an account and panics on error; only meant for demo code
// where a broken replay should stop the show loudly.
func MustReplay(ctx context.Context, repo *Repository, id string) *Account {
	a, err := repo.Load(ctx, id)
	if err != nil {
		panic(err)
	}
	return a
}
