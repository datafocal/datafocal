package query

import (
	"context"
	"fmt"
	"sync"
)

// InvalidRequestError marks a client-input error — a malformed request the
// caller must fix (maps to HTTP 400). Any other error a tool returns is treated
// as a server/infrastructure failure (HTTP 500), so a transient backend outage
// is never reported to the caller as a permanent bad request. The API layer
// distinguishes the two with errors.As.
//
// This is the minimal seam; the full typed error surface (codes, hints,
// suggested fields) lands with the registry work.
type InvalidRequestError struct{ msg string }

func (e *InvalidRequestError) Error() string { return e.msg }

// Invalidf builds an InvalidRequestError.
func Invalidf(format string, args ...any) error {
	return &InvalidRequestError{msg: fmt.Sprintf(format, args...)}
}

const (
	ErrCodeUnboundedScan = "unbounded_scan"
	// ErrCodeScanTooLarge is a scan the caller may have, just not this wide: the read
	// would touch more segments than the call is allowed to. Distinct from
	// ErrCodeUnboundedScan, which is a request with no time bound at all, because the
	// re-plan differs — narrow the window, or raise the bound deliberately.
	ErrCodeScanTooLarge = "scan_too_large"
	// ErrCodeUnusableSeries is a series that resolved but cannot carry the answer: too
	// few bins for a lag, or constant so nothing correlates against it. Distinct from
	// ErrCodeInvalidRequest because the measure and predicates were accepted, so the
	// re-plan is to rescope the series rather than to fix the call.
	ErrCodeUnusableSeries = "unusable_series"
	ErrCodeUnknownField   = "unknown_field"
	// ErrCodeTooLarge is a request past its memory budget, unlike ErrCodeScanTooLarge's segment count.
	ErrCodeTooLarge = "too_large"
	// ErrCodeBusy is a request refused because the querier is serving as many as it may at once.
	ErrCodeBusy = "busy"
	// ErrCodeInvalidRequest and ErrCodeInternal are the codes the API layer stamps
	// on the two failures a tool reports without one: a malformed request and
	// anything else. They exist so that every response body a caller can receive is
	// an APIError, whatever its status — a client that has to parse one shape for a
	// re-plannable error and another for a bad request will get one of them wrong.
	ErrCodeInvalidRequest = "invalid_request"
	ErrCodeInternal       = "internal"
)

type APIError struct {
	Code            string   `json:"code"`
	Message         string   `json:"message"`
	Hint            string   `json:"hint,omitempty"`
	SuggestedFields []string `json:"suggested_fields,omitempty"`
	Retryable       bool     `json:"retryable"`
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func NewAPIError(code, message string) *APIError {
	return &APIError{Code: code, Message: message}
}

func (e *APIError) WithHint(h string) *APIError {
	e.Hint = h
	return e
}

func (e *APIError) WithSuggestedFields(f []string) *APIError {
	e.SuggestedFields = f
	return e
}

type memBudgetKey struct{}

type turnKey struct{}

// memBudget is the bytes a request may hold decoded. A read whose turn it is, the oldest
// not ended, may fill it; a later read only its first half, keeping the other for the
// reads before it. A charge that does not fit waits for a working goroutine to give
// memory back, and fails too_large once none is.
type memBudget struct {
	bytes int64
	mu    sync.Mutex
	used  int64
	// working counts the goroutines holding memory they will give back without charging more.
	working int
	// issued is the turns handed out, turn the oldest not yet ended.
	issued, turn int64
	ended        map[int64]bool
	// waiting is whether the read whose turn it is waits for room.
	waiting bool
	changed chan struct{}
}

// readTurn is one read's place in its budget's order.
type readTurn struct {
	b *memBudget
	n int64
}

// WithMemBudget is ctx carrying a budget of bytes for ChargeMem, or ctx itself for bytes <= 0.
func WithMemBudget(ctx context.Context, bytes int64) context.Context {
	if bytes <= 0 {
		return ctx
	}
	return context.WithValue(ctx, memBudgetKey{}, &memBudget{bytes: bytes, ended: map[int64]bool{}, changed: make(chan struct{})})
}

// RenewMemBudget is ctx with its budget whole again, for a request that reads in rounds,
// each letting go of what the last one held.
func RenewMemBudget(ctx context.Context) context.Context {
	if b, ok := ctx.Value(memBudgetKey{}).(*memBudget); ok {
		return WithMemBudget(ctx, b.bytes)
	}
	return ctx
}

// Turn is ctx for a read whose charges take no more than half the budget until every
// read that took a turn before it has ended, which end does.
func Turn(ctx context.Context) (context.Context, func()) {
	b, ok := ctx.Value(memBudgetKey{}).(*memBudget)
	if !ok {
		return ctx, func() {}
	}
	b.mu.Lock()
	t := readTurn{b: b, n: b.issued}
	b.issued++
	b.mu.Unlock()
	return context.WithValue(ctx, turnKey{}, t), func() {
		b.mu.Lock()
		for b.ended[t.n] = true; b.ended[b.turn]; b.turn++ {
			delete(b.ended, b.turn)
		}
		b.change()
		b.mu.Unlock()
	}
}

// Working counts the caller among the goroutines that hold memory of ctx's budget they
// will give back, so a charge that does not fit waits for them, until done is called.
func Working(ctx context.Context) (done func()) {
	b, ok := ctx.Value(memBudgetKey{}).(*memBudget)
	if !ok {
		return func() {}
	}
	b.mu.Lock()
	b.working++
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		b.working--
		b.change()
		b.mu.Unlock()
	}
}

// ChargeMem takes bytes from ctx's budget, waiting for ctx's turn and for room while a
// working goroutine can make some, and failing too_large when none can, or with ctx's
// error once it ends; a context without a budget is charged nothing.
func ChargeMem(ctx context.Context, bytes int64) error {
	b, ok := ctx.Value(memBudgetKey{}).(*memBudget)
	if !ok {
		return nil
	}
	t, read := ctx.Value(turnKey{}).(readTurn)
	read = read && t.b == b
	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		mine := read && t.n == b.turn
		switch {
		case read && !mine && b.used+bytes <= b.bytes/2:
			b.used += bytes
			return nil
		case read && !mine:
		case b.used+bytes <= b.bytes:
			b.used += bytes
			return nil
		case b.working == 0 && (mine || b.waiting || b.turn == b.issued):
			return NewAPIError(ErrCodeTooLarge, "this request read more than it may hold in memory; narrow the time range or the selector")
		default:
			b.waiting = b.waiting || mine
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			b.mu.Lock()
			return ctx.Err()
		}
		b.mu.Lock()
	}
}

// ReleaseMem gives back to ctx's budget bytes it was charged and no longer holds.
func ReleaseMem(ctx context.Context, bytes int64) {
	if b, ok := ctx.Value(memBudgetKey{}).(*memBudget); ok {
		b.mu.Lock()
		b.used -= bytes
		b.change()
		b.mu.Unlock()
	}
}

// change wakes every waiting charge to look again.
func (b *memBudget) change() {
	close(b.changed)
	b.changed, b.waiting = make(chan struct{}), false
}
