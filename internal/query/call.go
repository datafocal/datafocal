package query

import (
	"context"
	"sync/atomic"
)

// Call is what one tool call did that its envelope does not say: whether it read the clock,
// which makes its answer not one to keep, and where its column chunks came from.
type Call struct {
	ReadClock                           atomic.Bool
	ChunksHit, ChunksShared, ChunksRead atomic.Int64
}

type callKey struct{}

// WithCall is ctx recording what its call does in the returned Call.
func WithCall(ctx context.Context) (context.Context, *Call) {
	c := &Call{}
	return context.WithValue(ctx, callKey{}, c), c
}

// CallOf is ctx's Call, or a discarded one outside a tool call.
func CallOf(ctx context.Context) *Call {
	if c, ok := ctx.Value(callKey{}).(*Call); ok {
		return c
	}
	return &Call{}
}

// CacheStats is what a cache holds now and what it has counted since it was made.
type CacheStats struct {
	Entries                                    int
	Bytes, CapBytes                            int64
	Hits, Shared, Misses, Evictions, NotStored int64
}
