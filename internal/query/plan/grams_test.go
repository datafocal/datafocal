package plan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestGramsCachedAcrossPlans(t *testing.T) {
	b := segment.NewBuilder(segment.BuilderConfig{Tenant: "test", CreatedAt: time.Now()})
	require.NoError(t, b.Append(segment.LogRecord(time.Now(), "", "gateway request", nil), segment.ParsedPattern{}))
	body, err := b.Seal().Encode()
	require.NoError(t, err)
	reads := 0
	store := storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return body[max(0, int64(len(body))-n):], nil },
		GetRangeFn: func(_ string, offset, length int64) ([]byte, error) {
			reads++
			return body[offset : offset+length], nil
		},
	}
	descs := make([]segment.Descriptor, 600)
	for i := range descs {
		descs[i] = descWithKey(fmt.Sprint(i), fmt.Sprint(i))
	}
	p := New(Deps{Reader: segment.NewFooterCache(store)})
	sel := selector.Resolved{Window: boundedWindow(true), Where: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "slow query 1"}}}
	for pass := range 2 {
		reads = 0
		plan, err := p.Plan(context.Background(), descs, sel)
		require.NoError(t, err)
		require.Empty(t, plan.Items)
		require.Equal(t, 600*(1-pass), reads)
	}
	for _, pred := range []selector.Predicate{
		{Field: "message", Op: selector.OpNoMatch, Value: "slow query 1"},
		{Field: "message", Op: selector.OpMatch, Value: "sl"},
		{Field: "other", Op: selector.OpMatch, Value: "slow query 1"},
	} {
		sel.Where = []selector.Predicate{pred}
		plan, err := p.Plan(context.Background(), descs, sel)
		require.NoError(t, err)
		require.Len(t, plan.Items, 600)
	}
}
