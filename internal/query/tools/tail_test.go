package tools

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// TestTailReadsEachNewSegmentOnce: three followers each receive every line of the segments
// the catalog gains while they follow, once and in time order, for one GET per segment; one
// that disconnects receives none.
func TestTailReadsEachNewSegmentOnce(t *testing.T) {
	var mu sync.Mutex
	var descs []segment.Descriptor
	bodies, gets := map[string][]byte{}, map[string]int{}
	seal := func(service string, at time.Time, messages ...string) segment.Descriptor {
		b := segment.NewBuilder(segment.BuilderConfig{Tenant: "acme", Stream: segment.Stream{Service: service}, CreatedAt: at})
		for i, m := range messages {
			require.NoError(t, b.Append(segment.LogRecord(at.Add(time.Duration(i)*time.Second), "info", m, nil), segment.ParsedPattern{PatternID: "p", Pattern: "x"}))
		}
		sealed := b.Seal()
		sealed.Descriptor.Object.Key = sealed.Descriptor.ID
		body, err := sealed.Encode()
		require.NoError(t, err)
		bodies[sealed.Descriptor.ID] = body
		return sealed.Descriptor
	}
	start := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	descs = append(descs, seal("api", start, "before the follow"))
	cat := catalog.Fake{
		VersionFn: func() (catalog.Version, error) {
			mu.Lock()
			defer mu.Unlock()
			return catalog.Version(fmt.Sprint(len(descs))), nil
		},
		PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(descs), nil
		},
	}
	store := storage.Fake{GetSuffixFn: func(key string, _ int64) ([]byte, error) {
		mu.Lock()
		defer mu.Unlock()
		gets[key]++
		return bodies[key], nil
	}}
	tl := &tail{catalog: cat, store: store, every: 10 * time.Millisecond, followers: map[chan []LogLine]<-chan struct{}{}}

	batches := make([]chan []string, 3)
	cancels := make([]context.CancelFunc, 3)
	for i := range batches {
		batches[i] = make(chan []string, 10)
		var ctx context.Context
		ctx, cancels[i] = context.WithCancel(t.Context())
		go func() {
			_ = tl.follow(ctx, func(lines []LogLine) error {
				var messages []string
				for _, l := range lines {
					messages = append(messages, l.Message)
				}
				batches[i] <- messages
				return nil
			})
		}()
		require.Empty(t, <-batches[i], "the event saying the follow is attached")
	}
	cancels[2]()

	api, db := seal("api", start.Add(time.Minute), "a1", "a2"), seal("db", start.Add(time.Minute+time.Second/2), "d1", "d2")
	mu.Lock()
	descs = append(descs, api, db)
	mu.Unlock()
	for _, b := range batches[:2] {
		select {
		case got := <-b:
			assert.Equal(t, []string{"a1", "d1", "a2", "d2"}, got)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "a follower received no lines")
		}
	}
	cancels[0]()
	cancels[1]()
	assert.Empty(t, batches[2], "the follower that disconnected")
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, map[string]int{api.ID: 1, db.ID: 1}, gets)
}
