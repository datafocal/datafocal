// The race detector changes what a call allocates, so these budgets hold only without it.
//go:build !race

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// TestWideCorpusMemoryBudget bounds each cost at about twice what it measured, where a dense sketch per column held 335 MiB and a rank_fields allocated 4.6 GiB,
// and compacting an hour of 360 segments at twice an hour of 60's.
func TestWideCorpusMemoryBudget(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	corpus, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioWide)
	require.NoError(t, err)
	store := storage.NewFS(dir)

	idle := heapAfterGC()
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store, Summary: &summary.Config{}})
	require.NoError(t, app.Refresh(ctx))
	assert.LessOrEqual(t, heapAfterGC()-idle, uint64(128<<20), "heap the refresh holds")

	budgets := map[string]uint64{"rank_fields": 10 << 20, selector.StageAggregate: 4 << 20, "query": 10 << 20}
	for _, a := range wideArchetypes() {
		body, err := json.Marshal(a.Request)
		require.NoError(t, err)
		var env query.Envelope[json.RawMessage]
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		rec := httptest.NewRecorder()
		app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+a.Tool, bytes.NewReader(body)))
		runtime.ReadMemStats(&after)
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
		require.NotEmptyf(t, env.Meta.Snapshot, "%s refused: %s", a.Tool, rec.Body.Bytes())
		assert.LessOrEqual(t, after.TotalAlloc-before.TotalAlloc, budgets[a.Tool], a.Name)
		assert.Zero(t, env.Meta.Cost.FootersDecoded, "%s: the refresh left every footer cached", a.Name)
	}

	// The most heap merging an hour of the first n segments held in three runs, since a
	// sample can miss a peak; the next hour's first segment completes the hour.
	compactHour := func(n int) (most uint64) {
		inputs := append(slices.Clone(corpus.Descriptors[:n]), corpus.Descriptors[360])
		cat := catalog.Fake{
			VersionFn: func() (catalog.Version, error) { return "1", nil },
			PruneFn:   func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return inputs, nil },
		}
		// Collected often, the heap is what the merge holds rather than when the collector last ran.
		defer debug.SetGCPercent(debug.SetGCPercent(5))
		for range 3 {
			start := heapAfterGC()
			most = max(most, peakHeapDuring(func() {
				require.NoError(t, compact.New(cat, cat, segment.NewReader(store), store, tenant).Run(ctx, time.Now()))
			})-start)
		}
		return most
	}
	compactHour(360) // first, so neither measure counts the buffers the compressor keeps for the process
	// The merge's writer holds the merged segment until it is put, so it grows with the hour's
	// rows; decoding a row group's mostly null columns as their few values took 60's from 16
	// to 12 MB and left 360's at 20, so the bound is twice 60's rather than half again.
	assert.LessOrEqual(t, compactHour(360), compactHour(60)*2, "compacting an hour of 360 segments")
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC() // the second frees what the first left in sync.Pool victim caches
	return heapMetric("/memory/classes/heap/objects:bytes")
}

// peakHeapDuring is the most live heap a collection found during fn's run, sampled every
// millisecond, since the garbage awaiting one depends on what ran before.
func peakHeapDuring(fn func()) uint64 {
	done, peak := make(chan struct{}), make(chan uint64)
	go func() {
		most := heapMetric("/gc/heap/live:bytes")
		for tick := time.NewTicker(time.Millisecond); ; {
			select {
			case <-done:
				tick.Stop()
				peak <- most
				return
			case <-tick.C:
				most = max(most, heapMetric("/gc/heap/live:bytes"))
			}
		}
	}()
	fn()
	close(done)
	return <-peak
}

func heapMetric(name string) uint64 {
	sample := []metrics.Sample{{Name: name}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}
