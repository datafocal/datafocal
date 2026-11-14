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
	"runtime/metrics"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// TestWideCorpusMemoryBudget bounds each cost at about twice what it measured, where a dense sketch per column held 335 MiB and a rank_fields allocated 4.6 GiB.
func TestWideCorpusMemoryBudget(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioWide)
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
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC() // the second frees what the first left in sync.Pool victim caches
	return heapMetric("/memory/classes/heap/objects:bytes")
}

func heapMetric(name string) uint64 {
	sample := []metrics.Sample{{Name: name}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}
