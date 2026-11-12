package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/compact"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// TestValueAnomaliesOnAnAccessLog: one access-log pattern at a steady line rate reports its 5xx
// share shift, p99 shift and first 429 alike from footers and the summary, before and after its
// hour is compacted, and lists a row per status for no more GETs than one per pattern.
func TestValueAnomaliesOnAnAccessLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioAccess)
	require.NoError(t, err)
	store := storage.NewFS(dir)
	cat := catalog.New(store, tenant)
	start := seedcorpus.AccessStart()
	sel := selector.Selector{Time: selector.TimeSpec{From: start.Format(time.RFC3339), To: start.Add(seedcorpus.AccessDur * time.Second).Format(time.RFC3339)}}
	at := func(sec int) time.Time {
		return start.Add(time.Duration(sec) * time.Second).Truncate(summary.DefaultBinWidth)
	}
	want := []tools.Anomaly{
		{Kind: "mix_shift", Column: "status", Value: "503", At: at(seedcorpus.AccessErrorsAt)},
		{Kind: "new_value", Column: "status", Value: "429", At: at(seedcorpus.AccessThrottledAt)},
		{Kind: "drift", Column: "latency_ms", At: at(seedcorpus.AccessSlowAt)},
	}
	values := func(b summary.Builder) [][]summary.ValuePoint {
		require.NoError(t, b.Build(ctx))
		return [][]summary.ValuePoint{b.Store().ValueSlice("status", 0, 1<<62), b.Store().ValueSlice("latency_ms", 0, 1<<62)}
	}
	synced := summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})
	wantValues := values(synced)
	for _, compacted := range []bool{false, true} {
		var found [][]tools.Anomaly
		for _, warm := range []bool{false, true} {
			var anomalies tools.FindAnomaliesResponse
			answer(t, store, warm, "find_anomalies", tools.FindAnomaliesRequest{Selector: sel}, &anomalies)
			found = append(found, valueKinds(anomalies.Anomalies))
		}
		assert.Equal(t, [][]tools.Anomaly{want, want}, found, "compacted %v", compacted)

		var rows tools.PatternsResponse
		byCount := []string{"200", "500", "503", "429"}
		// A pattern's rows share its signal, so they rank by value.
		for sort, statuses := range map[string][]string{"count": byCount, "rate": byCount, "signal": {"200", "429", "500", "503"}} {
			byStatus := answer(t, store, false, "list_patterns", tools.PatternsRequest{Selector: sel, Sort: sort, By: "status"}, &rows)
			var got []string
			for _, p := range rows.Patterns {
				got = append(got, p.Value)
			}
			assert.Equal(t, statuses, got, sort)
			if !compacted {
				assert.Equal(t, answer(t, store, false, "list_patterns", tools.PatternsRequest{Selector: sel, Sort: sort}, &rows).GETs, byStatus.GETs)
			}
		}
		require.NoError(t, compact.New(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(store), store, tenant).Run(ctx, time.Now()))
	}
	assert.Equal(t, wantValues, values(synced), "a summary that counted the inputs skips their merge")
	assert.Equal(t, wantValues, values(summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})))
}

// answer decodes tool's answer to req from a querier over store, warm or cold, into data and returns its cost.
func answer(t *testing.T, store storage.Store, warm bool, tool string, req, data any) query.Cost {
	t.Helper()
	opts := querier.Options{Catalog: catalog.New(store, tenant), Store: store}
	if warm {
		opts.Summary = &summary.Config{}
	}
	app := querier.Open(opts)
	require.NoError(t, app.Refresh(context.Background()))
	body, err := json.Marshal(req)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(body)))
	env := query.Envelope[json.RawMessage]{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env), rec.Body.String())
	require.NoError(t, json.Unmarshal(env.Data, data))
	return env.Meta.Cost
}

// valueKinds is each value anomaly, head or member, as kind, column, value and bin.
func valueKinds(anomalies []tools.Anomaly) []tools.Anomaly {
	var out []tools.Anomaly
	for _, a := range anomalies {
		if a.Column != "" {
			out = append(out, tools.Anomaly{Kind: a.Kind, Column: a.Column, Value: a.Value, At: a.At})
		}
		out = append(out, valueKinds(a.Members)...)
	}
	return out
}
