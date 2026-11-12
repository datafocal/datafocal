package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
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
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// tokensColumn is a numeric column of the factory's transcripts.
const tokensColumn = "message.usage.output_tokens"

// TestRollupsOverTheFactoryCorpus merges every complete hour of the factory corpus: a summary
// read from the hours' rollups answers as one read from their footers, and a needle over a day
// reads each merged hour's filters and only the open hour's footers.
func TestRollupsOverTheFactoryCorpus(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioFactory)
	require.NoError(t, err)
	store := storage.NewFS(dir)
	cat := catalog.New(store, tenant)
	cfg := summary.Config{SketchColumns: []string{seedcorpus.SessionColumn, tokensColumn}}
	require.NoError(t, compact.NewWithConfig(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{}), segment.NewReader(store), store,
		compact.Config{Tenant: tenant, Summary: cfg}).Run(ctx, time.Now()))
	_, end := seedcorpus.FactoryWindow()
	from := end.Add(-23 * time.Hour) // inside the summary's retention
	day := selector.Selector{Time: selector.TimeSpec{From: rfc3339(from), To: rfc3339(end)}}
	withRollups := rollupQuerier(t, store, cat, cfg)

	t.Run("answers", func(t *testing.T) {
		withFooters := rollupQuerier(t, store, withoutRollups(cat), cfg)
		for _, r := range []struct {
			tool string
			req  any
		}{
			{"aggregate", tools.AggregateRequest{Selector: day, Measure: "count", GroupBy: segment.ColService, Bins: "1m"}},
			{"aggregate", tools.AggregateRequest{Selector: day, Measure: "count", GroupBy: segment.ColLevel, Bins: "1m"}},
			{"list_patterns", tools.PatternsRequest{Selector: day, Limit: 1000}},
			{"service_graph", tools.ServiceGraphRequest{Selector: day}},
		} {
			assert.JSONEq(t, string(rollupAnswer(t, withFooters, r.tool, r.req).Data), string(rollupAnswer(t, withRollups, r.tool, r.req).Data), "%s %+v", r.tool, r.req)
		}
		distinct := tools.AggregateRequest{Selector: day, Measure: "distinct:" + seedcorpus.SessionColumn, GroupBy: segment.ColService, Bins: "1h"}
		want, got := rollupSeries(t, withFooters, distinct), rollupSeries(t, withRollups, distinct)
		require.Len(t, got, len(want))
		for i, s := range want {
			for j, v := range s.Values {
				require.NotNil(t, got[i].Values[j], "%s bin %d", s.Key, j)
				assert.LessOrEqual(t, math.Abs(*got[i].Values[j]-*v), 3*sketch.HLLStandardError**v, "%s bin %d", s.Key, j)
			}
		}
	})

	// A footer-built summary folds a merged hour's segment-wide sketch into each of its bins, so
	// its hourly quantile weighs the hour's values by the bins they span: the hour's footer is
	// the answer a quantile from the rollup's bins is checked against.
	t.Run("quantiles", func(t *testing.T) {
		descs := live(t, cat, from, end)
		alone := map[int64]segment.Footer{} // an hour one merged segment alone holds, by its start
		for _, d := range descs {
			hour := d.TimeRange.Min.Truncate(time.Hour)
			shared := slices.ContainsFunc(descs, func(o segment.Descriptor) bool {
				return o.ID != d.ID && !o.TimeRange.Max.Before(hour) && o.TimeRange.Min.Before(hour.Add(time.Hour))
			})
			if d.Rollup.Key != "" && !shared && d.TimeRange.Max.Before(hour.Add(time.Hour)) {
				footer, _, err := segment.NewReader(store).Footer(ctx, d.Object)
				require.NoError(t, err)
				alone[hour.UnixMilli()] = footer
			}
		}
		compared := 0
		for _, s := range rollupSeries(t, withRollups, tools.AggregateRequest{Selector: day, Measure: "p95:" + tokensColumn, GroupBy: segment.ColService, Bins: "1h"}) {
			for j, v := range s.Values {
				want := alone[s.Start+int64(j)*s.Step].ForService(s.Key).Distributions[tokensColumn]
				if want == nil {
					continue
				}
				require.NotNil(t, v, "%s bin %d", s.Key, j)
				assert.InEpsilon(t, want.Quantile(0.95), *v, sketch.QuantileAccuracy, "%s bin %d", s.Key, j)
				compared++
			}
		}
		assert.Positive(t, compared)
	})

	t.Run("get_trace over a day", func(t *testing.T) {
		descs := live(t, cat, end.Add(-24*time.Hour), end)
		unmerged := slices.DeleteFunc(slices.Clone(descs), func(d segment.Descriptor) bool { return d.RollupFilters.Key != "" })
		// The open hour's session; Fields names its one column, so the call reads no footers to
		// find the session's other names.
		req := tools.TraceRequest{Field: seedcorpus.SessionColumn, Fields: []string{seedcorpus.SessionColumn}, Value: "9756b83d-a6d6-cf5c-4a6f-b1af3f3d7b7e",
			Time: selector.TimeSpec{From: rfc3339(end.Add(-24 * time.Hour)), To: rfc3339(end)}}
		app := querier.Open(querier.Options{Catalog: cat, Store: store})
		cold, warm := rollupAnswer(t, app, "get_trace", req).Meta.Cost, rollupAnswer(t, app, "get_trace", req).Meta.Cost
		assert.Equal(t, len(unmerged), cold.FootersDecoded, "the open hour's footers")
		// The repeat reads the filters, the footers and the chunks read whole from the cache, so
		// it reads at most the columns.
		assert.LessOrEqual(t, warm.GETs, cold.GETs-len(descs), "a filters object per merged hour and a footer per open segment")
	})
}

// rollupQuerier is a querier over store and cat with the summary cfg configures, built.
func rollupQuerier(t *testing.T, store storage.Store, cat catalog.Catalog, cfg summary.Config) querier.App {
	app := querier.Open(querier.Options{Catalog: cat, Store: store, Summary: &cfg})
	require.NoError(t, app.Refresh(context.Background()))
	return app
}

// withoutRollups is cat with no descriptor locating a rollup, so a summary reads each merged
// hour's footer.
func withoutRollups(cat catalog.Catalog) catalog.Catalog {
	return catalog.Fake{
		VersionFn: func() (catalog.Version, error) { return cat.Version(context.Background()) },
		PruneFn: func(v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
			descs, err := cat.Prune(context.Background(), v, sel)
			for i := range descs {
				descs[i].Rollup, descs[i].RollupFilters = segment.ObjectLocation{}, segment.ObjectLocation{}
			}
			return descs, err
		},
	}
}

// live is cat's newest descriptors overlapping from-to.
func live(t *testing.T, cat catalog.Catalog, from, to time.Time) []segment.Descriptor {
	descs, err := cat.Prune(context.Background(), version(t, cat), selector.Resolved{Window: selector.Window{From: from, To: to}})
	require.NoError(t, err)
	return descs
}

// rollupAnswer is app's answer to req, which must not be refused.
func rollupAnswer(t *testing.T, app querier.App, tool string, req any) query.Envelope[json.RawMessage] {
	body, err := json.Marshal(req)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+tool, bytes.NewReader(body)))
	var env query.Envelope[json.RawMessage]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))
	require.NotEmpty(t, env.Meta.Snapshot, "%s refused: %s", tool, rec.Body.Bytes())
	return env
}

// rollupSeries is app's aggregate series for req.
func rollupSeries(t *testing.T, app querier.App, req tools.AggregateRequest) []tools.Series {
	var resp tools.AggregateResponse
	require.NoError(t, json.Unmarshal(rollupAnswer(t, app, "aggregate", req).Data, &resp))
	require.NotEmpty(t, resp.Series)
	return resp.Series
}
