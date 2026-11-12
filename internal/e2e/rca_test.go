package e2e

import (
	"encoding/json"
	"math"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/topology"
)

// aggregateData is the db p95 series, the gateway status breakdown, the gateway
// error-rate ratio, and filtered-quantile results, so the cross-path Data
// comparison covers all aggregate measure types.
type aggregateData struct {
	DBP95            tools.AggregateResponse
	GatewayHTTP      tools.AggregateResponse
	GatewayErrorRate tools.AggregateResponse
	FilteredQuantile tools.AggregateResponse
}

// assertAggregate proves the two shapes the design's acceptance criteria name,
// each against the bins the ramp that produced it actually spans rather than
// against the whole window.
//
// db p95 is grouped by instance, so the two co-tenant instances stay separate:
// db-3 steps from DBWriteP95Ramp's 40ms floor to its 1200ms ceiling across the
// ramp, and db-1 holds the floor in both bins. Grouping by service instead
// averages them into one series that peaks lower than either instance's truth.
//
// The gateway 5xx ramp is asserted per bin over the whole ten-minute grid
// against seedcorpus.Gateway5xxPerMinute — the bins ErrorRateRamp brackets are
// where the step lands, and the ones outside it pin the flat parts.
func assertAggregate(t *testing.T, h *toolHarness) any {
	t.Helper()

	const p95Bins = 15 * time.Second
	dbEnv := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
		Selector: withPredicates(incidentSelector(),
			selector.Predicate{Field: "service", Op: selector.OpEq, Value: "db"}),
		Measure: "p95",
		GroupBy: "instance",
		Bins:    "15s",
	})
	ramp := seedcorpus.DBWriteP95Ramp
	before, after := ramp.BinBefore(p95Bins), ramp.BinAfter(p95Bins)
	// InEpsilon, not InDelta: DDSketch's guarantee is relative, so an absolute
	// delta would be far too loose at 1200ms and far too tight at 40ms. The tolerance
	// is the sampling one, not the sketch one: a 15s bin holds a couple of hundred
	// rows, and the spread of their 95th order statistic dwarfs the sketch's own error.
	for _, want := range []struct {
		instance      string
		before, after float64
	}{
		{seedcorpus.SaturatedDB, ramp.From, ramp.To},
		{seedcorpus.HealthyDB, ramp.From, ramp.From},
	} {
		series := findSeriesByKey(dbEnv.Data.Series, want.instance)
		require.NotNilf(t, series, "expected a series keyed %q in the aggregate response", want.instance)
		require.Greaterf(t, len(series.Values), after,
			"the 15s grid must reach bin %d, the first settled bin after the p95 ramp", after)
		assert.InEpsilonf(t, want.before, *series.Values[before], seedcorpus.QuantileSampleRelTolerance,
			"%s p95 in bin %d, the last bin before the ramp starts: want ~%.0fms, got %.1fms",
			want.instance, before, want.before, *series.Values[before])
		assert.InEpsilonf(t, want.after, *series.Values[after], seedcorpus.QuantileSampleRelTolerance,
			"%s p95 in bin %d, the first bin after the ramp settles: want ~%.0fms, got %.1fms",
			want.instance, after, want.after, *series.Values[after])
	}

	gwEnv := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
		Selector: withPredicates(incidentSelector(),
			selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"}),
		Measure: "count",
		GroupBy: "status",
		Bins:    "1m",
	})
	require.NotEmpty(t, gwEnv.Data.Series, "gateway count by status must return series")
	assert.Equal(t, seedcorpus.Gateway5xxPerMinute, sum5xxPerBin(gwEnv.Data.Series),
		"gateway 5xx per minute must match the seeded error-rate ramp bin for bin")
	// The ramp is a step, not just a nonzero count: the bin before it starts must
	// stay under the ones after it settle by more than an order of magnitude.
	errRamp := seedcorpus.ErrorRateRamp
	quiet := seedcorpus.Gateway5xxPerMinute[errRamp.BinBefore(time.Minute)]
	loud := seedcorpus.Gateway5xxPerMinute[errRamp.BinAfter(time.Minute)]
	assert.Greater(t, loud, 10*quiet,
		"the seeded 5xx ramp must be a step: bin %.0f before vs %.0f after", quiet, loud)

	// Gateway error-rate ratio: 5xx over every gateway request, per bin.
	errorRateEnv := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
		Selector: withPredicates(incidentSelector(),
			selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"}),
		Measure:   "ratio",
		Numerator: []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}},
		GroupBy:   "service",
		Bins:      "1m",
	})
	require.NotEmpty(t, errorRateEnv.Data.Series, "gateway error-rate ratio must return series")

	// Checked by reconstruction: rate × the bin's known traffic must return the seeded
	// 5xx count, which pins denominator and numerator at once.
	rates := seriesValues(errorRateEnv.Data.Series[0])
	require.Len(t, rates, len(seedcorpus.Gateway5xxPerMinute), "one rate per incident bin")
	for i, want := range seedcorpus.Gateway5xxPerMinute {
		assert.InDeltaf(t, want, rates[i]*seedcorpus.GatewayLinesPerMinute[i], 1e-9,
			"bin %d: error rate %.6f x %.0f gateway lines must reconstruct the seeded 5xx count",
			i, rates[i], seedcorpus.GatewayLinesPerMinute[i])
	}

	// The issue's whole argument, on the shared corpus: across GatewayTrafficStep the
	// traffic doubles while the failure fraction holds, so the 5xx count jumps and the
	// error rate does not. A caller watching the count sees an incident that is not
	// happening; only the ratio reports the service as unchanged.
	postStep := seedcorpus.GatewayTrafficStep.Bin(time.Minute)
	preStep := postStep - 1
	counts := sum5xxPerBin(gwEnv.Data.Series)
	assert.Greaterf(t, counts[postStep], 1.5*counts[preStep],
		"the 5xx count must jump across the traffic step (bin %d=%.0f -> bin %d=%.0f)",
		preStep, counts[preStep], postStep, counts[postStep])
	assert.InEpsilonf(t, rates[preStep], rates[postStep], 0.15,
		"the error rate must not: bin %d=%.4f vs bin %d=%.4f",
		preStep, rates[preStep], postStep, rates[postStep])

	// The issue's motivating case: the p99 of one route. endpoint varies within the
	// gateway stream, so no footer sketch can subset it and this takes the scan path,
	// whose signature is Approx=false.
	routeP99 := func(route string) query.Envelope[tools.AggregateResponse] {
		return postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
			Selector: withPredicates(incidentSelector(),
				selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"},
				selector.Predicate{Field: "endpoint", Op: selector.OpEq, Value: route}),
			// Named because "POST /users 503 1200 ms" carries two numeric params, so a
			// bare p99 is ambiguous on this stream and gets refused rather than guessed.
			Measure: "p99:duration_ms",
			GroupBy: "service",
			Bins:    "1m",
		})
	}
	routeP99Env := routeP99("/users")
	require.NotEmpty(t, routeP99Env.Data.Series, "route-scoped p99 must return series")
	assert.False(t, routeP99Env.Meta.Approx, "a scanned quantile is exact, not a sketch estimate")

	// Both routes take that same scan path, so only the predicate separates them —
	// /users carries the 5xx latency ramp and /health stays healthy (gen.go:428). Equal
	// values mean the filter was dropped, the silent-unfiltered bug this path replaced.
	healthyRouteEnv := routeP99("/health")
	require.NotEmpty(t, healthyRouteEnv.Data.Series)
	assert.NotEqual(t, seriesValues(healthyRouteEnv.Data.Series[0]), seriesValues(routeP99Env.Data.Series[0]),
		"a route-scoped p99 that ignores the route has silently dropped the filter")

	return aggregateData{
		DBP95:            dbEnv.Data,
		GatewayHTTP:      gwEnv.Data,
		GatewayErrorRate: errorRateEnv.Data,
		FilteredQuantile: routeP99Env.Data,
	}
}

// sum5xxPerBin collapses a status-keyed aggregate response into one 5xx count
// per bin, in bin order. Every 5xx status the gateway emits is summed rather
// than a chosen one, so a seed that shifts a failure from 503 to 500 does not
// silently halve the assertion.
func sum5xxPerBin(series []tools.Series) []float64 {
	var out []float64
	for _, s := range series {
		if len(s.Key) != 3 || s.Key[0] != '5' {
			continue
		}
		for i, v := range s.Values {
			for len(out) <= i {
				out = append(out, 0)
			}
			out[i] += *v
		}
	}
	return out
}

// assertCorrelate pins lag 0 and direction "none" because two slow co-ramps a few
// seconds apart peak at lag zero on every grid ParseBins accepts, so the corpus
// proves co-movement but not ordering.
func assertCorrelate(t *testing.T, h *toolHarness) any {
	t.Helper()

	writesID := patternIDForPrefix("wrote <*> rows to events")
	slowID := patternIDForPrefix("slow query ")
	require.NotEmpty(t, writesID, "seedcorpus must expose a 'wrote * rows to events' pattern")
	require.NotEmpty(t, slowID, "seedcorpus must expose a 'slow query * ms' pattern")

	req := tools.CorrelateRequest{
		Selector:      incidentSelector(),
		SeriesA:       tools.SeriesRef{PatternID: writesID},
		SeriesB:       tools.SeriesRef{PatternID: slowID},
		MaxLagSeconds: 120,
	}
	env := postTool[tools.CorrelateResponse](t, h, "correlate", req)

	assert.GreaterOrEqualf(t, env.Data.Rho, seedcorpus.CorrelateRhoFloor,
		"the two ramps must come back strongly co-moving; got rho=%.4f", env.Data.Rho)
	assert.LessOrEqual(t, env.Data.Rho, 1.0, "rho must stay inside [-1, 1]")
	assert.Equalf(t, seedcorpus.CorrelateExpectedLag.Seconds(), env.Data.LagSeconds,
		"expected the peak at lag %s; got %.0fs", seedcorpus.CorrelateExpectedLag, env.Data.LagSeconds)
	assert.Equal(t, seedcorpus.CorrelateExpectedDirect, env.Data.Direction,
		"a lag-zero peak must report direction %q", seedcorpus.CorrelateExpectedDirect)

	return env.Data
}

// attributeWritesData is both groupings of the host-7 attribution, so the
// cross-path Data comparison covers each of them.
type attributeWritesData struct {
	ByPod     tools.AttributeWritesResponse
	ByService tools.AttributeWritesResponse
}

// assertAttributeWrites attributes activity on the saturated host (host-7) at
// both resolutions the corpus can answer.
//
// group_by=pod is the resolvable one, so the exact line counts are asserted
// against it: every workload stream stamps its pod, so both co-tenants match and
// db-3-0 (58%) outranks analytics-export-0 (42%) — these are log lines, and db-3
// is the chattier of the two (seedcorpus.HostSevenPodLines).
//
// group_by=service is asserted as the known limitation it is: it credits
// analytics-export with 100% of a host it drives 42% of, because the default
// grouping derives "db-3" from pod db-3-0 and no row's service is "db-3". It stays
// because it is the only grouping the summary can key counts on, which makes it the
// suite's one summary-versus-scan comparison — 3 GETs against 33 for the same
// numbers.
func assertAttributeWrites(t *testing.T, h *toolHarness) any {
	t.Helper()

	byPod := postTool[tools.AttributeWritesResponse](t, h, "attribute_writes", tools.AttributeWritesRequest{
		Selector: incidentSelector(),
		Entity:   tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
		GroupBy:  "pod",
	})
	assert.Equal(t, seedcorpus.HostSevenPodLines, breakdownLines(byPod.Data.Breakdown),
		"group_by=pod must total every host-7 pod's lines exactly")
	assert.Equal(t, seedcorpus.PodsOn(seedcorpus.SaturatedHost), []string{
		seedcorpus.AnalyticsExportPod, seedcorpus.SaturatedDBPod,
	}, "the fixture's host-7 placements must be the two pods this assertion names")
	require.Len(t, byPod.Data.Breakdown, 2, "host-7 hosts exactly two pods")
	assert.Equal(t, seedcorpus.SaturatedDBPod, byPod.Data.Breakdown[0].Key,
		"the breakdown must rank by lines, and db-3-0 contributes more of them")
	assert.InDelta(t, 100.0, byPod.Data.Breakdown[0].Pct+byPod.Data.Breakdown[1].Pct, 1e-9,
		"the shares of an entity's whole breakdown must sum to 100%%")

	byService := postTool[tools.AttributeWritesResponse](t, h, "attribute_writes", tools.AttributeWritesRequest{
		Selector: incidentSelector(),
		Entity:   tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
	})
	assert.Equal(t, seedcorpus.HostSevenServiceLines, breakdownLines(byService.Data.Breakdown),
		"group_by=service must resolve both co-tenants to their services — see HostSevenServiceLines")

	return attributeWritesData{ByPod: byPod.Data, ByService: byService.Data}
}

// assertGetTrace resolves a known correlated request_id — the seed's very
// first correlated slot writes request_id="req-000-00" on both gateway and
// checkout, deriving a nested two-span waterfall. db does not share
// request_ids with the app layer, so it must not appear.
func assertGetTrace(t *testing.T, h *toolHarness) any {
	t.Helper()

	req := tools.TraceRequest{
		Field: "request_id",
		Value: "req-000-00",
	}
	env := postTool[tools.TraceResponse](t, h, "get_trace", req)

	assert.Equal(t, "request_id", env.Data.Field)
	assert.Equal(t, "req-000-00", env.Data.Value)
	require.Len(t, env.Data.Spans, 2, "correlated request must reconstruct exactly two spans (gateway + checkout)")

	services := map[string]bool{}
	for _, sp := range env.Data.Spans {
		services[sp.Service] = true
	}
	assert.True(t, services["gateway"], "trace must include gateway span")
	assert.True(t, services["checkout"], "trace must include checkout span")
	assert.False(t, services["db"], "db does not share request_id with the app layer; must not appear in the trace")

	return env.Data
}

// assertTraceShape merges the incident's gateway POST /users requests, so their root
// node is in every sampled trace.
func assertTraceShape(t *testing.T, h *toolHarness) any {
	t.Helper()

	env := postTool[tools.TraceShapeResponse](t, h, "trace_shape", tools.TraceShapeRequest{
		Selector: incidentSelector(), Field: "request_id", Sample: 20, Root: []selector.Predicate{
			{Field: "service", Op: selector.OpEq, Value: "gateway"},
			{Field: "message", Op: selector.OpMatch, Value: "POST /users"},
		},
	})
	require.Equal(t, 20, env.Data.Traces)
	assert.Equal(t, 1.0, env.Data.Nodes[0].Presence)
	return env.Data
}

// searchLogsData is both halves of the search surface, so the cross-path Data
// comparison covers each of them.
type searchLogsData struct {
	Needle tools.SearchResponse
	Scan   tools.SearchResponse
}

// assertSearchLogs runs both of the tool's modes.
//
// The needle lookup resolves an id equality predicate to the rows carrying it.
// req-000-00 is the seed's first correlated slot, written on both gateway and
// checkout, so both services must come back and every line must carry the id asked
// for.
//
// The scan is what a selector without an id predicate falls into. Over the healthy
// window it must return only recommendations' lines, newest first, each of one of the
// rollout's patterns (seedcorpus.HealthyPatterns).
func assertSearchLogs(t *testing.T, h *toolHarness) any {
	t.Helper()

	needleEnv := postTool[tools.SearchResponse](t, h, "search_logs", tools.SearchRequest{
		Selector: withPredicates(incidentSelector(),
			selector.Predicate{Field: "request_id", Op: selector.OpEq, Value: "req-000-00"}),
		Limit: 50,
	})
	assert.Equal(t, "needle", needleEnv.Data.Mode)
	require.NotEmpty(t, needleEnv.Data.Lines, "needle lookup on req-000-00 must return the correlated rows")
	svcSeen := map[string]bool{}
	for _, line := range needleEnv.Data.Lines {
		svcSeen[line.Service] = true
		assert.Equal(t, "req-000-00", line.Attributes["request_id"])
	}
	assert.True(t, svcSeen["gateway"], "needle results must include the calling gateway span")
	assert.True(t, svcSeen["checkout"], "needle results must include the called checkout span")

	scanEnv := postTool[tools.SearchResponse](t, h, "search_logs", tools.SearchRequest{
		Selector: withPredicates(healthySelector(),
			selector.Predicate{Field: "service", Op: selector.OpEq, Value: "recommendations"}),
		Limit: 50,
	})
	assert.Equal(t, "scan", scanEnv.Data.Mode)
	require.Len(t, scanEnv.Data.Lines, 50, "the scan must fill its limit from the healthy window")

	wantIDs := make([]string, 0, len(seedcorpus.HealthyPatterns))
	for _, tmpl := range seedcorpus.HealthyPatterns {
		wantIDs = append(wantIDs, patternIDByStr[tmpl])
	}
	for i, line := range scanEnv.Data.Lines {
		assert.Equal(t, "recommendations", line.Service)
		assert.Contains(t, wantIDs, line.PatternID)
		if i > 0 {
			prev, err := time.Parse(time.RFC3339Nano, scanEnv.Data.Lines[i-1].Timestamp)
			require.NoError(t, err)
			at, err := time.Parse(time.RFC3339Nano, line.Timestamp)
			require.NoError(t, err)
			assert.False(t, at.After(prev), "lines must be newest first: %s after %s", line.Timestamp, scanEnv.Data.Lines[i-1].Timestamp)
		}
	}

	return searchLogsData{Needle: needleEnv.Data, Scan: scanEnv.Data}
}

// TestSearchLogsGroupIsTheUnionOfItsAlternatives: a search under any(service=a,
// service=b) finds exactly the patterns of the lines the two single-service searches find between them.
func TestSearchLogsGroupIsTheUnionOfItsAlternatives(t *testing.T) {
	h := newHarness(t, false)
	search := func(preds ...selector.Predicate) []string {
		env := postTool[tools.SearchResponse](t, h, "search_logs", tools.SearchRequest{
			Selector: withPredicates(incidentSelector(), preds...),
			Limit:    100000,
		})
		ids := make([]string, 0, len(env.Data.Lines))
		for _, l := range env.Data.Lines {
			ids = append(ids, l.PatternID)
		}
		slices.Sort(ids)
		return slices.Compact(ids)
	}
	gateway := selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"}
	checkout := selector.Predicate{Field: "service", Op: selector.OpEq, Value: "checkout"}

	union := slices.Concat(search(gateway), search(checkout))
	slices.Sort(union)
	union = slices.Compact(union)
	got := search(selector.Predicate{Any: []selector.Predicate{gateway, checkout}})
	slices.Sort(got)
	require.NotEmpty(t, union)
	assert.Equal(t, union, got)
	assert.NotEqual(t, search(), got, "the group must narrow the search")
}

func TestSearchLogsRejectsANestedGroup(t *testing.T) {
	h := newHarness(t, false)
	inner := selector.Predicate{Any: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "gateway"}}}
	status, body := postToolRaw(t, h, "search_logs", tools.SearchRequest{
		Selector: withPredicates(incidentSelector(), selector.Predicate{Any: []selector.Predicate{inner}}),
	})
	require.Equalf(t, http.StatusBadRequest, status, "body: %s", body)
	var apiErr query.APIError
	require.NoError(t, json.Unmarshal(body, &apiErr))
	assert.Equal(t, query.ErrCodeInvalidRequest, apiErr.Code)
}

// assertRankFields discriminates the incident window: the ranked fields must
// include the id columns the seed carries (request_id, trace_id, user_id) and
// classify them as type "id" with a high signal score.
func assertRankFields(t *testing.T, h *toolHarness) any {
	t.Helper()

	req := tools.FieldsRequest{Selector: incidentSelector()}
	env := postTool[tools.FieldsResponse](t, h, "rank_fields", req)
	require.NotEmpty(t, env.Data.Fields, "rank_fields must return at least one field over the incident window")

	byName := make(map[string]tools.FieldStat, len(env.Data.Fields))
	for _, f := range env.Data.Fields {
		byName[f.Name] = f
	}
	for name := range byName {
		assert.NotRegexp(t, `^(ts|pattern_id|param_\d+|.*_pattern)$`, name, "rank_fields leaves out the columns written for the engine's reads")
	}
	for _, id := range []string{"request_id", "trace_id", "user_id"} {
		f, ok := byName[id]
		require.Truef(t, ok, "rank_fields must surface %q from the incident window", id)
		assert.Equal(t, "id", f.Type, "%q must be classified as type=id", id)
		assert.True(t, f.HighSignal, "%q must be marked high_signal", id)
		assert.Greater(t, f.Cardinality, int64(1), "%q must have cardinality > 1", id)
	}

	return env.Data
}

// assertDescribeSchema checks the incident window was written by one profile, read from
// footers alone; its golden pins the whole response, every column included.
func assertDescribeSchema(t *testing.T, h *toolHarness) any {
	t.Helper()

	env := postTool[tools.DescribeSchemaResponse](t, h, "describe_schema", tools.FieldsRequest{Selector: incidentSelector()})
	assert.Equal(t, []tools.Producer{{Profile: "auto", Version: 1, Degradation: "none", Segments: 322}}, env.Data.Producers)
	assert.Equal(t, env.Meta.Cost.FootersDecoded, env.Meta.Cost.GETs, "describe_schema reads footers only")

	return env.Data
}

// assertListPatterns runs sort=signal over the healthy window and asserts the exact
// pattern set the recommendations rollout emits (seedcorpus.HealthyPatterns), all
// attributed to recommendations — the only service in that window — with a populated
// signal score.
//
// The design's "three patterns marked status=new" criterion is not asserted; the
// HealthyPatterns doc comment records why and what it would take.
func assertListPatterns(t *testing.T, h *toolHarness) any {
	t.Helper()

	req := tools.PatternsRequest{
		Selector: healthySelector(),
		Sort:     "signal",
		Limit:    100,
	}
	env := postTool[tools.PatternsResponse](t, h, "list_patterns", req)
	require.NotEmpty(t, env.Data.Patterns, "list_patterns must return patterns for the healthy window")

	seen := make(map[string]struct{}, len(env.Data.Patterns))
	for _, p := range env.Data.Patterns {
		assert.Equal(t, "recommendations", p.Service,
			"healthy-window patterns must all be attributed to the recommendations service; got %q for %s",
			p.Service, p.PatternID)
		assert.Greater(t, p.Signal, 0.0,
			"sort=signal must populate a positive Signal score; %s has %v", p.PatternID, p.Signal)
		seen[p.PatternID] = struct{}{}
	}
	// Pattern carries only the content-hash id, so the expected patterns are
	// resolved through the corpus's own pattern map.
	for _, want := range seedcorpus.HealthyPatterns {
		id := patternIDByStr[want]
		require.NotEmptyf(t, id, "seedcorpus must emit the healthy pattern %q", want)
		assert.Containsf(t, seen, id,
			"list_patterns must surface the healthy pattern %q (id %s)", want, id)
	}

	return env.Data
}

// assertServiceGraph asserts the topology carries every pod→host placement the
// scheduler stream records — not a sample of two — because that list is the
// corpus's only path from a workload to the host it saturates (no trace_id
// spans layers). This fixture is summary-only because service_graph returns an
// APIError when no summary store is wired.
func assertServiceGraph(t *testing.T, h *toolHarness) any {
	t.Helper()

	req := tools.ServiceGraphRequest{
		Selector: incidentSelector(),
		Limit:    500,
	}
	env := postTool[tools.ServiceGraphResponse](t, h, "service_graph", req)

	require.NotEmpty(t, env.Data.Edges, "service_graph must return topology edges over the incident window")
	assert.LessOrEqual(t, env.Meta.Cost.GETs, 4322, "shared values come from the footers the graph folds, so they cost no GET past them and the sampled traces")
	for _, p := range seedcorpus.Placements {
		assert.Truef(t, hasEdge(env.Data.Edges, p.Pod, p.Host),
			"topology must include the scheduled edge %s → %s", p.Pod, p.Host)
	}

	return env.Data
}

// assertCoOccurrence keeps the exact trace-id pairs separate from the graph's
// shared-value edges, which also see request_id and user_id.
func assertCoOccurrence(t *testing.T, h *toolHarness) any {
	t.Helper()

	env := postTool[tools.CoOccurrenceResponse](t, h, "co_occurrence", tools.CoOccurrenceRequest{
		Selector: incidentSelector(), Entity: "service", Shared: "trace_id",
		MaxGapSeconds: int(summary.DefaultBinWidth.Seconds()), Limit: 100000,
	})
	require.Equal(t, 1.0, env.Meta.Coverage, "the limit must keep every pair")
	pairs := map[[2]string]bool{}
	for _, p := range env.Data.Pairs {
		pairs[[2]string{p.A, p.B}] = true
	}

	graph := postTool[tools.ServiceGraphResponse](t, h, "service_graph", tools.ServiceGraphRequest{Selector: incidentSelector(), Limit: 2000})
	edges := map[[2]string]bool{}
	for _, e := range graph.Data.Edges {
		if e.Method == string(topology.EdgeCoOccurrence) {
			edges[[2]string{e.From, e.To}] = true
		}
	}
	assert.Empty(t, pairs)
	assert.Equal(t, map[[2]string]bool{
		{"checkout", "recommendations"}: true, {"gateway", "recommendations"}: true,
	}, edges, "gateway and checkout are linked by their calls instead")
	return env.Data
}

// assertFunnel asserts that the requests gateway hands to checkout, which logs its half
// first, reach a checkout-then-gateway funnel's second step exactly as co_occurrence
// pairs the two services on request_id.
func assertFunnel(t *testing.T, h *toolHarness) any {
	t.Helper()

	env := postTool[tools.FunnelResponse](t, h, "funnel", tools.FunnelRequest{
		Selector: incidentSelector(), Entity: "request_id", Steps: [][]selector.Predicate{
			{{Field: "service", Op: selector.OpEq, Value: "checkout"}},
			{{Field: "service", Op: selector.OpEq, Value: "gateway"}},
		},
	})
	pairs := postTool[tools.CoOccurrenceResponse](t, h, "co_occurrence", tools.CoOccurrenceRequest{
		Selector: incidentSelector(), Entity: "service", Shared: "request_id",
		MaxGapSeconds: seedcorpus.IncidentDur, Limit: 100000,
	})
	var shared int64
	for _, p := range pairs.Data.Pairs {
		if p.A == "checkout" && p.B == "gateway" {
			shared++
		}
	}
	require.Positive(t, shared)
	assert.Equal(t, shared, env.Data.Steps[1].Reached)
	return env.Data
}

// assertRCAWalk drives the walk the way a caller actually reaches it: it names the
// symptom it observed and nothing else. Candidate selection is the tool's job — an
// operator staring at slow queries does not know that analytics-export's writes are
// upstream, and if they did they would not need the walk — so passing an explicit
// candidate list here would exercise the one path production never takes.
//
// Running under the matrix's two harnesses is what makes this the auto-pick gate: the
// nil-summary pass has to rank candidates off segment footers and reach the same
// answer the summary-served pass reaches, or TestToolsOverSeededCorpus fails on the
// byte-identical Data comparison.
func assertRCAWalk(t *testing.T, h *toolHarness) any {
	t.Helper()

	symptomID := patternIDForPrefix("slow query ")
	require.NotEmpty(t, symptomID, "seedcorpus must expose a 'slow query * ms' pattern")

	req := tools.RCAWalkRequest{
		Selector: incidentSelector(),
		Symptom:  tools.SeriesRef{PatternID: symptomID},
		Entity:   tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
		MinRho:   0.5,
	}
	env := postTool[tools.RCAWalkResponse](t, h, "rca_walk", req)

	assert.Equal(t, symptomID, env.Data.Symptom, "walk must start from the requested symptom")

	// Compare the whole sequence of rendered prefixes, since pattern ids are content
	// hashes and a length-only check passes for a chain of the wrong patterns.
	gotChain := make([]string, 0, len(env.Data.Chain))
	for _, hop := range env.Data.Chain {
		tmpl, ok := buildResult.Patterns[hop.PatternTo]
		require.Truef(t, ok, "hop pattern %q is not in the corpus", hop.PatternTo)
		gotChain = append(gotChain, tmpl)
		assert.NotEmpty(t, hop.AdmittedBy, "each hop must record which gate admitted it")
		assert.NotEmpty(t, hop.From, "each hop names the service it left")
		assert.NotEmpty(t, hop.To, "each hop names the service it reached")
	}
	require.Lenf(t, gotChain, len(seedcorpus.RCAExpectedChain),
		"chain must match the expected corpus output; got %q", gotChain)
	for i, prefix := range seedcorpus.RCAExpectedChain {
		assert.Truef(t, strings.HasPrefix(gotChain[i], prefix),
			"chain[%d] = %q, want a pattern starting %q", i, gotChain[i], prefix)
	}

	for i := 1; i < len(env.Data.Chain); i++ {
		assert.GreaterOrEqual(t, env.Data.Chain[i].LagSeconds, env.Data.Chain[i-1].LagSeconds,
			"chain must be ordered by lag ascending")
	}

	// The gate has to reject, not just admit, and every candidate it dropped must name
	// the gate that dropped it.
	require.NotEmpty(t, env.Data.Rejected, "candidates must be rejected by the gates")
	reasons := make(map[tools.RejectReason]int)
	var coincidence *tools.RCARejection
	for i, rej := range env.Data.Rejected {
		require.NotEmpty(t, rej.RejectedBy, "each rejection must state which gate dropped it")
		reasons[rej.RejectedBy]++
		if strings.HasPrefix(buildResult.Patterns[rej.PatternTo], seedcorpus.RCACoincidence) {
			coincidence = &env.Data.Rejected[i]
		}
	}
	assert.Positivef(t, reasons[seedcorpus.RCAExpectedRejectionReason],
		"at least one rejection must use the expected gate reason %q; got %v",
		seedcorpus.RCAExpectedRejectionReason, reasons)

	// Asserting rho cleared the floor is what makes this rejection evidence about the
	// gate, since one at |rho| < min_rho would prove nothing about the mechanism.
	if assert.NotNilf(t, coincidence, "the %q pattern must appear in the rejections", seedcorpus.RCACoincidence) {
		assert.Equal(t, tools.RejectReason(seedcorpus.RCACoincidenceRejectedBy), coincidence.RejectedBy,
			"a co-moving pattern with no mechanism must be dropped by the corroboration gate")
		assert.Greaterf(t, math.Abs(coincidence.Rho), req.MinRho,
			"%q must clear the rho floor, or its rejection says nothing about the gate", seedcorpus.RCACoincidence)
	}

	// Auto-pick ranked every pattern in the window and the corpus holds fewer than
	// the default cap, so nothing was cut and coverage stays whole.
	assert.Equal(t, 1.0, env.Meta.Coverage)

	// byte-identical: same call + same snapshot must produce the same Data.
	env2 := postTool[tools.RCAWalkResponse](t, h, "rca_walk", req)
	a, err := json.Marshal(env.Data)
	require.NoError(t, err)
	b, err := json.Marshal(env2.Data)
	require.NoError(t, err)
	assert.Equal(t, string(a), string(b),
		"rca_walk: same call + same snapshot must produce byte-identical Data")

	return env.Data
}

// TestRCAWalkSymptomShapes is the symptom-shape axis the RCA benchmark scores: every way
// of saying "this is what went wrong" must reach the walk over the real corpus, since a
// shape that refuses at the request is a hole in the benchmark rather than a result. The
// gates are agnostic to how a series was measured, so this asserts each shape resolves
// and leaves the chain's content to the tests that seed a known cause.
func TestRCAWalkSymptomShapes(t *testing.T) {
	h := newHarness(t, true)
	symptomID := patternIDForPrefix("POST /users")
	require.NotEmpty(t, symptomID, "seedcorpus must expose a 'POST /users' pattern")
	errSlice := []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}}

	tests := []struct {
		name    string
		measure string
		where   []selector.Predicate
	}{
		{name: "count"},
		{name: "filtered_count", where: errSlice},
		{name: "error_rate", measure: "ratio", where: errSlice},
		{name: "error_time_share", measure: "ratio:duration_ms", where: errSlice},
		{name: "latency_quantile", measure: "p99:duration_ms"},
		{name: "filtered_quantile", measure: "p99:duration_ms", where: errSlice},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := postTool[tools.RCAWalkResponse](t, h, "rca_walk", tools.RCAWalkRequest{
				Selector: incidentSelector(),
				Symptom:  tools.SeriesRef{PatternID: symptomID, Measure: tt.measure, Where: tt.where},
				Entity:   tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
				MinRho:   0.5,
			})
			assert.Equal(t, symptomID, env.Data.Symptom, "the walk must start from the symptom it was given")
		})
	}
}

// TestAggregateSumRatioIsTimeWeighted: over the seeded incident only ~4% of gateway
// requests fail, but a failure runs 200-1200ms against a healthy 8-25ms, so those few
// own most of the service's time. That gap is the argument for "ratio:<field>" — an
// alert on the line-weighted rate reads 4% and stays quiet while the latency budget
// burns.
func TestAggregateSumRatioIsTimeWeighted(t *testing.T) {
	h := newHarness(t, true)
	gateway := withPredicates(incidentSelector(),
		selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"})

	errorRate := func(measure string) []float64 {
		env := postTool[tools.AggregateResponse](t, h, "aggregate", tools.AggregateRequest{
			Selector: gateway, Measure: measure, GroupBy: "service", Bins: "1m",
			Numerator: []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}},
		})
		require.Len(t, env.Data.Series, 1)
		return seriesValues(env.Data.Series[0])
	}

	byLines, byTime := errorRate("ratio"), errorRate("ratio:duration_ms")
	// The first bin where ErrorRateRamp has settled, so both are at their plateau.
	settled := seedcorpus.ErrorRateRamp.BinAfter(time.Minute)
	require.Greater(t, len(byLines), settled)
	require.Len(t, byTime, len(byLines), "both measures share the grid")

	for i := range byLines {
		assert.LessOrEqualf(t, byTime[i], 1.0, "bin %d: a share of total time cannot exceed 1", i)
		assert.GreaterOrEqualf(t, byTime[i], 0.0, "bin %d: a share of total time cannot be negative", i)
	}
	assert.Less(t, byLines[settled], 0.10, "only a small fraction of requests fail")
	assert.Greater(t, byTime[settled], 0.50, "but they own most of the time spent")
	assert.Greaterf(t, byTime[settled], 10*byLines[settled],
		"the two ratios must disagree by an order of magnitude: %.4f of requests vs %.4f of time",
		byLines[settled], byTime[settled])
}

// TestAggregateListsTraces lists the incident's gateway→checkout requests in one call,
// failing ones first, each spanning the events get_trace reports for it.
func TestAggregateListsTraces(t *testing.T) {
	h := newHarness(t, true)
	where := func(field, value string) []selector.Predicate {
		return []selector.Predicate{{Field: field, Op: selector.OpEq, Value: value}}
	}
	req := tools.AggregateRequest{
		Selector: incidentSelector(), GroupBy: "request_id", Bins: "all", Sort: "2:desc,0:asc", Limit: 20,
		Measures: []selector.MeasureSpec{{Measure: "min:ts"}, {Measure: "max:ts"}, {Measure: "count", Where: where("level", "error")},
			{Measure: "count", Where: where("service", "gateway")}, {Measure: "count", Where: where("service", "checkout")}},
		Having: []selector.HavingClause{{Index: 3, Op: selector.OpGt}, {Index: 4, Op: selector.OpGt}},
	}
	env := postTool[tools.AggregateResponse](t, h, "aggregate", req)
	require.Len(t, env.Data.Series, 20)
	assert.Positive(t, *env.Data.Series[0].MeasureValues[2][0], "a failing request sorts first")
	for _, s := range env.Data.Series {
		trace := postTool[tools.TraceResponse](t, h, "get_trace", tools.TraceRequest{Field: "request_id", Value: s.Key, Time: incidentTimeSpec()})
		// A seeded line is stamped when its span ends.
		ends := make([]float64, len(trace.Data.Spans))
		for i, span := range trace.Data.Spans {
			ends[i] = float64(span.StartMs + span.DurMs)
		}
		assert.Equal(t, []float64{slices.Min(ends), slices.Max(ends)}, []float64{*s.MeasureValues[0][0], *s.MeasureValues[1][0]}, s.Key)
	}
	again := postTool[tools.AggregateResponse](t, h, "aggregate", req)
	assert.Equal(t, env.Data, again.Data, "the same request and snapshot answer the same")
}

// TestRCAWalkRatioSymptom is acceptance #3 over the seeded corpus: an error-rate
// symptom (5xx / total on the gateway's user-create pattern) drives the walk. The
// chain is asserted by the service each hop reached rather than by matching rendered
// pattern text, which db's own healthy "query ok" pattern would also satisfy.
func TestRCAWalkRatioSymptom(t *testing.T) {
	h := newHarness(t, true) // With summary for determinism

	symptomID := patternIDForPrefix("POST /users")
	require.NotEmpty(t, symptomID, "seedcorpus must expose a 'POST /users' pattern")

	env := postTool[tools.RCAWalkResponse](t, h, "rca_walk", tools.RCAWalkRequest{
		Selector: incidentSelector(),
		Symptom: tools.SeriesRef{
			PatternID: symptomID,
			Measure:   "ratio",
			Where:     []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}},
		},
		Entity: tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
		MinRho: 0.5,
	})

	assert.Equal(t, symptomID, env.Data.Symptom, "walk must start from the ratio symptom")
	require.NotEmpty(t, env.Data.Chain, "ratio symptom must produce a chain to the origin")
	for _, hop := range env.Data.Chain {
		assert.NotEmpty(t, hop.AdmittedBy, "each hop must record which gate admitted it")
	}
	assert.Truef(t, slices.ContainsFunc(env.Data.Chain, func(h tools.RCAHop) bool { return h.To == "db" }),
		"the ratio symptom must reach the db service, got chain %+v", env.Data.Chain)
}

// correlateEqualityCheck is the cross-path assertion for correlate: direction
// and lag must match exactly (they are the reasoning-critical fields the RCA
// caller acts on), but rho differs slightly across the two paths because the
// summary accumulates per-bin CountMin counts while the scan path merges
// per-segment CountMin estimates, and CountMin is a bounded-error sketch. A
// 5% absolute delta on rho keeps the assertion honest without pretending the
// two paths compute a byte-identical scalar.
func correlateEqualityCheck(t *testing.T, noSummary, withSummary any) {
	t.Helper()
	a, aOK := noSummary.(tools.CorrelateResponse)
	b, bOK := withSummary.(tools.CorrelateResponse)
	require.Truef(t, aOK && bOK, "correlate equality inputs must be tools.CorrelateResponse, got %T and %T", noSummary, withSummary)

	assert.Equal(t, a.Direction, b.Direction,
		"correlate direction must match across nil-summary (%s) and summary-populated (%s) paths",
		a.Direction, b.Direction)
	assert.Equal(t, a.LagSeconds, b.LagSeconds,
		"correlate lag must match across paths (%.0fs vs %.0fs)", a.LagSeconds, b.LagSeconds)
	assert.InDelta(t, a.Rho, b.Rho, 0.05,
		"correlate rho must be within 0.05 across paths (%.4f vs %.4f)", a.Rho, b.Rho)
}

// --- small assertion helpers ---

// seriesValues is one series' values in the bins that have one, the shape a test
// compares when it cares that two answers differ rather than what either one is.
func seriesValues(s tools.Series) []float64 {
	var out []float64
	for _, v := range s.Values {
		if v != nil {
			out = append(out, *v)
		}
	}
	return out
}

// findSeriesByKey returns the pointer to the series whose Key == key, or nil.
// Returning a pointer lets the caller distinguish "no such series" from an
// empty one.
func findSeriesByKey(series []tools.Series, key string) *tools.Series {
	for i := range series {
		if series[i].Key == key {
			return &series[i]
		}
	}
	return nil
}

// breakdownLines reduces a breakdown to key→lines, so an assertion compares the
// whole attribution against the fixture in one shot instead of picking shares
// out of it one at a time.
func breakdownLines(shares []stats.Share) map[string]int64 {
	lines := make(map[string]int64, len(shares))
	for _, s := range shares {
		lines[s.Key] = s.Lines
	}
	return lines
}

// hasEdge reports whether edges contains a directed edge from → to.
func hasEdge(edges []tools.GraphEdge, from, to string) bool {
	for _, e := range edges {
		if e.From == from && e.To == to {
			return true
		}
	}
	return false
}

// assertFindAnomalies expects no host-7 disk spike, since that pattern logs one line a
// second however saturated the disk, and gateway spikes from GatewayTrafficStep.
func assertFindAnomalies(t *testing.T, h *toolHarness) any {
	t.Helper()

	req := tools.FindAnomaliesRequest{Selector: incidentSelector()}
	env := postTool[tools.FindAnomaliesResponse](t, h, "find_anomalies", req)
	spikes, newPatterns := anomalyPatterns(env.Data)
	// POST /users is routine and its rise from GatewayTrafficStep, under half again, within its variation.
	assert.Equal(t, []string{"slow query <*> ms", "GET /health <*> <*> ms", "job queued depth <*> type export",
		"wrote <*> rows to pageviews", "wrote <*> rows to sessions", "wrote <*> rows to clickstream", "wrote <*> rows to events",
		"job queued depth <*> type index", "job queued depth <*> type reindex"}, spikes)
	// The corpus begins at the window's start, so nothing places a pattern before it.
	assert.Contains(t, newPatterns, "wrote <*> rows to events", "a pattern first seen at the window's start is new")

	// From a minute in, only the slow queries, which begin after it, are new.
	start, end := seedcorpus.IncidentWindow()
	later := postTool[tools.FindAnomaliesResponse](t, h, "find_anomalies", tools.FindAnomaliesRequest{
		Selector: selector.Selector{Time: selector.TimeSpec{From: start.Add(time.Minute).Format(time.RFC3339), To: end.Format(time.RFC3339)}},
	})
	_, newPatterns = anomalyPatterns(later.Data)
	assert.Equal(t, []string{"slow query <*> ms"}, newPatterns)

	// byte-identical: same call + same snapshot must produce the same Data.
	req.Snapshot = env.Meta.Snapshot
	env2 := postTool[tools.FindAnomaliesResponse](t, h, "find_anomalies", req)
	a, err := json.Marshal(env.Data)
	require.NoError(t, err)
	b, err := json.Marshal(env2.Data)
	require.NoError(t, err)
	assert.Equal(t, string(a), string(b),
		"find_anomalies: same call + same snapshot must produce byte-identical Data")

	return env.Data
}

// anomalyPatterns returns the rendered patterns of resp's spikes and new patterns, in rank order.
func anomalyPatterns(resp tools.FindAnomaliesResponse) (spikes, newPatterns []string) {
	var all []tools.Anomaly
	for _, head := range resp.Anomalies {
		all = append(append(all, head), head.Members...)
	}
	for _, a := range all {
		if a.Kind == "spike" {
			spikes = append(spikes, buildResult.Patterns[a.PatternID])
		} else {
			newPatterns = append(newPatterns, buildResult.Patterns[a.PatternID])
		}
	}
	return spikes, newPatterns
}

// assertExplainSpike expects gateway's 5xx rows to point at /users, the one route
// GatewayTrafficStep degrades, and db's reads, which share every mix with its writes,
// to explain nothing.
func assertExplainSpike(t *testing.T, h *toolHarness) any {
	t.Helper()

	errRows := []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}}
	req := tools.ExplainSpikeRequest{Selector: incidentSelector(), Slice: errRows}
	env := postTool[tools.ExplainSpikeResponse](t, h, "explain_spike", req)
	require.NotEmpty(t, env.Data.Shifts)
	for _, s := range env.Data.Shifts {
		assert.NotContains(t, []string{"trace_id", "request_id", "user_id"}, s.Field)
	}

	// Errors are 5xx by construction, so the slice names both rather than have one restate the other.
	gateway := postTool[tools.ExplainSpikeResponse](t, h, "explain_spike", tools.ExplainSpikeRequest{
		Selector: withPredicates(incidentSelector(), selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"}),
		Slice:    append(errRows, selector.Predicate{Field: "status", Op: selector.OpGe, Value: "500"}),
	})
	require.NotEmpty(t, gateway.Data.Shifts)
	assert.Equal(t, "endpoint=/users", gateway.Data.Shifts[0].Field+"="+gateway.Data.Shifts[0].Value)

	reads := postTool[tools.ExplainSpikeResponse](t, h, "explain_spike", tools.ExplainSpikeRequest{
		Selector: withPredicates(incidentSelector(), selector.Predicate{Field: "service", Op: selector.OpEq, Value: "db"}),
		Slice:    []selector.Predicate{{Field: "op", Op: selector.OpEq, Value: "read"}},
	})
	assert.Empty(t, reads.Data.Shifts, "a slice whose mix matches its baseline explains nothing")

	// byte-identical: same call + same snapshot must produce the same Data.
	req.Snapshot = env.Meta.Snapshot
	env2 := postTool[tools.ExplainSpikeResponse](t, h, "explain_spike", req)
	a, err := json.Marshal(env.Data)
	require.NoError(t, err)
	b, err := json.Marshal(env2.Data)
	require.NoError(t, err)
	assert.Equal(t, string(a), string(b),
		"explain_spike: same call + same snapshot must produce byte-identical Data")

	return env.Data
}
