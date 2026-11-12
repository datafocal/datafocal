package tools_test

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/topology"
)

// sgBase is the fixed instant service_graph tests anchor against.
// Epoch-aligned so the bin boundaries are deterministic across runs.
var sgBase = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

// sgStore returns a store whose bins are an hour wide and whose retention outlasts
// any test, so nothing a test observes is evicted before it is read. It declares
// coverage from sgBase, which every test seeds at or after: without the claim the
// tool cannot tell an edge that is absent from an edge it never looked for, and
// falls back to folding footers.
func sgStore(t *testing.T, v catalog.Version, segs ...summary.Segment) summary.Store {
	t.Helper()
	store, err := summary.NewStore(summary.Config{
		BinWidth:  time.Hour,
		Retention: 365 * 24 * time.Hour,
		Topology:  topology.Config{MinEdgeWeight: 0.5},
	}, v, sgBase, segs...)
	require.NoError(t, err)
	return store
}

// sgSchedulerDesc builds a descriptor with pod/host stream labels, as an
// agent-collected Kubernetes scheduler stream carries them.
func sgSchedulerDesc(id, pod, host string, ts time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: "scheduler", Labels: map[string]string{"pod": pod, "host": host}},
		TimeRange: segment.TimeRange{Min: ts, Max: ts.Add(time.Minute)},
	}
}

// sgServiceDesc builds a descriptor for a named service without pod/host labels.
func sgServiceDesc(id, service string, ts time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: service},
		TimeRange: segment.TimeRange{Min: ts, Max: ts.Add(time.Minute)},
	}
}

// sgFooterWithTraceHLL returns a footer whose rows are all service's, its trace_id HLL
// holding the traces named by prefix, so a test can build overlaps large enough to
// clear the sketches' noise floor.
func sgFooterWithTraceHLL(service, prefix string, from, to int) segment.Footer {
	h := sketch.NewHLL()
	for i := from; i < to; i++ {
		h.Add([]byte(prefix + "-" + strconv.Itoa(i)))
	}
	return segment.FooterWithServices(segment.Footer{}, map[string]segment.Footer{
		service: {DistinctCounts: map[string]*sketch.HLL{segment.ColTraceID: h}},
	})
}

// TestServiceGraph pins the graph each source of topology yields, and that the same
// request answers byte-identically twice.
func TestServiceGraph(t *testing.T) {
	type seedFn func(t *testing.T, w storage.Store, cat catalog.Catalog)
	lateBase := sgBase.Add(2 * time.Hour)
	inHour := window(sgBase, sgBase.Add(time.Hour))
	placementEdge := func(from, to string) tools.GraphEdge {
		return tools.GraphEdge{From: from, To: to, Method: "topology", Weight: 1, Confidence: 1}
	}

	tests := []struct {
		name string
		// summarized are stream-labelled descriptors added to a store synced to the
		// corpus; without any, the store is left cold.
		summarized []segment.Descriptor
		seed       seedFn
		// cold answers first with no summary at all and then from a summary built over the corpus.
		cold     bool
		req      tools.ServiceGraphRequest
		want     tools.ServiceGraphResponse
		wantScan bool
	}{
		{
			// A stream that declares its own placement yields pod→host without any
			// segment being read.
			name:       "placement from stream labels",
			summarized: []segment.Descriptor{sgSchedulerDesc("seg-1", "db-3-0", "host-7", sgBase)},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{{ID: "host-7", Kind: "resource"}, {ID: "db-3-0", Kind: "workload"}},
				Edges: []tools.GraphEdge{placementEdge("db-3-0", "host-7")},
			},
		},
		{
			// The summary is a cache, so a nil summary folds the window's footers into the same graph and bills the reads.
			name: "stream labels without a summary match a warm summary",
			seed: func(t *testing.T, w storage.Store, cat catalog.Catalog) {
				writeLabeledSegment(t, w, cat, "scheduler", map[string]string{"pod": "db-3-0", "host": "host-7"},
					messageEvents(sgBase, "scheduling loop ran"))
			},
			cold: true,
			req:  tools.ServiceGraphRequest{Selector: selector.Selector{Time: inHour}},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{{ID: "host-7", Kind: "resource"}, {ID: "db-3-0", Kind: "workload"}, {ID: "scheduler", Kind: "workload", Rate: 1.0 / 3600}},
				Edges: []tools.GraphEdge{placementEdge("db-3-0", "host-7")},
			},
			wantScan: true,
		},
		{
			// The placement is only in log content, so the edge exists only if the
			// tool scans the placement stream, and the scan is billed.
			name: "placement from a placement scan",
			seed: func(t *testing.T, w storage.Store, cat catalog.Catalog) {
				writeSegment(t, w, cat, "scheduler", []segment.Record{
					placementEvent(sgBase, "db-3-0", "host-7"),
					placementEvent(sgBase.Add(time.Second), "analytics-export-0", "host-7"),
				})
			},
			req: tools.ServiceGraphRequest{Selector: selector.Selector{Time: inHour}},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{
					{ID: "host-7", Kind: "resource"},
					{ID: "analytics-export-0", Kind: "workload"},
					{ID: "db-3-0", Kind: "workload"},
					{ID: "scheduler", Kind: "workload", Rate: 2.0 / 3600},
				},
				Edges: []tools.GraphEdge{placementEdge("analytics-export-0", "host-7"), placementEdge("db-3-0", "host-7")},
			},
			wantScan: true,
		},
		{
			// Every placement field is the caller's to name, entity_field included.
			name: "placement scan with a non-default vocabulary",
			seed: func(t *testing.T, w storage.Store, cat catalog.Catalog) {
				writeSegment(t, w, cat, "nomad", []segment.Record{segment.LogRecord(sgBase, "info", "placed alloc-9f2c on client-2", map[string]string{"alloc": "alloc-9f2c", "client": "client-2"})})
			},
			req: tools.ServiceGraphRequest{
				Selector:  selector.Selector{Time: inHour},
				Placement: tools.PlacementRef{Service: "nomad", SubjectField: "alloc", EntityField: "client"},
			},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{{ID: "client-2", Kind: "resource"}, {ID: "alloc-9f2c", Kind: "workload"}, {ID: "nomad", Kind: "workload", Rate: 1.0 / 3600}},
				Edges: []tools.GraphEdge{placementEdge("alloc-9f2c", "client-2")},
			},
			wantScan: true,
		},
		{
			// A service scope reaches the workloads its own stream declares, so
			// gateway-0, which no db stream claims, is excluded.
			name: "service scope expands to declared workloads",
			seed: func(t *testing.T, w storage.Store, cat catalog.Catalog) {
				writeSegment(t, w, cat, "scheduler", []segment.Record{
					placementEvent(sgBase, "db-3-0", "host-7"),
					placementEvent(sgBase.Add(time.Second), "gateway-0", "host-4"),
				})
				writeLabeledSegment(t, w, cat, "db", map[string]string{"pod": "db-3-0"}, messageEvents(sgBase, "query ok"))
			},
			req: tools.ServiceGraphRequest{Selector: selector.Selector{
				Time:  inHour,
				Where: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "db"}},
			}},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{{ID: "host-7", Kind: "resource"}, {ID: "db", Kind: "workload", Rate: 1.0 / 3600}, {ID: "db-3-0", Kind: "workload"}},
				Edges: []tools.GraphEdge{placementEdge("db-3-0", "host-7")},
			},
			wantScan: true,
		},
		{
			name: "edges outside the window are excluded",
			summarized: []segment.Descriptor{
				sgSchedulerDesc("early", "pod-1", "host-1", sgBase),
				sgSchedulerDesc("late", "pod-2", "host-2", lateBase),
			},
			req: tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(lateBase, lateBase.Add(time.Hour))}},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{{ID: "host-2", Kind: "resource"}, {ID: "pod-2", Kind: "workload"}},
				Edges: []tools.GraphEdge{placementEdge("pod-2", "host-2")},
			},
		},
		{
			name: "summarized and scanned placements together",
			summarized: []segment.Descriptor{
				sgSchedulerDesc("seg-1", "db-3-0", "host-7", sgBase),
				sgSchedulerDesc("seg-2", "pod-A", "host-X", sgBase),
			},
			seed: func(t *testing.T, w storage.Store, cat catalog.Catalog) {
				writeSegment(t, w, cat, "scheduler", []segment.Record{
					placementEvent(sgBase, "gateway-0", "host-4"),
					placementEvent(sgBase.Add(time.Second), "jobs-0", "host-6"),
				})
			},
			req: tools.ServiceGraphRequest{Selector: selector.Selector{Time: inHour}},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{
					{ID: "host-4", Kind: "resource"},
					{ID: "host-6", Kind: "resource"},
					{ID: "host-7", Kind: "resource"},
					{ID: "host-X", Kind: "resource"},
					{ID: "db-3-0", Kind: "workload"},
					{ID: "gateway-0", Kind: "workload"},
					{ID: "jobs-0", Kind: "workload"},
					{ID: "pod-A", Kind: "workload"},
					{ID: "scheduler", Kind: "workload", Rate: 2.0 / 3600},
				},
				Edges: []tools.GraphEdge{
					placementEdge("db-3-0", "host-7"),
					placementEdge("gateway-0", "host-4"),
					placementEdge("jobs-0", "host-6"),
					placementEdge("pod-A", "host-X"),
				},
			},
			wantScan: true,
		},
		{
			// A stream's group label outranks the workload's name, and a host without one groups by name.
			name: "group by a stream label",
			seed: func(t *testing.T, w storage.Store, cat catalog.Catalog) {
				for pod, host := range map[string]string{"db-3-0": "host-7", "cache-0": "host-8"} {
					writeLabeledSegment(t, w, cat, "scheduler", map[string]string{"pod": pod, "host": host, "team": "storage"},
						messageEvents(sgBase, "scheduling loop ran"))
				}
			},
			cold: true,
			req:  tools.ServiceGraphRequest{Selector: selector.Selector{Time: inHour}, GroupBy: "team"},
			want: tools.ServiceGraphResponse{
				Nodes: []tools.GraphNode{
					{ID: "group:host", Kind: "group", Members: 2, MemberIDs: []string{"host-7", "host-8"}, Group: "host"},
					{ID: "group:storage", Kind: "group", Rate: 0.0005555555555555556, Members: 3, MemberIDs: []string{"cache-0", "db-3-0", "scheduler"}, Group: "storage"},
				},
				Edges: []tools.GraphEdge{{From: "group:storage", To: "group:host", Method: "topology", Weight: 2, Confidence: 1}},
			},
			wantScan: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, w, cat := newCorpus(t)
			if tt.seed != nil {
				tt.seed(t, w, cat)
			}
			cr, rd := readers(dir)
			var segs []summary.Segment
			for _, d := range tt.summarized {
				segs = append(segs, summary.Segment{Descriptor: d})
			}
			var version catalog.Version
			if len(segs) > 0 {
				version = catalogVersion(t, cr)
			}
			store := sgStore(t, version, segs...)
			if tt.cold {
				store = nil
			}
			req := tt.req

			ctx := context.Background()
			first, err := toolsOver(cr, rd, store).ServiceGraph(ctx, req)
			require.NoError(t, err)
			if tt.cold {
				// Built only after the cold call, so that call's footer reads are not already cached.
				store = wireSummary(t, cr, rd, summary.Config{})
			}
			second, err := toolsOver(cr, rd, store).ServiceGraph(ctx, req)
			require.NoError(t, err)

			assert.Equal(t, tt.want, first.Data)
			assert.Equal(t, first.Data, second.Data, "the response must be identical across calls")
			assert.Equal(t, 1.0, first.Meta.Coverage)
			assert.True(t, first.Meta.Approx)
			assert.Equal(t, tt.wantScan, first.Meta.Cost.GETs > 0, "GETs = %d", first.Meta.Cost.GETs)
		})
	}
}

// TestServiceGraphCoOccurrence pins the edges shared trace_ids yield and that a limit keeps the heaviest of them.
func TestServiceGraphCoOccurrence(t *testing.T) {
	type traces struct {
		prefix   string
		from, to int
	}
	tests := []struct {
		name string
		// traces are the trace_ids svc-a, svc-b and svc-c carry, in that order.
		traces [3]traces
		limit  int
		// wantTotal is the edge count without a limit, wantKept the count under it, and wantHeaviest the first edge, its weight and confidence to 10%.
		wantTotal, wantKept int
		wantHeaviest        tools.GraphEdge
		wantCoverage        float64
	}{
		{
			// Halves of a 2000-trace range overlap in 1000 of their 1500, and svc-c is disjoint from both.
			name:         "only services sharing traces get an edge",
			traces:       [3]traces{{"tr", 0, 1500}, {"tr", 500, 2000}, {"other", 0, 1500}},
			wantTotal:    1,
			wantKept:     1,
			wantHeaviest: tools.GraphEdge{From: "svc-a", To: "svc-b", Method: "co-occurrence", Weight: 1000, Confidence: 1000.0 / 1500},
			wantCoverage: 1,
		},
		{
			// Graded overlaps of a↔b 1500, a↔c 1000 and b↔c 2000 of 2500 leave b↔c as the one edge kept.
			name:         "limit keeps the heaviest",
			traces:       [3]traces{{"tr", 0, 3000}, {"tr", 1500, 4000}, {"tr", 2000, 5000}},
			limit:        1,
			wantTotal:    3,
			wantKept:     1,
			wantHeaviest: tools.GraphEdge{From: "svc-b", To: "svc-c", Method: "co-occurrence", Weight: 2000, Confidence: 2000.0 / 2500},
			wantCoverage: 1.0 / 3.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var segs []summary.Segment
			for i, tr := range tt.traces {
				svc := "svc-" + string(rune('a'+i))
				segs = append(segs, summary.Segment{Descriptor: sgServiceDesc("seg-"+svc, svc, sgBase), Footer: sgFooterWithTraceHLL(svc, tr.prefix, tr.from, tr.to)})
			}
			dir, _, _ := newCorpus(t)
			cr, rd := readers(dir)
			store := sgStore(t, catalogVersion(t, cr), segs...)

			ctx := context.Background()
			sel := selector.Selector{}
			full, err := toolsOver(cr, rd, store).ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: sel})
			require.NoError(t, err)
			require.Len(t, full.Data.Edges, tt.wantTotal, "got %v", full.Data.Edges)
			require.Equal(t, 1.0, full.Meta.Coverage)

			env, err := toolsOver(cr, rd, store).ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: sel, Limit: tt.limit})
			require.NoError(t, err)
			require.Len(t, env.Data.Edges, tt.wantKept)
			assert.Equal(t, full.Data.Edges[:tt.wantKept], env.Data.Edges, "the kept edges must be the heaviest")
			assert.Equal(t, tt.wantCoverage, env.Meta.Coverage)

			heaviest := env.Data.Edges[0]
			assert.InEpsilon(t, tt.wantHeaviest.Weight, heaviest.Weight, 0.1)
			assert.InEpsilon(t, tt.wantHeaviest.Confidence, heaviest.Confidence, 0.1)
			heaviest.Weight, heaviest.Confidence = tt.wantHeaviest.Weight, tt.wantHeaviest.Confidence
			assert.Equal(t, tt.wantHeaviest, heaviest)
		})
	}
}

// TestServiceGraphNodeMetrics pins that a workload node's rate, error ratio and p95 are
// aggregate's for that service and window, and that a summary serves them without a read.
func TestServiceGraphNodeMetrics(t *testing.T) {
	dir, w, cat := newCorpus(t)
	// svc-b's errors are a segment of their own, so its level settles the error rule unread, and
	// the services share no trace, whose values only a read lists.
	for _, seg := range []struct {
		service, level string
		count          int
	}{{"svc-a", "info", 20}, {"svc-b", "info", 15}, {"svc-b", "error", 5}} {
		events := make([]segment.Record, seg.count)
		for i := range events {
			events[i] = logEvent(sgBase.Add(time.Duration(i)*time.Second), seg.level, "trace_id",
				fmt.Sprintf("%s-trace-%d", seg.service, i), fmt.Sprintf("query %s %d ms", seg.level, i+1))
		}
		writeSegment(t, w, cat, seg.service, events)
	}
	cr, rd := readers(dir)
	store := wireSummary(t, cr, rd, summary.Config{BinWidth: 15 * time.Second, SketchColumns: summaryColumns})
	sel := selector.Selector{Time: window(sgBase, sgBase.Add(time.Hour))}

	ctx := context.Background()
	env, err := toolsOver(cr, rd, store).ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: sel})
	require.NoError(t, err)
	assert.Equal(t, query.Cost{}, env.Meta.Cost, "a summary-served window read data")

	aggregate := func(measure string, numerator ...selector.Predicate) map[string]float64 {
		res, err := toolsOver(cr, rd, nil).Aggregate(ctx, tools.AggregateRequest{
			Selector: sel, Measure: measure, GroupBy: "service", Numerator: numerator,
		})
		require.NoError(t, err)
		byService := map[string]float64{}
		for _, s := range res.Data.Series {
			byService[s.Key] = *s.Values[0]
		}
		return byService
	}
	rates, p95s := aggregate("rate"), aggregate("p95:duration_ms")
	errorRatios := aggregate("ratio", selector.Predicate{Field: "level", Op: selector.OpEq, Value: "error"})

	require.Len(t, env.Data.Nodes, 2)
	for _, n := range env.Data.Nodes {
		assert.Equal(t, tools.GraphNode{
			ID: n.ID, Kind: "workload", Rate: rates[n.ID], ErrorRatio: errorRatios[n.ID], P95Ms: n.P95Ms,
		}, n)
		assert.InEpsilon(t, p95s[n.ID], n.P95Ms, 0.05, "%s p95", n.ID)
	}
	assert.InEpsilon(t, 0.25, env.Data.Nodes[1].ErrorRatio, 1e-9, "svc-b logged 5 errors in 20")

	grouped, err := toolsOver(cr, rd, store).ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: sel, GroupBy: "auto"})
	require.NoError(t, err)
	require.Len(t, grouped.Data.Nodes, 1)
	assert.InEpsilon(t, rates["svc-a"]+rates["svc-b"], grouped.Data.Nodes[0].Rate, 1e-9)
	assert.InEpsilon(t, 0.125, grouped.Data.Nodes[0].ErrorRatio, 1e-9, "the group logged 5 errors in 40")
}

// TestServiceGraphCorrelation: two services whose lines spike a bin apart are joined
// leader to follower beside the co-occurrence edge their shared traces already give
// them, and over segments sealed inside one bin the summary serves the counts without a
// data GET.
func TestServiceGraphCorrelation(t *testing.T) {
	dir, w, cat := newCorpus(t)
	const bin = 15 * time.Second
	for service, counts := range map[string][]int{"svc-a": {1, 1, 6, 1, 1, 1, 1, 1}, "svc-b": {1, 6, 1, 1, 1, 1, 1, 1}} {
		for b, n := range counts {
			events := make([]segment.Record, n)
			for i := range events {
				events[i] = logEvent(sgBase.Add(time.Duration(b)*bin+time.Duration(i)*time.Second), "info",
					"trace_id", fmt.Sprintf("trace-%d", i), service+" handled request")
			}
			writeSegment(t, w, cat, service, events)
		}
	}
	cr, rd := uncachedReaders(dir)
	store := wireSummary(t, cr, rd, summary.Config{BinWidth: bin})
	req := tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(sgBase, sgBase.Add(8*bin))}, Correlation: true}

	env, err := toolsOver(cr, rd, store).ServiceGraph(context.Background(), req)
	require.NoError(t, err)
	uncorrelated, err := toolsOver(cr, rd, store).ServiceGraph(context.Background(), tools.ServiceGraphRequest{Selector: req.Selector})
	require.NoError(t, err)
	assert.Equal(t, uncorrelated.Meta.Cost, env.Meta.Cost, "a summary-served window read data for the counts")
	require.Len(t, env.Data.Edges, 2)
	assert.Equal(t, "co-occurrence", env.Data.Edges[0].Method)
	edge := env.Data.Edges[1]
	assert.Equal(t, tools.GraphEdge{
		From: "svc-b", To: "svc-a", Method: "correlation", Weight: edge.Rho, Confidence: edge.Rho,
		Rho: edge.Rho, LagSeconds: bin.Seconds(),
	}, edge, "svc-b spikes first, so it leads")
	assert.GreaterOrEqual(t, edge.Rho, 0.5)
	require.Len(t, env.Data.Nodes, 2)
	for i, id := range []string{"svc-a", "svc-b"} {
		assert.Equal(t, id, env.Data.Nodes[i].ID)
		assert.Equal(t, "workload", env.Data.Nodes[i].Kind)
	}
}

// TestServiceGraphCorrelationOverTheSeededIncident: the services the incident moves are
// linked by correlation, agreeing with rca_walk's hops on lag, and the same request
// answers byte-identically twice.
func TestServiceGraphCorrelationOverTheSeededIncident(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	built, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioAll)
	require.NoError(t, err)
	cr, rd := readers(dir)
	tl := toolsOver(cr, rd, wireSummary(t, cr, rd, summary.Config{}))
	from, to := seedcorpus.IncidentWindow()
	sel := selector.Selector{Time: window(from, to)}

	req := tools.ServiceGraphRequest{Selector: sel, Limit: 2000, Correlation: true}
	env, err := tl.ServiceGraph(ctx, req)
	require.NoError(t, err)
	correlated := map[[2]string]tools.GraphEdge{}
	linked := map[string]bool{}
	for _, e := range env.Data.Edges {
		if e.Method != "correlation" {
			continue
		}
		assert.GreaterOrEqual(t, e.LagSeconds, 0.0, "%s→%s: an edge runs from the leader", e.From, e.To)
		correlated[[2]string{min(e.From, e.To), max(e.From, e.To)}] = e
		linked[e.From], linked[e.To] = true, true
	}
	// host is not among them: it logs one disk line a second however saturated it is,
	// so its counts are flat and the ramp is only in the values.
	for _, service := range []string{"db", "jobs", "gateway"} {
		assert.Truef(t, linked[service], "%s must be linked by correlation", service)
	}

	symptom := ""
	for id, tmpl := range built.Patterns {
		if strings.HasPrefix(tmpl, "slow query ") {
			symptom = id
		}
	}
	walk, err := tl.RCAWalk(ctx, tools.RCAWalkRequest{
		Selector: sel, Symptom: tools.SeriesRef{PatternID: symptom},
		Entity: tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost}, MinRho: 0.5,
	})
	require.NoError(t, err)
	require.NotEmpty(t, walk.Data.Chain)
	for _, hop := range walk.Data.Chain {
		e, ok := correlated[[2]string{min(hop.From, hop.To), max(hop.From, hop.To)}]
		if assert.Truef(t, ok, "rca_walk hop %s→%s has no correlation edge", hop.From, hop.To) {
			assert.Equal(t, math.Abs(hop.LagSeconds), e.LagSeconds, "%s→%s lag", hop.From, hop.To)
		}
	}

	again, err := tl.ServiceGraph(ctx, req)
	require.NoError(t, err)
	a, err := json.Marshal(env.Data)
	require.NoError(t, err)
	b, err := json.Marshal(again.Data)
	require.NoError(t, err)
	assert.Equal(t, string(a), string(b))
}

// TestServiceGraphGroupBy: 20,000 workloads over 300 name prefixes group into one node per
// prefix and one for the hosts, bundling every placement at a fixed allocation budget per
// workload, and past max_nodes the lightest groups fold into other without losing weight.
func TestServiceGraphGroupBy(t *testing.T) {
	const pods, prefixes = 20000, 300
	segs := make([]summary.Segment, pods)
	for i := range segs {
		pod := fmt.Sprintf("svc%d-%d", i%prefixes, i)
		segs[i] = summary.Segment{Descriptor: sgSchedulerDesc("seg-"+pod, pod, "host-"+strconv.Itoa(i%10), sgBase)}
	}
	dir, _, _ := newCorpus(t)
	cr, rd := readers(dir)
	store, err := summary.NewStore(summary.Config{
		BinWidth: time.Hour, Retention: 365 * 24 * time.Hour, Topology: topology.Config{MaxNodes: 2 * pods},
	}, catalogVersion(t, cr), sgBase, segs...)
	require.NoError(t, err)
	tl, ctx := toolsOver(cr, rd, store), context.Background()
	tests := []struct {
		name       string
		maxNodes   int
		wantNodes  int
		wantMerged int
	}{
		{name: "under the default cap", wantNodes: prefixes + 1},
		// Each prefix group weighs the same, so the 98 kept beside the hosts are the first by ID.
		{name: "past max_nodes", maxNodes: 100, wantNodes: 100, wantMerged: prefixes + 1 - 99},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var env query.Envelope[tools.ServiceGraphResponse]
			allocs := testing.AllocsPerRun(1, func() {
				var err error
				env, err = tl.ServiceGraph(ctx, tools.ServiceGraphRequest{GroupBy: "auto", MaxNodes: tt.maxNodes, Limit: 2000})
				require.NoError(t, err)
			})
			assert.Less(t, allocs/pods, 4.0, "%.0f allocs over %d workloads", allocs, pods)
			assert.Len(t, env.Data.Nodes, tt.wantNodes)
			assert.Equal(t, tt.wantMerged, env.Data.Merged)
			members, ids, weight := 0, 0, 0.0
			for _, n := range env.Data.Nodes {
				members += n.Members
				ids += len(n.MemberIDs)
			}
			for _, e := range env.Data.Edges {
				weight += e.Weight
			}
			assert.Equal(t, pods+10, members, "every workload and host is in one node")
			assert.Equal(t, members, ids, "a node lists every member folded into it")
			assert.Equal(t, float64(pods), weight, "bundled weights sum to the ungrouped total")
			assert.Equal(t, 1.0, env.Meta.Coverage)
		})
	}
}

// TestServiceGraphFocusOverTheSeededIncident: two hops from db-3-0 reach its host and the
// host's other tenant, every other node folds into a stub for its group but a lone scheduler,
// and gateway calls checkout.
func TestServiceGraphFocusOverTheSeededIncident(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	_, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioAll)
	require.NoError(t, err)
	cr, rd := readers(dir)
	tl := toolsOver(cr, rd, wireSummary(t, cr, rd, summary.Config{}))
	from, to := seedcorpus.IncidentWindow()

	req := tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(from, to)}, Focus: "db-3-0", Hops: 2}
	env, err := tl.ServiceGraph(ctx, req)
	require.NoError(t, err)
	stub := func(group string, rate float64, members ...string) tools.GraphNode {
		return tools.GraphNode{ID: "group:" + group, Kind: "group", Rate: rate, Members: len(members), MemberIDs: members, Group: group}
	}
	placement := func(from, to string) tools.GraphEdge {
		return tools.GraphEdge{From: from, To: to, Method: "topology", Weight: 1, Confidence: 1}
	}
	assert.Equal(t, tools.ServiceGraphResponse{
		Nodes: []tools.GraphNode{
			stub("analytics", 14.236666666666666, "analytics-export"),
			{ID: "group:checkout", Kind: "group", Rate: 25, Members: 2, MemberIDs: []string{"checkout", "checkout-0"}, Group: "checkout", ErrorRatio: 0.04019999999999999},
			stub("db", 32, "db", "db-1-0"),
			{ID: "group:gateway", Kind: "group", Rate: 42, Members: 2, MemberIDs: []string{"gateway", "gateway-0"}, Group: "gateway", ErrorRatio: 0.02634920634920635},
			stub("host", 2.4, "host", "host-2", "host-4", "host-5", "host-6", "host-8"), stub("jobs", 16.785, "jobs", "jobs-0"),
			stub("recommendations", 0, "recommendations", "recommendations-0"),
			{ID: "host-7", Kind: "resource"},
			{ID: "analytics-export-0", Kind: "workload"},
			{ID: "db-3-0", Kind: "workload"},
			{ID: "scheduler", Kind: "workload", Rate: 0.013333333333333334},
		},
		Edges: []tools.GraphEdge{
			{From: "group:gateway", To: "group:recommendations", Method: "co-occurrence", Weight: 2983, Confidence: 1},
			{From: "group:checkout", To: "group:recommendations", Method: "co-occurrence", Weight: 2961, Confidence: 1},
			{From: "group:gateway", To: "group:checkout", Method: "call", Weight: 388, Confidence: 1, Calls: 388, P50Ms: 14, P95Ms: 58281},
			{From: "group:checkout", To: "group:gateway", Method: "call", Weight: 89, Confidence: 1, Calls: 89, P50Ms: 736, P95Ms: 46085},
			placement("analytics-export-0", "host-7"),
			placement("db-3-0", "host-7"),
			placement("group:checkout", "group:host"),
			placement("group:db", "group:host"),
			placement("group:gateway", "group:host"),
			placement("group:jobs", "group:host"),
			placement("group:recommendations", "group:host"),
		},
	}, env.Data)

	again, err := tl.ServiceGraph(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, env.Data, again.Data, "the response must be identical across calls")

	_, err = tl.ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: req.Selector, Focus: "db-9-0"})
	assert.ErrorContains(t, err, `focus "db-9-0" is not a node`)
}

func TestServiceGraphCorrelationColumns(t *testing.T) {
	for _, column := range []string{"request_id", "request" + "Id"} {
		t.Run(column, func(t *testing.T) {
			dir, w, cat := newCorpus(t)
			for _, service := range []string{"frontend", "api", "alone"} {
				events := make([]segment.Record, 1000)
				field, prefix := "request_id", "request"
				if service == "api" {
					field = column
				}
				if service == "alone" {
					prefix = "unrelated"
				}
				for i := range events {
					events[i] = segment.LogRecord(sgBase.Add(time.Duration(i)*time.Millisecond), "info", "request", map[string]string{field: fmt.Sprintf("%s-%d", prefix, i/2)})
				}
				writeSegment(t, w, cat, service, events)
			}
			cr, rd := readers(dir)
			req := tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(sgBase, sgBase.Add(time.Hour))}}
			ctx := context.Background()
			cold, err := toolsOver(cr, rd, nil).ServiceGraph(ctx, req)
			require.NoError(t, err)
			require.Len(t, cold.Data.Edges, 1)
			assert.Equal(t, "api", cold.Data.Edges[0].From)
			assert.Equal(t, "frontend", cold.Data.Edges[0].To)
			assert.InDelta(t, 500, cold.Data.Edges[0].Weight, 25)
			assert.Equal(t, tools.SharedValue{Field: column, Value: "request-0", FromLines: 2, ToLines: 2}, cold.Data.Edges[0].Shared[0], "named by api, the edge's From")
			assert.Len(t, cold.Data.Edges[0].Shared, 3)
			require.Len(t, cold.Data.Nodes, 3)
			store := wireSummary(t, cr, rd, summary.Config{})
			warm, err := toolsOver(cr, rd, store).ServiceGraph(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, cold.Data, warm.Data)
			assert.LessOrEqual(t, warm.Meta.Cost.GETs, cold.Meta.Cost.GETs)
			req.Focus = "alone"
			focused, err := toolsOver(cr, rd, store).ServiceGraph(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, tools.ServiceGraphResponse{Nodes: cold.Data.Nodes[:1], Edges: []tools.GraphEdge{}}, focused.Data)
			req.Focus = ""
			req.Selector.Time = window(sgBase.Add(10*time.Minute), sgBase.Add(11*time.Minute))
			short, err := toolsOver(cr, rd, nil).ServiceGraph(ctx, req)
			require.NoError(t, err)
			assert.Equal(t, cold.Data.Edges, short.Data.Edges)
			req.Selector.Time = window(sgBase.Add(time.Hour), sgBase.Add(2*time.Hour))
			outside, err := toolsOver(cr, rd, nil).ServiceGraph(ctx, req)
			require.NoError(t, err)
			assert.Empty(t, outside.Data.Edges)
		})
	}
}

func TestServiceGraphFactoryTasks(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	corpus, err := seedcorpus.Build(ctx, dir, seedcorpus.ScenarioFactory)
	require.NoError(t, err)
	cr, rd := readers(dir)
	_, to := seedcorpus.FactoryWindow()
	from := to.Add(-24 * time.Hour)
	tl := toolsOver(cr, rd, wireSummary(t, cr, rd, summary.Config{}))
	day, err := tl.ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(from, to)}})
	require.NoError(t, err)
	ids := make([]string, len(day.Data.Nodes))
	for i, node := range day.Data.Nodes {
		ids[i] = node.ID
	}
	assert.Equal(t, []string{"etcd", "kindnet", "kube-scheduler", "orchestrator", "postgres", "querier", "sandbox-s", "workspace"}, ids)
	// The workspace's transcripts spell the orchestrator's session column in camel case.
	hours, sessions, transcriptSession := map[string]time.Time{}, map[[2]string]int64{}, "session"+"Id"
	for _, d := range corpus.Descriptors {
		if d.TimeRange.Max.Before(from) {
			continue
		}
		records, _, _, err := rd.Records(ctx, d.Object)
		require.NoError(t, err)
		for _, r := range records {
			if task := r.Text("task"); task != "" {
				hours[task] = r.Timestamp.Truncate(time.Hour)
			}
			if r.Text(segment.ColService) == "workspace" {
				sessions[[2]string{"session", r.Text("session")}]++
				sessions[[2]string{transcriptSession, r.Text(transcriptSession)}]++
			}
		}
	}
	coldCat, coldRd := readers(dir)
	cold, err := toolsOver(coldCat, coldRd, nil).ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(from, to)}})
	require.NoError(t, err)
	assert.Equal(t, day.Data, cold.Data)
	assert.Equal(t, 4282, cold.Meta.Cost.GETs, "shared values cost no GET past the footers and the sampled traces")
	var session tools.GraphEdge
	for _, e := range day.Data.Edges {
		if e.Method == "co-occurrence" {
			assert.NotEmpty(t, e.Shared, "%s-%s", e.From, e.To)
		}
		if e.From == "orchestrator" && e.To == "workspace" {
			session = e
		}
	}
	require.NotEmpty(t, session.Shared)
	v := session.Shared[0]
	assert.Equal(t, "session", v.Field, "named by the orchestrator, the edge's From")
	assert.Equal(t, sessions[[2]string{transcriptSession, v.Value}], v.ToLines, "the workspace's name for it")
	assert.Zero(t, sessions[[2]string{"session", v.Value}])
	for task, truth := range corpus.Truth.Tasks {
		hour, ok := hours[task]
		require.True(t, ok, task)
		env, err := tl.ServiceGraph(ctx, tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(hour, hour.Add(time.Hour))}})
		require.NoError(t, err)
		i := slices.IndexFunc(env.Data.Edges, func(e tools.GraphEdge) bool { return e.From == truth.Services[0] && e.To == truth.Services[1] })
		require.GreaterOrEqual(t, i, 0, task)
		e := env.Data.Edges[i]
		if e.Method == "call" { // the hour's lines nest in time
			continue
		}
		assert.Equal(t, [3]any{"co-occurrence", 1.0, 1.0}, [3]any{e.Method, e.Weight, e.Confidence}, task)
		require.Len(t, e.Shared, 1, task)
		assert.Equal(t, [3]any{"task", task, int64(2)}, [3]any{e.Shared[0].Field, e.Shared[0].Value, e.Shared[0].ToLines}, task)
		// A segment holding one of the orchestrator's two lines of a task does not count it, so its sketch credits the hour one line.
		assert.Contains(t, []int64{1, 2}, e.Shared[0].FromLines, task)
	}
}

// TestServiceGraphCallEdgesFromTiming: 300 traces of a 12-service call tree, its lines sharing
// only the id req, give the tree's 11 call edges, read from how the lines nest in time or from
// a rid/caller pair, with the sampled callee spans' errors and latency, in under 8 KB.
func TestServiceGraphCallEdgesFromTiming(t *testing.T) {
	type span struct {
		service, parent string
		start, end      int
	}
	tree := []span{{"frontend", "", 0, 200}, {"api-gateway", "frontend", 1, 199}, {"auth", "api-gateway", 2, 10},
		{"cart", "api-gateway", 11, 40}, {"redis", "cart", 12, 20}, {"checkout", "api-gateway", 41, 190},
		{"inventory", "checkout", 42, 70}, {"postgres", "inventory", 43, 60}, {"payment", "checkout", 71, 111},
		{"fraud", "payment", 72, 80}, {"stripe-proxy", "payment", 81, 90}, {"email", "checkout", 170, 185}}
	const traces = 300
	var want []tools.GraphEdge
	for _, s := range tree[1:] {
		dur := int64(s.end - s.start)
		want = append(want, tools.GraphEdge{From: s.parent, To: s.service, Method: "call", Weight: traces, Confidence: 1, Calls: traces, P50Ms: dur, P95Ms: dur})
	}
	// The first 100 traces are sampled: payment's last 40-89 ms and err one in 10, stripe-proxy's 9-28 ms and one in 7.
	want[7].ErrorRatio, want[7].P50Ms, want[7].P95Ms = 0.1, 64, 87
	want[9].ErrorRatio, want[9].P50Ms, want[9].P95Ms = 0.15, 18, 27
	slices.SortFunc(want, func(a, b tools.GraphEdge) int {
		return cmp.Or(strings.Compare(a.From, b.From), strings.Compare(a.To, b.To))
	})
	for _, parented := range []bool{false, true} {
		t.Run(fmt.Sprintf("parented=%v", parented), func(t *testing.T) {
			dir, w, cat := newCorpus(t)
			for _, s := range tree {
				var events []segment.Record
				for i := range traces {
					at := func(ms int) time.Time {
						return sgBase.Add(time.Duration(i)*time.Second + time.Duration(ms)*time.Millisecond)
					}
					end := s.end + map[string]int{"payment": i % 50, "stripe-proxy": i % 20}[s.service]
					level := map[bool]string{false: "info", true: "error"}[s.service == "payment" && i%10 == 0 || s.service == "stripe-proxy" && i%7 == 0]
					ids := map[string]string{"req": fmt.Sprintf("r-%d", i)}
					if parented {
						ids["rid"] = fmt.Sprintf("%s-%d", s.service, i)
						if s.parent != "" {
							ids["caller"] = fmt.Sprintf("%s-%d", s.parent, i)
						}
						events = append(events, segment.LogRecord(at(s.start), "info", "called", ids))
					}
					attrs := maps.Clone(ids)
					attrs["duration_ms"] = strconv.Itoa(end - s.start)
					events = append(events, segment.LogRecord(at(end), level, "handled", attrs))
				}
				writeSegment(t, w, cat, s.service, events)
			}
			cr, rd := readers(dir)
			tl, ctx := toolsOver(cr, rd, nil), context.Background()
			req := tools.ServiceGraphRequest{Selector: selector.Selector{Time: window(sgBase, sgBase.Add(time.Hour))}}
			env, err := tl.ServiceGraph(ctx, req)
			require.NoError(t, err)
			got := env.Data.Edges
			for i := range got {
				assert.InDelta(t, traces, got[i].Calls, traces/10, "%s → %s", got[i].From, got[i].To)
				got[i].Calls, got[i].Weight = traces, traces
			}
			slices.SortFunc(got, func(a, b tools.GraphEdge) int {
				return cmp.Or(strings.Compare(a.From, b.From), strings.Compare(a.To, b.To))
			})
			assert.Equal(t, want, got)
			body, err := json.Marshal(env.Data)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(body), 8192)
			if parented {
				return
			}
			req.GroupBy = "auto"
			grouped, err := tl.ServiceGraph(ctx, req)
			require.NoError(t, err)
			assert.Len(t, grouped.Data.Nodes, len(tree), "a lone api-gateway keeps its name")
			writeSegment(t, w, cat, "api-admin", []segment.Record{segment.LogRecord(sgBase, "info", "handled", nil)})
			grouped, err = tl.ServiceGraph(ctx, req)
			require.NoError(t, err)
			assert.Contains(t, grouped.Data.Nodes, tools.GraphNode{ID: "group:api", Kind: "group", Group: "api", Rate: 301.0 / 3600,
				Members: 2, MemberIDs: []string{"api-admin", "api-gateway"}})
		})
	}
}
