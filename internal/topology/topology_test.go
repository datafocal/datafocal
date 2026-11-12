package topology

import (
	"cmp"
	"context"
	"fmt"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
)

// topoBase is the fixed instant topology tests anchor against. Bins are
// epoch-aligned, so a wall-clock base would land in a different bin each run.
var topoBase = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

// topoBin is the bin every descriptor built at topoBase falls into under
// topoConfig's one-hour width, which is also the LastBin a view reports.
var topoBin = topoBase.UnixMilli()

// topoConfig is a wide-bin, long-retention config that does not accidentally
// evict anything during a test.
func topoConfig() Config {
	return Config{
		MinEdgeWeight: 0.5,
	}
}

func schedulerDesc(id, pod, host string) segment.Descriptor {
	return schedulerDescAt(id, pod, host, topoBase)
}

func schedulerDescAt(id, pod, host string, ts time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: "scheduler", Labels: map[string]string{"pod": pod, "host": host}},
		TimeRange: segment.TimeRange{Min: ts, Max: ts.Add(time.Minute)},
	}
}

func serviceDesc(id, service string, ts time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: service},
		TimeRange: segment.TimeRange{Min: ts, Max: ts.Add(time.Minute)},
	}
}

// footerWithHLL is a footer whose rows are all service's, carrying values as trace ids.
func footerWithHLL(service string, values ...string) segment.Footer {
	h := sketch.NewHLL()
	for _, v := range values {
		h.Add([]byte(v))
	}
	return segment.FooterWithServices(segment.Footer{}, map[string]segment.Footer{
		service: {DistinctCounts: map[string]*sketch.HLL{segment.ColTraceID: h}},
	})
}

// footerWithTraceRange is footerWithHLL at a scale where the sketch estimates
// rather than counts, which is where the noise floor matters.
func footerWithTraceRange(service, prefix string, from, to int) segment.Footer {
	values := make([]string, 0, to-from)
	for i := from; i < to; i++ {
		values = append(values, prefix+"-"+strconv.Itoa(i))
	}
	return footerWithHLL(service, values...)
}

// TestTopologyPlacementFromStreamLabels is the acceptance-criterion oracle: a
// descriptor whose stream declares a workload and the resource it sits on produces
// one workload→resource edge with Method=EdgePlacement, and nothing else.
//
// The cases are the same graph over four platforms' vocabulary. They exist as a
// table because the only thing that varies is which label names the pair —
// evidence that the artifact is about contention and not about Kubernetes.
func TestTopologyPlacementFromStreamLabels(t *testing.T) {
	tests := []struct {
		name          string
		workloadLabel string
		resourceLabel string
		workload      string
		resource      string
	}{
		{name: "kubernetes defaults", workload: "db-3-0", resource: "host-7"},
		{name: "nomad", workloadLabel: "alloc", resourceLabel: "client", workload: "alloc-9f2c", resource: "client-2"},
		{name: "warehouse", workloadLabel: "model", resourceLabel: "warehouse", workload: "fct_orders", resource: "wh_transform"},
		{name: "kafka", workloadLabel: "consumer_group", resourceLabel: "broker", workload: "cg-ingest", resource: "broker-3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := topoConfig()
			cfg.WorkloadLabel, cfg.ResourceLabel = tt.workloadLabel, tt.resourceLabel
			g := New(cfg)

			desc := segment.Descriptor{
				ID: "seg-1",
				Stream: segment.Stream{Service: "scheduler", Labels: map[string]string{
					cmp.Or(tt.workloadLabel, DefaultWorkloadLabel): tt.workload,
					cmp.Or(tt.resourceLabel, DefaultResourceLabel): tt.resource,
				}},
				TimeRange: segment.TimeRange{Min: topoBase, Max: topoBase.Add(time.Minute)},
			}
			require.NoError(t, g.Add(desc, segment.Footer{}))

			want := View{
				Nodes: []Node{
					{ID: tt.resource, Kind: NodeResource, LastBin: topoBin},
					{ID: tt.workload, Kind: NodeWorkload, LastBin: topoBin},
				},
				Edges: []Edge{
					{From: tt.workload, To: tt.resource, Method: EdgePlacement, Weight: 1, LastBin: topoBin, Confidence: 1},
				},
			}
			assert.Equal(t, want, g.Edges(Filter{}))
		})
	}
}

// TestTopologySharedTracesAcrossSegments is the regression the accumulating
// sketch exists for: a service seals many segments in a bin, and an overlap with
// traces from any of them has to be visible — not just the last one's.
func TestTopologySharedTracesAcrossSegments(t *testing.T) {
	g := New(topoConfig())

	// svc-a's four traces arrive in two segments; svc-b shares the first two.
	require.NoError(t, g.Add(serviceDesc("a-1", "svc-a", topoBase), footerWithHLL("svc-a", "tr-1", "tr-2")))
	require.NoError(t, g.Add(serviceDesc("a-2", "svc-a", topoBase), footerWithHLL("svc-a", "tr-3", "tr-4")))
	require.NoError(t, g.Add(serviceDesc("b-1", "svc-b", topoBase), footerWithHLL("svc-b", "tr-1", "tr-2")))

	want := View{
		Nodes: []Node{
			{ID: "svc-a", Kind: NodeWorkload, LastBin: topoBin},
			{ID: "svc-b", Kind: NodeWorkload, LastBin: topoBin},
		},
		Edges: []Edge{
			{From: "svc-a", To: "svc-b", Method: EdgeCoOccurrence, Weight: 2, LastBin: topoBin, Confidence: 1, Fields: [][2]string{{"trace_id", "trace_id"}}},
		},
	}
	assert.Equal(t, want, g.Edges(Filter{}))
}

// TestTopologyObserveOrderIndependent pins the contract that makes the artifact a
// function of the corpus: the same segments in any order produce the same graph.
// A weight accumulated per arrival instead of recomputed per bin fails this.
func TestTopologyObserveOrderIndependent(t *testing.T) {
	type observation struct {
		desc   segment.Descriptor
		footer segment.Footer
	}
	observations := []observation{
		{serviceDesc("a-1", "svc-a", topoBase), footerWithHLL("svc-a", "tr-1", "tr-2", "tr-3")},
		{serviceDesc("a-2", "svc-a", topoBase), footerWithHLL("svc-a", "tr-4")},
		{serviceDesc("b-1", "svc-b", topoBase), footerWithHLL("svc-b", "tr-2", "tr-3", "tr-9")},
		{schedulerDesc("s-1", "db-3-0", "host-7"), segment.Footer{}},
	}

	forward := New(topoConfig())
	for _, o := range observations {
		require.NoError(t, forward.Add(o.desc, o.footer))
	}
	reverse := New(topoConfig())
	for i := len(observations) - 1; i >= 0; i-- {
		require.NoError(t, reverse.Add(observations[i].desc, observations[i].footer))
	}

	assert.Equal(t, forward.Edges(Filter{}), reverse.Edges(Filter{}))
}

// TestTopologyNonUnitsDrawNoEdge: a column whose values name no unit of work draws no
// co-occurrence, though one that does, sharing as many values, does: one value every service
// carries labels the deployment, and values every segment repeats are names. An edge drawn
// from a column before it read as a label is recomputed without it.
func TestTopologyNonUnitsDrawNoEdge(t *testing.T) {
	footer := func(service string, columns map[string][]string) segment.Footer {
		counts := map[string]*sketch.HLL{}
		for column, values := range columns {
			counts[column] = sketch.NewHLL()
			for _, v := range values {
				counts[column].Add([]byte(v))
			}
		}
		return segment.FooterWithServices(segment.Footer{}, map[string]segment.Footer{service: {DistinctCounts: counts}})
	}
	shared := func(fields ...[2]string) []Edge {
		return []Edge{{From: "svc-a", To: "svc-b", Method: EdgeCoOccurrence, Weight: 2, LastBin: topoBin, Confidence: 1, Fields: fields}}
	}
	steps := []string{"lookup", "serialize"}
	for _, tc := range []struct {
		name string
		// segments is each service's segments' values by column, added in order, with the
		// edges between svc-a and svc-b read after the first n segments.
		segments        []map[string]map[string][]string
		early           int
		wantEarly, want []Edge
	}{
		{
			name: "one value three services carry is a label",
			segments: []map[string]map[string][]string{
				{"svc-a": {"run": {"run-1"}, segment.ColTraceID: {"tr-1", "tr-2"}}},
				{"svc-b": {"run": {"run-1"}, segment.ColTraceID: {"tr-1", "tr-2"}}},
				{"svc-c": {"run": {"run-1"}, segment.ColTraceID: {"tr-7"}}},
			},
			early:     2,
			wantEarly: []Edge{{From: "svc-a", To: "svc-b", Method: EdgeCoOccurrence, Weight: 2, LastBin: topoBin, Confidence: 1, Fields: [][2]string{{"run", "run"}, {segment.ColTraceID, segment.ColTraceID}}}},
			want:      shared([2]string{segment.ColTraceID, segment.ColTraceID}),
		},
		{
			name: "names every segment repeats are no unit of work, fresh values are",
			segments: []map[string]map[string][]string{
				{"svc-a": {"step": steps, "req": {"r-1"}}}, {"svc-a": {"step": steps, "req": {"r-2"}}}, {"svc-a": {"step": steps}},
				{"svc-b": {"step": steps, "req": {"r-1"}}}, {"svc-b": {"step": steps, "req": {"r-2"}}}, {"svc-b": {"step": steps}},
			},
			want: shared([2]string{"req", "req"}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := New(topoConfig())
			for i, bySvc := range tc.segments {
				if i == tc.early && tc.early > 0 {
					assert.Equal(t, tc.wantEarly, g.Edges(Filter{}).Edges, "early")
				}
				for service, columns := range bySvc {
					require.NoError(t, g.Add(serviceDesc(strconv.Itoa(i), service, topoBase), footer(service, columns)))
				}
			}
			var got []Edge
			for _, e := range g.Edges(Filter{}).Edges {
				if e.From == "svc-a" && e.To == "svc-b" {
					got = append(got, e)
				}
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestTopologyReObserveIsNoOp pins idempotence at the graph, not just at the
// Store's descriptor-ID guard: a retried seal must not double an edge's weight.
func TestTopologyReObserveIsNoOp(t *testing.T) {
	g := New(topoConfig())

	desc, footer := serviceDesc("a-1", "svc-a", topoBase), footerWithHLL("svc-a", "tr-1", "tr-2")
	peer := serviceDesc("b-1", "svc-b", topoBase)
	require.NoError(t, g.Add(desc, footer))
	require.NoError(t, g.Add(peer, footerWithHLL("svc-b", "tr-1", "tr-2")))
	require.NoError(t, g.Add(schedulerDesc("s-1", "db-3-0", "host-7"), segment.Footer{}))
	once := g.Edges(Filter{})

	require.NoError(t, g.Add(desc, footer))
	require.NoError(t, g.Add(schedulerDesc("s-1", "db-3-0", "host-7"), segment.Footer{}))

	assert.Equal(t, once, g.Edges(Filter{}))
}

// TestTopologyDisjointServicesAtScale is the noise-floor test. Inclusion-exclusion
// over two sketches returns a positive overlap for services that share nothing —
// tens of traces at ten thousand distinct each — and an edge drawn from that
// number is an invented dependency.
func TestTopologyDisjointServicesAtScale(t *testing.T) {
	g := New(topoConfig())

	require.NoError(t, g.Add(serviceDesc("a", "svc-a", topoBase), footerWithTraceRange("svc-a", "a", 0, 10_000)))
	require.NoError(t, g.Add(serviceDesc("b", "svc-b", topoBase), footerWithTraceRange("svc-b", "b", 0, 10_000)))

	// No edges at all means no nodes either: Snapshot reports the endpoints of the
	// edges it kept.
	assert.Equal(t, View{Nodes: []Node{}, Edges: []Edge{}},
		g.Edges(Filter{}))
}

// TestTopologyRealOverlapAtScale is the other half of the noise floor: a genuine
// overlap large enough to clear it is reported, at roughly its true size.
func TestTopologyRealOverlapAtScale(t *testing.T) {
	g := New(topoConfig())

	require.NoError(t, g.Add(serviceDesc("a", "svc-a", topoBase), footerWithTraceRange("svc-a", "tr", 0, 10_000)))
	require.NoError(t, g.Add(serviceDesc("b", "svc-b", topoBase), footerWithTraceRange("svc-b", "tr", 8_000, 18_000)))

	view := g.Edges(Filter{})
	require.Len(t, view.Edges, 1, "expected one co-occurrence edge, got %v", view.Edges)
	assert.InEpsilon(t, 2_000.0, view.Edges[0].Weight, 0.1, "overlap estimate should be near the true 2000")
	assert.InEpsilon(t, 0.2, view.Edges[0].Confidence, 0.1, "2000 shared of the 10000 each carries")
}

// TestTopologyConfidenceOfFullOverlap pins that two services sharing every trace in
// every bin are confident to within the sketches' error, and never past 1.
func TestTopologyConfidenceOfFullOverlap(t *testing.T) {
	g := New(topoConfig())

	for i, start := range []time.Time{topoBase, topoBase.Add(time.Hour)} {
		from, to := i*10_000, (i+1)*10_000
		n := strconv.Itoa(i)
		require.NoError(t, g.Add(serviceDesc("a-"+n, "svc-a", start), footerWithTraceRange("svc-a", "tr", from, to)))
		require.NoError(t, g.Add(serviceDesc("b-"+n, "svc-b", start), footerWithTraceRange("svc-b", "tr", from, to)))
	}

	view := g.Edges(Filter{})
	require.Len(t, view.Edges, 1, "expected one co-occurrence edge, got %v", view.Edges)
	assert.InDelta(t, 1.0, view.Edges[0].Confidence, 3*sketch.HLLStandardError)
	assert.LessOrEqual(t, view.Edges[0].Confidence, 1.0)
}

// TestTopologyWindowSumsRequestedBins pins per-bin weights: a placement seen in
// two bins weighs 2 over both and 1 over either, so a query about a past window
// sees that window's evidence and not a lifetime total.
func TestTopologyWindowSumsRequestedBins(t *testing.T) {
	g := New(topoConfig())

	later := topoBase.Add(time.Hour)
	require.NoError(t, g.Add(schedulerDescAt("s-1", "db-3-0", "host-7", topoBase), segment.Footer{}))
	require.NoError(t, g.Add(schedulerDescAt("s-2", "db-3-0", "host-7", later), segment.Footer{}))

	both := g.Edges(Filter{})
	require.Len(t, both.Edges, 1)
	assert.Equal(t, 2.0, both.Edges[0].Weight, "both bins should sum")

	first := g.Edges(Filter{From: topoBin, To: later.UnixMilli()})
	require.Len(t, first.Edges, 1)
	assert.Equal(t, Edge{
		From: "db-3-0", To: "host-7", Method: EdgePlacement, Weight: 1, LastBin: topoBin, Confidence: 1,
	}, first.Edges[0])
}

// TestTopologyServiceScopeEitherEndpoint pins the filter's either-endpoint rule:
// naming a pod returns the host it sits on, and drops edges touching neither.
func TestTopologyServiceScopeEitherEndpoint(t *testing.T) {
	g := New(topoConfig())

	require.NoError(t, g.Add(schedulerDesc("s-1", "db-3-0", "host-7"), segment.Footer{}))
	require.NoError(t, g.Add(schedulerDesc("s-2", "gateway-0", "host-4"), segment.Footer{}))

	view := g.Edges(Filter{Nodes: []string{"db-3-0"}})

	assert.Equal(t, []Edge{
		{From: "db-3-0", To: "host-7", Method: EdgePlacement, Weight: 1, LastBin: topoBin, Confidence: 1},
	}, view.Edges)
}

// TestTopologyEvictRetentionStart verifies that an edge older than the retention start is
// dropped and an edge at or after the horizon survives.
func TestTopologyEvictRetentionStart(t *testing.T) {
	cfg := Config{
		MinEdgeWeight: 0.5,
	}
	g := New(cfg)

	t1 := topoBase
	t2 := topoBase.Add(time.Hour)
	require.NoError(t, g.Add(schedulerDescAt("old", "pod-old", "host-old", t1), segment.Footer{}))
	require.NoError(t, g.Add(schedulerDescAt("new", "pod-new", "host-new", t2), segment.Footer{}))

	// Evict the old hour and keep the new one.
	newBin := t2.UnixMilli()
	g.Evict(newBin)

	want := View{
		Nodes: []Node{
			{ID: "host-new", Kind: NodeResource, LastBin: newBin},
			{ID: "pod-new", Kind: NodeWorkload, LastBin: newBin},
		},
		Edges: []Edge{
			{From: "pod-new", To: "host-new", Method: EdgePlacement, Weight: 1, LastBin: newBin, Confidence: 1},
		},
	}
	assert.Equal(t, want, g.Edges(Filter{}))
}

// TestTopologySnapshotDeterministic verifies that repeated Snapshot calls with
// the same state return byte-identical Nodes and Edges slices.
func TestTopologySnapshotDeterministic(t *testing.T) {
	g := New(topoConfig())

	for i := range 5 {
		require.NoError(t, g.Add(
			schedulerDesc(fmt.Sprintf("seg-%d", i),
				fmt.Sprintf("pod-%d", i), fmt.Sprintf("host-%d", i%3)),
			segment.Footer{}))
	}

	assert.Equal(t, g.Edges(Filter{}), g.Edges(Filter{}))
}

// TestTopologyMinEdgeWeightPrunes verifies the weight floor hides the lighter of
// two real edges, and that raising the caller's floor is what does it — the edge
// is still in the graph for a wider window to find.
func TestTopologyMinEdgeWeightPrunes(t *testing.T) {
	g := New(topoConfig())

	// db-3-0's placement is seen in two bins (weight 2); gateway-0's in one.
	require.NoError(t, g.Add(schedulerDescAt("s-1", "db-3-0", "host-7", topoBase), segment.Footer{}))
	require.NoError(t, g.Add(schedulerDescAt("s-2", "db-3-0", "host-7", topoBase.Add(time.Hour)), segment.Footer{}))
	require.NoError(t, g.Add(schedulerDescAt("s-3", "gateway-0", "host-4", topoBase), segment.Footer{}))

	assert.Len(t, g.Edges(Filter{}).Edges, 2, "both edges clear the default floor")

	heavy := g.Edges(Filter{MinWeight: 2})
	assert.Equal(t, []Edge{
		{From: "db-3-0", To: "host-7", Method: EdgePlacement, Weight: 2,
			LastBin: topoBase.Add(time.Hour).UnixMilli(), Confidence: 1},
	}, heavy.Edges)
}

// TestTopologyMaxCapsBounded verifies that a runaway sequence of Observe calls
// does not grow the graph past MaxNodes and MaxEdges.
func TestTopologyMaxCapsBounded(t *testing.T) {
	const maxNodes = 10
	const maxEdges = 8

	cfg := Config{
		MinEdgeWeight: 0.5,
		MaxNodes:      maxNodes,
		MaxEdges:      maxEdges,
	}
	g := New(cfg)

	// Feed 100 distinct pod→host pairs — far more than MaxNodes/MaxEdges.
	for i := range 100 {
		desc := schedulerDesc(
			fmt.Sprintf("seg-%d", i),
			fmt.Sprintf("pod-%d", i),
			fmt.Sprintf("host-%d", i),
		)
		require.NoError(t, g.Add(desc, segment.Footer{}))
	}

	view := g.Edges(Filter{MinWeight: 0.001})
	assert.LessOrEqual(t, len(view.Nodes), maxNodes, "nodes must not exceed MaxNodes")
	assert.LessOrEqual(t, len(view.Edges), maxEdges, "edges must not exceed MaxEdges")
}

func TestHourlySketchBudget(t *testing.T) {
	g := New(Config{}).(*graph)
	footer := footerWithHLL("svc", "request")
	view := footer.ForService("svc")
	view.DistinctCounts["request_id"] = view.DistinctCounts[segment.ColTraceID]
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := range 24 * 60 * 6 {
		desc := serviceDesc(strconv.Itoa(i), "svc", topoBase.Add(time.Duration(i)*10*time.Second))
		desc.TimeRange.Max = desc.TimeRange.Min
		require.NoError(t, g.Add(desc, footer))
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	accumulators := 0
	for _, byKey := range g.sketches {
		accumulators += len(byKey)
	}
	assert.Equal(t, 48, accumulators)
	assert.Less(t, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(1<<20))
	runtime.KeepAlive(g)
}

// sealedFooter is the footer a segment of lines, each a service's columns, decodes to.
func sealedFooter(t *testing.T, lines ...map[string]string) segment.Footer {
	t.Helper()
	b := segment.NewBuilder(segment.BuilderConfig{Tenant: "acme", CreatedAt: topoBase})
	for i, columns := range lines {
		require.NoError(t, b.Append(segment.LogRecord(topoBase.Add(time.Duration(i)*time.Millisecond), "info", "request", columns), segment.ParsedPattern{}))
	}
	obj, err := b.Seal().Encode()
	require.NoError(t, err)
	store := storage.NewFS(t.TempDir())
	require.NoError(t, store.Put(context.Background(), "seg", obj))
	footer, _, err := segment.NewFooterCache(store).Footer(context.Background(), segment.ObjectLocation{Key: "seg"})
	require.NoError(t, err)
	return footer
}

// repeat is n copies of a line.
func repeat(n int, columns map[string]string) []map[string]string {
	lines := make([]map[string]string, n)
	for i := range lines {
		lines[i] = columns
	}
	return lines
}

func TestTopologyReferenceEdges(t *testing.T) {
	quiet := func(service string) []map[string]string {
		return repeat(2, map[string]string{segment.ColService: service})
	}
	ztunnel := func(src, dst string) map[string]string {
		return map[string]string{segment.ColService: "ztunnel", "src.workload": src, "dst.workload": dst, "direction": "outbound"}
	}
	sidecar := func(service, authority string) map[string]string {
		return map[string]string{segment.ColService: service, "authority": `"` + authority + `:8080"`,
			"upstream_cluster": "outbound|8080||" + authority, "user_agent": "curl"}
	}
	edge := func(from, to string, weight float64, fields ...[2]string) Edge {
		return Edge{From: from, To: to, Method: EdgeReference, Weight: weight, LastBin: topoBin, Confidence: 1, Fields: fields}
	}
	for _, tc := range []struct {
		name     string
		segments [][]map[string]string
		want     []Edge
	}{
		{
			name: "a line naming both ends draws caller to callee, not its emitter to either",
			segments: [][]map[string]string{quiet("web"), quiet("cart"), quiet("checkout"), append(
				repeat(6, ztunnel("web-66656c4c48-tscj9", "cart-7798cb9c6d-lcd67")), repeat(4, ztunnel("web-66656c4c48-tscj9", "checkout-0"))...)},
			want: []Edge{edge("web", "cart", 6, [2]string{"src.workload", "dst.workload"}),
				edge("web", "checkout", 4, [2]string{"src.workload", "dst.workload"})},
		},
		{
			name: "a line naming its callee draws its emitter to it, two columns naming one call once",
			segments: [][]map[string]string{
				repeat(5, sidecar("cart", "checkout.shop.svc.cluster.local")),
				repeat(3, sidecar("checkout", "checkout.shop.svc.cluster.local")),
			},
			want: []Edge{edge("cart", "checkout", 5, [2]string{segment.ColService, "authority"}, [2]string{segment.ColService, "upstream_cluster"})},
		},
		{
			name: "a call through an intermediary draws its caller to its callee and the intermediary to the callee",
			segments: [][]map[string]string{quiet("web"), quiet("cart"), repeat(5, sidecar("gateway", "cart.shop.svc.cluster.local")),
				repeat(4, map[string]string{segment.ColService: "netlog", "src.workload": "web-66656c4c48-tscj9",
					"dst.workload": "gateway-7798cb9c6d-lcd67", "dst.service": "cart.shop.svc.cluster.local"})},
			want: []Edge{edge("gateway", "cart", 5, [2]string{segment.ColService, "authority"}, [2]string{segment.ColService, "upstream_cluster"}),
				edge("web", "cart", 4, [2]string{"src.workload", "dst.service"})},
		},
		{
			name: "a call to the intermediary itself keeps its edge",
			segments: [][]map[string]string{quiet("web"), quiet("cart"), repeat(5, sidecar("gateway", "cart.shop.svc.cluster.local")),
				repeat(4, map[string]string{segment.ColService: "netlog", "src.workload": "web-66656c4c48-tscj9",
					"dst.workload": "gateway-7798cb9c6d-lcd67", "dst.service": "cart.shop.svc.cluster.local"}),
				repeat(4, map[string]string{segment.ColService: "netlog", "src.workload": "web-66656c4c48-tscj9",
					"dst.workload": "gateway-7798cb9c6d-lcd67", "dst.service": "gateway.shop.svc.cluster.local"})},
			want: []Edge{edge("gateway", "cart", 5, [2]string{segment.ColService, "authority"}, [2]string{segment.ColService, "upstream_cluster"}),
				edge("web", "cart", 4, [2]string{"src.workload", "dst.service"}),
				edge("web", "gateway", 8, [2]string{"src.workload", "dst.service"}, [2]string{"src.workload", "dst.workload"})},
		},
		{
			name:     "a column naming the line's caller draws the caller to the emitter",
			segments: [][]map[string]string{quiet("web"), repeat(4, map[string]string{segment.ColService: "cart", "client": "web"})},
			want:     []Edge{edge("web", "cart", 4, [2]string{"client", segment.ColService})},
		},
		{
			name: "a line mentioning a pod by its bare name draws nothing",
			segments: [][]map[string]string{quiet("data-warehouse"), repeat(4, map[string]string{
				segment.ColService: "istio-cni-node", "name": "data-warehouse-5"})},
		},
		{
			name: "a request's id reading as a pod name draws nothing",
			segments: [][]map[string]string{quiet("web"), {
				{segment.ColService: "cart", "caller": "web-1"}, {segment.ColService: "cart", "caller": "web-1"},
				{segment.ColService: "cart", "caller": "web-2"}, {segment.ColService: "cart", "caller": "web-2"}}},
		},
		{
			name: "values naming no service draw nothing",
			segments: [][]map[string]string{quiet("cart"), repeat(4, map[string]string{
				segment.ColService: "web", "region": "eu-west", "peer": "10.244.0.9:8080"})},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := New(topoConfig())
			for i, lines := range tc.segments {
				require.NoError(t, g.Add(serviceDesc(strconv.Itoa(i), "s", topoBase), sealedFooter(t, lines...)))
			}
			var got []Edge
			for _, e := range g.Edges(Filter{}).Edges {
				if e.Method == EdgeReference {
					got = append(got, e)
				}
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
