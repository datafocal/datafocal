package summary

import (
	"fmt"
	"github.com/datafocal/datafocal/internal/catalog"
	"runtime"
	"runtime/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/topology"
)

// storeBase is the fixed instant every store test's segments hang off. Bins are
// epoch-aligned, so a wall-clock base would put the same segment in a different
// bin on every run.
var storeBase = time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)

func TestStoreAddIdempotent(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	desc := testDescriptor("seg1", "svc-a", 10, []string{"tmpl-1", "tmpl-2"})
	footer := testFooter("svc-a", 10, []string{"tmpl-1", "tmpl-2"})

	require.NoError(t, store.Add(desc, footer, Rollup{}))
	require.True(t, store.HasAdded("seg1"))

	points1 := store.CountSlice("", 0, 1<<62)

	// A second Add with the same descriptor is a no-op.
	require.NoError(t, store.Add(desc, footer, Rollup{}))

	assert.Equal(t, points1, store.CountSlice("", 0, 1<<62), "series should not change on re-seal")
}

// TestStoreVersionTracksSyncedEpoch: the version is the catalog epoch the content
// has been declared complete for, not a count of merges. Sealing alone must not
// move it — a half-merged epoch that advertised itself would serve short answers —
// and it never goes backwards, so a pin taken from it stays meaningful.
func TestStoreVersionTracksSyncedEpoch(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	assert.Zero(t, store.CatalogVersion(), "a store that has absorbed nothing is at 0")

	require.NoError(t, store.Add(
		testDescriptor("seg1", "svc-a", 10, []string{"tmpl-1"}), testFooter("svc-a", 10, []string{"tmpl-1"}), Rollup{}))
	assert.Zero(t, store.CatalogVersion(), "merging a segment does not by itself claim an epoch")

	store.MarkSynced("4")
	assert.Equal(t, catalog.Version("4"), store.CatalogVersion())

	store.MarkSynced("2")
	assert.Equal(t, catalog.Version("4"), store.CatalogVersion(), "version must not regress")
}

// TestStoreExistedBefore: the store answers novelty only in the direction it has
// evidence for. It rules a pattern out when it folded a segment carrying it in an
// earlier bin, and otherwise declines — an absent or too-recent entry says nothing
// about the corpus, so the caller goes to the manifest instead of reading the
// store's silence as "new".
func TestStoreExistedBefore(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})

	desc := testDescriptor("seg1", "svc-a", 20, []string{"tmpl-1", "tmpl-2"})
	footer := testFooter("svc-a", 20, []string{"tmpl-1", "tmpl-2"})
	require.NoError(t, store.Add(desc, footer, Rollup{}))
	// Re-sealing observes nothing, so it cannot move the pattern's first bin
	// forward and turn a rule-out into a decline.
	require.NoError(t, store.Add(desc, footer, Rollup{}))

	later := storeBase.Add(2 * time.Hour)
	seg2 := testDescriptor("seg2", "svc-a", 10, []string{"tmpl-1", "tmpl-3"})
	seg2.TimeRange = segment.TimeRange{Min: later, Max: later.Add(30 * time.Minute)}
	require.NoError(t, store.Add(seg2, testFooter("svc-a", 10, []string{"tmpl-1", "tmpl-3"}), Rollup{}))

	assert.True(t, store.ExistedBefore("tmpl-1", later),
		"a pattern folded in an earlier bin demonstrably existed then")
	assert.False(t, store.ExistedBefore("tmpl-1", storeBase),
		"the bin a pattern was first folded in is not before itself")
	assert.False(t, store.ExistedBefore("tmpl-3", later),
		"a pattern first folded in this very bin is not ruled out")
	assert.False(t, store.ExistedBefore("never-folded", later),
		"an absent entry is not evidence about the corpus")
}

func TestStoreSeenIndexesTheStoredPatternSet(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	desc := testDescriptor("seg1", "svc-a", 60, []string{"tmpl-1", "tmpl-2", "tmpl-3"})
	// The footer adds a pattern the descriptor omits and drops two it lists.
	require.NoError(t, store.Add(desc, testFooter("svc-a", 60, []string{"tmpl-1", "footer-only"}), Rollup{}))

	_, ok := store.seen.entry("footer-only")
	assert.True(t, ok, "a pattern the series counted must be indexed")
	_, ok = store.seen.entry("tmpl-2")
	assert.False(t, ok, "a pattern the series never counted must not be indexed")
}

// TestStoreCanServe: the series can answer only windows its points tile exactly
// and that start inside the span it has declared complete. Anything untileable
// straddles a point across two requested bins, which the series has no sub-bin
// resolution to split; anything below the declared floor reads as bins with no
// lines rather than as bins the store never looked for. Either way it declines and
// the caller scans.
func TestStoreCanServe(t *testing.T) {
	store := newStore(Config{BinWidth: time.Minute})
	store.SetCompleteFrom(storeBase)
	aligned := storeBase.Add(12 * time.Hour)

	tests := []struct {
		name  string
		width time.Duration
		from  time.Time
		want  bool
	}{
		{"exactly one series bin", time.Minute, aligned, true},
		{"a whole number of series bins", 5 * time.Minute, aligned, true},
		{"narrower than a series bin", 15 * time.Second, aligned, false},
		{"not a whole number of series bins", 90 * time.Second, aligned, false},
		{"window starts mid-bin", time.Minute, aligned.Add(20 * time.Second), false},
		{"window starts before the declared floor", time.Minute, storeBase.Add(-time.Minute), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, store.CanServe(tc.width, tc.from))
		})
	}
}

// TestStoreCompleteFromMustBeDeclared: adding a segment claims nothing about earlier bins, so without SetCompleteFrom every read scans.
func TestStoreCompleteFromMustBeDeclared(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	require.NoError(t, store.Add(
		testDescriptor("seg1", "svc-a", 10, []string{"tmpl-1"}), testFooter("svc-a", 10, []string{"tmpl-1"}), Rollup{}))
	assert.False(t, store.CompleteFrom(storeBase.UnixMilli()), "adding a segment is not a complete-from claim")
	assert.False(t, store.CanServe(time.Hour, storeBase))

	floor := storeBase.Add(4 * time.Hour)
	store.SetCompleteFrom(floor)
	assert.False(t, store.CompleteFrom(floor.Add(-time.Hour).UnixMilli()))
	assert.True(t, store.CompleteFrom(floor.UnixMilli()))
}

// TestStoreCoverageFollowsEviction: a declared floor does not outlive the points
// below the retention horizon. The horizon wins whenever it is the later of the two,
// so a long-running store stops claiming the hours it has aged out.
func TestStoreCoverageFollowsEviction(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 2 * time.Hour})
	store.SetCompleteFrom(storeBase)
	require.True(t, store.CompleteFrom(storeBase.UnixMilli()))

	// A segment six hours on drags the horizon past the declared floor.
	desc := testDescriptor("late", "svc-a", 10, []string{"tmpl-1"})
	desc.TimeRange = segment.TimeRange{Min: storeBase.Add(6 * time.Hour), Max: storeBase.Add(6 * time.Hour)}
	require.NoError(t, store.Add(desc, testFooter("svc-a", 10, []string{"tmpl-1"}), Rollup{}))

	assert.False(t, store.CompleteFrom(storeBase.UnixMilli()), "the declared floor has been evicted")
	assert.True(t, store.CompleteFrom(storeBase.Add(5*time.Hour).UnixMilli()))
}

// TestStoreRetentionEvictsEverything: one horizon drives the series, the signal
// index, and the sealed-descriptor set, so they cannot disagree about what is
// still live. A sealed set that never shrank was an unbounded map keyed by every
// descriptor the process ever saw.
func TestStoreRetentionEvictsEverything(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 2 * time.Hour})

	old := testDescriptor("old", "svc", 10, []string{"old-tmpl"})
	require.NoError(t, store.Add(old, testFooter("svc", 10, []string{"old-tmpl"}), Rollup{}))
	require.True(t, store.HasAdded("old"))

	recent := testDescriptor("recent", "svc", 10, []string{"recent-tmpl"})
	recent.TimeRange = segment.TimeRange{Min: storeBase.Add(10 * time.Hour), Max: storeBase.Add(10*time.Hour + time.Minute)}
	require.NoError(t, store.Add(recent, testFooter("svc", 10, []string{"recent-tmpl"}), Rollup{}))

	assert.False(t, store.HasAdded("old"), "a descriptor older than the retention start is forgotten")
	assert.True(t, store.HasAdded("recent"))

	_, ok := store.seen.entry("old-tmpl")
	assert.False(t, ok, "a pattern last seen before the horizon is dropped from the signal index")

	points := store.CountSlice("", 0, 1<<62)
	require.Len(t, points, 1)
	assert.Equal(t, "recent-tmpl", points[0].Key.PatternID)
}

func TestStoreAddSeriesMatchesDirectMerge(t *testing.T) {
	segments := []struct {
		id       string
		service  string
		rows     int
		patterns []string
		min, max time.Time
	}{
		{"seg1", "svc-a", 100, []string{"tmpl-1", "tmpl-2"}, storeBase, storeBase.Add(time.Minute)},
		{"seg2", "svc-a", 50, []string{"tmpl-1", "tmpl-3"}, storeBase.Add(1 * time.Hour), storeBase.Add(time.Hour + time.Minute)},
		{"seg3", "svc-b", 75, []string{"tmpl-1", "tmpl-2"}, storeBase, storeBase.Add(time.Minute)},
	}

	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	want := make(map[patternService]int64)
	for _, seg := range segments {
		desc := segment.Descriptor{
			ID:         seg.id,
			Stream:     segment.Stream{Service: seg.service},
			TimeRange:  segment.TimeRange{Min: seg.min, Max: seg.max},
			Rows:       seg.rows,
			PatternIDs: seg.patterns,
		}
		footer := testFooter(seg.service, seg.rows, seg.patterns)
		require.NoError(t, store.Add(desc, footer, Rollup{}))

		for _, tmpl := range seg.patterns {
			key := patternService{tmpl, seg.service}
			want[key] += int64(footer.ServicePatternCounts[segment.ServicePattern{Service: seg.service, Pattern: tmpl}])
		}
	}

	got := make(map[patternService]int64)
	for _, point := range store.CountSlice("", 0, 1<<62) {
		got[patternService{point.Key.PatternID, point.Key.Service}] += point.Lines
	}
	assert.Equal(t, want, got)
}

// patternService collapses a CountKey's bin away, so a total can be compared
// across however many bins a segment happened to land in.
type patternService struct {
	pattern string
	service string
}

func TestStoreAddMultipleServices(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})

	require.NoError(t, store.Add(
		testDescriptor("seg1", "svc-a", 100, []string{"tmpl-1"}), testFooter("svc-a", 100, []string{"tmpl-1"}), Rollup{}))
	require.NoError(t, store.Add(
		testDescriptor("seg2", "svc-b", 50, []string{"tmpl-1"}), testFooter("svc-b", 50, []string{"tmpl-1"}), Rollup{}))

	bin := storeBase.UnixMilli()
	assert.Equal(t,
		[]CountPoint{{Key: CountKey{PatternID: "tmpl-1", Service: "svc-a", Bin: bin}, Lines: 100}},
		store.CountSlice("svc-a", 0, 1<<62))
	assert.Equal(t,
		[]CountPoint{{Key: CountKey{PatternID: "tmpl-1", Service: "svc-b", Bin: bin}, Lines: 50}},
		store.CountSlice("svc-b", 0, 1<<62))
	assert.Len(t, store.CountSlice("", 0, 1<<62), 2,
		"an all-services query should include both svc-a and svc-b")
}

// TestStoreAddPopulatesTopology: Add a scheduler descriptor with pod/host
// labels and assert the topology is reachable through Store.TopologyView.
func TestStoreAddPopulatesTopology(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	desc := segment.Descriptor{
		ID: "sched-1",
		Stream: segment.Stream{
			Service: "scheduler",
			Labels:  map[string]string{"pod": "pod-1", "host": "host-1"},
		},
		TimeRange: segment.TimeRange{Min: storeBase, Max: storeBase.Add(30 * time.Minute)},
	}

	require.NoError(t, store.Add(desc, segment.Footer{}, Rollup{}))

	bin := storeBase.UnixMilli()
	assert.Equal(t, topology.View{
		Nodes: []topology.Node{
			{ID: "host-1", Kind: topology.NodeResource, LastBin: bin},
			{ID: "pod-1", Kind: topology.NodeWorkload, LastBin: bin},
		},
		Edges: []topology.Edge{
			{From: "pod-1", To: "host-1", Method: topology.EdgePlacement, Weight: 1, LastBin: bin, Confidence: 1},
		},
	}, store.TopologyView(topology.Filter{}))
}

// TestStoreTopologyHourly: topology bins stay hourly with finer series bins.
func TestStoreTopologyHourly(t *testing.T) {
	store := newStore(Config{
		BinWidth:  time.Second,
		Retention: 365 * 24 * time.Hour,
	})

	// Two segments a minute apart share one topology bin, so the placement weighs 1.
	for i, ts := range []time.Time{storeBase, storeBase.Add(time.Minute)} {
		require.NoError(t, store.Add(segment.Descriptor{
			ID:        fmt.Sprintf("sched-%d", i),
			Stream:    segment.Stream{Service: "scheduler", Labels: map[string]string{"pod": "pod-1", "host": "host-1"}},
			TimeRange: segment.TimeRange{Min: ts, Max: ts},
		}, segment.Footer{}, Rollup{}))
	}

	view := store.TopologyView(topology.Filter{})
	require.Len(t, view.Edges, 1)
	assert.Equal(t, 1.0, view.Edges[0].Weight)
}

// TestStoreTopologyEvictsWithRetentionStart: an edge older than the retention start
// is evicted when a newer segment advances the horizon past it.
func TestStoreTopologyEvictsWithRetentionStart(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 2 * time.Hour})

	old := segment.Descriptor{
		ID: "sched-old",
		Stream: segment.Stream{
			Service: "scheduler",
			Labels:  map[string]string{"pod": "pod-1", "host": "host-1"},
		},
		TimeRange: segment.TimeRange{Min: storeBase, Max: storeBase.Add(30 * time.Minute)},
	}
	require.NoError(t, store.Add(old, segment.Footer{}, Rollup{}))

	view := store.TopologyView(topology.Filter{})
	require.Len(t, view.Edges, 1, "old edge must exist before horizon advance")

	// Advance the newest bin past the retention window so the old edge is evicted.
	recent := testDescriptor("recent", "svc", 10, []string{"tmpl-1"})
	recent.TimeRange = segment.TimeRange{
		Min: storeBase.Add(10 * time.Hour),
		Max: storeBase.Add(11 * time.Hour),
	}
	require.NoError(t, store.Add(recent, testFooter("svc", 10, []string{"tmpl-1"}), Rollup{}))

	view = store.TopologyView(topology.Filter{})
	assert.Empty(t, view.Edges, "old edge must be evicted once horizon advances past its bin")
}

// TestStoreEvictsTheOldestValueSeries: past MaxValueSeries the series whose newest bin is
// oldest goes, whatever its lines, and the store counts it and holds the column only after it.
func TestStoreEvictsTheOldestValueSeries(t *testing.T) {
	bin := func(m int) int64 { return storeBase.Add(time.Duration(m) * time.Minute).UnixMilli() }
	values := ValueCounts{
		{PatternID: "tmpl-1", Column: "status", Value: "500", Bin: bin(0)}: 9,
		{PatternID: "tmpl-1", Column: "status", Value: "500", Bin: bin(1)}: 9,
		{PatternID: "tmpl-1", Column: "status", Value: "200", Bin: bin(2)}: 1,
		{PatternID: "tmpl-1", Column: "status", Value: "503", Bin: bin(3)}: 1,
	}
	s := newStore(Config{MaxValueSeries: 2})
	require.NoError(t, s.Add(testDescriptor("seg1", "svc", 20, nil), segment.Footer{}, Rollup{Values: values}))
	assert.Equal(t, []ValuePoint{{PatternID: "tmpl-1", Value: "200", Bin: bin(2), Count: 1}, {PatternID: "tmpl-1", Value: "503", Bin: bin(3), Count: 1}},
		s.ValueSlice("status", 0, 1<<62))
	assert.Equal(t, 1, s.valueSeriesEvicted())
	assert.Equal(t, []bool{false, true}, []bool{s.HoldsValues("status", bin(1)), s.HoldsValues("status", bin(2))})
}

// TestStoreValueSeriesHeapBudget bounds the heap the value series hold at twice the 32 bytes
// a point they measured, where a key per bin held 476 with each add evicting twice the limit.
func TestStoreValueSeriesHeapBudget(t *testing.T) {
	const limit, bins = 2000, 100
	values := func(seg int) ValueCounts {
		counts := ValueCounts{}
		for series := range 3 * limit {
			for b := range bins {
				// Built apart, as a decoded rollup's keys are, rather than sharing one string.
				key := ValueKey{PatternID: fmt.Sprintf("tmpl-%d", series%50), Column: fmt.Sprintf("col-%d", series%20),
					Value: fmt.Sprintf("value-%d-%d", seg, series), Bin: storeBase.Add(time.Duration(seg*bins+b) * 15 * time.Second).UnixMilli()}
				counts[key] = 1
			}
		}
		return counts
	}
	idle := heapAfterGC()
	s := newStore(Config{MaxValueSeries: limit, MaxColumnValues: 1 << 20})
	for seg := range 3 {
		require.NoError(t, s.Add(testDescriptor(fmt.Sprint("seg", seg), "svc", 1, nil), segment.Footer{}, Rollup{Values: values(seg)}))
	}
	held := heapAfterGC() - idle
	assert.LessOrEqual(t, held, uint64(limit*bins*64), "%d bytes a point", held/(limit*bins))
	runtime.KeepAlive(s)
}

// TestStoreDropsAColumnPastMaxColumnValues: a column whose distinct values pass
// MaxColumnValues, as a per-node one does across a fleet, drops all its series for good and
// is no longer held, while a narrower column keeps its series.
func TestStoreDropsAColumnPastMaxColumnValues(t *testing.T) {
	bin := storeBase.UnixMilli()
	s := newStore(Config{MaxColumnValues: 2})
	add := func(id string, values ValueCounts) {
		require.NoError(t, s.Add(testDescriptor(id, "svc", 1, nil), segment.Footer{}, Rollup{Values: values}))
	}
	for i, host := range []string{"host-1", "host-2", "host-3"} {
		add(fmt.Sprint("seg", i), ValueCounts{
			{PatternID: "tmpl-1", Column: "host", Value: host, Bin: bin}:    1,
			{PatternID: "tmpl-1", Column: "status", Value: "200", Bin: bin}: 1,
		})
		assert.Equal(t, i < 2, s.HoldsValues("host", 0), "after %s", host)
	}
	add("seg3", ValueCounts{{PatternID: "tmpl-1", Column: "host", Value: "host-1", Bin: bin}: 1})
	assert.False(t, s.HoldsValues("host", 0), "a dropped column stays dropped")
	assert.Empty(t, s.ValueSlice("host", 0, 1<<62))
	assert.True(t, s.HoldsValues("status", 0))
	assert.Equal(t, []ValuePoint{{PatternID: "tmpl-1", Value: "200", Bin: bin, Count: 3}}, s.ValueSlice("status", 0, 1<<62))
}

// --- Helpers ---

func testDescriptor(id, service string, rows int, patterns []string) segment.Descriptor {
	return segment.Descriptor{
		ID:         id,
		Stream:     segment.Stream{Service: service},
		TimeRange:  segment.TimeRange{Min: storeBase, Max: storeBase.Add(30 * time.Minute)},
		Rows:       rows,
		PatternIDs: patterns,
	}
}

// testFooter is a footer whose rows are all service's, split evenly over patterns, with
// an empty trace and latency sketch.
func testFooter(service string, rows int, patterns []string) segment.Footer {
	f := segment.Footer{Rows: rows, Patterns: segment.NewPresentSet(), ServicePatternCounts: map[segment.ServicePattern]int{}}
	for _, t := range patterns {
		f.Patterns.Add(t)
		f.ServicePatternCounts[segment.ServicePattern{Service: service, Pattern: t}] = rows / len(patterns)
	}
	return segment.FooterWithServices(f, map[string]segment.Footer{service: {
		Rows:           rows,
		DistinctCounts: map[string]*sketch.HLL{segment.ColTraceID: sketch.NewHLL()},
		Distributions: map[string]*sketch.DDSketch{
			segment.ColDurationMs: sketch.NewDDSketch(sketch.QuantileAccuracy),
		},
	}})
}

// TestStoreBinWidth verifies that BinWidth returns the series width.
func TestStoreBinWidth(t *testing.T) {
	cfg := Config{BinWidth: 30 * time.Second}
	store := newStore(cfg)

	assert.Equal(t, 30*time.Second, store.BinWidth())
}

// TestStoreConcurrentReadDuringAdd is a -race probe: the store's lock is the only one
// guarding its bin table, seen index and graph.
func TestStoreConcurrentReadDuringAdd(t *testing.T) {
	store := newStore(Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 50 {
			desc := testDescriptor(fmt.Sprintf("seg%d", i), "svc", 3, []string{"tmpl-1"})
			assert.NoError(t, store.Add(desc, testFooter("svc", 3, []string{"tmpl-1"}), Rollup{}))
		}
	}()
	for range 50 {
		store.CountSlice("", 0, 1<<62)
		_, err := store.SketchSlice("", 0, 1<<62)
		require.NoError(t, err)
		store.TopologyView(topology.Filter{})
	}
	<-done
}

// TestStoreSkipsAMergeOfSegmentsItCounted: a merged segment adds nothing to a store that
// counted its inputs, so that store holds what one given only the merged segment holds.
func TestStoreSkipsAMergeOfSegmentsItCounted(t *testing.T) {
	cfg := Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour}
	patterns := []string{"tmpl-1"}
	merged := testDescriptor("ab", "svc", 20, patterns)
	merged.Supersedes = []string{"a", "b"}
	inputs, err := NewStore(cfg, "3", storeBase,
		Segment{Descriptor: testDescriptor("a", "svc", 10, patterns), Footer: testFooter("svc", 10, patterns)},
		Segment{Descriptor: testDescriptor("b", "svc", 10, patterns), Footer: testFooter("svc", 10, patterns)},
		Segment{Descriptor: merged, Footer: testFooter("svc", 20, patterns)})
	require.NoError(t, err)
	alone, err := NewStore(cfg, "1", storeBase, Segment{Descriptor: merged, Footer: testFooter("svc", 20, patterns)})
	require.NoError(t, err)

	assert.Equal(t, alone.CountSlice("", 0, 1<<62), inputs.CountSlice("", 0, 1<<62))
	assert.True(t, inputs.HasAdded("ab"))
}

func heapAfterGC() uint64 {
	runtime.GC()
	runtime.GC() // the second frees what the first left in sync.Pool victim caches
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	metrics.Read(sample)
	return sample[0].Value.Uint64()
}
