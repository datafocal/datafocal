package summary

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/topology"
	"github.com/datafocal/datafocal/internal/value"
)

// --- Fakes ---

// servingCatalog is a catalog snapshotting at epoch whose Prune returns descs.
func servingCatalog(epoch uint64, descs []segment.Descriptor) catalog.Fake {
	return catalog.Fake{
		VersionFn: func() (catalog.Version, error) { return catalog.Version(strconv.FormatUint(epoch, 10)), nil },
		PruneFn:   func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return descs, nil },
	}
}

// footerReader serves footers by object key and counts its reads, which a build makes
// concurrently; a key absent from footers fails the read.
func footerReader(footers map[string]segment.Footer) (segment.Fake, *atomic.Int64) {
	reads := new(atomic.Int64)
	return segment.Fake{FooterFn: func(loc segment.ObjectLocation) (segment.Footer, segment.Cost, error) {
		reads.Add(1)
		f, ok := footers[loc.Key]
		if !ok {
			return segment.Footer{}, segment.Cost{GETs: 1}, fmt.Errorf("%s: no such object", loc.Key)
		}
		return f, segment.Cost{GETs: 1}, nil
	}}, reads
}

func builderDesc(id string, patterns []string, min, max time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:         id,
		Stream:     segment.Stream{Service: "svc"},
		TimeRange:  segment.TimeRange{Min: min, Max: max},
		Rows:       10,
		PatternIDs: patterns,
		Object:     segment.ObjectLocation{Key: id},
	}
}

// --- Tests ---

func TestBuilderReplayOrderIndependence(t *testing.T) {
	base := storeBase
	patterns := []string{"tmpl-1"}
	footer := testFooter("svc", 10, patterns)

	descs := []segment.Descriptor{
		builderDesc("seg-c", patterns, base.Add(2*time.Hour), base.Add(3*time.Hour)),
		builderDesc("seg-a", patterns, base, base.Add(time.Hour)),
		builderDesc("seg-b", patterns, base.Add(time.Hour), base.Add(2*time.Hour)),
	}

	footers := make(map[string]segment.Footer, len(descs))
	for _, d := range descs {
		footers[d.Object.Key] = footer
	}

	// Build with shuffled order.
	cat := servingCatalog(0, descs)
	rd, _ := footerReader(footers)
	b := NewBuilder(cat, rd, Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	require.NoError(t, b.Build(context.Background()))

	shuffled := b.Store().CountSlice("", 0, 1<<62)

	// Build with ascending order.
	ascending := make([]segment.Descriptor, len(descs))
	ascending[0] = descs[1] // seg-a
	ascending[1] = descs[2] // seg-b
	ascending[2] = descs[0] // seg-c

	cat2 := servingCatalog(0, ascending)
	rd2, _ := footerReader(footers)
	b2 := NewBuilder(cat2, rd2, Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	require.NoError(t, b2.Build(context.Background()))

	ascResult := b2.Store().CountSlice("", 0, 1<<62)

	assert.Equal(t, ascResult, shuffled,
		"building from shuffled descriptors must produce the same CountSlice as ascending")
}

func TestBuilderRetentionKeepsNewestWindow(t *testing.T) {
	base := storeBase
	patterns := []string{"tmpl-1"}
	footer := testFooter("svc", 10, patterns)

	// 5 segments spanning 5 hours; retention is 2 hours.
	var descs []segment.Descriptor
	footers := make(map[string]segment.Footer)
	for i := range 5 {
		d := builderDesc("seg-"+string(rune('a'+i)), patterns,
			base.Add(time.Duration(i)*time.Hour),
			base.Add(time.Duration(i+1)*time.Hour))
		descs = append(descs, d)
		footers[d.Object.Key] = footer
	}

	cat := servingCatalog(0, descs)
	rd, reads := footerReader(footers)
	b := NewBuilder(cat, rd, Config{BinWidth: time.Hour, Retention: 2 * time.Hour})
	require.NoError(t, b.Build(context.Background()))

	// The newest descriptor max is base+5h. Retention floor = base+5h - 2h = base+3h.
	// Descriptors whose Max < floor are dropped: seg-a (max=base+1h), seg-b (max=base+2h).
	// Retained: seg-c (max=base+3h == floor), seg-d, seg-e.
	// The builder never reads footers for dropped descriptors.
	assert.Equal(t, int64(3), reads.Load(), "builder should read only retained descriptors' footers")

	assert.False(t, b.Store().HasAdded("seg-a"), "seg-a should be outside retention")
	assert.False(t, b.Store().HasAdded("seg-b"), "seg-b should be outside retention")
	assert.True(t, b.Store().HasAdded("seg-c"), "seg-c should be inside retention")
	assert.True(t, b.Store().HasAdded("seg-d"), "seg-d should be inside retention")
	assert.True(t, b.Store().HasAdded("seg-e"), "seg-e should be inside retention")
}

func TestBuilderIncrementalRefresh(t *testing.T) {
	base := storeBase
	patterns := []string{"tmpl-1"}
	footer := testFooter("svc", 10, patterns)

	initial := []segment.Descriptor{
		builderDesc("seg-a", patterns, base, base.Add(time.Hour)),
		builderDesc("seg-b", patterns, base.Add(time.Hour), base.Add(2*time.Hour)),
	}

	footers := map[string]segment.Footer{
		"seg-a": footer,
		"seg-b": footer,
		"seg-c": footer,
	}

	descs := initial
	cat := catalog.Fake{PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return descs, nil }}
	rd, reads := footerReader(footers)
	b := NewBuilder(cat, rd, Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	require.NoError(t, b.Build(context.Background()))

	callsAfterBuild := reads.Load()
	require.Equal(t, int64(2), callsAfterBuild, "Build should read exactly 2 footers")

	// Append a new segment and refresh.
	descs = append(descs,
		builderDesc("seg-c", patterns, base.Add(2*time.Hour), base.Add(3*time.Hour)))
	require.NoError(t, b.Build(context.Background()))

	assert.Equal(t, callsAfterBuild+1, reads.Load(),
		"Build should read exactly 1 new footer (the already-sealed descriptors are skipped)")

	assert.True(t, b.Store().HasAdded("seg-c"), "new segment should be sealed after Build")

	// Re-build with no new segments reads 0 footers.
	callsBefore := reads.Load()
	require.NoError(t, b.Build(context.Background()))
	assert.Equal(t, callsBefore, reads.Load(), "re-build with no new segments reads 0 footers")
}

// TestBuilderReadsFootersConcurrently has each footer read wait for a second one in
// flight, which a build reading one at a time never reaches.
func TestBuilderReadsFootersConcurrently(t *testing.T) {
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	var descs []segment.Descriptor
	for i := range 4 {
		descs = append(descs, builderDesc(fmt.Sprint("s", i), []string{"tmpl"}, day.Add(time.Duration(i)*time.Minute), day.Add(time.Duration(i)*time.Minute)))
	}
	var inFlight atomic.Int64
	overlapped := make(chan struct{})
	var once sync.Once
	rd := segment.Fake{FooterFn: func(segment.ObjectLocation) (segment.Footer, segment.Cost, error) {
		if inFlight.Add(1) >= 2 {
			once.Do(func() { close(overlapped) })
		}
		defer inFlight.Add(-1)
		select {
		case <-overlapped:
			return segment.Footer{}, segment.Cost{}, nil
		case <-time.After(5 * time.Second):
			return segment.Footer{}, segment.Cost{}, errors.New("no other footer read was in flight")
		}
	}}
	b := NewBuilder(servingCatalog(uint64(len(descs)), descs), rd, Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})
	require.NoError(t, b.Build(context.Background()))
}

func TestBuilderEmptyCatalog(t *testing.T) {
	cat := catalog.Fake{}
	rd, _ := footerReader(map[string]segment.Footer{})
	b := NewBuilder(cat, rd, Config{})
	require.NoError(t, b.Build(context.Background()))

	assert.Zero(t, b.Store().CatalogVersion(), "empty build yields no version")
	assert.Empty(t, b.Store().CountSlice("", 0, 1<<62), "empty build yields no points")
}

// TestBuilderAbsorbsCatalogEpoch: a clean build adopts the snapshot's epoch as
// the store's version, which is what a read snapshot is matched against.
func TestBuilderAbsorbsCatalogEpoch(t *testing.T) {
	base := storeBase
	patterns := []string{"tmpl-1"}
	descs := []segment.Descriptor{builderDesc("seg-a", patterns, base, base.Add(time.Hour))}

	cat := servingCatalog(7, descs)
	rd, _ := footerReader(map[string]segment.Footer{"seg-a": testFooter("svc", 10, patterns)})
	b := NewBuilder(cat, rd, Config{BinWidth: time.Hour})
	require.NoError(t, b.Build(context.Background()))

	assert.Equal(t, catalog.Version("7"), b.Store().CatalogVersion())
}

// TestBuilderSkipsUnreadableFooters: one unreadable object costs the store that
// segment, not the sync. The later descriptors are still merged, every failure is
// reported, and the store keeps its previous epoch so nothing serves from a
// partial summary until a refresh gets the whole epoch in.
func TestBuilderSkipsUnreadableFooters(t *testing.T) {
	base := storeBase
	patterns := []string{"tmpl-1"}
	footer := testFooter("svc", 10, patterns)

	var descs []segment.Descriptor
	footers := make(map[string]segment.Footer)
	for i := range 3 {
		d := builderDesc("seg-"+string(rune('a'+i)), patterns,
			base.Add(time.Duration(i)*time.Hour), base.Add(time.Duration(i+1)*time.Hour))
		descs = append(descs, d)
		footers[d.Object.Key] = footer
	}

	cat := servingCatalog(3, descs)
	delete(footers, "seg-a")
	rd, reads := footerReader(footers)
	b := NewBuilder(cat, rd, Config{BinWidth: time.Hour, Retention: 365 * 24 * time.Hour})

	err := b.Build(context.Background())
	require.ErrorContains(t, err, "seg-a")
	assert.Equal(t, int64(3), reads.Load(), "a bad footer must not stop later descriptors being read")
	assert.False(t, b.Store().HasAdded("seg-a"))
	assert.True(t, b.Store().HasAdded("seg-b"))
	assert.True(t, b.Store().HasAdded("seg-c"))
	assert.Zero(t, b.Store().CatalogVersion(), "a partial sync must not claim the epoch")

	// Clearing the failure lets the next refresh finish the epoch: the skipped
	// descriptor is retried, and only then does the store claim it.
	footers["seg-a"] = footer
	require.NoError(t, b.Build(context.Background()))
	assert.True(t, b.Store().HasAdded("seg-a"))
	assert.Equal(t, catalog.Version("3"), b.Store().CatalogVersion())
}

// TestBuilderBoundsRefreshAtRetentionStart: every refresh after the first asks the catalog only from the store's retention start.
func TestBuilderBoundsRefreshAtRetentionStart(t *testing.T) {
	base := storeBase
	patterns := []string{"tmpl-1"}

	var descs []segment.Descriptor
	footers := make(map[string]segment.Footer)
	for i := range 5 {
		d := builderDesc("seg-"+string(rune('a'+i)), patterns,
			base.Add(time.Duration(i)*time.Hour), base.Add(time.Duration(i+1)*time.Hour))
		descs = append(descs, d)
		footers[d.Object.Key] = testFooter("svc", 10, patterns)
	}

	var sels []selector.Resolved // every selector Prune was asked for, in order
	cat := catalog.Fake{PruneFn: func(_ catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
		sels = append(sels, sel)
		return descs, nil
	}}
	rd, _ := footerReader(footers)
	b := NewBuilder(cat, rd, Config{BinWidth: time.Hour, Retention: 2 * time.Hour})
	require.NoError(t, b.Build(context.Background()))
	require.NoError(t, b.Build(context.Background()))

	horizon := time.UnixMilli(b.Store().(*store).bins.RetentionStart()).UTC()
	want := []selector.Resolved{
		{},
		{Window: selector.Window{From: horizon}},
	}
	assert.Equal(t, want, sels)

	// The horizon is the newest *populated* bin less retention. seg-e spans
	// [base+4h, base+5h), which tiles one bin starting at base+4h, so the horizon is
	// base+2h — one bin below the base+3h floor retainedDescriptors computes from
	// the same descriptors' Max. The bound must be the lower of the two: it is the
	// catalog that is being asked to drop descriptors, and dropping one the series
	// would have kept is unrecoverable.
	assert.Equal(t, base.Add(2*time.Hour).UTC(), horizon)
}

// quarterHourCfg bins narrower than an hour and retains long enough that nothing ages out.
var quarterHourCfg = Config{BinWidth: 15 * time.Minute, Retention: 365 * 24 * time.Hour}

// TestBuilderLoadWindowBoundsEveryBuild: the load window bounds every build, and the store never claims to be complete below it.
func TestBuilderLoadWindowBoundsEveryBuild(t *testing.T) {
	patterns := []string{"tmpl-1"}
	footers := make(map[string]segment.Footer)
	var descs []segment.Descriptor
	for i := 1; i <= 4; i++ {
		end := storeBase.Add(time.Duration(i) * 30 * time.Minute)
		d := builderDesc(fmt.Sprintf("seg-%d", i), patterns, end.Add(-10*time.Minute), end)
		descs = append(descs, d)
		footers[d.Object.Key] = testFooter("svc", 10, patterns)
	}

	cfg := quarterHourCfg
	cfg.LoadWindow = 30 * time.Minute

	rd, reads := footerReader(footers)
	cat := catalog.Fake{PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return descs, nil }}
	b := NewBuilder(cat, rd, cfg)
	require.NoError(t, b.Build(context.Background()))

	// seg-4's newest bin is base+2h, so the prewarm floor is base+1h30m: seg-3 and
	// seg-4 are read, seg-1 and seg-2 never are.
	floor := storeBase.Add(90 * time.Minute)
	assert.Equal(t, int64(2), reads.Load())
	assert.True(t, b.Store().CompleteFrom(floor.UnixMilli()))
	assert.False(t, b.Store().CompleteFrom(floor.Add(-time.Millisecond).UnixMilli()),
		"the store must not claim the hours prewarm kept it from reading")

	// One new segment costs one new footer. Retention is a year here, so a floor that
	// were not sticky would fall back to it and read all five.
	end := storeBase.Add(150 * time.Minute)
	newest := builderDesc("seg-5", patterns, end.Add(-10*time.Minute), end)
	footers[newest.Object.Key] = testFooter("svc", 10, patterns)
	descs = append(descs, newest)

	require.NoError(t, b.Build(context.Background()))
	assert.Equal(t, int64(3), reads.Load())
	assert.True(t, b.Store().CompleteFrom(floor.UnixMilli()), "the floor does not move as the window grows past it")
}

// --- Refresh ---

// TestBuilderRefreshBuildsBeforeWaiting: the interval is an hour, so a store that is
// warm shortly after the call can only have built before the first tick. That is the
// property a caller depends on — every tool answers from the scan path until the
// store reaches the catalog's epoch, and waiting an interval first would leave them
// there for the whole hour.
func TestBuilderRefreshBuildsBeforeWaiting(t *testing.T) {
	patterns := []string{"tmpl-1"}
	descs := []segment.Descriptor{
		builderDesc("seg-a", patterns, storeBase, storeBase.Add(time.Hour)),
	}
	cat := servingCatalog(9, descs)
	rd, _ := footerReader(map[string]segment.Footer{"seg-a": testFooter("svc", 10, patterns)})

	cfg := quarterHourCfg
	cfg.RefreshInterval = time.Hour

	b := NewBuilder(cat, rd, cfg)
	go b.Refresh(t.Context())

	require.Eventually(t, func() bool { return b.Store().CatalogVersion() == "9" }, time.Second, time.Millisecond)
}

// TestBuilderRefreshRunsUntilCancelled: a build that fails must not end the loop —
// otherwise one object-store outage leaves the process on the scan path forever — and
// cancelling ctx must end it, since the caller owns the goroutine.
func TestBuilderRefreshRunsUntilCancelled(t *testing.T) {
	cfg := quarterHourCfg
	cfg.RefreshInterval = time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	failing := catalog.Fake{VersionFn: func() (catalog.Version, error) { return catalog.Version("0"), errors.New("boom") }}
	b := NewBuilder(failing, segment.Fake{}, cfg)

	done := make(chan struct{})
	go func() {
		defer close(done)
		b.Refresh(ctx)
	}()

	select {
	case <-done:
		t.Fatal("Refresh returned on a failed build")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Refresh outlived its context")
	}
}

// TestBuilderCountsAMultiBinSegmentFromItsRows: rows and their level values in one 15s bin land
// there however long the descriptor's range, read from one GET per column per row group beyond
// the footer, and a failed column read leaves the segment to the next build.
func TestBuilderCountsAMultiBinSegmentFromItsRows(t *testing.T) {
	sb := segment.NewBuilder(segment.BuilderConfig{Stream: segment.Stream{Service: "svc"}, CreatedAt: storeBase})
	for i := range 10 {
		require.NoError(t, sb.Append(segment.LogRecord(storeBase.Add(time.Duration(i)*time.Second), "info", "m", nil),
			segment.ParsedPattern{PatternID: "tmpl-1"}))
	}
	sealed := sb.Seal()
	obj, err := sealed.Encode()
	require.NoError(t, err)
	var columnGETs atomic.Int64
	var failRange error
	store := storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { columnGETs.Add(1); return obj[off : off+n], failRange },
	}
	want := []CountPoint{{Key: CountKey{PatternID: "tmpl-1", Service: "svc", Bin: storeBase.UnixMilli()}, Lines: 10}}
	wantLevels := []ValuePoint{{PatternID: "tmpl-1", Value: "info", Bin: storeBase.UnixMilli(), Count: 10}}
	build := func(max time.Time) (Builder, error) {
		columnGETs.Store(0)
		desc := sealed.Descriptor
		desc.TimeRange.Max = max
		b := NewBuilder(servingCatalog(1, []segment.Descriptor{desc}), segment.NewReader(store), Config{})
		return b, b.Build(context.Background())
	}
	for max, wantGETs := range map[time.Time]int64{storeBase.Add(14 * time.Second): 0, storeBase.Add(time.Hour): 1} {
		b, err := build(max)
		require.NoError(t, err)
		assert.Equal(t, want, b.Store().CountSlice("", 0, 1<<62))
		assert.Equal(t, wantLevels, b.Store().ValueSlice(segment.ColLevel, 0, 1<<62))
		assert.Equal(t, wantGETs, columnGETs.Load())
	}

	failRange = errors.New("store down")
	b, err := build(storeBase.Add(time.Hour))
	require.Error(t, err)
	assert.False(t, b.Store().HasAdded(sealed.Descriptor.ID))
}

// mergedHour is a 45-row segment's descriptor, the same segment as a merged hour whose rollup
// rollupCfg built, and a store of both counting its reads by key.
func mergedHour(t *testing.T, rollupCfg Config) (sealed, merged segment.Descriptor, store storage.Fake, reads map[string]int) {
	sb := segment.NewBuilder(segment.BuilderConfig{Stream: segment.Stream{Service: "svc"}, CreatedAt: storeBase})
	for i := range 45 {
		r := segment.LogRecord(storeBase.Add(time.Duration(i)*time.Second), "info", "m", map[string]string{segment.ColTraceID: fmt.Sprint("t", i%7)})
		duration := segment.Column{Name: segment.ColDurationMs, Type: value.TypeFloat, Value: value.Value{Float: float64(i * i)}}
		require.NoError(t, sb.Append(r, segment.ParsedPattern{PatternID: "tmpl-1", Columns: []segment.Column{duration}}))
	}
	built := sb.Seal()
	obj, err := built.Encode()
	require.NoError(t, err)
	var rollup []byte
	reads = map[string]int{}
	store = storage.Fake{
		GetSuffixFn: func(key string, n int64) ([]byte, error) {
			reads[key]++
			body := map[bool][]byte{false: obj, true: rollup}[key == "hour.rollup"]
			return body[max(0, int64(len(body))-n):], nil
		},
		GetRangeFn: func(key string, off, n int64) ([]byte, error) { reads[key]++; return obj[off : off+n], nil },
	}
	footer, _, err := segment.NewReader(store).Footer(context.Background(), built.Descriptor.Object)
	require.NoError(t, err)
	r, err := BuildRollup(context.Background(), segment.NewReader(store), built.Descriptor, footer, rollupCfg)
	require.NoError(t, err)
	rollup, err = r.Encode()
	require.NoError(t, err)
	merged = built.Descriptor
	merged.Rollup.Key = "hour.rollup"
	return built.Descriptor, merged, store, reads
}

// TestBuilderAddsAMergedHourFromItsRollup: a merged hour's rollup gives the summary what its
// footer and rows would, with a distribution per bin, and nothing of the segment is read.
func TestBuilderAddsAMergedHourFromItsRollup(t *testing.T) {
	ctx := context.Background()
	cfg := Config{SketchColumns: []string{segment.ColDurationMs}}
	sealed, merged, store, reads := mergedHour(t, cfg)
	full := NewBuilder(servingCatalog(1, []segment.Descriptor{sealed}), segment.NewReader(store), cfg)
	require.NoError(t, full.Build(ctx))

	clear(reads)
	b := NewBuilder(servingCatalog(1, []segment.Descriptor{merged}), segment.NewReader(store), cfg)
	require.NoError(t, b.Build(ctx))
	assert.Equal(t, map[string]int{"hour.rollup": 1}, reads)
	assert.Equal(t, full.Store().CountSlice("", 0, 1<<62), b.Store().CountSlice("", 0, 1<<62))
	assert.Equal(t, full.Store().ValueSlice(segment.ColLevel, 0, 1<<62), b.Store().ValueSlice(segment.ColLevel, 0, 1<<62))
	assert.Equal(t, full.Store().TopologyView(topology.Filter{}), b.Store().TopologyView(topology.Filter{}))
	points, err := b.Store().SketchSlice("", 0, 1<<62)
	require.NoError(t, err)
	var p95s []float64
	for _, p := range points {
		p95s = append(p95s, math.Round(p.Quantiles[segment.ColDurationMs].Quantile(0.95)))
	}
	assert.Equal(t, []float64{169, 789, 1863}, p95s, "each bin's own p95, near 13², 28² and 43²")
}

// TestBuilderAnswersASketchColumnTheRollupLacksFromItsFooter: a sketch column a merged hour's
// rollup was not built for is answered from the rollup's footer, as a footer-built summary does.
func TestBuilderAnswersASketchColumnTheRollupLacksFromItsFooter(t *testing.T) {
	ctx := context.Background()
	sealed, merged, store, _ := mergedHour(t, Config{SketchColumns: []string{segment.ColTraceID}})
	cfg := Config{SketchColumns: []string{segment.ColTraceID, segment.ColDurationMs}}
	full := NewBuilder(servingCatalog(1, []segment.Descriptor{sealed}), segment.NewReader(store), cfg)
	require.NoError(t, full.Build(ctx))
	b := NewBuilder(servingCatalog(1, []segment.Descriptor{merged}), segment.NewReader(store), cfg)
	require.NoError(t, b.Build(ctx))

	want, err := full.Store().SketchSlice("", 0, 1<<62)
	require.NoError(t, err)
	got, err := b.Store().SketchSlice("", 0, 1<<62)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range want {
		require.NotNil(t, got[i].Quantiles[segment.ColDurationMs], "bin %d", i)
		assert.Equal(t, want[i].Quantiles[segment.ColDurationMs], got[i].Quantiles[segment.ColDurationMs], "bin %d", i)
		assert.Equal(t, want[i].Distinct[segment.ColDurationMs], got[i].Distinct[segment.ColDurationMs], "bin %d", i)
	}
}
