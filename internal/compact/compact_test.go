package compact

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

var hour = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)

// build seals a row at each of ats, each its own pattern, as a segment created at createdAt.
func build(t *testing.T, createdAt time.Time, ats ...time.Time) segment.Sealed {
	b := segment.NewBuilder(segment.BuilderConfig{Tenant: "acme", Stream: segment.Stream{Service: "api"}, CreatedAt: createdAt})
	for _, at := range ats {
		id := fmt.Sprint("p", at.Minute())
		require.NoError(t, b.Append(segment.LogRecord(at, "info", "m", nil), segment.ParsedPattern{PatternID: id, Pattern: "text of " + id}))
	}
	sealed := b.Seal()
	sealed.Descriptor.Object.Key = segment.BuildSegmentKey("acme", segment.TierRaw, sealed.Descriptor)
	return sealed
}

// publish writes a one-row segment at at and commits it.
func publish(t *testing.T, store storage.Store, cat catalog.Catalog, at time.Time) segment.Descriptor {
	sealed := build(t, at, at)
	body, err := sealed.Encode()
	require.NoError(t, err)
	require.NoError(t, store.Put(context.Background(), sealed.Descriptor.Object.Key, body))
	require.NoError(t, cat.Append(context.Background(), sealed.Descriptor))
	return sealed.Descriptor
}

// TestRunMergesACompleteHourOnce: an hour's segments become the one segment sealing all
// their rows would, once a later hour's segment completes it and all have settled; a
// failed commit and a repeated pass leave one merge.
func TestRunMergesACompleteHourOnce(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	version := func() catalog.Version {
		v, err := cat.Version(ctx)
		require.NoError(t, err)
		return v
	}
	run := func(cat catalog.Catalog, settled time.Time) error {
		return New(cat, catalog.NewSnapshotter(cat, catalog.SnapshotConfig{MinCommits: 5}), segment.NewReader(store), store, "acme").Run(ctx, settled)
	}
	later := hour.Add(2 * time.Hour)
	ats := []time.Time{hour.Add(time.Minute), hour.Add(2 * time.Minute), hour.Add(30 * time.Minute)}
	var inputs []string
	for _, at := range ats {
		inputs = append(inputs, publish(t, store, cat, at).ID)
	}
	require.NoError(t, run(cat, later))
	assert.Equal(t, catalog.Version("3"), version(), "an hour no later segment completes stays")

	next := publish(t, store, cat, hour.Add(time.Hour))
	require.NoError(t, run(cat, ats[2]))
	assert.Equal(t, catalog.Version("4"), version(), "an hour with a segment created since settled stays")
	require.Error(t, New(catalog.Fake{
		VersionFn: func() (catalog.Version, error) { return cat.Version(ctx) },
		PruneFn: func(v catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
			return cat.Prune(ctx, v, sel)
		},
		AppendFn: func(segment.Descriptor) error { return errors.New("crash") },
	}, catalog.Fake{}, segment.NewReader(store), store, "acme").Run(ctx, later))
	require.NoError(t, run(cat, later))
	require.NoError(t, run(cat, later))
	assert.Equal(t, catalog.Version("5"), version(), "the passes after the crashed one commit one merge")
	snapshots, err := store.List(ctx, "acme/catalog/snapshot/", "")
	require.NoError(t, err)
	assert.Len(t, snapshots, 1, "the pass past MinCommits commits snapshots the catalog")

	want := build(t, ats[2], ats...).Descriptor
	slices.Sort(inputs)
	want.Supersedes = inputs
	want.Object.Key = segment.BuildSegmentKey("acme", segment.TierHour, want)
	want.Rollup.Key = strings.TrimSuffix(want.Object.Key, ".seg") + ".rollup"
	want.RollupFilters.Key = strings.TrimSuffix(want.Object.Key, ".seg") + ".filters"
	got, err := cat.Prune(ctx, version(), selector.Resolved{})
	require.NoError(t, err)
	assert.ElementsMatch(t, []segment.Descriptor{want, next}, got)
	footer, _, err := segment.NewReader(store).Footer(ctx, want.Object)
	require.NoError(t, err)
	text, _ := footer.PatternText("p30")
	assert.Equal(t, "text of p30", text)
	body, _, err := segment.NewReader(store).Rollup(ctx, want.Rollup)
	require.NoError(t, err)
	rollup, err := summary.DecodeRollup(body)
	require.NoError(t, err)
	counts := summary.BinCounts{}
	for _, at := range ats {
		counts[summary.CountKey{PatternID: fmt.Sprint("p", at.Minute()), Service: "api", Bin: at.UnixMilli()}] = 1
	}
	assert.Equal(t, counts, rollup.Counts, "the passes after the crashed one commit one rollup, of every input's rows")
}

// TestRunMergesPastAFailedHour: a store failing one hour's reads costs only that hour, which
// the run names, and the run reads the newest hour first.
func TestRunMergesPastAFailedHour(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewFS(t.TempDir())
	cat := catalog.New(fs, "acme")
	older := publish(t, fs, cat, hour.Add(time.Minute))
	publish(t, fs, cat, hour.Add(2*time.Minute))
	newer := []segment.Descriptor{publish(t, fs, cat, hour.Add(61*time.Minute)), publish(t, fs, cat, hour.Add(62*time.Minute))}
	publish(t, fs, cat, hour.Add(2*time.Hour))
	var (
		mu   sync.Mutex
		read []string
	)
	store := storage.Fake{
		GetSuffixFn: func(key string, n int64) ([]byte, error) {
			mu.Lock()
			read = append(read, key)
			mu.Unlock()
			if key == older.Object.Key {
				return nil, errors.New("read failed")
			}
			return fs.GetSuffix(ctx, key, n)
		},
		GetRangeFn: func(key string, offset, length int64) ([]byte, error) { return fs.GetRange(ctx, key, offset, length) },
		PutFn:      func(key string, body []byte) error { return fs.Put(ctx, key, body) },
	}

	err := New(cat, catalog.Fake{}, segment.NewReader(store), store, "acme").Run(ctx, hour.Add(3*time.Hour))
	require.ErrorContains(t, err, "api hour "+hour.Format(time.RFC3339))
	assert.Contains(t, []string{newer[0].Object.Key, newer[1].Object.Key}, read[0], "the newest hour first")
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	live, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	merged := slices.IndexFunc(live, func(d segment.Descriptor) bool { return len(d.Supersedes) > 0 })
	require.GreaterOrEqual(t, merged, 0, "the newer hour merged")
	assert.ElementsMatch(t, []string{newer[0].ID, newer[1].ID}, live[merged].Supersedes)
	assert.Len(t, live, 4, "the older hour's two segments, the merge and the next hour's")
}

// TestRunRemergesAnHourWithALateSegment: a segment arriving in an hour already merged is merged
// with it again, leaving the hour one rollup holding its rows, which a summary counts once.
func TestRunRemergesAnHourWithALateSegment(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	run := func(settled time.Time) {
		require.NoError(t, New(cat, catalog.Fake{}, segment.NewReader(store), store, "acme").Run(ctx, settled))
	}
	synced := summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})
	lines := func(b summary.Builder) map[string]int64 {
		require.NoError(t, b.Build(ctx))
		out := map[string]int64{}
		for _, p := range b.Store().CountSlice("", 0, 1<<62) {
			out[p.Key.PatternID] += p.Lines
		}
		return out
	}
	publish(t, store, cat, hour.Add(time.Minute))
	publish(t, store, cat, hour.Add(2*time.Minute))
	publish(t, store, cat, hour.Add(time.Hour))
	run(hour.Add(2 * time.Hour))
	lines(synced)

	late := build(t, hour.Add(90*time.Minute), hour.Add(3*time.Minute))
	body, err := late.Encode()
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, late.Descriptor.Object.Key, body))
	require.NoError(t, cat.Append(ctx, late.Descriptor))
	lines(synced)
	run(hour.Add(3 * time.Hour))

	v, err := cat.Version(ctx)
	require.NoError(t, err)
	live, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	live = slices.DeleteFunc(live, func(d segment.Descriptor) bool { return d.Rollup.Key == "" })
	require.Len(t, live, 1, "one rollup for the hour")
	assert.Contains(t, live[0].Supersedes, late.Descriptor.ID)
	rollup, _, err := segment.NewReader(store).Rollup(ctx, live[0].Rollup)
	require.NoError(t, err)
	r, err := summary.DecodeRollup(rollup)
	require.NoError(t, err)
	assert.Equal(t, int64(1), r.Counts[summary.CountKey{PatternID: "p3", Service: "api", Bin: hour.Add(3 * time.Minute).UnixMilli()}], "the rollup holds the late row")
	want := map[string]int64{"p0": 1, "p1": 1, "p2": 1, "p3": 1}
	assert.Equal(t, want, lines(synced), "a summary that counted the late segment skips its merge")
	assert.Equal(t, want, lines(summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})))
}

// TestRunLeavesOutASegmentTooLargeToDecode: a segment with a row group past what a merge
// decodes stays as it is, and the rest of its hour merges without it.
func TestRunLeavesOutASegmentTooLargeToDecode(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	at := hour.Add(time.Minute)
	sealed := build(t, at, at, at.Add(time.Second), at.Add(2*time.Second))
	body, err := sealed.Encode()
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, sealed.Descriptor.Object.Key, body))
	require.NoError(t, cat.Append(ctx, sealed.Descriptor))
	big := sealed.Descriptor
	small := []string{publish(t, store, cat, hour.Add(2*time.Minute)).ID, publish(t, store, cat, hour.Add(3*time.Minute)).ID}
	publish(t, store, cat, hour.Add(time.Hour))
	footer, _, err := segment.NewReader(store).Footer(ctx, big.Object)
	require.NoError(t, err)
	limit := maxInputGroupCells
	t.Cleanup(func() { maxInputGroupCells = limit })
	maxInputGroupCells = footer.LargestGroupCells() - 1

	require.NoError(t, New(cat, catalog.Fake{}, segment.NewReader(store), store, "acme").Run(ctx, hour.Add(2*time.Hour)))
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	live, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	merged := slices.IndexFunc(live, func(d segment.Descriptor) bool { return len(d.Supersedes) > 0 })
	require.GreaterOrEqual(t, merged, 0, "the rest of the hour merged")
	slices.Sort(small)
	assert.Equal(t, small, live[merged].Supersedes)
	assert.True(t, slices.ContainsFunc(live, func(d segment.Descriptor) bool { return d.ID == big.ID }), "the large segment stays")
}

// TestRunMergesSettledWindowsThenTheHour: an hour not yet complete merges each complete window
// with more than one segment, the hour's merge then supersedes them, and a summary that
// counted the raw segments, like one that counts only the merges, counts each row once.
func TestRunMergesSettledWindowsThenTheHour(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	run := func(settled time.Time) []segment.Descriptor {
		c := NewWithConfig(cat, catalog.Fake{}, segment.NewReader(store), store, Config{Tenant: "acme", Window: 5 * time.Minute})
		require.NoError(t, c.Run(ctx, settled))
		v, err := cat.Version(ctx)
		require.NoError(t, err)
		live, err := cat.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		return live
	}
	tiers := func(live []segment.Descriptor) map[string]int {
		out := map[string]int{}
		for _, d := range live {
			out[strings.Split(d.Object.Key, "/")[1]] += len(d.Supersedes)
		}
		return out
	}
	lines := func(b summary.Builder) map[string]int64 {
		require.NoError(t, b.Build(ctx))
		out := map[string]int64{}
		for _, p := range b.Store().CountSlice("", 0, 1<<62) {
			out[p.Key.PatternID] += p.Lines
		}
		return out
	}
	synced := summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})
	for _, minute := range []time.Duration{1, 2, 6, 7, 11} {
		publish(t, store, cat, hour.Add(minute*time.Minute))
	}
	lines(synced)

	assert.Equal(t, map[string]int{"window": 4, "raw": 0}, tiers(run(hour.Add(time.Hour))),
		"the first two windows merge; the third, which no later segment completes, stays")
	lines(synced)
	publish(t, store, cat, hour.Add(time.Hour))
	assert.Equal(t, map[string]int{"hour": 3, "raw": 0}, tiers(run(hour.Add(2*time.Hour))),
		"the hour supersedes its two windows' merges and the segment left")

	want := map[string]int64{"p1": 1, "p2": 1, "p6": 1, "p7": 1, "p11": 1, "p0": 1}
	assert.Equal(t, want, lines(synced))
	assert.Equal(t, want, lines(summary.NewBuilder(cat, segment.NewReader(store), summary.Config{})))
}

// TestMergeReadsRawInputsWholeAndAhead: a merge reads a raw input whole in one GET, and reads
// several at once rather than one after another, but no more than readAhead of its 20, so it
// holds a window of inputs rather than the hour's.
func TestMergeReadsRawInputsWholeAndAhead(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewFS(t.TempDir())
	cat := catalog.New(fs, "acme")
	raw := map[string]bool{}
	for minute := range 20 {
		raw[publish(t, fs, cat, hour.Add(time.Duration(minute+1)*time.Minute)).Object.Key] = true
	}
	publish(t, fs, cat, hour.Add(time.Hour))
	var (
		mu                     sync.Mutex
		reads                  = map[string]int{}
		inFlight, mostInFlight int
	)
	count := func(key string) func() {
		mu.Lock()
		defer mu.Unlock()
		if raw[key] {
			reads[key]++
		}
		inFlight++
		mostInFlight = max(mostInFlight, inFlight)
		return func() {
			time.Sleep(time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
		}
	}
	store := storage.Fake{
		GetSuffixFn: func(key string, n int64) ([]byte, error) { defer count(key)(); return fs.GetSuffix(ctx, key, n) },
		GetRangeFn: func(key string, offset, length int64) ([]byte, error) {
			defer count(key)()
			return fs.GetRange(ctx, key, offset, length)
		},
		PutFn: func(key string, body []byte) error { return fs.Put(ctx, key, body) },
	}

	require.NoError(t, New(cat, catalog.Fake{}, segment.NewReader(store), store, "acme").Run(ctx, hour.Add(2*time.Hour)))
	require.Len(t, reads, 20)
	for key, n := range reads {
		assert.Equal(t, 1, n, "%s: read whole, once", key)
	}
	assert.Greater(t, mostInFlight, 1, "inputs are read several at once, not one after another")
	assert.LessOrEqual(t, mostInFlight, readAhead, "inputs are read a window at a time, not the hour at once")
}

// TestRunFiltersTheHourByEachInputsIDs: the merged hour's filters hold a column an input
// filtered as an id, here req, even where the merge's rows, the other input's req repeating,
// no longer make it one.
func TestRunFiltersTheHourByEachInputsIDs(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	for i, reqs := range [][]string{{"r-0", "r-1", "r-2", "r-3", "r-4", "r-5", "r-6", "r-7"}, slices.Repeat([]string{"r-8"}, 40)} {
		at := hour.Add(time.Duration(i+1) * time.Minute)
		b := segment.NewBuilder(segment.BuilderConfig{Tenant: "acme", Stream: segment.Stream{Service: "api"}, CreatedAt: at})
		for j, req := range reqs {
			record := segment.LogRecord(at.Add(time.Duration(j)*time.Millisecond), "info", "m", map[string]string{"req": req})
			require.NoError(t, b.Append(record, segment.ParsedPattern{PatternID: "p", Pattern: "m"}))
		}
		sealed := b.Seal()
		sealed.Descriptor.Object.Key = segment.BuildSegmentKey("acme", segment.TierRaw, sealed.Descriptor)
		body, err := sealed.Encode()
		require.NoError(t, err)
		require.NoError(t, store.Put(ctx, sealed.Descriptor.Object.Key, body))
		require.NoError(t, cat.Append(ctx, sealed.Descriptor))
	}
	publish(t, store, cat, hour.Add(time.Hour))
	rd := segment.NewReader(store)
	require.NoError(t, New(cat, catalog.Fake{}, rd, store, "acme").Run(ctx, hour.Add(2*time.Hour)))

	v, err := cat.Version(ctx)
	require.NoError(t, err)
	descs, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	i := slices.IndexFunc(descs, func(d segment.Descriptor) bool { return len(d.Supersedes) > 0 })
	require.GreaterOrEqual(t, i, 0)
	footer, _, err := rd.Footer(ctx, descs[i].Object)
	require.NoError(t, err)
	assert.False(t, footer.HasFilter("req"), "48 rows of 9 values are no id")
	filters, _, err := rd.Filters(ctx, descs[i].RollupFilters)
	require.NoError(t, err)
	require.Contains(t, filters, "req")
	for _, req := range []string{"r-0", "r-7", "r-8"} {
		assert.True(t, filters["req"].MayContain(req), req)
	}
}
