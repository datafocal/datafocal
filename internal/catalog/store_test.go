package catalog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func desc(id, service string, min, max time.Time) segment.Descriptor {
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: service, Labels: map[string]string{"env": "prod"}},
		TimeRange: segment.TimeRange{Min: min, Max: max},
		Rows:      100,
		Services:  []string{service},
		Object:    segment.ObjectLocation{Provider: "fs", Key: "acme/" + id + ".seg"},
		CreatedAt: min,
	}
}

// sealedDesc describes a segment holding one row per entry of rowServices, an empty
// entry being a row with no service column, under a stream whose service may differ.
func sealedDesc(id, streamService string, rowServices []string) segment.Descriptor {
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	var services []string
	for _, s := range rowServices {
		if s != "" && !slices.Contains(services, s) {
			services = append(services, s)
		}
	}
	slices.Sort(services)
	return segment.Descriptor{
		ID:        id,
		Stream:    segment.Stream{Service: streamService},
		TimeRange: segment.TimeRange{Min: day, Max: day.Add(time.Hour)},
		Rows:      len(rowServices),
		Services:  services,
		Object:    segment.ObjectLocation{Provider: "fs", Key: "acme/" + id + ".seg"},
		CreatedAt: day,
	}
}

func ids(ds []segment.Descriptor) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	sort.Strings(out)
	return out
}

func TestFSCatalogPrune(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w := catalog.New(storage.NewFS(dir), "acme")
	r := catalog.New(storage.NewFS(dir), "acme")

	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	// checkout: [00:00, 01:00] and [02:00, 03:00]; payments: [00:30, 01:30].
	descs := []segment.Descriptor{
		desc("a", "checkout", day, day.Add(time.Hour)),
		desc("b", "payments", day.Add(30*time.Minute), day.Add(90*time.Minute)),
		desc("c", "checkout", day.Add(2*time.Hour), day.Add(3*time.Hour)),
	}
	for _, d := range descs {
		if err := w.Append(ctx, d); err != nil {
			t.Fatalf("append %s: %v", d.ID, err)
		}
	}
	if err := catalog.New(storage.NewFS(dir), "other").Append(ctx, desc("d", "checkout", day, day.Add(time.Hour))); err != nil {
		t.Fatalf("append d: %v", err)
	}

	version, err := r.Version(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if version != "3" { // only acme's three records
		t.Fatalf("version = %s, want 3", version)
	}

	// Time-only prune over [00:15, 02:15]: a and b overlap, c starts at 02:00
	// (overlaps), so a, b, c all survive; only tenant "acme" is read.
	got, err := r.Prune(ctx, version, selector.Resolved{
		Window: selector.Window{From: day.Add(15 * time.Minute), To: day.Add(135 * time.Minute)},
	})
	if err != nil {
		t.Fatalf("prune time: %v", err)
	}
	if want := []string{"a", "b", "c"}; !equal(ids(got), want) {
		t.Fatalf("time prune = %v, want %v", ids(got), want)
	}

	// Tighter window [00:15, 00:45]: only a and b overlap.
	got, err = r.Prune(ctx, version, selector.Resolved{
		Window: selector.Window{From: day.Add(15 * time.Minute), To: day.Add(45 * time.Minute)},
	})
	if err != nil {
		t.Fatalf("prune tight: %v", err)
	}
	if want := []string{"a", "b"}; !equal(ids(got), want) {
		t.Fatalf("tight prune = %v, want %v", ids(got), want)
	}

	// Time + service=checkout across the whole day: a and c (not payments b).
	got, err = r.Prune(ctx, version, selector.Resolved{
		Window: selector.Window{From: day, To: day.Add(4 * time.Hour)},
		Where:  []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "checkout"}},
	})
	if err != nil {
		t.Fatalf("prune service: %v", err)
	}
	if want := []string{"a", "c"}; !equal(ids(got), want) {
		t.Fatalf("service prune = %v, want %v", ids(got), want)
	}

	// Label predicate that no segment satisfies prunes everything.
	got, err = r.Prune(ctx, version, selector.Resolved{
		Where: []selector.Predicate{{Field: "env", Op: selector.OpEq, Value: "staging"}},
	})
	if err != nil {
		t.Fatalf("prune label: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("label prune = %v, want none", ids(got))
	}

	// A predicate over a field the manifest does not carry is conservative: it
	// prunes nothing (no time bound = all three acme segments).
	got, err = r.Prune(ctx, version, selector.Resolved{
		Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: "xyz"}},
	})
	if err != nil {
		t.Fatalf("prune unknown field: %v", err)
	}
	if want := []string{"a", "b", "c"}; !equal(ids(got), want) {
		t.Fatalf("unknown-field prune = %v, want %v", ids(got), want)
	}
}

func TestFSCatalogEmpty(t *testing.T) {
	ctx := context.Background()
	r := catalog.New(storage.NewFS(t.TempDir()), "acme")
	version, err := r.Version(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if version != "0" {
		t.Fatalf("version = %s, want 0", version)
	}
	got, err := r.Prune(ctx, version, selector.Resolved{})
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("prune = %v, want empty", got)
	}
}

// TestFSCatalogSnapshotPin: a pinned version reads what was committed by then, so a later
// append is unseen, a merge hides its inputs only from the versions that include it, and a
// merge committed twice is read once.
func TestFSCatalogSnapshotPin(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w := catalog.New(storage.NewFS(dir), "acme")
	r := catalog.New(storage.NewFS(dir), "acme")

	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	merged := desc("ab", "checkout", day, day.Add(time.Hour))
	merged.Supersedes = []string{"a", "b"}
	for _, d := range []segment.Descriptor{desc("a", "checkout", day, day.Add(time.Hour)), desc("b", "checkout", day, day.Add(time.Hour)), merged, merged} {
		require.NoError(t, w.Append(ctx, d))
	}
	for _, tc := range []struct {
		version catalog.Version
		want    []string
	}{{"1", []string{"a"}}, {"2", []string{"a", "b"}}, {"3", []string{"ab"}}, {"4", []string{"ab"}}} {
		got, err := r.Prune(ctx, tc.version, selector.Resolved{})
		require.NoError(t, err)
		assert.Equal(t, tc.want, ids(got), "version %s", tc.version)
	}
}

func TestFSCatalogRoundTripsTypedZoneMaps(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w := catalog.New(storage.NewFS(dir), "acme")
	r := catalog.New(storage.NewFS(dir), "acme")

	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	want := segment.Descriptor{
		ID:        "typed-zonemap",
		Stream:    segment.Stream{Service: "checkout", Labels: map[string]string{"env": "prod"}},
		TimeRange: segment.TimeRange{Min: day, Max: day.Add(time.Hour)},
		Rows:      100,
		ZoneMaps: segment.ZoneMaps{
			"service": {
				Type: value.TypeString,
				Min:  value.Value{String: "checkout"},
				Max:  value.Value{String: "checkout"},
			},
			"duration_ms": {
				Type: value.TypeRepeated | value.TypeInt,
				Min:  value.Value{Int: 2},
				Max:  value.Value{Int: 10},
			},
		},
		Object:    segment.ObjectLocation{Provider: "fs", Key: "acme/typed-zonemap.seg"},
		CreatedAt: day,
	}

	require.NoError(t, w.Append(ctx, want))
	version, err := r.Version(ctx)
	require.NoError(t, err)
	got, err := r.Prune(ctx, version, selector.Resolved{})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, want, got[0])
	assert.True(t, got[0].ZoneMaps["duration_ms"].Type.Repeated())
	assert.Equal(t, value.TypeInt, got[0].ZoneMaps["duration_ms"].Type.Scalar())
}

// TestFSCatalogPrunesNumericLabelsNumerically is the regression test for the drift
// recorded on matchOp (store.go): a numeric stream label compared as text prunes
// away segments the scan would keep, because "1000" sorts below "999" as text and
// above it as a number.
func TestFSCatalogPrunesNumericLabelsNumerically(t *testing.T) {
	tests := []struct {
		name  string
		op    selector.Op
		value string
		want  bool
	}{
		{name: "gt below the label keeps it", op: selector.OpGt, value: "999", want: true},
		{name: "ge at the label keeps it", op: selector.OpGe, value: "1000", want: true},
		{name: "lt above the label keeps it", op: selector.OpLt, value: "1001", want: true},
		{name: "lt below the label prunes it", op: selector.OpLt, value: "999", want: false},
		{name: "gt above the label prunes it", op: selector.OpGt, value: "1000", want: false},
	}

	ctx := context.Background()
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	d := desc("numeric-label", "checkout", day, day.Add(time.Hour))
	d.Stream.Labels = map[string]string{"status": "1000"}
	require.NoError(t, catalog.New(storage.NewFS(dir), "acme").Append(ctx, d))
	r := catalog.New(storage.NewFS(dir), "acme")
	version, err := r.Version(ctx)
	require.NoError(t, err)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := r.Prune(ctx, version, selector.Resolved{
				Where: []selector.Predicate{{Field: "status", Op: tc.op, Value: tc.value}},
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, len(got) == 1, "status %s %s against label 1000", tc.op, tc.value)
		})
	}
}

// TestFSCatalogZoneMapEncodingIsCompact pins that a bound writes only the Value field
// its type selects, because the catalog parses every descriptor on a cache miss.
func TestFSCatalogZoneMapEncodingIsCompact(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	line := appendAndReadManifest(t, ctx, segment.Descriptor{
		ID:        "compact",
		Stream:    segment.Stream{Service: "checkout"},
		TimeRange: segment.TimeRange{Min: day, Max: day.Add(time.Hour)},
		ZoneMaps: segment.ZoneMaps{
			"duration_ms": {Type: value.TypeInt, Min: value.Value{Int: 2}, Max: value.Value{Int: 10}, PresentRows: 100},
			"level":       {Type: value.TypeString, Min: value.Value{String: "error"}, Max: value.Value{String: "info"}, PresentRows: 100},
		},
		Rows:      100,
		CreatedAt: day,
	})

	assert.Contains(t, line, `"zone_maps":{"duration_ms":{"type":2,"min":{"int":2},"max":{"int":10},"present_rows":100},`+
		`"level":{"type":1,"min":{"string":"error"},"max":{"string":"info"},"present_rows":100}}`)
}

func TestFSCatalogManifestEncodingIsDeterministic(t *testing.T) {
	ctx := context.Background()
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	desc := segment.Descriptor{
		ID:        "deterministic",
		Stream:    segment.Stream{Service: "checkout", Labels: map[string]string{"env": "prod", "region": "iad"}},
		TimeRange: segment.TimeRange{Min: day, Max: day.Add(time.Hour)},
		Rows:      100,
		PatternIDs: []string{
			"tmpl_login",
			"tmpl_checkout",
		},
		ZoneMaps: segment.ZoneMaps{
			"duration_ms": {
				Type: value.TypeInt,
				Min:  value.Value{Int: 2},
				Max:  value.Value{Int: 10},
			},
			"service": {
				Type: value.TypeString,
				Min:  value.Value{String: "checkout"},
				Max:  value.Value{String: "checkout"},
			},
		},
		Object:    segment.ObjectLocation{Provider: "fs", Key: "acme/deterministic.seg"},
		CreatedAt: day,
	}

	first := appendAndReadManifest(t, ctx, desc)
	second := appendAndReadManifest(t, ctx, desc)

	assert.Equal(t, first, second)
}

// TestFSCatalogPrunesServiceByRows pins that a service predicate is decided by the
// rows' service column, not the stream's: the two differ for a per-host collector
// and for a stream with no service whose rows carry one as an attribute.
func TestFSCatalogPrunesServiceByRows(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	w := catalog.New(storage.NewFS(dir), "acme")
	for _, d := range []segment.Descriptor{
		sealedDesc("multi", "host-1", []string{"api", "worker", "api"}),
		sealedDesc("unnamed", "", []string{"api", "api"}),
		sealedDesc("partial", "api", []string{"api", ""}),
		sealedDesc("worker", "api", []string{"worker", "worker"}),
		sealedDesc("none", "api", []string{"", ""}),
	} {
		require.NoError(t, w.Append(ctx, d))
	}
	r := catalog.New(storage.NewFS(dir), "acme")
	version, err := r.Version(ctx)
	require.NoError(t, err)

	sel := selector.Resolved{
		Where: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "api"}},
	}
	got, err := r.Prune(ctx, version, sel)
	require.NoError(t, err)
	assert.Equal(t, []string{"multi", "partial", "unnamed"}, ids(got))

	again, err := r.Prune(ctx, version, sel)
	require.NoError(t, err)
	assert.Equal(t, got, again)
}

// TestFSCatalogPruneHasNoFalseNegatives checks Prune against the rows themselves: no
// predicate may drop a multi-source segment holding a row it matches.
func TestFSCatalogPruneHasNoFalseNegatives(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	rng := rand.New(rand.NewPCG(1, 2))
	services := []string{"api", "worker", "db", ""}
	streams := []string{"", "host-1", "api"}
	values := []string{"api", "worker", "db", "ap", "zzz", ""}

	for trial := range 200 {
		rows := make([]string, 1+rng.IntN(5))
		for i := range rows {
			rows[i] = services[rng.IntN(len(services))]
		}
		tenant := fmt.Sprintf("t%d", trial)
		w, r := catalog.New(storage.NewFS(dir), tenant), catalog.New(storage.NewFS(dir), tenant)
		require.NoError(t, w.Append(ctx, sealedDesc("s", streams[rng.IntN(len(streams))], rows)))
		version, err := r.Version(ctx)
		require.NoError(t, err)

		for _, op := range selector.AllOps {
			for _, v := range values {
				p := selector.Predicate{Field: "service", Op: op, Value: v}
				matched := slices.ContainsFunc(rows, func(s string) bool { return p.Parse().Matches(s, s != "") })
				if !matched {
					continue
				}
				got, err := r.Prune(ctx, version, selector.Resolved{Where: []selector.Predicate{p}})
				require.NoError(t, err)
				assert.Len(t, got, 1, "rows %q, service %s %q", rows, op, v)
			}
		}
	}
}

// TestCatalogConcurrentAppends pins that appends racing for the same seq each land in
// their own commit rather than overwriting one another.
func TestCatalogConcurrentAppends(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	const n = 20
	var wg sync.WaitGroup
	for w := range 2 {
		cat := catalog.New(storage.NewFS(dir), "acme")
		wg.Go(func() {
			for i := range n / 2 {
				assert.NoError(t, cat.Append(ctx, desc(fmt.Sprintf("w%d-%d", w, i), "checkout", day, day)))
			}
		})
	}
	wg.Wait()

	r := catalog.New(storage.NewFS(dir), "acme")
	version, err := r.Version(ctx)
	require.NoError(t, err)
	assert.Equal(t, catalog.Version(strconv.Itoa(n)), version)
	got, err := r.Prune(ctx, version, selector.Resolved{})
	require.NoError(t, err)
	assert.Len(t, slices.Compact(ids(got)), n)
}

// TestCatalogAppendClaimsTheSeqAfterALostRace pins that an Append whose seq another
// writer committed after the listing moves on to the next seq.
func TestCatalogAppendClaimsTheSeqAfterALostRace(t *testing.T) {
	var put []string
	store := storage.Fake{
		PutIfAbsentFn: func(key string, _ []byte) error {
			put = append(put, key)
			if key == "acme/catalog/v1/00000000000000000000.json" {
				return storage.ErrExists
			}
			return nil
		},
	}
	require.NoError(t, catalog.New(store, "acme").Append(context.Background(), desc("a", "checkout", time.Time{}, time.Time{})))
	assert.Equal(t, []string{
		"acme/catalog/v1/00000000000000000000.json",
		"acme/catalog/v1/00000000000000000001.json",
	}, put)
}

// TestCatalogAppendListsAfterTheLastCommitSeen pins that a second Append lists only
// the commits after the first's, rather than every commit in the catalog.
func TestCatalogAppendListsAfterTheLastCommitSeen(t *testing.T) {
	var listedAfter []string
	store := storage.Fake{ListFn: func(_, after string) ([]string, error) {
		listedAfter = append(listedAfter, after)
		return nil, nil
	}}
	cat := catalog.New(store, "acme")
	for _, id := range []string{"a", "b"} {
		require.NoError(t, cat.Append(context.Background(), desc(id, "checkout", time.Time{}, time.Time{})))
	}
	assert.Equal(t, []string{"", "acme/catalog/v1/00000000000000000000.json"}, listedAfter)
}

// TestCatalogAppendCommitsSeveralAtOnce pins that descriptors appended together land in
// one commit, which a reader sees whole, beside the single-descriptor commits before it.
func TestCatalogAppendCommitsSeveralAtOnce(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	w := catalog.New(store, "acme")
	require.NoError(t, w.Append(ctx, desc("a", "checkout", day, day)))
	require.NoError(t, w.Append(ctx, desc("b", "checkout", day, day), desc("c", "gateway", day, day)))

	keys, err := store.List(ctx, "acme/catalog/v1/", "")
	require.NoError(t, err)
	assert.Len(t, keys, 2, "two commits")
	r := catalog.New(store, "acme")
	version, err := r.Version(ctx)
	require.NoError(t, err)
	assert.Equal(t, catalog.Version("3"), version)
	got, err := r.Prune(ctx, version, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, ids(got))
}

func appendAndReadManifest(t *testing.T, ctx context.Context, desc segment.Descriptor) string {
	t.Helper()
	store := storage.NewFS(t.TempDir())
	require.NoError(t, catalog.New(store, "acme").Append(ctx, desc))
	b, err := store.GetSuffix(ctx, "acme/catalog/v1/00000000000000000000.json", 1<<20)
	require.NoError(t, err)
	return string(b)
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCatalogAppendRetries(t *testing.T) {
	for _, landed := range []bool{false, true} {
		t.Run(fmt.Sprintf("landed=%t", landed), func(t *testing.T) {
			ctx := t.Context()
			backing := storage.NewFS(t.TempDir())
			puts := 0
			other := desc("other", "checkout", time.Time{}, time.Time{})
			store := storage.Fake{
				PutIfAbsentFn: func(key string, body []byte) error {
					puts++
					if puts == 1 {
						if !landed {
							body, _ = json.Marshal(other)
						}
						require.NoError(t, backing.PutIfAbsent(ctx, key, body))
						return fmt.Errorf("timeout")
					}
					return backing.PutIfAbsent(ctx, key, body)
				},
				GetSuffixFn: func(key string, n int64) ([]byte, error) {
					return backing.GetSuffix(ctx, key, n)
				},
			}
			own := desc("own", "checkout", time.Time{}, time.Time{})
			require.NoError(t, catalog.New(store, "acme").Append(ctx, own))
			reader := catalog.New(backing, "acme")
			v, err := reader.Version(ctx)
			require.NoError(t, err)
			got, err := reader.Prune(ctx, v, selector.Resolved{})
			require.NoError(t, err)
			want := []segment.Descriptor{own}
			if !landed {
				want = append([]segment.Descriptor{other}, want...)
			}
			assert.Equal(t, want, got)
		})
	}
}

// TestCatalogLoadReadsTheNewestSnapshotAndItsTail pins S7: a cold load costs one snapshot
// GET and the commits after it, however many commits precede the snapshot.
func TestCatalogLoadReadsTheNewestSnapshotAndItsTail(t *testing.T) {
	objects := map[string][]byte{}
	var gets, lists int
	store := memStore(objects, &gets, &lists)
	commit := func(seq int) {
		objects[fmt.Sprintf("acme/catalog/v1/%020d.json", seq)] = fmt.Appendf(nil, `{"id":"s%d"}`, seq)
	}
	for seq := range 99_001 {
		commit(seq)
	}
	require.NoError(t, catalog.NewSnapshotter(catalog.New(store, "acme"), catalog.SnapshotConfig{MinCommits: 1}).Snapshot(t.Context()))
	for seq := 99_001; seq < 100_000; seq++ {
		commit(seq)
	}

	gets, lists = 0, 0
	v, err := catalog.New(store, "acme").Version(t.Context())
	require.NoError(t, err)
	assert.Equal(t, catalog.Version("100000"), v)
	assert.Equal(t, 3, lists, "LISTs: the snapshot pointer, the commits, and the writer commits since the snapshot")
	assert.Equal(t, 2+999, gets, "GETs: the snapshot pointer, the snapshot and the commits after it")
}

// TestCatalogSnapshotsDoNotChangePrune checks, over random single- and multi-descriptor
// commits with supersedes, that a catalog read through snapshots prunes as one with none,
// or answers snapshot_expired for a version a snapshot folded.
func TestCatalogSnapshotsDoNotChangePrune(t *testing.T) {
	ctx := t.Context()
	rng := rand.New(rand.NewPCG(7, 11))
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	plain, snapped := storage.NewFS(t.TempDir()), storage.NewFS(t.TempDir())
	writers := []catalog.Catalog{catalog.New(plain, "acme"), catalog.New(snapped, "acme")}
	snapshotter := catalog.NewSnapshotter(catalog.New(snapped, "acme"), catalog.SnapshotConfig{MinCommits: 1})
	sel := selector.Resolved{Window: selector.Window{From: day.Add(7 * time.Hour), To: day.Add(8 * time.Hour)}}
	n := 0
	for range 40 {
		var ds []segment.Descriptor
		for range 1 + rng.IntN(3) {
			d := desc(fmt.Sprint(n), "checkout", day.Add(time.Duration(rng.IntN(9))*time.Hour), day.Add(9*time.Hour))
			if n > 0 && rng.IntN(4) == 0 {
				d.Supersedes = []string{fmt.Sprint(rng.IntN(n))}
			}
			ds, n = append(ds, d), n+1
		}
		for _, w := range writers {
			require.NoError(t, w.Append(ctx, ds...))
		}
		if rng.IntN(4) > 0 {
			continue
		}
		require.NoError(t, snapshotter.Snapshot(ctx))
		want, got := catalog.New(plain, "acme"), catalog.New(snapped, "acme")
		for i := range n + 1 {
			v := catalog.Version(strconv.Itoa(i))
			wantDs, err := want.Prune(ctx, v, sel)
			require.NoError(t, err)
			gotDs, err := got.Prune(ctx, v, sel)
			require.NoError(t, err)
			require.Equalf(t, wantDs, gotDs, "version %s", v)
		}
	}
}

// TestSnapshottersRacingLeaveOneSnapshot pins that two snapshotters folding the same
// commits write one snapshot and both succeed, and that the next waits for MinCommits more.
func TestSnapshottersRacingLeaveOneSnapshot(t *testing.T) {
	ctx := t.Context()
	store := storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	appendN := func(n int) {
		for range n {
			require.NoError(t, cat.Append(ctx, desc("a", "checkout", time.Time{}, time.Time{})))
		}
	}
	appendN(10)
	var wg sync.WaitGroup
	for range 2 {
		snapshotter := catalog.NewSnapshotter(catalog.New(store, "acme"), catalog.SnapshotConfig{MinCommits: 5})
		wg.Go(func() { assert.NoError(t, snapshotter.Snapshot(ctx)) })
	}
	wg.Wait()
	snapshotter := catalog.NewSnapshotter(catalog.New(store, "acme"), catalog.SnapshotConfig{MinCommits: 5})
	appendN(4)
	require.NoError(t, snapshotter.Snapshot(ctx))
	appendN(1)
	require.NoError(t, snapshotter.Snapshot(ctx))

	keys, err := store.List(ctx, "acme/catalog/snapshot/v3/", "")
	require.NoError(t, err)
	assert.Equal(t, []string{
		"acme/catalog/snapshot/v3/00000000000000000000.json.zst",
		"acme/catalog/snapshot/v3/00000000000000000001.json.zst",
	}, keys)
}

// memStore is a Store over objects that counts its GETs and LISTs, a GET of a missing
// object failing as a deleted one does.
func memStore(objects map[string][]byte, gets, lists *int) storage.Fake {
	put := func(key string, body []byte) error { objects[key] = body; return nil }
	return storage.Fake{
		GetSuffixFn: func(key string, _ int64) ([]byte, error) {
			*gets++
			body, ok := objects[key]
			if !ok {
				return nil, fs.ErrNotExist
			}
			return body, nil
		},
		PutFn:         put,
		PutIfAbsentFn: put,
		ListFn: func(prefix, after string) ([]string, error) {
			*lists++
			var keys []string
			for key := range objects {
				if strings.HasPrefix(key, prefix) && key > after {
					keys = append(keys, key)
				}
			}
			slices.Sort(keys)
			return keys, nil
		},
	}
}

// TestCatalogSnapshotKeepsTheRetainedHours pins that, with no grace, a cold load reads a snapshot of the
// retained merged stream-hours and the open hour's raw segments, the same whether the
// history is 90 days or 180, and that what it folded away is never read.
func TestCatalogSnapshotKeepsTheRetainedHours(t *testing.T) {
	ctx := t.Context()
	const retention = 7 * 24 * time.Hour
	now := time.Now()
	decoder, err := zstd.NewReader(nil)
	require.NoError(t, err)
	defer decoder.Close()
	snapshotBytes := func(days int) int {
		objects := map[string][]byte{}
		var gets, lists int
		store, seq := memStore(objects, &gets, &lists), 0
		commit := func(ds ...segment.Descriptor) {
			body, err := json.Marshal(ds)
			require.NoError(t, err)
			objects[fmt.Sprintf("acme/catalog/v1/%020d.json", seq)], seq = body, seq+1
		}
		first := now.Truncate(time.Hour).Add(-time.Duration(days) * 24 * time.Hour)
		for hour := first; ; hour = hour.Add(time.Hour) {
			var raws []segment.Descriptor
			for i := range time.Duration(6) {
				raws = append(raws, desc(fmt.Sprint(hour.Unix(), "-", i), "checkout", hour.Add(i*10*time.Minute), hour.Add((i+1)*10*time.Minute)))
			}
			commit(raws...)
			if hour.Add(time.Hour).After(now) {
				break
			}
			merged := desc(fmt.Sprint(hour.Unix()), "checkout", hour, hour.Add(time.Hour))
			merged.Supersedes = ids(raws)
			commit(merged)
		}
		require.NoError(t, catalog.NewSnapshotter(catalog.NewWithConfig(store, "acme", catalog.Config{Retention: retention}), catalog.SnapshotConfig{}).Snapshot(ctx))

		gets, lists = 0, 0
		cat := catalog.NewWithConfig(store, "acme", catalog.Config{Retention: retention})
		v, err := cat.Version(ctx)
		require.NoError(t, err)
		assert.Equal(t, catalog.Version(strconv.Itoa(seq/2*7+6)), v, "versions count the folded descriptors")
		assert.Equal(t, []int{3, 2}, []int{lists, gets}, "LISTs and GETs: the pointer and the snapshot, the commits and the writer commits listed empty")
		live, err := cat.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		assert.Len(t, live, 7*24+6)
		past, err := cat.Prune(ctx, v, selector.Resolved{Window: selector.Window{From: first, To: first.Add(time.Hour)}})
		require.NoError(t, err)
		assert.Empty(t, past)
		assert.Equal(t, 2, gets, "nothing past retention is read")
		_, err = cat.Prune(ctx, catalog.Version(strconv.Itoa(seq/2*7+5)), selector.Resolved{})
		require.ErrorIs(t, err, catalog.ErrSnapshotExpired)
		// Compression varies with the timestamps and commit numbers, even for equally sized snapshots.
		raw, err := decoder.DecodeAll(objects[string(objects["acme/catalog/newest-snapshot"])], nil)
		require.NoError(t, err)
		return len(raw)
	}
	assert.InDelta(t, snapshotBytes(90), snapshotBytes(180), 16)
}

// TestCatalogLoadsPastAnExpiredSnapshot pins that a cold load whose pointer names a
// snapshot a lifecycle rule deleted folds from the commits and answers what it did with it.
func TestCatalogLoadsPastAnExpiredSnapshot(t *testing.T) {
	ctx := t.Context()
	const retention = 7 * 24 * time.Hour
	now := time.Now().Truncate(time.Hour)
	objects := map[string][]byte{}
	var gets, lists int
	store := memStore(objects, &gets, &lists)
	merged := desc("merged", "checkout", now.Add(-2*time.Hour), now.Add(-time.Hour))
	merged.Supersedes = []string{"raw"}
	w := catalog.NewWithRetention(store, "acme", retention)
	require.NoError(t, w.Append(ctx, desc("old", "checkout", now.Add(-30*24*time.Hour), now.Add(-29*24*time.Hour))))
	require.NoError(t, w.Append(ctx, desc("raw", "checkout", now.Add(-2*time.Hour), now.Add(-time.Hour))))
	require.NoError(t, w.Append(ctx, merged, desc("open", "checkout", now, now.Add(time.Minute))))
	require.NoError(t, catalog.NewSnapshotter(w, catalog.SnapshotConfig{MinCommits: 1}).Snapshot(ctx))
	require.NoError(t, w.Append(ctx, desc("after", "gateway", now, now.Add(time.Minute))))

	read := func() (catalog.Version, []segment.Descriptor) {
		cat := catalog.NewWithRetention(store, "acme", retention)
		v, err := cat.Version(ctx)
		require.NoError(t, err)
		ds, err := cat.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		return v, ds
	}
	wantV, want := read()
	pointed := string(objects["acme/catalog/newest-snapshot"])
	require.Contains(t, objects, pointed)
	delete(objects, pointed)
	gotV, got := read()
	assert.Equal(t, wantV, gotV)
	assert.Equal(t, want, got)
	assert.Equal(t, []string{"after", "merged", "open"}, ids(got))
}

// TestCatalogReadsTheLayoutBeforeRetention pins that a catalog written before snapshots
// had a pointer and segments a tier, with a snapshot under catalog/snapshot/v1/ holding
// every descriptor, answers from its commits.
func TestCatalogReadsTheLayoutBeforeRetention(t *testing.T) {
	ctx := t.Context()
	day := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	store := storage.NewFS(t.TempDir())
	var all []segment.Descriptor
	for i, id := range []string{"a", "b", "c"} {
		d := desc(id, "checkout", day.Add(time.Duration(i)*time.Hour), day.Add(time.Duration(i+1)*time.Hour))
		d.Object.Key = fmt.Sprintf("acme/2026/07/18/%02d/f1/%s.seg", i, id)
		if id == "c" {
			d.Supersedes = []string{"a"}
		}
		body, err := json.Marshal(d)
		require.NoError(t, err)
		require.NoError(t, store.Put(ctx, fmt.Sprintf("acme/catalog/v1/%020d.json", i), body))
		all = append(all, d)
	}
	body, err := json.Marshal(all[:2])
	require.NoError(t, err)
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	require.NoError(t, store.Put(ctx, "acme/catalog/snapshot/v1/00000000000000000001.json.zst", enc.EncodeAll(body, nil)))

	cat := catalog.New(store, "acme")
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	assert.Equal(t, catalog.Version("3"), v)
	got, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, all[1:], got)
}
