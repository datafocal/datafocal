package collector

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

// flushOne flushes c, which must seal exactly one segment, and returns its descriptor.
func flushOne(t *testing.T, c Collector) segment.Descriptor {
	t.Helper()
	descs, err := c.Flush(context.Background())
	require.NoError(t, err)
	require.Len(t, descs, 1, "Flush sealed one segment")
	return descs[0]
}

func newTestCollector(t *testing.T, store storage.Store, cat catalog.Catalog, opts ...Option) *collector {
	t.Helper()
	cfg := DefaultConfig()
	for _, opt := range append([]Option{WithTenant("tenant-a"), WithService("api")}, opts...) {
		opt(&cfg)
	}
	c, err := New(cfg, Dependencies{
		Store:         store,
		CatalogWriter: cat,
		Clock:         fixedClock{now: time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return c.(*collector)
}

func TestCollectSealsTypedParameterColumnsAsOrdinaryColumns(t *testing.T) {
	ctx := context.Background()
	store := storage.NewFS(t.TempDir())
	c := newTestCollector(t, store, catalog.Fake{})

	_, err := c.Collect(ctx, segment.LogRecord(time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC), "info", "slow query 12.5 ms", nil))
	require.NoError(t, err)

	desc := flushOne(t, c)

	footer, _, err := segment.NewReader(store).Footer(ctx, desc.Object)
	require.NoError(t, err)

	assert.Equal(t, value.TypeFloat, footer.ZoneMaps["duration_ms"].Type.Scalar())
	assert.Equal(t, 12.5, footer.ZoneMaps["duration_ms"].Min.Float)
	assert.Equal(t, 12.5, footer.ZoneMaps["duration_ms"].Max.Float)
	assertColumn(t, ctx, store, desc.Object, "duration_ms", []string{"12.5"})
}

func TestCollectSealsRealSegment(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	store := storage.NewFS(t.TempDir())
	var appended []segment.Descriptor
	cat := catalog.Fake{AppendFn: func(d segment.Descriptor) error { appended = append(appended, d); return nil }}
	c := newTestCollector(t, store, cat)

	// Digit-bearing tokens are masked, so db-1/db-7 and 500/404 each collapse to one
	// pattern and "user login ok" is a third, parameter-free pattern.
	events := []segment.Record{
		segment.LogRecord(time.Time{}, "warn", "connection timeout to db-1", map[string]string{"trace_id": "t1"}),
		segment.LogRecord(time.Time{}, "warn", "connection timeout to db-7", map[string]string{"trace_id": "t2"}),
		segment.LogRecord(time.Time{}, "error", "request 500 failed", map[string]string{"trace_id": "t3"}),
		segment.LogRecord(time.Time{}, "error", "request 404 failed", map[string]string{"trace_id": "t4"}),
		segment.LogRecord(time.Time{}, "info", "user login ok", map[string]string{"trace_id": "t5"}),
	}
	for i, e := range events {
		e.Timestamp = now.Add(time.Duration(i) * time.Second)
		if _, err := c.Collect(ctx, e); err != nil {
			t.Fatalf("Collect(%d) error = %v", i, err)
		}
	}

	desc := flushOne(t, c)

	if desc.Rows != len(events) {
		t.Fatalf("descriptor rows = %d, want %d", desc.Rows, len(events))
	}
	if got := len(desc.PatternIDs); got != 3 {
		t.Fatalf("descriptor pattern ids = %d, want 3", got)
	}
	if len(appended) != 1 || appended[0].ID != desc.ID {
		t.Fatalf("catalog descriptors = %+v, want one matching %q", appended, desc.ID)
	}

	// Footer decoded from the encoded object bytes.
	footer, _, err := segment.NewReader(store).Footer(ctx, desc.Object)
	if err != nil {
		t.Fatalf("Footer() error = %v", err)
	}
	if got := len(footer.Patterns); got != 3 {
		t.Fatalf("footer pattern set size = %d, want 3", got)
	}
	for _, id := range desc.PatternIDs {
		if !footer.Patterns.Contains(id) {
			t.Fatalf("footer pattern set missing %q", id)
		}
	}

	// Zone maps: level spans error..warn; ts is populated and ordered.
	if zm := footer.ZoneMaps["level"]; zm.Type.Scalar() != value.TypeString || zm.Min.String != "error" || zm.Max.String != "warn" {
		t.Fatalf("level zone map = %+v, want {error warn}", zm)
	}
	if zm := footer.ZoneMaps["ts"]; zm.Type.Scalar() != value.TypeTimestamp || zm.Min.Time.IsZero() || zm.Min.Time.After(zm.Max.Time) {
		t.Fatalf("ts zone map = %+v, want non-empty and min<=max", zm)
	}

	// Re-read columns through the reader and check exact per-row values.
	assertColumn(t, ctx, store, desc.Object, "level",
		[]string{"warn", "warn", "error", "error", "info"})
	assertColumn(t, ctx, store, desc.Object, "message",
		[]string{
			"connection timeout to db-1",
			"connection timeout to db-7",
			"request 500 failed",
			"request 404 failed",
			"user login ok",
		})
	assertColumn(t, ctx, store, desc.Object, "service",
		[]string{"api", "api", "api", "api", "api"})
	assertColumn(t, ctx, store, desc.Object, "param_1",
		[]string{"db-1", "db-7", "500", "404", ""})

	tmpl := readColumn(t, ctx, store, desc.Object, "pattern_id")
	if len(tmpl) != len(events) {
		t.Fatalf("pattern_id column len = %d, want %d", len(tmpl), len(events))
	}
	if tmpl[0] != tmpl[1] || tmpl[2] != tmpl[3] {
		t.Fatalf("pattern_id column did not collapse pairs: %v", tmpl)
	}
	if distinct := distinctCount(tmpl); distinct != 3 {
		t.Fatalf("distinct pattern_ids in column = %d, want 3", distinct)
	}

	// Sketches must be real, not empty stand-ins.
	if hll := footer.DistinctCounts["trace_id"]; hll == nil || hll.Estimate() < 3 {
		t.Fatalf("trace_id HLL missing or low estimate: %v", hll)
	}
	if counts, ok := footer.ValueCounts("service"); !ok || counts["api"] != 5 {
		t.Fatalf("service value counts = %v, want api counted 5 times", counts)
	}
}

func TestCollectFlushesOnMaxRows(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	store := storage.NewFS(t.TempDir())
	c := newTestCollector(t, store, catalog.Fake{}, WithMaxRows(1))

	result, err := c.Collect(ctx, segment.LogRecord(now, "error", "request 500 failed", map[string]string{"trace_id": "trace-1"}))
	if err != nil {
		t.Fatalf("Collect() error = %v", err)
	}
	if len(result.Descriptors) != 1 {
		t.Fatalf("Collect() sealed %d segments at MaxRows=1, want 1", len(result.Descriptors))
	}
	desc := result.Descriptors[0]
	if desc.Rows != 1 {
		t.Fatalf("descriptor rows = %d, want 1", desc.Rows)
	}
	if desc.Object.Key == "" {
		t.Fatal("descriptor object key is empty")
	}
	if got := len(desc.PatternIDs); got != 1 {
		t.Fatalf("pattern id count = %d, want 1", got)
	}
	if zm := desc.ZoneMaps["level"]; zm.Min.String != "error" {
		t.Fatalf("level zone map = %#v, want error", desc.ZoneMaps["level"])
	}
	if _, err := store.GetSuffix(ctx, desc.Object.Key, 1); err != nil {
		t.Fatal("flushed segment was not written to the store")
	}
}

// One collector following thirty containers seals one segment per flush, not thirty.
func TestFlushSealsEveryServiceIntoOneSegment(t *testing.T) {
	ctx := context.Background()
	c := newTestCollector(t, storage.NewFS(t.TempDir()), catalog.Fake{})
	var want []string
	for i := range 30 {
		service := fmt.Sprintf("svc-%02d", i)
		want = append(want, service)
		_, err := c.Collect(ctx, segment.LogRecord(time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC), "info", "ok", map[string]string{"service": service}))
		require.NoError(t, err)
	}
	desc := flushOne(t, c)
	assert.Equal(t, want, desc.Services)
}

func TestFlushEmptySegmentIsNoop(t *testing.T) {
	store := storage.NewFS(t.TempDir())
	var appended []segment.Descriptor
	cat := catalog.Fake{AppendFn: func(d segment.Descriptor) error { appended = append(appended, d); return nil }}
	c := newTestCollector(t, store, cat)

	descs, err := c.Flush(context.Background())
	if err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if len(descs) != 0 {
		t.Fatalf("empty flush sealed %d segments, want none", len(descs))
	}
	if got := len(appended); got != 0 {
		t.Fatalf("catalog descriptors = %d, want 0", got)
	}
}

func readColumn(t *testing.T, ctx context.Context, store storage.Store, loc segment.ObjectLocation, col string) []string {
	t.Helper()
	rd := segment.NewReader(store)
	footer, _, err := rd.Footer(ctx, loc)
	require.NoError(t, err)
	columns, _, err := rd.Columns(ctx, loc, footer, 0, nil, col)
	require.NoError(t, err)
	require.Contains(t, columns, col)
	out := make([]string, columns[col].Len())
	for i, v := range columns[col].Rows() {
		out[i] = string(v)
	}
	return out
}

func assertColumn(t *testing.T, ctx context.Context, store storage.Store, loc segment.ObjectLocation, col string, want []string) {
	t.Helper()
	got := readColumn(t, ctx, store, loc, col)
	if len(got) != len(want) {
		t.Fatalf("column %q = %v, want %v", col, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("column %q row %d = %q, want %q", col, i, got[i], want[i])
		}
	}
}

func distinctCount(values []string) int {
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		seen[v] = struct{}{}
	}
	return len(seen)
}

// TestFlushAlignment: with AlignTo set no sealed segment may span a bucket boundary,
// because summary.MergeFooter apportions a straddler across bins and marks every bin it
// touches Estimated. The off case is the control — the same three events do straddle
// without it, which makes the on case evidence about alignment and not about the
// fixture.
func TestFlushAlignment(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	// Two events in the 12:30 bucket and one in 12:31.
	at := []time.Time{base, base.Add(30 * time.Second), base.Add(70 * time.Second)}

	tests := []struct {
		name     string
		alignTo  time.Duration
		wantSeal int
	}{
		{name: "aligned_seals_per_bucket", alignTo: time.Minute, wantSeal: 2},
		{name: "unaligned_keeps_one_straddling_segment", alignTo: 0, wantSeal: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			c := newTestCollector(t, storage.NewFS(t.TempDir()), catalog.Fake{},
				WithFlushAlignment(tt.alignTo), WithMaxRows(1000))

			var sealed []segment.Descriptor
			for _, ts := range at {
				res, err := c.Collect(ctx, segment.LogRecord(ts, "info", "handled request", nil))
				if err != nil {
					t.Fatalf("Collect() error = %v", err)
				}
				sealed = append(sealed, res.Descriptors...)
			}
			final, err := c.Flush(ctx)
			if err != nil {
				t.Fatalf("Flush() error = %v", err)
			}
			sealed = append(sealed, final...)

			if len(sealed) != tt.wantSeal {
				t.Fatalf("sealed %d segments, want %d", len(sealed), tt.wantSeal)
			}
			for _, d := range sealed {
				oneBucket := d.TimeRange.Min.Truncate(time.Minute).Equal(d.TimeRange.Max.Truncate(time.Minute))
				if want := tt.alignTo > 0; oneBucket != want {
					t.Errorf("segment %s spans one bucket = %v, want %v (%s..%s)",
						d.ID, oneBucket, want, d.TimeRange.Min, d.TimeRange.Max)
				}
			}
		})
	}
}

func minutesFrom(base time.Time, n int) []time.Time {
	at := make([]time.Time, n)
	for i := range at {
		at[i] = base.Add(time.Duration(i) * time.Minute)
	}
	return at
}

// TestAlignmentHoldsForInterleavedBuckets: catching up on files read at different speeds
// interleaves their timestamps, and every segment must still lie in one bucket without a
// seal per crossing, which made a segment per record.
func TestAlignmentHoldsForInterleavedBuckets(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	c := newTestCollector(t, storage.NewFS(t.TempDir()), catalog.Fake{}, WithFlushAlignment(time.Minute), WithMaxRows(1000))
	for _, ts := range []time.Time{base, base.Add(5 * time.Minute), base.Add(10 * time.Second), base.Add(5*time.Minute + 10*time.Second), base.Add(20 * time.Second)} {
		res, err := c.Collect(context.Background(), segment.LogRecord(ts, "info", "handled request", nil))
		require.NoError(t, err)
		require.Zero(t, res.Cut, "a crossing cuts nothing")
	}
	descs, err := c.Flush(context.Background())
	require.NoError(t, err)
	var got []segment.TimeRange
	for _, d := range descs {
		got = append(got, d.TimeRange)
	}
	assert.Equal(t, []segment.TimeRange{
		{Min: base, Max: base.Add(20 * time.Second)},
		{Min: base.Add(5 * time.Minute), Max: base.Add(5*time.Minute + 10*time.Second)},
	}, got)
}

// TestCollectReportsWhetherItsRecordWasSealed: a size seal takes the record that filled
// it and a cut at the open-bucket bound is made before it, and a caller saving its read
// position at a seal must know which, or a restart rereads the record or skips it.
func TestCollectReportsWhetherItsRecordWasSealed(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	type outcome struct {
		Seals        int
		RecordSealed bool
	}
	tests := []struct {
		name string
		opts []Option
		at   []time.Time
		want outcome
	}{
		{name: "a buffered record", opts: []Option{WithMaxRows(10)}, at: []time.Time{base, base.Add(time.Second)}, want: outcome{}},
		{name: "a size seal holds its record", opts: []Option{WithMaxRows(2)}, at: []time.Time{base, base.Add(time.Second)}, want: outcome{Seals: 1, RecordSealed: true}},
		{name: "a record in another bucket opens it rather than cutting", opts: []Option{WithMaxRows(10), WithFlushAlignment(time.Minute)},
			at: []time.Time{base, base.Add(time.Minute)}, want: outcome{}},
		{name: "a batch at the open-bucket bound is cut before its record", opts: []Option{WithMaxRows(100), WithFlushAlignment(time.Minute)},
			at: minutesFrom(base, maxOpenBuckets+1), want: outcome{Seals: maxOpenBuckets}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCollector(t, storage.NewFS(t.TempDir()), catalog.Fake{}, tt.opts...)
			var last CollectResult
			for _, ts := range tt.at {
				var err error
				if last, err = c.Collect(context.Background(), segment.LogRecord(ts, "info", "handled request", nil)); err != nil {
					t.Fatalf("Collect() error = %v", err)
				}
			}
			if got := (outcome{Seals: len(last.Descriptors), RecordSealed: last.RecordSealed}); got != tt.want {
				t.Fatalf("last Collect() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// The pattern comes from the message, so key order and other field values do not split it.
func TestJSONLinePatternComesFromItsMessage(t *testing.T) {
	ctx := context.Background()
	p, err := logformat.New(logformat.Config{Format: "json"})
	require.NoError(t, err)
	for _, tc := range []struct {
		name     string
		lines    [2]string
		wantSame bool
	}{
		{name: "same message", lines: [2]string{`{"service":"nginx","msg":"request done"}`, `{"msg":"request done","service":"checkout"}`}, wantSame: true},
		{name: "different message", lines: [2]string{`{"service":"nginx","msg":"request done"}`, `{"service":"nginx","msg":"disk full"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := storage.NewFS(t.TempDir())
			c := newTestCollector(t, store, catalog.Fake{})
			for _, line := range tc.lines {
				r, _ := p.Parse(line)
				_, err := c.Collect(ctx, r)
				require.NoError(t, err)
			}
			desc := flushOne(t, c)
			ids := readColumn(t, ctx, store, desc.Object, segment.ColPatternID)
			assert.Equal(t, tc.wantSame, ids[0] == ids[1], "pattern_ids %v", ids)
		})
	}
}

// A message's pattern_id is the masked message alone, which is what lets
// the seeded corpus name its patterns without a collector.
func TestMessagePatternIDIsMaskedMessage(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	store := storage.NewFS(t.TempDir())
	c := newTestCollector(t, store, catalog.Fake{})

	messages := []string{"connection timeout to db-1", "connection timeout to db-7", "request 500 failed", "user login ok", ""}
	var want []string
	for i, m := range messages {
		text := pattern.Mask(m)
		if m == "" {
			text = "{}"
		}
		want = append(want, pattern.ID(text))
		_, err := c.Collect(ctx, segment.LogRecord(now.Add(time.Duration(i)*time.Second), "info", m, map[string]string{"host": "a"}))
		require.NoError(t, err)
	}
	desc := flushOne(t, c)
	assert.Equal(t, want, readColumn(t, ctx, store, desc.Object, segment.ColPatternID))
}

// Every event brings a new pattern and a new column name, so a budget of two
// breaches once into column names and again into the catch-all.
func TestPatternGuardDegradesHighEntropyStream(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	store := storage.NewFS(t.TempDir())
	c := newTestCollector(t, store, catalog.Fake{})
	p := pattern.New(2)
	c.pattern = p

	collect := func(from, to int) segment.Footer {
		for i := from; i < to; i++ {
			_, err := c.Collect(ctx, segment.LogRecord(now.Add(time.Duration(i)*time.Second), "", strings.Repeat("word ", i+1), map[string]string{"key_" + strconv.Itoa(i): "v"}))
			require.NoError(t, err)
		}
		desc := flushOne(t, c)
		footer, _, err := segment.NewReader(store).Footer(ctx, desc.Object)
		require.NoError(t, err)
		return footer
	}

	want := segment.PatternProfile{Name: "auto", Version: 1, Components: []string{"extract(message)", "structure"}}
	want.Degradation = pattern.LevelColumnNames
	assert.Equal(t, want, collect(0, 3).PatternProfile)
	want.Degradation = pattern.LevelCatchAll
	assert.Equal(t, want, collect(3, 6).PatternProfile)
}

// TestSealInBackgroundCommitsWhatInlineDoes: sealing in the background must publish the
// same segments in the same order as sealing inline, each reported once, though Collect
// returns a cut before its segment is durable.
func TestSealInBackgroundCommitsWhatInlineDoes(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC)
	run := func(background bool) (committed, reported []segment.Descriptor, cuts int) {
		var mu sync.Mutex
		cat := catalog.Fake{AppendFn: func(d segment.Descriptor) error {
			mu.Lock()
			defer mu.Unlock()
			committed = append(committed, d)
			return nil
		}}
		c := newTestCollector(t, storage.NewFS(t.TempDir()), cat, WithMaxRows(2), WithFlushAlignment(time.Minute),
			func(cfg *Config) { cfg.SealInBackground = background })
		// Two in 12:30, then a minute later, so both a size and an alignment cut fall.
		for _, ts := range []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second), base.Add(time.Minute), base.Add(61 * time.Second)} {
			res, err := c.Collect(context.Background(), segment.LogRecord(ts, "info", "handled request", nil))
			require.NoError(t, err)
			reported = append(reported, res.Descriptors...)
			cuts += res.Cut
		}
		last, err := c.Flush(context.Background())
		require.NoError(t, err)
		reported = append(reported, last...)
		return committed, reported, cuts
	}
	wantCommitted, _, wantCuts := run(false)
	committed, reported, cuts := run(true)
	assert.Equal(t, wantCommitted, committed)
	assert.Equal(t, wantCuts, cuts)
	// Each segment is reported once, by the Collect or Flush during which it became durable.
	assert.Equal(t, wantCommitted, reported)
}

func TestRetryingSealBoundsWaitAndBatches(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		offline := fmt.Errorf("offline")
		c := newTestCollector(t, storage.Fake{PutFn: func(string, []byte) error { return offline }}, catalog.Fake{},
			WithMaxRows(1), WithSealRetry(time.Second), func(cfg *Config) { cfg.SealInBackground = true })
		for range maxSealing {
			_, err := c.Collect(t.Context(), segment.LogRecord(time.Now(), "info", "sealing", nil))
			require.NoError(t, err)
		}
		synctest.Wait()
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		_, err := c.Flush(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		done := make(chan error, 1)
		go func() {
			_, err := c.Collect(t.Context(), segment.LogRecord(time.Now(), "info", "two", nil))
			done <- err
		}()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("next cut returned before seal ended: %v", err)
		default:
		}
		assert.EqualValues(t, 1, c.openStats().Rows)
		require.ErrorIs(t, <-done, offline)
	})
}

// TestCollectAllocations pins the allocations of one Collect into an open batch, under CI's
// toolchain and -race; lower the budget when a change removes one.
func TestCollectAllocations(t *testing.T) {
	c := newTestCollector(t, storage.Discard, catalog.Discard)
	r := segment.LogRecord(time.Date(2026, 7, 18, 12, 30, 0, 0, time.UTC), "error", "query took 12.5 ms on shard 7", map[string]string{"method": "GET"})
	got := testing.AllocsPerRun(1000, func() {
		_, err := c.Collect(context.Background(), r)
		require.NoError(t, err)
	})
	assert.LessOrEqual(t, got, 15.0)
}
