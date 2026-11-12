package plan

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
)

// footersByKey is a Reader serving footers by object key.
func footersByKey(footers map[string]segment.Footer) segment.Fake {
	return segment.Fake{FooterFn: func(loc segment.ObjectLocation) (segment.Footer, segment.Cost, error) {
		return footers[loc.Key], segment.Cost{GETs: 1}, nil
	}}
}

func descWithKey(id, key string) segment.Descriptor {
	return segment.Descriptor{ID: id, Object: segment.ObjectLocation{Key: key}}
}

// --- deterministic unit tests ----------------------------------------------

func TestPrune(t *testing.T) {
	a, b, c := descWithKey("A", "a"), descWithKey("B", "b"), descWithKey("C", "c")
	catalogDown := errors.New("catalog down")
	latencies := sketch.NewDDSketch(0.01)
	for _, v := range []float64{10, 20, 30, 40, 50} {
		latencies.Add(v)
	}
	where := func(preds ...selector.Predicate) selector.Resolved {
		return selector.Resolved{Window: boundedWindow(true), Where: preds}
	}
	levelError := selector.Predicate{Field: "level", Op: selector.OpEq, Value: "error"}

	tests := []struct {
		name     string
		descs    []segment.Descriptor
		pruneErr error
		footers  map[string]segment.Footer
		sel      selector.Resolved
		want     Plan
		wantErr  error
	}{
		{
			name:    "no predicates keeps every row group",
			descs:   []segment.Descriptor{a},
			footers: map[string]segment.Footer{"a": {RowGroups: 1}},
			sel:     selector.Resolved{Window: boundedWindow(true)},
			want:    Plan{Cost: query.Cost{GETs: 1, FootersDecoded: 1, Segments: 1}, Items: []WorkItem{{Segment: a, Footer: segment.Footer{RowGroups: 1}}}},
		},
		{
			name:  "a descriptor settling a predicate false costs no footer",
			descs: []segment.Descriptor{{ID: "D", Stream: segment.Stream{Labels: map[string]string{"level": "info"}}}},
			sel:   where(levelError),
			want:  Plan{},
		},
		{
			name: "match without a time bound is an unbounded scan",
			sel:  selector.Resolved{Where: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "boom"}}},
			wantErr: &query.APIError{
				Code:    query.ErrCodeUnboundedScan,
				Message: "match predicates require a bounded time range; unbounded full-text search is not supported",
			},
		},
		{
			name:  "present set prunes a segment without the pattern",
			descs: []segment.Descriptor{a, b},
			footers: map[string]segment.Footer{
				"a": {RowGroups: 2, Patterns: presentSet("tmpl_1")},
				"b": {RowGroups: 2, Patterns: presentSet("tmpl_9")},
			},
			sel: where(selector.Predicate{Field: "pattern_id", Op: selector.OpEq, Value: "tmpl_1"}),
			want: Plan{Cost: query.Cost{GETs: 2, FootersDecoded: 2, Segments: 1}, Items: []WorkItem{
				{Segment: a, Footer: segment.Footer{RowGroups: 2, Patterns: presentSet("tmpl_1")}, RowGroup: 0},
				{Segment: a, Footer: segment.Footer{RowGroups: 2, Patterns: presentSet("tmpl_1")}, RowGroup: 1},
			}},
		},
		{
			name:    "one footer GET per catalog-pruned descriptor",
			descs:   []segment.Descriptor{a, b, c},
			footers: map[string]segment.Footer{"a": {RowGroups: 1}, "b": {RowGroups: 1}, "c": {RowGroups: 1}},
			sel:     where(levelError),
			want: Plan{Cost: query.Cost{GETs: 3, FootersDecoded: 3, Segments: 3}, Items: []WorkItem{
				{Segment: a, Footer: segment.Footer{RowGroups: 1}},
				{Segment: b, Footer: segment.Footer{RowGroups: 1}},
				{Segment: c, Footer: segment.Footer{RowGroups: 1}},
			}},
		},
		{
			name:    "items follow catalog order with the sorted union of predicate columns",
			descs:   []segment.Descriptor{a, b},
			footers: map[string]segment.Footer{"a": {RowGroups: 2}, "b": {RowGroups: 3}},
			sel:     where(selector.Predicate{Field: "service", Op: selector.OpEq, Value: "api"}, levelError),
			want: Plan{Cost: query.Cost{GETs: 2, FootersDecoded: 2, Segments: 2}, Items: []WorkItem{
				{Segment: a, Footer: segment.Footer{RowGroups: 2}, RowGroup: 0},
				{Segment: a, Footer: segment.Footer{RowGroups: 2}, RowGroup: 1},
				{Segment: b, Footer: segment.Footer{RowGroups: 3}, RowGroup: 0},
				{Segment: b, Footer: segment.Footer{RowGroups: 3}, RowGroup: 1},
				{Segment: b, Footer: segment.Footer{RowGroups: 3}, RowGroup: 2},
			}},
		},
		{
			name:  "row-group zone maps keep only the row group that may match",
			descs: []segment.Descriptor{a},
			footers: map[string]segment.Footer{"a": {
				RowGroups:        3,
				RowGroupZoneMaps: groupZoneMaps("info", "error", "info"),
			}},
			sel: where(levelError),
			want: Plan{Cost: query.Cost{GETs: 1, FootersDecoded: 1, Segments: 1}, Items: []WorkItem{
				{Segment: a, Footer: segment.Footer{RowGroups: 3, RowGroupZoneMaps: groupZoneMaps("info", "error", "info")}, RowGroup: 1},
			}},
		},
		{
			name:  "latency distribution prunes an out-of-range segment",
			descs: []segment.Descriptor{a},
			footers: map[string]segment.Footer{"a": {
				RowGroups:     1,
				Distributions: map[string]*sketch.DDSketch{"duration_ms": latencies},
			}},
			sel:  where(selector.Predicate{Field: "duration_ms", Op: selector.OpGt, Value: "1000"}),
			want: Plan{Cost: query.Cost{GETs: 1, FootersDecoded: 1}},
		},
		{
			name:     "catalog error is wrapped",
			pruneErr: catalogDown,
			sel:      where(levelError),
			wantErr:  fmt.Errorf("plan: prune: %w", catalogDown),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cat := catalog.Fake{PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) {
				return tt.descs, tt.pruneErr
			}}
			rd := footersByKey(tt.footers)

			got, err := New(Deps{Catalog: cat, Reader: rd}).Prune(context.Background(), catalog.Version("0"), tt.sel)
			assert.Equal(t, tt.wantErr, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPruneAsksFiltersOnEquality: an equality predicate on a filtered column reads its
// filter section and drops the segment it rules out, and no other predicate reads one.
func TestPruneAsksFiltersOnEquality(t *testing.T) {
	a, b := descWithKey("A", "a"), descWithKey("B", "b")
	footer := segment.FooterWithFilters(segment.Footer{RowGroups: 1}, "order_id")
	tests := []struct {
		name      string
		op        selector.Op
		want      Plan
		wantReads int
	}{
		{name: "eq", op: selector.OpEq, wantReads: 2, want: Plan{
			Cost:  query.Cost{GETs: 4, BytesRead: 20, FootersDecoded: 2, Segments: 1},
			Items: []WorkItem{{Segment: a, Footer: footer}},
		}},
		{name: "ne", op: selector.OpNe, want: Plan{
			Cost:  query.Cost{GETs: 2, FootersDecoded: 2, Segments: 2},
			Items: []WorkItem{{Segment: a, Footer: footer}, {Segment: b, Footer: footer}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reads := 0
			rd := footersByKey(map[string]segment.Footer{"a": footer, "b": footer})
			rd.MayContainFn = func(loc segment.ObjectLocation, _ segment.Footer, _, _ string) (bool, bool, segment.Cost, error) {
				reads++
				return true, loc.Key == "a", segment.Cost{GETs: 1, Bytes: 10}, nil
			}
			sel := selector.Resolved{Window: boundedWindow(true), Where: []selector.Predicate{{Field: "order_id", Op: tt.op, Value: "o-1"}}}

			got, err := New(Deps{Reader: rd}).Plan(context.Background(), []segment.Descriptor{a, b}, sel)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantReads, reads)
		})
	}
}

// TestPlanFooterReadsMatchSerial: reading footers concurrently returns the serial plan,
// its items in the same order and its cost the same.
func TestPlanFooterReadsMatchSerial(t *testing.T) {
	descs := make([]segment.Descriptor, 100)
	footers := make(map[string]segment.Footer, len(descs))
	for i := range descs {
		key := strconv.Itoa(i)
		descs[i] = descWithKey(key, key)
		footers[key] = segment.FooterWithFilters(segment.Footer{
			RowGroups:        3,
			Patterns:         presentSet("tmpl_" + strconv.Itoa(i%3)),
			RowGroupZoneMaps: groupZoneMaps("info", []string{"info", "error"}[i%2], "error"),
		}, "order_id")
	}
	rd := footersByKey(footers)
	rd.MayContainFn = func(loc segment.ObjectLocation, _ segment.Footer, _, _ string) (bool, bool, segment.Cost, error) {
		return true, len(loc.Key)%2 == 0, segment.Cost{GETs: 1, Bytes: 10}, nil
	}
	sel := selector.Resolved{Window: boundedWindow(true), Where: []selector.Predicate{
		{Field: "pattern_id", Op: selector.OpEq, Value: "tmpl_1"},
		{Field: "level", Op: selector.OpEq, Value: "error"},
		{Field: "order_id", Op: selector.OpEq, Value: "o-1"},
	}}
	serial, err := New(Deps{Reader: rd}).Plan(context.Background(), descs, sel)
	assert.NoError(t, err)
	assert.NotEmpty(t, serial.Items)

	for _, reads := range []int{1, 4, 64} {
		got, err := New(Deps{Reader: rd, FooterReads: reads}).Plan(context.Background(), descs, sel)
		assert.NoError(t, err, "FooterReads=%d", reads)
		assert.Equal(t, serial, got, "FooterReads=%d", reads)
	}
}

// TestPlanFooterReadErrorWaitsForReaders: a failing footer read is Plan's error, and every
// read Plan started has returned by the time it does.
func TestPlanFooterReadErrorWaitsForReaders(t *testing.T) {
	descs := make([]segment.Descriptor, 64)
	for i := range descs {
		descs[i] = descWithKey(strconv.Itoa(i), strconv.Itoa(i))
	}
	readFailed := errors.New("read failed")
	failed := make(chan struct{})
	var reading atomic.Int32
	rd := segment.Fake{FooterFn: func(loc segment.ObjectLocation) (segment.Footer, segment.Cost, error) {
		reading.Add(1)
		defer reading.Add(-1)
		if loc.Key == "5" {
			defer close(failed)
			return segment.Footer{}, segment.Cost{}, readFailed
		}
		<-failed // in flight while the read fails
		return segment.Footer{RowGroups: 1}, segment.Cost{GETs: 1}, nil
	}}

	_, err := New(Deps{Reader: rd, FooterReads: 8}).Plan(context.Background(), descs, selector.Resolved{Window: boundedWindow(true)})
	assert.ErrorIs(t, err, readFailed)
	assert.Zero(t, reading.Load())
}

// TestPruneAsksRollupsBeforeFooters: a merged hour whose filters rule out the id looked up
// costs the filters' read and no footer.
func TestPruneAsksRollupsBeforeFooters(t *testing.T) {
	a, b := descWithKey("A", "a"), descWithKey("B", "b")
	a.RollupFilters.Key, b.RollupFilters.Key = "o-a", "o-b"
	footer := segment.Footer{RowGroups: 1}
	rd := footersByKey(map[string]segment.Footer{"a": footer, "b": footer})
	rd.FiltersFn = func(loc segment.ObjectLocation) (map[string]*sketch.XorFilter, segment.Cost, error) {
		ids, err := sketch.NewXorFilter([][]byte{[]byte(loc.Key)})
		return map[string]*sketch.XorFilter{"order_id": ids}, segment.Cost{GETs: 1, Bytes: 10}, err
	}
	sel := selector.Resolved{Window: boundedWindow(true), Where: []selector.Predicate{{Field: "order_id", Op: selector.OpEq, Value: "o-a"}}}

	got, err := New(Deps{Reader: rd}).Plan(context.Background(), []segment.Descriptor{a, b}, sel)
	assert.NoError(t, err)
	assert.Equal(t, Plan{Cost: query.Cost{GETs: 3, BytesRead: 20, FootersDecoded: 1, Segments: 1}, Items: []WorkItem{{Segment: a, Footer: footer}}}, got)
}

// TestPruneReadsAMergedHoursFiltersOnce: a needle over a merged hour, on an id by its name or
// by its values alone, reads only its filters object, fewer bytes than its footer, and a repeat
// reads it from the cache.
func TestPruneReadsAMergedHoursFiltersOnce(t *testing.T) {
	ctx := context.Background()
	at := time.Unix(0, 0).UTC()
	b := segment.NewBuilder(segment.BuilderConfig{Stream: segment.Stream{Service: "api"}, CreatedAt: at})
	for i := range 2000 { // a value per row, so trace_id and req are filtered
		r := segment.LogRecord(at.Add(time.Duration(i)*time.Second), "info", "m", map[string]string{segment.ColTraceID: fmt.Sprint("t", i), "req": fmt.Sprint("r", i)})
		assert.NoError(t, b.Append(r, segment.ParsedPattern{PatternID: "p"}))
	}
	sealed := b.Seal()
	body, err := sealed.Encode()
	assert.NoError(t, err)
	desc := sealed.Descriptor
	desc.Object.Key, desc.RollupFilters.Key = "hour.seg", "hour.filters"
	objects := map[string][]byte{desc.Object.Key: body}
	var footerBytes int64
	reads := map[string]int{}
	store := storage.Fake{GetSuffixFn: func(key string, n int64) ([]byte, error) {
		reads[key]++
		tail := objects[key][max(0, int64(len(objects[key]))-n):]
		if key == desc.Object.Key {
			footerBytes += int64(len(tail))
		}
		return tail, nil
	}}
	footer, _, err := segment.NewReader(store).Footer(ctx, desc.Object)
	assert.NoError(t, err)
	filters, err := footer.Filters(body)
	assert.NoError(t, err)
	objects[desc.RollupFilters.Key], err = segment.EncodeFilters(filters)
	assert.NoError(t, err)
	for _, field := range []string{segment.ColTraceID, "req"} {
		clear(reads)
		p := New(Deps{Reader: segment.NewFooterCache(store)})
		sel := selector.Resolved{Window: boundedWindow(true), Where: []selector.Predicate{{Field: field, Op: selector.OpEq, Value: "absent"}}}

		got, err := p.Plan(ctx, []segment.Descriptor{desc}, sel)
		assert.NoError(t, err)
		assert.Equal(t, map[string]int{"hour.filters": 1}, reads, field)
		assert.Equal(t, query.Cost{GETs: 1, BytesRead: int64(len(objects["hour.filters"]))}, got.Cost, field)
		assert.Less(t, got.Cost.BytesRead, footerBytes, "the filters object is smaller than the footer")

		again, err := p.Plan(ctx, []segment.Descriptor{desc}, sel)
		assert.NoError(t, err)
		assert.Equal(t, query.Cost{}, again.Cost, "a repeat reads the filters from the cache")
	}
}

// --- oracle-style property test --------------------------------------------

// genRow is a synthetic row used to build a faithful footer AND to run the
// ground-truth naive scan the planner is checked against.
type genRow struct {
	patternID  string
	level      string
	traceID    string
	durationMS float64
	ts         time.Time
}

// TestPrune_ZeroFalseNegatives_Oracle fuzz-generates segments, footers, and
// predicates and asserts the planner never drops a row group a ground-truth full
// scan would have matched (the zero-false-negatives invariant).
func TestPrune_ZeroFalseNegatives_Oracle(t *testing.T) {
	rng := rand.New(rand.NewSource(0xC0FFEE)) //nolint:gosec // deterministic test fixture

	const iterations = 400
	for iter := 0; iter < iterations; iter++ {
		nSegs := 1 + rng.Intn(5)
		descs := make([]segment.Descriptor, 0, nSegs)
		footers := make(map[string]segment.Footer, nSegs)
		filters := make(map[string]map[string]*sketch.XorFilter, nSegs)
		groupsBySeg := make(map[string][][]genRow, nSegs)

		for s := 0; s < nSegs; s++ {
			id := "seg" + strconv.Itoa(s)
			key := "k" + strconv.Itoa(s)
			groups := make([][]genRow, 1+rng.Intn(3))
			for g := range groups {
				groups[g] = genRows(rng)
			}
			descs = append(descs, descWithKey(id, key))
			footers[key], filters[key] = buildFooter(groups)
			if rng.Intn(2) == 0 { // a merged hour, whose filters object holds its segment's filters
				descs[s].RollupFilters.Key = key
			}
			groupsBySeg[id] = groups
		}

		preds := genPredicates(rng)
		cat := catalog.Fake{PruneFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) { return descs, nil }}
		rd := footersByKey(footers)
		rd.MayContainFn = func(loc segment.ObjectLocation, _ segment.Footer, column, value string) (bool, bool, segment.Cost, error) {
			return true, filters[loc.Key][column].MayContain(value), segment.Cost{}, nil
		}
		rd.FiltersFn = func(loc segment.ObjectLocation) (map[string]*sketch.XorFilter, segment.Cost, error) {
			return filters[loc.Key], segment.Cost{}, nil
		}
		sel := selector.Resolved{Window: boundedWindow(true), Where: preds}

		p, err := New(Deps{Catalog: cat, Reader: rd}).Prune(context.Background(), catalog.Version("0"), sel)
		if err != nil {
			t.Fatalf("iter %d: Prune() error = %v (preds=%+v)", iter, err, preds)
		}

		survivors := make(map[string]bool)
		for _, it := range p.Items {
			survivors[it.Segment.ID+"/"+strconv.Itoa(it.RowGroup)] = true
		}

		for _, d := range descs {
			for g, rows := range groupsBySeg[d.ID] {
				if segmentMatchesOracle(rows, preds) && !survivors[d.ID+"/"+strconv.Itoa(g)] {
					t.Fatalf("iter %d: FALSE NEGATIVE — segment %s row group %d has a matching row but was pruned\n preds=%+v\n rows=%+v",
						iter, d.ID, g, preds, rows)
				}
			}
		}

		if p.Cost.GETs > len(descs) {
			t.Fatalf("iter %d: Cost.GETs = %d, want %d", iter, p.Cost.GETs, len(descs))
		}
	}
}

// --- oracle helpers ---------------------------------------------------------

var (
	genPatterns = []string{"t0", "t1", "t2", "t3", "t4"}
	genLevels   = []string{"debug", "info", "warn", "error"}
	genTraces   = []string{"tr0", "tr1", "tr2", "tr3", "tr4", "tr5", "tr6", "tr7"}
	genBase     = time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
)

func genRows(rng *rand.Rand) []genRow {
	n := 1 + rng.Intn(6)
	rows := make([]genRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, genRow{
			patternID:  genPatterns[rng.Intn(len(genPatterns))],
			level:      genLevels[rng.Intn(len(genLevels))],
			traceID:    genTraces[rng.Intn(len(genTraces))],
			durationMS: float64(rng.Intn(100)),
			ts:         genBase.Add(time.Duration(rng.Intn(100)) * time.Second),
		})
	}
	return rows
}

func genPredicates(rng *rand.Rand) []selector.Predicate {
	n := 1 + rng.Intn(3)
	preds := make([]selector.Predicate, 0, n)
	for i := 0; i < n; i++ {
		preds = append(preds, genPredicate(rng))
	}
	return preds
}

func genPredicate(rng *rand.Rand) selector.Predicate {
	switch rng.Intn(7) {
	case 0:
		return selector.Predicate{Field: "pattern_id", Op: eqOrNe(rng), Value: genPatterns[rng.Intn(len(genPatterns))]}
	case 1:
		if rng.Intn(4) == 0 {
			return selector.Predicate{Field: "level", Op: selector.OpExists}
		}
		return selector.Predicate{Field: "level", Op: eqOrNe(rng), Value: genLevels[rng.Intn(len(genLevels))]}
	case 2:
		return selector.Predicate{Field: "trace_id", Op: eqOrNe(rng), Value: genTraces[rng.Intn(len(genTraces))]}
	case 3:
		op := []selector.Op{selector.OpLt, selector.OpLe, selector.OpGt, selector.OpGe}[rng.Intn(4)]
		return selector.Predicate{Field: "duration_ms", Op: op, Value: strconv.Itoa(rng.Intn(120))}
	case 4:
		op := []selector.Op{selector.OpLt, selector.OpLe, selector.OpGt, selector.OpGe}[rng.Intn(4)]
		return selector.Predicate{Field: "ts", Op: op, Value: genBase.Add(time.Duration(rng.Intn(120)) * time.Second).Format(time.RFC3339)}
	default:
		// Scan-only match (safe with the bounded time window in the selector).
		return selector.Predicate{Field: "message", Op: selector.OpMatch, Value: "x"}
	}
}

func eqOrNe(rng *rand.Rand) selector.Op {
	if rng.Intn(2) == 0 {
		return selector.OpEq
	}
	return selector.OpNe
}

// buildFooter constructs a footer that faithfully summarizes each row group's rows:
// present-set over pattern_ids, zone maps per row group and for their union, and a
// membership filter over each string column.
func buildFooter(groups [][]genRow) (segment.Footer, map[string]*sketch.XorFilter) {
	f := segment.Footer{RowGroups: len(groups), Patterns: segment.NewPresentSet()}
	var all []genRow
	keys := map[string][][]byte{}
	for _, rows := range groups {
		for _, r := range rows {
			f.Patterns.Add(r.patternID)
			keys["pattern_id"] = append(keys["pattern_id"], []byte(r.patternID))
			keys["level"] = append(keys["level"], []byte(r.level))
			keys["trace_id"] = append(keys["trace_id"], []byte(r.traceID))
		}
		all = append(all, rows...)
		f.RowGroupZoneMaps = append(f.RowGroupZoneMaps, genZoneMaps(rows))
	}
	f.ZoneMaps = genZoneMaps(all)
	filters := map[string]*sketch.XorFilter{}
	for column, k := range keys {
		filters[column], _ = sketch.NewXorFilter(k)
	}
	return segment.FooterWithFilters(f, "pattern_id", "level", "trace_id"), filters
}

func genZoneMaps(rows []genRow) segment.ZoneMaps {
	zm := segment.ZoneMaps{}
	for _, r := range rows {
		zm.Update("pattern_id", value.TypeString, value.Value{String: r.patternID})
		zm.Update("level", value.TypeString, value.Value{String: r.level})
		zm.Update("trace_id", value.TypeString, value.Value{String: r.traceID})
		zm.Update("duration_ms", value.TypeFloat, value.Value{Float: r.durationMS})
		zm.Update("ts", value.TypeTimestamp, value.Value{Time: r.ts})
	}
	return zm
}

// groupZoneMaps gives each row group a level zone map holding one value.
func groupZoneMaps(levels ...string) []segment.ZoneMaps {
	zms := make([]segment.ZoneMaps, len(levels))
	for i, level := range levels {
		zms[i].Update("level", value.TypeString, value.Value{String: level})
	}
	return zms
}

// segmentMatchesOracle is the ground truth: a segment matches iff some single
// row satisfies every predicate (logical AND).
func segmentMatchesOracle(rows []genRow, preds []selector.Predicate) bool {
	for _, r := range rows {
		all := true
		for _, p := range preds {
			if !rowMatchesOracle(r, p) {
				all = false
				break
			}
		}
		if all {
			return true
		}
	}
	return false
}

func rowMatchesOracle(r genRow, p selector.Predicate) bool {
	switch p.Field {
	case "pattern_id":
		return eqNe(r.patternID, p.Value, p.Op)
	case "level":
		if p.Op == selector.OpExists {
			return true
		}
		return eqNe(r.level, p.Value, p.Op)
	case "trace_id":
		return eqNe(r.traceID, p.Value, p.Op)
	case "duration_ms":
		v, _ := strconv.ParseFloat(p.Value, 64)
		switch p.Op {
		case selector.OpLt:
			return r.durationMS < v
		case selector.OpLe:
			return r.durationMS <= v
		case selector.OpGt:
			return r.durationMS > v
		case selector.OpGe:
			return r.durationMS >= v
		}
	case "ts":
		return p.Parse().Matches(value.Format(value.TypeTimestamp, value.Value{Time: r.ts}), true)
	case "message":
		// Scan-only: the planner never prunes on match, so ground truth treats
		// the row as a potential match to keep the superset relation sound.
		return true
	}
	return true
}

func eqNe(have, want string, op selector.Op) bool {
	if op == selector.OpNe {
		return have != want
	}
	return have == want
}

func presentSet(vals ...string) segment.PresentSet {
	ps := segment.NewPresentSet()
	for _, v := range vals {
		ps.Add(v)
	}
	return ps
}

// TestPlannerSummaryHoldsTheCallersSnapshot pins the summary gate: the store must hold
// exactly the caller's snapshot.
func TestPlannerSummaryHoldsTheCallersSnapshot(t *testing.T) {
	store := &summary.Fake{CatalogVersionFn: func() catalog.Version { return "3" }}
	planner := New(Deps{Summary: store})
	synced := catalog.Version("3")

	assert.Same(t, store, planner.Summary(synced))
	assert.Nil(t, planner.Summary(catalog.Version("4")), "a snapshot the store has not synced")
	assert.Nil(t, New(Deps{}).Summary(synced), "no summary configured")
}

// TestPruneReadsTheCallersSnapshot pins that the planner prunes the snapshot it is
// handed rather than resolving its own.
func TestPruneReadsTheCallersSnapshot(t *testing.T) {
	var got catalog.Version
	cat := catalog.Fake{
		VersionFn: func() (catalog.Version, error) { return catalog.Version("99"), nil },
		PruneFn: func(snap catalog.Version, _ selector.Resolved) ([]segment.Descriptor, error) {
			got = snap
			return nil, nil
		},
	}
	_, err := New(Deps{Catalog: cat}).Prune(context.Background(), catalog.Version("7"), selector.Resolved{})
	assert.NoError(t, err)
	assert.Equal(t, catalog.Version("7"), got)
}

// TestNewDerived evaluates each form a definition takes over one row, and refuses one
// that does not compile, naming what it could not resolve.
func TestNewDerived(t *testing.T) {
	row := map[string]string{"status": "503", "bytes": "1500000000", "empty": ""}
	value := func(col string) ([]byte, bool) {
		v, ok := row[col]
		return []byte(v), ok
	}
	tables := map[string]map[string]string{"class": {"200": "2xx", "503": "5xx"}}
	tests := []struct {
		expr, want string
		ok         bool
	}{
		{expr: "bytes / 1e9", want: "1.5", ok: true},
		{expr: "(bytes - 5e8) * 2 / 1e9", want: "2", ok: true},
		{expr: "lookup(status, class)", want: "5xx", ok: true},
		{expr: `lookup(missing, class, "other")`, want: "other", ok: true},
		{expr: "lookup(missing, class)"},
		{expr: "coalesce(empty, missing / 2, 7)", want: "7", ok: true},
		{expr: "bytes / 0"},
	}
	for _, tt := range tests {
		d, err := NewDerived([]DerivedColumn{{Name: "x", Expr: tt.expr}}, tables)
		assert.NoError(t, err, tt.expr)
		got, derived := d.Column("x", 1, func(col string, _ int) ([]byte, bool) { return value(col) })
		assert.True(t, derived, tt.expr)
		assert.Equal(t, tt.want, string(got[0]), tt.expr)
		assert.Equal(t, tt.ok, got[0] != nil, tt.expr)
	}

	// An operator's result reaches the next as a number, so each further one allocates nothing.
	allocs := func(expr string) float64 {
		d, err := NewDerived([]DerivedColumn{{Name: "x", Expr: expr}}, tables)
		assert.NoError(t, err, expr)
		return testing.AllocsPerRun(100, func() { d.Column("x", 1, func(col string, _ int) ([]byte, bool) { return value(col) }) })
	}
	assert.Equal(t, allocs("bytes / 1e9"), allocs("(bytes - 5e8) * 3 / 2e9"))

	for expr, want := range map[string]string{
		"lookup(status, prices)": `derived column x: unknown lookup table "prices"`,
		"round(bytes)":           "derived column x: unknown function round",
		"bytes +":                `derived column x: unexpected ""`,
		"y * 2":                  "derived column x reads derived column y",
	} {
		_, err := NewDerived([]DerivedColumn{{Name: "x", Expr: expr}, {Name: "y", Expr: "bytes"}}, tables)
		assert.EqualError(t, err, want, expr)
	}
	d, err := NewDerived(nil, tables)
	assert.NoError(t, err)
	assert.Equal(t, Derived{}, d)
}
