package series

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/summary"
)

var (
	t0      = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b0, b1  = t0.UnixMilli(), t0.Add(time.Minute).UnixMilli()
	bins    = Bins{Width: time.Minute, From: t0, To: t0.Add(2 * time.Minute)}
	version = catalog.Version("5")
	level   = selector.Predicate{Field: segment.ColLevel, Op: selector.OpEq, Value: "error"}
	errDown = errors.New("store errDown")
)

// request is a count by pattern_id over bins, filtered to level=error.
func request() Request {
	return Request{
		Version:  version,
		Selector: selector.Resolved{Window: bins.Window(), Where: []selector.Predicate{level}},
		Window:   bins.Window(),
		Measure:  Measure{Kind: KindCount},
		Bins:     bins,
	}
}

// rowsAt is one row of seg per offset from t0, each carrying values.
func rowsAt(seg *segment.Descriptor, values map[string]string, offsets ...time.Duration) []scan.Row {
	raw := map[string][]byte{}
	for col, v := range values {
		raw[col] = []byte(v)
	}
	rows := make([]scan.Row, len(offsets))
	for i, at := range offsets {
		rows[i] = scan.NewRow(seg, t0.Add(at), raw)
	}
	return rows
}

// seams serves a case's canned planner and scanner answers and records what Read asked.
type seams struct {
	store    summary.Store
	segments []segment.Descriptor
	plan     plan.Plan
	rows     []scan.Row
	scanCost query.Cost
	failAt   string // "segments", "plan" or "scan" fails with errDown

	segmentReads int
	planned      []segment.Descriptor
	query        *scan.Query
}

func (s *seams) reader() Reader {
	fail := func(stage string) error {
		if s.failAt == stage {
			return errDown
		}
		return nil
	}
	planner := plan.Fake{
		SegmentsFn: func(v catalog.Version, _ selector.Resolved) ([]segment.Descriptor, error) {
			s.segmentReads++
			if v != version {
				return nil, errors.New("segments read at another version")
			}
			return s.segments, fail("segments")
		},
		PlanFn: func(descs []segment.Descriptor, _ selector.Resolved) (plan.Plan, error) {
			s.planned = descs
			return s.plan, fail("plan")
		},
		SummaryFn: func(v catalog.Version) summary.Store {
			if v != version {
				return nil
			}
			return s.store
		},
	}
	scanner := scan.Fake{ScanFn: func(p plan.Plan, q scan.Query) scan.Canned {
		if s.query = &q; p.Cost != s.plan.Cost || len(p.Items) != len(s.plan.Items) {
			return scan.Canned{Err: errors.New("scanned a plan the planner did not return")}
		}
		return scan.Canned{Rows: s.rows, Cost: s.scanCost, Err: fail("scan")}
	}}
	return New(planner, scanner)
}

// summaryStore holds pattern p1 for service api: four lines inside the first bin, and
// two from a segment straddling both bins, one in each.
func summaryStore(t *testing.T) summary.Store {
	footer := func(rows int) segment.Footer {
		counts := map[segment.ServicePattern]int{{Service: "api", Pattern: "p1"}: rows}
		return segment.FooterWithServices(segment.Footer{ServicePatternCounts: counts}, map[string]segment.Footer{"api": {Rows: rows}})
	}
	store, err := summary.NewStore(summary.Config{BinWidth: time.Minute}, version, t0, []summary.Segment{
		{Descriptor: segment.Descriptor{ID: "a", Rows: 4, Stream: segment.Stream{Service: "api"}, TimeRange: segment.TimeRange{Min: t0, Max: t0.Add(30 * time.Second)}}, Footer: footer(4)},
		{Descriptor: segment.Descriptor{ID: "b", Rows: 2, Stream: segment.Stream{Service: "api"}, TimeRange: segment.TimeRange{Min: t0.Add(50 * time.Second), Max: t0.Add(70 * time.Second)}}, Footer: footer(2),
			Counts: summary.BinCounts{{PatternID: "p1", Service: "api", Bin: t0.UnixMilli()}: 1, {PatternID: "p1", Service: "api", Bin: t0.Add(time.Minute).UnixMilli()}: 1}},
	}...)
	require.NoError(t, err)
	return store
}

// TestRead drives Read through fake seams: each case is a request and the planner's and
// scanner's answers in, and the result and what each seam was asked out.
func TestRead(t *testing.T) {
	seg := segment.Descriptor{ID: "s1", TimeRange: segment.TimeRange{Min: t0, Max: t0.Add(90 * time.Second)}}
	oneItem := plan.Plan{Items: []plan.WorkItem{{Segment: seg}}, Cost: query.Cost{GETs: 1}}
	p1 := map[string]string{segment.ColPatternID: "p1"}

	api := segment.Descriptor{ID: "a", Stream: segment.Stream{Labels: map[string]string{"service": "api"}},
		TimeRange: segment.TimeRange{Min: t0, Max: t0.Add(time.Second)}}
	bare := api
	bare.ID = "b"
	users := sketch.NewHLL()
	for _, u := range []string{"u1", "u2", "u3"} {
		users.Add([]byte(u))
	}
	store := summaryStore(t)
	fromSummary := func(r *Request) { r.Summary, r.Selector.Where, r.GroupBy = true, nil, segment.ColPatternID }
	empty := Result{Values: Values{}, Coverage: 1}

	tests := []struct {
		name   string
		mutate func(*Request)
		seams  seams
		// maxValues lowers the exact quantile's memory bound for the case.
		maxValues int64

		want             Result
		wantErr          error
		wantCode         string
		wantRefused      bool
		wantSegmentReads int
		wantPlanned      []segment.Descriptor
		wantQuery        *scan.Query
	}{
		{
			name:  "a count reads the planned rows into bins, billing both stages",
			seams: seams{segments: []segment.Descriptor{seg}, plan: oneItem, rows: rowsAt(&seg, p1, 0, 10*time.Second, 70*time.Second), scanCost: query.Cost{GETs: 2, BytesRead: 40}},
			want:  Result{Values: Values{"p1": {b0: 2, b1: 1}}, Cost: query.Cost{GETs: 3, BytesRead: 40}, Coverage: 1},
			wantQuery: &scan.Query{
				Window:  bins.Window(),
				Where:   selector.ParsePredicates([]selector.Predicate{level}),
				Columns: []string{segment.ColTimestamp, segment.ColPatternID},
				Require: []string{segment.ColPatternID},
			},
			wantSegmentReads: 1,
			wantPlanned:      []segment.Descriptor{seg},
		},
		{
			name:   "the summary answers a count it can apply, reading no segment",
			mutate: fromSummary,
			seams:  seams{store: store},
			want:   Result{Values: Values{"p1": {b0: 5, b1: 1}}, Coverage: 1},
		},
		{
			name:             "a token that does not let the summary answer reads the segments",
			mutate:           func(r *Request) { fromSummary(r); r.Summary = false },
			seams:            seams{store: store},
			want:             empty,
			wantSegmentReads: 1,
		},
		{
			name:             "a predicate the summary does not key reads the segments",
			mutate:           func(r *Request) { fromSummary(r); r.Selector.Where = []selector.Predicate{level} },
			seams:            seams{store: store},
			want:             empty,
			wantSegmentReads: 1,
		},
		{
			name:             "bins the summary cannot tile read the segments",
			mutate:           func(r *Request) { fromSummary(r); r.Bins.Width = 90 * time.Second },
			seams:            seams{store: store},
			want:             empty,
			wantSegmentReads: 1,
		},
		{
			name: "a distinct the footers answer reads no rows and counts a bare footer against coverage",
			mutate: func(r *Request) {
				r.Selector.Where, r.GroupBy, r.Measure = nil, "service", Measure{Kind: KindDistinct, Field: "user"}
			},
			seams: seams{segments: []segment.Descriptor{api, bare}, plan: plan.Plan{Cost: query.Cost{GETs: 2}, Items: []plan.WorkItem{
				{Segment: api, Footer: segment.Footer{DistinctCounts: map[string]*sketch.HLL{"user": users}}},
				{Segment: bare},
			}}},
			want:             Result{Values: Values{"api": {b0: users.Estimate()}}, Cost: query.Cost{GETs: 2}, Coverage: 0.5, Approx: true},
			wantSegmentReads: 1,
			wantPlanned:      []segment.Descriptor{api, bare},
		},
		{
			name: "a ratio divides two totals from one scan and leaves 0/0 bins absent",
			mutate: func(r *Request) {
				r.Selector.Where, r.Measure, r.Numerator = nil, Measure{Kind: KindRatio}, []selector.Predicate{level}
			},
			seams: seams{segments: []segment.Descriptor{seg}, plan: oneItem, scanCost: query.Cost{GETs: 1}, rows: append(
				rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", segment.ColLevel: "error"}, 0, time.Second),
				rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", segment.ColLevel: "info"}, 2*time.Second)...)},
			want:             Result{Values: Values{"p1": {b0: 2.0 / 3}}, Cost: query.Cost{GETs: 2}, Coverage: 1},
			wantSegmentReads: 1,
		},
		{
			name:   "patterns narrow the segments, keeping one that lists none",
			mutate: func(r *Request) { r.Patterns = map[string]struct{}{"p1": {}} },
			seams: seams{segments: []segment.Descriptor{
				{ID: "p1", PatternIDs: []string{"p1"}}, {ID: "p2", PatternIDs: []string{"p2"}}, {ID: "none"},
			}},
			want:             empty,
			wantSegmentReads: 1,
			wantPlanned:      []segment.Descriptor{{ID: "p1", PatternIDs: []string{"p1"}}, {ID: "none"}},
		},
		{
			name:             "a scan past its bound refuses before planning",
			mutate:           func(r *Request) { r.Bound = Bound{Segments: 1} },
			seams:            seams{segments: []segment.Descriptor{{ID: "a"}, {ID: "b"}}},
			wantCode:         query.ErrCodeScanTooLarge,
			wantSegmentReads: 1,
		},
		{
			name:      "an exact quantile past its memory bound refuses",
			mutate:    func(r *Request) { r.Measure = Measure{Kind: KindQuantile, Field: "duration_ms", Quantile: 0.99} },
			maxValues: 2,
			seams: seams{segments: []segment.Descriptor{seg}, plan: oneItem,
				rows: rowsAt(&seg, map[string]string{"duration_ms": "1"}, 0, time.Second, 2*time.Second)},
			wantCode:         query.ErrCodeScanTooLarge,
			wantSegmentReads: 1,
		},
		{name: "a catalog failure", seams: seams{failAt: "segments"}, wantErr: errDown, wantSegmentReads: 1},
		{name: "a footer failure", seams: seams{segments: []segment.Descriptor{seg}, failAt: "plan"}, wantErr: errDown, wantSegmentReads: 1},
		{name: "a scan failure", seams: seams{segments: []segment.Descriptor{seg}, plan: oneItem, failAt: "scan"}, wantErr: errDown, wantSegmentReads: 1},
		{name: "no bin width", mutate: func(r *Request) { r.Bins.Width = 0 }, wantRefused: true},
		{name: "a numerator without a ratio", mutate: func(r *Request) { r.Numerator = []selector.Predicate{level} }, wantRefused: true},
		{name: "a ratio without a numerator", mutate: func(r *Request) { r.Measure.Kind = KindRatio }, wantRefused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.maxValues > 0 {
				defer func(prev int64) { maxCollectedValues = prev }(maxCollectedValues)
				maxCollectedValues = tt.maxValues
			}
			req := request()
			if tt.mutate != nil {
				tt.mutate(&req)
			}
			s := tt.seams
			res, err := s.reader().Read(context.Background(), req)

			switch {
			case tt.wantCode != "":
				var apiErr *query.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.Code)
			case tt.wantErr != nil:
				assert.ErrorIs(t, err, tt.wantErr)
			case tt.wantRefused:
				assert.Error(t, err)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.want, res)
			}
			assert.Equal(t, tt.wantSegmentReads, s.segmentReads, "catalog reads")
			if tt.wantPlanned != nil {
				assert.Equal(t, tt.wantPlanned, s.planned)
			}
			if tt.wantQuery != nil {
				assert.Equal(t, tt.wantQuery, s.query)
			}
		})
	}
}

// TestQuantileOf pins the nearest-rank quantile definition the scan path uses, so
// the oracle tests and any future cross-check agree on one method.
func TestQuantileOf(t *testing.T) {
	vals := []float64{50, 10, 30, 20, 40} // unsorted on purpose
	assert.Equal(t, 50.0, QuantileOf(vals, 0.99), "top rank")
	assert.Equal(t, 10.0, QuantileOf(vals, 0.01), "bottom rank")
	assert.Equal(t, 30.0, QuantileOf(vals, 0.5), "median at ceil(0.5*5)=3")
	assert.Equal(t, 0.0, QuantileOf(nil, 0.5), "no values, no quantile")
}

// TestScalarObserveZeroAlloc: a scalar quantile, and a folded sequence measure of a number or
// the time, reads each row's value and key off the row unformatted, so folding a row allocates
// nothing beyond its bin's growth.
func TestScalarObserveZeroAlloc(t *testing.T) {
	key := binKey{column: "instance", bins: bins, multiplied: new(bool)}
	sequence := func(kind Kind, field string) measure {
		return &binSequences{binKey: key, kind: kind, field: field, samples: map[string]map[int64][]sample{}}
	}
	for _, tc := range []struct {
		name string
		m    measure
		want Values
	}{
		{name: "quantile", m: &binQuantiles{binKey: key, field: "latency_ms", quantile: 0.95, values: map[string]map[int64][]float64{}}, want: Values{"db-1": {b0: 12.5}}},
		{name: "last", m: sequence(KindLast, "latency_ms"), want: Values{"db-1": {b0: 12.5}}},
		{name: "last time", m: sequence(KindLast, segment.ColTimestamp), want: Values{"db-1": {b0: float64(t0.UnixMilli())}}},
		{name: "span", m: sequence(KindSpan, "latency_ms"), want: Values{"db-1": {b0: 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := scan.NewRow(&segment.Descriptor{}, t0, map[string][]byte{"instance": []byte("db-1"), "latency_ms": []byte("12.5"),
				segment.ColTimestamp: []byte(t0.Format(time.RFC3339Nano))})
			assert.Zero(t, testing.AllocsPerRun(1000, func() { require.NoError(t, tc.m.observe(row)) }))
			assert.Equal(t, tc.want, tc.m.result().Values)
		})
	}
}

// TestReadMeasuresInOneScan reads three measures, one over its own predicate, from one scan,
// of the catalog's segments or of the ones a request names.
func TestReadMeasuresInOneScan(t *testing.T) {
	seg := segment.Descriptor{ID: "s1", TimeRange: segment.TimeRange{Min: t0, Max: t0.Add(90 * time.Second)},
		ZoneMaps: map[string]segment.ZoneMap{segment.ColLevel: {}}}
	rows := slices.Concat(rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", segment.ColLevel: "error"}, 0),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", segment.ColLevel: "info"}, 10*time.Second),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p2", segment.ColLevel: "info"}, 70*time.Second))
	scans := 0
	scanner := scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned { scans++; return scan.Canned{Rows: rows} }}
	planner := plan.Fake{SegmentsFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) {
		return []segment.Descriptor{seg}, nil
	}}
	req := request()
	req.Selector.Where = nil
	req.Measures = []FilteredMeasure{{Measure: Measure{Kind: KindCount}}, {Measure: Measure{Kind: KindCount}, Where: []selector.Predicate{level}},
		{Measure: Measure{Kind: KindLast, Field: segment.ColLevel}}}

	got, err := New(planner, scanner).Read(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1, scans)
	assert.Equal(t, Result{Coverage: 1, Measures: []Result{
		{Values: Values{"p1": {b0: 2}, "p2": {b1: 1}}},
		{Values: Values{"p1": {b0: 1}}},
		{Values: Values{"p1": {b0: 0}, "p2": {b1: 0}}, Labels: Labels{"p1": {b0: "info"}, "p2": {b1: "info"}}},
	}}, got)

	// The segments a request names are read in place of the catalog's, which has none here.
	req.Segments = []segment.Descriptor{seg}
	named, err := New(plan.Fake{}, scanner).Read(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, got, named)
}

// TestReadSequencesSkipEmptyCells reads a model column a, "", a, absent, b: the empty and
// absent cells are no value, so the model changes once and its longest run is a's two.
func TestReadSequencesSkipEmptyCells(t *testing.T) {
	seg := segment.Descriptor{ID: "s1", TimeRange: segment.TimeRange{Min: t0, Max: t0.Add(50 * time.Second)},
		ZoneMaps: map[string]segment.ZoneMap{"model": {}}}
	rows := slices.Concat(rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", "model": "a"}, 0, 20*time.Second),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", "model": ""}, 10*time.Second),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p1"}, 30*time.Second),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", "model": "b"}, 40*time.Second))
	scanner := scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned { return scan.Canned{Rows: rows} }}
	planner := plan.Fake{SegmentsFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) {
		return []segment.Descriptor{seg}, nil
	}}
	req := request()
	req.Selector.Where = nil
	for _, k := range []Kind{KindFirst, KindLast, KindChanges, KindMaxRun} {
		req.Measures = append(req.Measures, FilteredMeasure{Measure: Measure{Kind: k, Field: "model"}})
	}

	got, err := New(planner, scanner).Read(context.Background(), req)
	require.NoError(t, err)
	label := func(v string) Labels { return Labels{"p1": {b0: v}} }
	assert.Equal(t, Result{Coverage: 1, Measures: []Result{
		{Values: Values{"p1": {b0: 0}}, Labels: label("a")},
		{Values: Values{"p1": {b0: 0}}, Labels: label("b")},
		{Values: Values{"p1": {b0: 1}}, Labels: label("")},
		{Values: Values{"p1": {b0: 2}}, Labels: label("a")},
	}}, got)
}

// TestReadSpan reads turn at 0, 2s and 5s and a row without it at 8s in p1, and turn once in
// p2: p1 spans the 5000 ms between its first and last turn, p2 none.
func TestReadSpan(t *testing.T) {
	seg := segment.Descriptor{ID: "s1", TimeRange: segment.TimeRange{Min: t0, Max: t0.Add(10 * time.Second)},
		ZoneMaps: map[string]segment.ZoneMap{"turn": {}}}
	rows := slices.Concat(rowsAt(&seg, map[string]string{segment.ColPatternID: "p1", "turn": "a"}, 0, 2*time.Second, 5*time.Second),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p1"}, 8*time.Second),
		rowsAt(&seg, map[string]string{segment.ColPatternID: "p2", "turn": "b"}, time.Second))
	scanner := scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned { return scan.Canned{Rows: rows} }}
	planner := plan.Fake{SegmentsFn: func(catalog.Version, selector.Resolved) ([]segment.Descriptor, error) {
		return []segment.Descriptor{seg}, nil
	}}
	req := request()
	req.Selector.Where = nil
	req.Measures = []FilteredMeasure{{Measure: Measure{Kind: KindSpan, Field: "turn"}}}

	got, err := New(planner, scanner).Read(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, Result{Coverage: 1, Measures: []Result{
		{Values: Values{"p1": {b0: 5000}, "p2": {b0: 0}}, Labels: Labels{"p1": {b0: ""}, "p2": {b0: ""}}},
	}}, got)
}

// TestFoldedSequencesKeepOnlyTheirAnswer folds 200 samples, out of time order and with
// repeated times and values, one at a time, and requires each folded kind to answer as
// reducing them all does, with no more than two kept.
func TestFoldedSequencesKeepOnlyTheirAnswer(t *testing.T) {
	var all []sample
	for i := range 200 {
		n := (i * 37) % 11
		all = append(all, sample{at: int64((i * 53) % 17), text: strconv.Itoa(n), num: float64(n), numeric: n%3 > 0})
	}
	for _, kind := range []Kind{KindFirst, KindLast, KindMin, KindMax, KindSpan} {
		kept := []sample{all[0]}
		for _, s := range all[1:] {
			kept = fold(kind, kept, s)
		}
		assert.LessOrEqual(t, len(kept), 2, kind.String())
		want, wantLabel := reduceSequence(kind, slices.Clone(all))
		got, gotLabel := reduceSequence(kind, kept)
		assert.Equal(t, [2]any{want, wantLabel}, [2]any{got, gotLabel}, kind.String())
	}
}

// TestFoldedSequencesHoldAGroupsAnswer: a folded sequence measure keeps a group's answer,
// not its rows, so a thousand rows of one group hold at most two samples under a cap of one.
func TestFoldedSequencesHoldAGroupsAnswer(t *testing.T) {
	defer func(prev int64) { maxCollectedValues = prev }(maxCollectedValues)
	maxCollectedValues = 1
	for _, kind := range []Kind{KindFirst, KindLast, KindMin, KindMax, KindSpan} {
		m := &binSequences{binKey: binKey{column: "trace_id", bins: bins, multiplied: new(bool)}, kind: kind, field: "n",
			samples: map[string]map[int64][]sample{}}
		for i := range 1000 {
			row := scan.NewRow(&segment.Descriptor{}, t0.Add(time.Duration(i)*time.Millisecond),
				map[string][]byte{"trace_id": []byte("t-1"), "n": []byte(strconv.Itoa(i % 7))})
			require.NoError(t, m.observe(row), kind.String())
		}
		assert.LessOrEqual(t, len(m.samples["t-1"][b0]), 2, kind.String())
	}
}
