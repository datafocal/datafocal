// Package tools implements the query tool surface: one method per
// tool, JSON in and a query.Envelope out. Every measure over bins comes from one
// series.Reader, and every other read goes through one plan.Planner and one
// scan.Scanner, which keeps the import graph one-way:
// api → tools → {series, plan, scan, summary} → {catalog, segment}.
package tools

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
)

// AggregateRequest is the wire request for the aggregate tool.
// The full tool supports every measure (count|rate|p50/p95/p99|distinct:<field>|
// sum:<field>), arbitrary group_by, and time-bin granularities that the design's
// ParseBins enumerates.
type AggregateRequest struct {
	Selector selector.Selector `json:"selector"`
	Measure  string            `json:"measure" jsonschema:"count|rate|ratio|ratio:<field>|p50|p95|p99|distinct:<field>|sum:<field>|first:<field>|last:<field>|min:<field>|max:<field>|max_run:<field>|changes:<field>|span:<field>; a span is the ms from a group's first row holding <field> to its last; a sum, quantile, min or max may read len(<field>) or bytes(<field>) for <field>"`
	GroupBy  string            `json:"group_by" jsonschema:"service|pattern_id|<field>|none; none is one series of every row, empty is by pattern_id"`
	Bins     string            `json:"bins,omitempty" jsonschema:"<n>s|<n>m|<n>h|<n>d of at least 15s, such as 10m|auto|all"`
	Snapshot string            `json:"snapshot,omitempty"`
	Limit    int               `json:"limit,omitempty" jsonschema:"groups per page; defaults to 100"`
	Sort     string            `json:"sort,omitempty" jsonschema:"value_desc|value_asc|key, or <measure index>:desc|asc comma-separated with later ones breaking ties; value_desc by default, ordering groups by their first measure's total over their bins"`
	Cursor   string            `json:"cursor,omitempty" jsonschema:"opaque pagination cursor from a previous response"`

	// Numerator makes the ratio measure (selector AND numerator) / (selector) per
	// (group, bin) — of row counts for "ratio", of sums over the named column for
	// "ratio:<field>". Required for a ratio and rejected otherwise (validateNumerator),
	// and carried as typed predicates so the injection-safe contract holds.
	Numerator []selector.Predicate `json:"numerator,omitempty" jsonschema:"predicates selecting a ratio measure's numerator; required for measure=ratio, rejected otherwise; denominator is the selector scope"`

	// MaxScanSegments bounds the segments one scan may read before refusing with
	// scan_too_large, matching rca_walk's bound of the same name: 2048 by default,
	// negative uncapped, counted against reads rather than the window. A ratio's two
	// count passes are bounded separately, so it may read up to twice this many.
	MaxScanSegments int `json:"max_scan_segments,omitempty" jsonschema:"cap on segments one scan may read; defaults to 2048, negative means uncapped"`

	// MaxScanBytes bounds the same scan by the estimated bytes of the segments in scope,
	// which is the limit that holds once compaction makes a segment 128-256 MB rather
	// than an L0 5-50 MB. Defaults to 4 GB; negative is uncapped, and whichever bound
	// trips first refuses.
	MaxScanBytes int64 `json:"max_scan_bytes,omitempty" jsonschema:"cap on estimated bytes one scan may read; defaults to 4 GB, negative means uncapped"`

	Measures []selector.MeasureSpec  `json:"measures,omitempty" jsonschema:"several measures read in one scan, each over the rows its own where also matches; replaces measure and numerator"`
	Having   []selector.HavingClause `json:"having,omitempty" jsonschema:"keeps the groups whose measure's total over their bins satisfies every comparison"`

	// Rollup, when set, answers with its series over the groups in place of the groups,
	// which are neither sorted nor paged.
	Rollup *selector.Rollup `json:"rollup,omitempty"`

	// OrdinalOf bins each row by its 1-based position among the rows of its value of this
	// id column, in time order, counting only the rows OrdinalWhere matches; Bins is then a
	// width in positions and a series' start the first position of its first bin.
	OrdinalOf    string               `json:"ordinal_of,omitempty" jsonschema:"bin by a row's position within its value of this id column, e.g. turn N of every session_id"`
	OrdinalWhere []selector.Predicate `json:"ordinal_where,omitempty" jsonschema:"the rows that count as a position, e.g. type=user; a row between two takes the earlier's"`
}

// The aggregate sort orders, and how many groups a page holds by default.
const (
	sortValueDesc         = "value_desc"
	sortValueAsc          = "value_asc"
	sortKey               = "key"
	defaultAggregateLimit = 100
)

// Series is one group's measure over its bins, as columns: bin i starts at Start + i×Step,
// and every array holds one entry per bin, null where the group has no value.
type Series struct {
	Key           string       `json:"key"`
	Start         int64        `json:"start"` // first bin's start, unix ms, or with ordinal_of its first position
	Step          int64        `json:"step"`  // bin width, ms or positions
	Values        []*float64   `json:"values,omitempty" tstype:"(number | null)[]"`
	Baselines     []*float64   `json:"baselines,omitempty" tstype:"(number | null)[]"`
	Labels        []string     `json:"labels,omitempty"`
	MeasureValues [][]*float64 `json:"measure_values,omitempty" tstype:"(number | null)[][]"`
	MeasureLabels [][]string   `json:"measure_labels,omitempty"`
}

// group is one group's points, only the bins with a value when sparse, which
// aggregateEnvelope lays onto the grid as a Series.
type group struct {
	Key    string
	Points []point
}

// point is a group's value in the bin starting at T.
type point struct {
	T      int64
	V      float64
	Label  string    // a string first, last, min or max, or a max_run's value
	Values []float64 // each of the request's Measures', in order
	Labels []string
	// Absent marks each of Values a sequence measure has no row for, null on the wire.
	Absent []bool
}

// AggregateResponse is the wire response: one Series per group, and the fields joined
// when group_by goes by several names.
type AggregateResponse struct {
	Series []Series `json:"series"`
	Fields []string `json:"fields,omitempty"`
	// Multiplied is set when a record was counted once under each element of a group_by
	// list column, so the series may sum to more than the records.
	Multiplied bool `json:"multiplied,omitempty"`
}

// Aggregate resolves the measure and the bins, then reads the series and shapes it with
// a baseline per key.
func (t toolSet) Aggregate(ctx context.Context, req AggregateRequest) (query.Envelope[AggregateResponse], error) {
	var zero query.Envelope[AggregateResponse]

	measure, each, err := aggregateMeasures(req)
	if err != nil {
		return zero, err
	}
	sel, err := validateSelector(ctx, "aggregate", req.Selector)
	if err != nil {
		return zero, err
	}

	cur, terms, err := aggregateCursor(&req)
	if err != nil {
		return zero, err
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "aggregate", cmp.Or(cur.Snapshot, req.Snapshot))
	if err != nil {
		return zero, err
	}
	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return zero, fmt.Errorf("aggregate: prune: %w", err)
	}
	binFrom, binTo := binWindow(ctx, descs, sel.Window)
	bins, err := aggregateBins(req, measure, each, binFrom, binTo)
	if err != nil {
		return zero, err
	}
	roll, each, err := rollupMeasures(req.Rollup, each, binFrom, binTo)
	if err != nil {
		return zero, err
	}

	// A ratio's rows are those in its bins, as for every other tool's series; any other
	// aggregate's are those in the selector's window, which may be open.
	ratio := measure.Kind == series.KindRatio
	window := sel.Window
	if ratio {
		window = bins.Window()
	}
	res, fields, err := t.readJoined(ctx, snap, series.Request{
		Version:      snap.catalog,
		Summary:      snap.summaryServes(),
		Selector:     sel,
		Window:       window,
		Measure:      measure,
		Numerator:    req.Numerator,
		GroupBy:      groupByOf(req.GroupBy),
		Bins:         bins,
		Measures:     each,
		OrdinalOf:    req.OrdinalOf,
		OrdinalWhere: req.OrdinalWhere,
		Bound: series.Bound{
			Segments: cmp.Or(req.MaxScanSegments, defaultMaxScanSegments),
			Bytes:    cmp.Or(req.MaxScanBytes, defaultMaxScanBytes),
		},
	})
	if err != nil {
		return zero, err
	}
	if req.OrdinalOf != "" {
		bins.To = time.UnixMilli(lastBin(res, bins)).Add(bins.Width)
	}
	out := having(groupSeries(res, bins, each, ratio || measure.Kind.Sequence()), req.Having)
	if req.Rollup != nil {
		return aggregateEnvelope(snap, roll.bins, roll.apply(out, res.Measures[len(each)-1].Values), res, "", fields), nil
	}
	out, cur.Offset = pageSeries(out, terms, cur.Offset, req.Limit)
	next := ""
	if cur.Offset > 0 {
		next = encodePatternsCursor(patternsCursor{Snapshot: snap.String(), Selector: aggregateFingerprint(req), Offset: cur.Offset})
	}
	return aggregateEnvelope(snap, bins, out, res, next, fields), nil
}

// groupByOf is a group_by as the series reads it: none, one series of every row.
func groupByOf(groupBy string) string {
	if groupBy == "none" {
		return series.Total
	}
	return groupBy
}

// groupFields is the fields req groups by: its group_by and that field's equivalents, unless
// a ratio or ordinal measure ties each row to its group_by value alone or the selector
// requires the field, leaving no line for another to group.
func (t toolSet) groupFields(ctx context.Context, snap snapshot, req series.Request) ([]string, query.Cost, error) {
	requires := func(p selector.Predicate) bool {
		return p.Field == req.GroupBy && (p.Op == selector.OpExists || p.Op == selector.OpEq && p.Value != "")
	}
	if req.GroupBy == series.Total || req.Measure.Kind == series.KindRatio || req.OrdinalOf != "" || slices.ContainsFunc(req.Selector.Where, requires) {
		return []string{req.GroupBy}, query.Cost{}, nil
	}
	fields, cost, err := t.equivalents(ctx, snap, req.Selector.Window, req.GroupBy, false)
	if err != nil {
		return nil, cost, fmt.Errorf("aggregate: %w", err)
	}
	return fields, cost, nil
}

// readJoined reads req grouped by each of its groupFields in turn over the lines holding none
// of the fields before it, so a line counts under the first it holds, and merges the reads by key.
func (t toolSet) readJoined(ctx context.Context, snap snapshot, req series.Request) (series.Result, []string, error) {
	fields, cost, err := t.groupFields(ctx, snap, req)
	if err != nil {
		return series.Result{}, nil, err
	}
	var joined series.Result
	where := req.Selector.Where
	for i, field := range fields {
		req.GroupBy, req.Selector.Where = field, where
		res, err := t.Series.Read(ctx, req)
		if err != nil {
			return series.Result{}, nil, err
		}
		where = append(slices.Clip(where), selector.Predicate{Field: field, Op: selector.OpEq, Value: ""})
		if i == 0 {
			joined = res
			continue
		}
		// The lines without the fields so far are this field's to group, "" or not.
		delete(joined.Values, "")
		delete(joined.Labels, "")
		joined.Values = joinBins(joined.Values, res.Values, joinMeasure(req.Measure.Kind))
		joined.Labels = joinBins(joined.Labels, res.Labels, keepFirst)
		for m := range joined.Measures {
			delete(joined.Measures[m].Values, "")
			delete(joined.Measures[m].Labels, "")
			joined.Measures[m].Values = joinBins(joined.Measures[m].Values, res.Measures[m].Values, joinMeasure(req.Measures[m].Kind))
			joined.Measures[m].Labels = joinBins(joined.Measures[m].Labels, res.Measures[m].Labels, keepFirst)
		}
		joined.Cost = addCost(joined.Cost, res.Cost)
		joined.Coverage = min(joined.Coverage, res.Coverage)
		joined.Approx = joined.Approx || res.Approx
		joined.Multiplied = joined.Multiplied || res.Multiplied
	}
	joined.Cost = addCost(joined.Cost, cost)
	return joined, fields, nil
}

// joinBins adds from's keys' bins to into's, join combining a bin both hold.
func joinBins[V any](into, from map[string]map[int64]V, join func(a, b V) V) map[string]map[int64]V {
	if into == nil {
		return from
	}
	for key, bins := range from {
		if into[key] == nil {
			into[key] = bins
			continue
		}
		for bin, v := range bins {
			if old, ok := into[key][bin]; ok {
				v = join(old, v)
			}
			into[key][bin] = v
		}
	}
	return into
}

// joinMeasure combines two reads' values of kind for one key and bin: counts, rates and sums
// add up, a min or max is the least or greatest, and any other is the first field's.
func joinMeasure(kind series.Kind) func(a, b float64) float64 {
	switch kind {
	case series.KindCount, series.KindRate, series.KindSum:
		return func(a, b float64) float64 { return a + b }
	case series.KindMin:
		return func(a, b float64) float64 { return min(a, b) }
	case series.KindMax:
		return func(a, b float64) float64 { return max(a, b) }
	}
	return keepFirst[float64]
}

// keepFirst is the first of two values.
func keepFirst[V any](a, _ V) V { return a }

// aggregateBins parses req's bins over from-to, or with ordinal_of as a width in positions
// on an open-ended grid from position 1, a position p read as p ms after the epoch.
func aggregateBins(req AggregateRequest, measure series.Measure, each []series.FilteredMeasure, from, to time.Time) (series.Bins, error) {
	if req.OrdinalOf == "" {
		return series.ParseBins(req.Bins, from, to)
	}
	overTime := func(m series.Measure) bool { return m.Kind == series.KindRate || m.Kind == series.KindRatio }
	if overTime(measure) || slices.ContainsFunc(each, func(f series.FilteredMeasure) bool { return overTime(f.Measure) }) {
		return series.Bins{}, query.Invalidf("aggregate: a rate or a ratio is over time, which ordinal_of bins by position instead")
	}
	width, err := strconv.Atoi(cmp.Or(req.Bins, "1"))
	if err != nil || width < 1 {
		return series.Bins{}, query.Invalidf("aggregate: with ordinal_of, bins is a width in positions, got %q", req.Bins)
	}
	return series.Bins{Width: time.Duration(width) * time.Millisecond, From: time.UnixMilli(1), To: time.UnixMilli(math.MaxInt64)}, nil
}

// lastBin is the start of the last of bins any of res's measures has a value in.
func lastBin(res series.Result, bins series.Bins) int64 {
	last := bins.From.UnixMilli()
	for _, r := range slices.Concat([]series.Result{res}, res.Measures) {
		for _, byBin := range r.Values {
			for b := range byBin {
				last = max(last, b)
			}
		}
	}
	return last
}

// groupSeries is res's series of its one measure, sparse as asked, or of each measure,
// sparse unless one of them is not a sequence measure.
func groupSeries(res series.Result, bins series.Bins, each []series.FilteredMeasure, sparse bool) []group {
	if len(each) == 0 {
		return buildSeries(res, bins, sparse)
	}
	dense := slices.ContainsFunc(each, func(m series.FilteredMeasure) bool { return !m.Kind.Sequence() })
	return buildEach(res, bins, each, !dense)
}

// aggregateMeasures parses req's measure, or each of its Measures, which a rollup's
// measure is read as the one of.
func aggregateMeasures(req AggregateRequest) (series.Measure, []series.FilteredMeasure, error) {
	specs := req.Measures
	switch {
	case len(specs) == 0 && req.Rollup != nil:
		if len(req.Numerator) > 0 {
			return series.Measure{}, nil, query.Invalidf("aggregate: a rollup reads a ratio as measures, two counts with its numerator as one's where")
		}
		specs = []selector.MeasureSpec{{Measure: req.Measure}}
	case len(specs) == 0:
		measure, err := series.ParseMeasure(req.Measure)
		if err == nil {
			err = validateNumerator("aggregate", measure.Kind, req.Numerator)
		}
		return measure, nil, err
	case req.Measure != "" || len(req.Numerator) > 0:
		return series.Measure{}, nil, query.Invalidf("aggregate: measures replaces measure and numerator; set one or the other")
	}
	each := make([]series.FilteredMeasure, len(specs))
	for i, spec := range specs {
		m, err := series.ParseMeasure(spec.Measure)
		switch {
		case err != nil:
			return series.Measure{}, nil, err
		case m.Kind == series.KindRatio:
			return series.Measure{}, nil, query.Invalidf("aggregate: measures[%d]: a ratio is two counts, one with its numerator as where", i)
		case m.Kind == series.KindQuantile && m.Field == "":
			return series.Measure{}, nil, query.Invalidf("aggregate: measures[%d]: a quantile among measures names its field", i)
		}
		each[i] = series.FilteredMeasure{Measure: m, Where: spec.Where, DistinctBy: spec.DistinctBy}
	}
	return series.Measure{}, each, nil
}

// havingOps are the orders of a group's total against a having value each comparison accepts.
var havingOps = map[selector.Op][]int{
	selector.OpEq: {0}, selector.OpNe: {-1, 1}, selector.OpLt: {-1}, selector.OpLe: {-1, 0}, selector.OpGt: {1}, selector.OpGe: {0, 1},
}

// checkHaving refuses a clause naming no measure or comparison.
func checkHaving(clauses []selector.HavingClause, measures int) error {
	if i := slices.IndexFunc(clauses, func(h selector.HavingClause) bool {
		return h.Index < 0 || h.Index >= measures || havingOps[h.Op] == nil
	}); i >= 0 {
		return query.Invalidf("aggregate: having[%d] needs a measure index under %d and one of %v", i, measures, slices.Sorted(maps.Keys(havingOps)))
	}
	return nil
}

// having keeps the series whose totals satisfy every clause.
func having(out []group, clauses []selector.HavingClause) []group {
	return slices.DeleteFunc(out, func(s group) bool { return !satisfies(s, clauses) })
}

// satisfies reports whether s's totals satisfy every clause.
func satisfies(s group, clauses []selector.HavingClause) bool {
	return !slices.ContainsFunc(clauses, func(h selector.HavingClause) bool {
		return !slices.Contains(havingOps[h.Op], cmp.Compare(seriesTotal(s, h.Index), h.Value))
	})
}

// sortTerm orders groups by one measure's total.
type sortTerm struct {
	index int
	desc  bool
}

// parseSort reads sort into the terms ordering the groups, none for key order.
func parseSort(sort string, measures int) ([]sortTerm, error) {
	switch sort {
	case sortValueDesc:
		return []sortTerm{{desc: true}}, nil
	case sortValueAsc:
		return []sortTerm{{}}, nil
	case sortKey:
		return nil, nil
	}
	var terms []sortTerm
	for _, term := range strings.Split(sort, ",") {
		index, dir, _ := strings.Cut(term, ":")
		i, err := strconv.Atoi(index)
		if err != nil || i < 0 || i >= measures || (dir != "asc" && dir != "desc") {
			return nil, query.Invalidf("aggregate: sort must be %s, %s, %s or, with measures, <measure index>:desc|asc comma-separated, got %q",
				sortValueDesc, sortValueAsc, sortKey, sort)
		}
		terms = append(terms, sortTerm{index: i, desc: dir == "desc"})
	}
	return terms, nil
}

// aggregateCursor checks req's having, checks and defaults its sort and limit, and
// decodes its cursor, refusing one made for a different request or snapshot.
func aggregateCursor(req *AggregateRequest) (patternsCursor, []sortTerm, error) {
	measures := max(len(req.Measures), 1)
	if err := checkHaving(req.Having, measures); err != nil {
		return patternsCursor{}, nil, err
	}
	req.Sort = cmp.Or(req.Sort, sortValueDesc)
	terms, err := parseSort(req.Sort, measures)
	if err != nil {
		return patternsCursor{}, nil, err
	}
	req.Limit = cmp.Or(max(req.Limit, 0), defaultAggregateLimit)
	cur, err := decodePatternsCursor(req.Cursor)
	switch {
	case err != nil:
		return cur, nil, query.Invalidf("aggregate: invalid cursor")
	case req.Cursor != "" && cur.Selector != aggregateFingerprint(*req):
		return cur, nil, query.Invalidf("aggregate: cursor does not match the request")
	case cur.Snapshot != "" && req.Snapshot != "" && cur.Snapshot != req.Snapshot:
		return cur, nil, query.Invalidf("aggregate: cursor snapshot %q does not match request snapshot %q", cur.Snapshot, req.Snapshot)
	}
	return cur, terms, nil
}

// aggregateFingerprint digests what a cursor's later pages must ask the same: everything
// but the page itself and the snapshot, which the cursor carries.
func aggregateFingerprint(req AggregateRequest) string {
	req.Cursor, req.Limit, req.Snapshot = "", 0, ""
	b, _ := json.Marshal(req)
	sum := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(sum[:8])
}

// pageSeries orders out, which buildSeries ordered by key, and returns limit of them from
// offset with the next page's offset, 0 after the last.
func pageSeries(out []group, terms []sortTerm, offset, limit int) ([]group, int) {
	slices.SortStableFunc(out, func(a, b group) int {
		for _, term := range terms {
			order := cmp.Compare(seriesTotal(a, term.index), seriesTotal(b, term.index))
			if term.desc {
				order = -order
			}
			if order != 0 {
				return order
			}
		}
		return 0
	})
	offset = min(offset, len(out))
	end := min(offset+limit, len(out))
	if end == len(out) {
		return out[offset:end], 0
	}
	return out[offset:end], end
}

// seriesTotal sums the points' values of the measure at index, or their V for one measure.
func seriesTotal(s group, index int) float64 {
	var sum float64
	for _, p := range s.Points {
		v, _ := p.at(index)
		sum += v
	}
	return sum
}

// at is p's value and label of the measure at index, or its V and Label for one measure.
func (p point) at(index int) (float64, string) {
	if p.Values == nil {
		return p.V, p.Label
	}
	label := ""
	if p.Labels != nil {
		label = p.Labels[index]
	}
	return p.Values[index], label
}

// aggregateEnvelope lays a page of groups onto grid as columns, next the cursor to the
// page after it.
func aggregateEnvelope(snap snapshot, grid series.Bins, out []group, res series.Result, next string, fields []string) query.Envelope[AggregateResponse] {
	resp := AggregateResponse{Series: make([]Series, len(out)), Multiplied: res.Multiplied}
	if len(fields) > 1 {
		resp.Fields = fields
	}
	starts := grid.Boundaries()
	for i, g := range out {
		resp.Series[i] = columns(g, starts, grid.Width.Milliseconds())
	}
	return query.Envelope[AggregateResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Cursor:   next,
			Coverage: res.Coverage,
			Approx:   res.Approx,
			Cost:     res.Cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}
}

// columns is g over the bins starting at starts, step apart: one array per measure, or
// for one measure its values, baselines and labels.
func columns(g group, starts []int64, step int64) Series {
	values, labels := lay(g.Points, starts)
	s := Series{Key: g.Key, Start: starts[0], Step: step}
	if len(g.Points) > 0 && g.Points[0].Values != nil {
		s.MeasureValues, s.MeasureLabels = values, labels
		return s
	}
	s.Values, s.Baselines = values[0], baselines(values[0])
	if labels != nil {
		s.Labels = labels[0]
	}
	return s
}

// lay puts points onto the bins starting at starts as one value and one label array per
// measure, the labels nil when no bin has one.
func lay(points []point, starts []int64) ([][]*float64, [][]string) {
	measures := 1
	if len(points) > 0 {
		measures = max(len(points[0].Values), 1)
	}
	values, labels := make([][]*float64, measures), make([][]string, measures)
	for m := range values {
		values[m], labels[m] = make([]*float64, len(starts)), make([]string, len(starts))
	}
	labelled := false
	for i, start := range starts {
		if len(points) == 0 || points[0].T != start {
			continue
		}
		for m := range values {
			if points[0].Absent != nil && points[0].Absent[m] {
				continue
			}
			v, label := points[0].at(m)
			values[m][i], labels[m][i] = &v, label
			labelled = labelled || label != ""
		}
		points = points[1:]
	}
	if !labelled {
		return values, nil
	}
	return values, labels
}

// baselines is the EWMA over values' non-null entries, each to 4 significant digits,
// null where values is.
func baselines(values []*float64) []*float64 {
	var present []float64
	for _, v := range values {
		if v != nil {
			present = append(present, *v)
		}
	}
	ewma := stats.EWMA(present, stats.DefaultLambda)
	out := make([]*float64, len(values))
	for i, v := range values {
		if v == nil {
			continue
		}
		b, _ := strconv.ParseFloat(strconv.FormatFloat(ewma[0], 'g', 4, 64), 64)
		out[i], ewma = &b, ewma[1:]
	}
	return out
}

// binWindow returns the effective binning window: user-supplied bounds win;
// otherwise the union of descriptor TimeRanges. When there are no bounds and
// no descriptors, both ends collapse to now so ParseBins still produces a
// zero-width single bin instead of erroring on an empty window.
func binWindow(ctx context.Context, descs []segment.Descriptor, w selector.Window) (from, to time.Time) {
	binFrom, binTo := w.From, w.To
	lo, hi := descSpan(descs)
	if binFrom.IsZero() {
		binFrom = lo
	}
	if binTo.IsZero() {
		binTo = hi
	}
	if binFrom.IsZero() {
		query.CallOf(ctx).ReadClock.Store(true)
		binFrom = time.Now().UTC()
	}
	if binTo.IsZero() || !binTo.After(binFrom) {
		binTo = binFrom.Add(time.Second)
	}
	return binFrom, binTo
}

// descSpan returns the union of the descriptors' TimeRanges.
func descSpan(descs []segment.Descriptor) (lo, hi time.Time) {
	for _, d := range descs {
		if lo.IsZero() || d.TimeRange.Min.Before(lo) {
			lo = d.TimeRange.Min
		}
		if hi.IsZero() || d.TimeRange.Max.After(hi) {
			hi = d.TimeRange.Max
		}
	}
	return lo, hi
}

// buildSeries turns the values into a []group ordered by key
// then bin start, deterministically enough that the same call on the same snapshot
// marshals to byte-identical JSON (the snapshot-pin contract). sparse omits the bins a
// group has no value for: a count's empty bin is a real zero, but a ratio's is 0/0 —
// neither zero nor one — and a sequence measure's has no rows to read, so the point is
// left out rather than invented.
func buildSeries(res series.Result, bins series.Bins, sparse bool) []group {
	boundaries := bins.Boundaries()
	out := make([]group, 0, len(res.Values))
	for _, k := range slices.Sorted(maps.Keys(res.Values)) {
		binMap := res.Values[k]
		points := make([]point, 0, len(boundaries))
		for _, b := range boundaries {
			v, ok := binMap[b]
			if !ok && sparse {
				continue
			}
			points = append(points, point{T: b, V: v, Label: res.Labels[k][b]})
		}
		out = append(out, group{Key: k, Points: points})
	}
	return out
}

// buildEach is buildSeries over each of res.Measures, a point carrying every measure's
// value and label, where a sparse point is left out only when no measure has a value.
func buildEach(res series.Result, bins series.Bins, each []series.FilteredMeasure, sparse bool) []group {
	keys := map[string]bool{}
	for _, m := range res.Measures {
		for k := range m.Values {
			keys[k] = true
		}
	}
	out := make([]group, 0, len(keys))
	for _, k := range slices.Sorted(maps.Keys(keys)) {
		var points []point
		for _, b := range bins.Boundaries() {
			if p, ok := eachPoint(res.Measures, each, k, b); ok || !sparse {
				points = append(points, p)
			}
		}
		out = append(out, group{Key: k, Points: points})
	}
	return out
}

// eachPoint is key's point in the bin starting at b, ok when any measure has a value there.
// A count's or a sum's empty bin is a zero, but a sequence measure's has no row to read, so
// it is absent rather than invented, as buildSeries leaves it out.
func eachPoint(measures []series.Result, each []series.FilteredMeasure, key string, b int64) (point, bool) {
	p, present := point{T: b, Values: make([]float64, len(measures))}, false
	for i, m := range measures {
		v, ok := m.Values[key][b]
		p.Values[i], present = v, present || ok
		if !ok && each[i].Kind.Sequence() {
			if p.Absent == nil {
				p.Absent = make([]bool, len(measures))
			}
			p.Absent[i] = true
		}
		if label := m.Labels[key][b]; label != "" {
			if p.Labels == nil {
				p.Labels = make([]string, len(measures))
			}
			p.Labels[i] = label
		}
	}
	return p, present
}

// densify projects a sparse per-bin map onto the dense, time-ordered slice the
// correlation math needs, filling every bin so both series stay index-aligned for a
// lag sweep. missing is what an empty bin holds: 0 for a count, which is an
// observation, and math.NaN() for a ratio's 0/0, which stats.CrossCorrelate skips
// rather than averaging in as a fabricated zero.
func densify(byBin map[int64]float64, bins series.Bins, missing float64) []float64 {
	return densifyAt(byBin, bins.Boundaries(), missing)
}

// densifyAt is densify over bins starting at starts, for a caller densifying many series of them.
func densifyAt(byBin map[int64]float64, starts []int64, missing float64) []float64 {
	out := make([]float64, len(starts))
	for i, start := range starts {
		v, ok := byBin[start]
		if !ok {
			v = missing
		}
		out[i] = v
	}
	return out
}

// rollup is a parsed selector.Rollup: what it reduces each cell of groups to, the
// measure column it reads, the label it groups by, -1 for none, and its bins.
type rollup struct {
	measure series.Measure
	column  int
	label   int
	where   []selector.HavingClause
	bins    series.Bins
	buckets []float64
}

// rollupMeasures parses r, when set, and adds to each the first row's time it bins a
// group by.
func rollupMeasures(r *selector.Rollup, each []series.FilteredMeasure, from, to time.Time) (rollup, []series.FilteredMeasure, error) {
	if r == nil {
		return rollup{}, each, nil
	}
	roll, err := parseRollup(*r, len(each), from, to)
	return roll, append(each, series.FilteredMeasure{Measure: series.Measure{Kind: series.KindMin, Field: segment.ColTimestamp}}), err
}

// parseRollup checks r against a first level of measures, binned over from-to.
func parseRollup(r selector.Rollup, measures int, from, to time.Time) (rollup, error) {
	out := rollup{label: -1, where: r.Where, buckets: r.Buckets}
	var err error
	if out.bins, err = series.ParseBins(r.Bins, from, to); err != nil {
		return out, err
	}
	if err := checkHaving(r.Where, measures); err != nil {
		return out, err
	}
	if r.GroupBy != "" {
		var ok bool
		if out.label, ok = indexed(r.GroupBy, "label", measures); !ok {
			return out, query.Invalidf("aggregate: rollup group_by must be label[i] with i under %d, got %q", measures, r.GroupBy)
		}
	}
	if !out.parseMeasure(r, measures) {
		return out, query.Invalidf("aggregate: rollup measure must be count, ratio, sum:values[i] or p<NN>:values[i], or values[i] with ascending buckets, with i under %d, got %q",
			measures, r.Measure)
	}
	return out, nil
}

// parseMeasure reads r's measure and the column it reads into out, ok when it is one a
// rollup computes.
func (out *rollup) parseMeasure(r selector.Rollup, measures int) bool {
	if len(r.Buckets) > 0 {
		column, ok := indexed(r.Measure, "values", measures)
		out.column = column
		return ok && slices.IsSorted(r.Buckets)
	}
	var err error
	if out.measure, err = series.ParseMeasure(r.Measure); err != nil {
		return false
	}
	switch out.measure.Kind {
	case series.KindCount, series.KindRatio:
		return out.measure.Field == ""
	case series.KindSum, series.KindQuantile:
		column, ok := indexed(out.measure.Field, "values", measures)
		out.column = column
		return ok
	}
	return false
}

// indexed reads s as name[i] with i under n.
func indexed(s, name string, n int) (int, bool) {
	inner, ok := strings.CutPrefix(s, name+"[")
	inner, closed := strings.CutSuffix(inner, "]")
	i, err := strconv.Atoi(inner)
	return i, ok && closed && err == nil && i >= 0 && i < n
}

// rollupCell is a rollup series' key and, for a histogram, the bucket's index.
type rollupCell struct {
	key    string
	bucket int
}

// apply rolls groups up into one series per label and bucket, a group binned by its
// first row's time, which is the least of its first values over its bins. The rows
// lacking the group_by field are no group.
func (r rollup) apply(groups []group, first series.Values) []group {
	cells := map[rollupCell]map[int64][]float64{}
	for _, g := range groups {
		if g.Key == "" {
			continue
		}
		cell, v := r.cell(g)
		bin := r.bins.BinStart(time.UnixMilli(int64(slices.Min(slices.Collect(maps.Values(first[g.Key]))))))
		if cells[cell] == nil {
			cells[cell] = map[int64][]float64{}
		}
		cells[cell][bin] = append(cells[cell][bin], v)
	}
	out := make([]group, 0, len(cells))
	for _, cell := range slices.SortedFunc(maps.Keys(cells), func(a, b rollupCell) int {
		return cmp.Or(cmp.Compare(a.key, b.key), cmp.Compare(a.bucket, b.bucket))
	}) {
		s := group{Key: strings.TrimSpace(cell.key + " " + r.bucketName(cell.bucket))}
		for _, bin := range slices.Sorted(maps.Keys(cells[cell])) {
			s.Points = append(s.Points, point{T: bin, V: r.reduce(cells[cell][bin])})
		}
		out = append(out, s)
	}
	return out
}

// cell is g's rollup cell and the value it adds there: 1 or 0 as it meets a ratio's
// where, else its total of the measure column.
func (r rollup) cell(g group) (rollupCell, float64) {
	cell, v := rollupCell{key: groupLabel(g, r.label)}, seriesTotal(g, r.column)
	if r.measure.Kind == series.KindRatio {
		v = 0
		if satisfies(g, r.where) {
			v = 1
		}
	}
	if len(r.buckets) > 0 {
		cell.bucket, _ = slices.BinarySearch(r.buckets, v)
		if cell.bucket < len(r.buckets) && r.buckets[cell.bucket] == v {
			cell.bucket++
		}
	}
	return cell, v
}

// groupLabel is the first label of g's measure at index, empty for index -1 or none.
func groupLabel(g group, index int) string {
	for _, p := range g.Points {
		if index >= 0 && index < len(p.Labels) && p.Labels[index] != "" {
			return p.Labels[index]
		}
	}
	return ""
}

// bucketName names the bucket at i as the range [lo,hi) of its edges, none without buckets.
func (r rollup) bucketName(i int) string {
	if len(r.buckets) == 0 {
		return ""
	}
	edges := slices.Concat([]float64{math.Inf(-1)}, r.buckets, []float64{math.Inf(1)})
	return "[" + strconv.FormatFloat(edges[i], 'g', -1, 64) + "," + strconv.FormatFloat(edges[i+1], 'g', -1, 64) + ")"
}

// reduce is the rollup's measure over one cell's values: their count for count and for a
// histogram, the share of them meeting where for ratio, their sum, or their quantile.
func (r rollup) reduce(vals []float64) float64 {
	switch {
	case len(r.buckets) > 0 || r.measure.Kind == series.KindCount:
		return float64(len(vals))
	case r.measure.Kind == series.KindQuantile:
		return series.QuantileOf(vals, r.measure.Quantile)
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	if r.measure.Kind == series.KindRatio {
		return sum / float64(len(vals))
	}
	return sum
}
