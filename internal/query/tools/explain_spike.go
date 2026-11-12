package tools

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
)

// shiftThreshold is the divergence below which a field's mix in the slice counts as
// its mix in the baseline. 0.1 nat is about one value going from a quarter of the
// rows to two fifths: well clear of sampling jitter over a few thousand rows, and
// low enough to keep a value that only part of the slice carries.
const shiftThreshold = 0.1

const (
	// minShare and minRise are how much of the slice a value must cover, and by how much
	// its share must rise over the rest's, to be news: a value on a handful of lines is not.
	minShare, minRise = 0.05, 0.05
	// maxShifts bounds the answer to what a reader takes in at a glance.
	maxShifts = 5
)

// ExplainSpikeRequest is the wire request for the explain_spike tool.
type ExplainSpikeRequest struct {
	Selector selector.Selector    `json:"selector"` // the scope
	Slice    []selector.Predicate `json:"slice"`    // the rows to explain, within the scope
	Snapshot string               `json:"snapshot,omitempty"`
}

// ExplainSpikeResponse is the wire response: the field values the slice over-represents,
// the largest lift first, and how many lines the slice and the rest of the scope hold.
type ExplainSpikeResponse struct {
	Shifts []Shift `json:"shifts"`
	Lines  int64   `json:"lines"`
	Rest   int64   `json:"rest"`
}

// Shift is one field value's share of the slice against its share of the rest of
// the scope, and the rows behind each share.
type Shift struct {
	Field         string  `json:"field"`
	Value         string  `json:"value"`
	SliceShare    float64 `json:"slice_share"`
	BaselineShare float64 `json:"baseline_share"`
	SliceCount    int64   `json:"slice_count"`
	BaselineCount int64   `json:"baseline_count"`
	// Also names, as field=value, the other fields whose shifts match this field's row for row.
	Also []string `json:"also,omitempty"`
}

// fieldShift is one field's shifts and the divergence that ranks it.
type fieldShift struct {
	field      string
	divergence float64
	shifts     []Shift
}

// ExplainSpike reports the enum field values the slice over-represents against the rest of
// its scope, the largest lift first.
func (t toolSet) ExplainSpike(ctx context.Context, req ExplainSpikeRequest) (query.Envelope[ExplainSpikeResponse], error) {
	var zero query.Envelope[ExplainSpikeResponse]

	sel, err := validateSelector(ctx, "explain_spike", req.Selector)
	if err != nil {
		return zero, err
	}
	if !sel.Window.Bounded() {
		return zero, query.Invalidf("explain_spike: a bounded time window is required")
	}
	if len(req.Slice) == 0 {
		return zero, query.Invalidf("explain_spike: slice is required — the predicates picking the rows to explain")
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "explain_spike", req.Snapshot)
	if err != nil {
		return zero, err
	}
	fields, held, cost, err := t.enumFields(ctx, snap, sel, req.Slice)
	if err != nil {
		return zero, err
	}

	narrows, pattern := scopes(req.Slice)
	if !held {
		// No segment holds a line of the slice, which shifts no field: only the scope is counted.
		narrows, fields = narrows[:1], nil
	}
	all, err := t.fieldCounts(ctx, snap, sel, narrows, slices.Concat([]string{segment.ColPatternID}, fields), &cost)
	if err != nil {
		return zero, err
	}
	resp := ExplainSpikeResponse{Shifts: []Shift{}}
	if held {
		resp.Lines = totalOf(all[0][1])
	}
	resp.Rest = totalOf(all[0][0]) - resp.Lines
	var ranked []fieldShift
	bounds := selector.ParsePredicates(req.Slice)
	for i, field := range fields {
		counts := all[i+1]
		if pattern > 0 && len(counts[pattern]) == 1 {
			continue // one value on every line of the pattern: the pattern fixes it
		}
		if fs, ok := shiftsOf(field, counts[1], within(field, bounds, counts[0])); ok {
			ranked = append(ranked, fs)
		}
	}
	slices.SortStableFunc(ranked, func(a, b fieldShift) int {
		return cmp.Or(cmp.Compare(b.divergence, a.divergence), cmp.Compare(a.field, b.field))
	})
	for _, fs := range collapseSame(ranked) {
		resp.Shifts = append(resp.Shifts, fs.shifts...)
	}
	slices.SortStableFunc(resp.Shifts, func(a, b Shift) int { return cmp.Compare(lift(b), lift(a)) })
	resp.Shifts = resp.Shifts[:min(len(resp.Shifts), maxShifts)]
	return query.Envelope[ExplainSpikeResponse]{
		Data: resp,
		Meta: query.Meta{Snapshot: snap.String(), Coverage: 1.0, Cost: cost, Tokens: query.EstimateTokens(resp)},
	}, nil
}

// scopes are the predicates narrowing the scope to the rows explain_spike counts each field
// over: none for the scope itself, the slice's and, when the slice picks a pattern, that
// pattern's, whose lines' fields the pattern may fix; pattern is which of them holds the
// pattern's lines, the slice itself when it picks nothing else, or -1 when it picks no
// pattern.
func scopes(slice []selector.Predicate) (narrows [][]selector.Predicate, pattern int) {
	patterns := slices.DeleteFunc(slices.Clone(slice), func(p selector.Predicate) bool {
		return p.Field != segment.ColPatternID
	})
	switch len(patterns) {
	case 0:
		return [][]selector.Predicate{nil, slice}, -1
	case len(slice):
		return [][]selector.Predicate{nil, slice}, 1
	}
	return [][]selector.Predicate{nil, slice, patterns}, 2
}

// within is scope's counts of field's values that the slice's bounds on field admit, so a
// field the slice bounds, as status >= 500, is compared with the rest of the scope inside
// the same bound rather than shifting by the bound alone.
func within(field string, bounds []selector.ParsedPredicate, scope map[string]int64) map[string]int64 {
	kept := scope
	for _, b := range bounds {
		if b.Field != field || len(b.Any) > 0 {
			continue
		}
		kept = maps.Clone(kept)
		maps.DeleteFunc(kept, func(v string, _ int64) bool { return !b.Matches(v, v != "") })
	}
	return kept
}

// lift is how many times more often the slice holds s's value than the rest does,
// unbounded for a value the rest never holds.
func lift(s Shift) float64 {
	if s.BaselineShare == 0 {
		return math.Inf(1)
	}
	return s.SliceShare / s.BaselineShare
}

// fieldCounts totals the rows of sel each of narrows picks by each field's values, all in
// one scan of the scope however many fields there are, adding what it read to cost:
// counts[f][n] is fields[f]'s totals over narrows[n].
func (t toolSet) fieldCounts(ctx context.Context, snap snapshot, sel selector.Resolved, narrows [][]selector.Predicate,
	fields []string, cost *query.Cost) ([][]map[string]int64, error) {
	req := totalsRequest(snap, sel, "", totalsGrid(t.summaryWidth(), sel.Window))
	for _, field := range fields {
		for _, where := range narrows {
			req.Measures = append(req.Measures, series.FilteredMeasure{Measure: req.Measure, Where: where, GroupBy: field})
		}
	}
	res, err := t.Series.Read(ctx, req)
	*cost = addCost(*cost, res.Cost)
	if err != nil {
		return nil, fmt.Errorf("explain_spike: counts: %w", err)
	}
	counts := make([][]map[string]int64, len(fields))
	for f := range fields {
		counts[f] = make([]map[string]int64, len(narrows))
		for n := range narrows {
			values := res.Measures[f*len(narrows)+n].Values
			counts[f][n] = keptTotals(values, slices.Collect(maps.Keys(values)))
		}
	}
	return counts, nil
}

// enumFields lists the fields of the scope's rows rank_fields would call enum, leaving out
// the ones the slice fixes to a value, whose shift would only restate the slice, and reports
// whether a segment in scope may hold the slice's lines, as its footer says.
func (t toolSet) enumFields(ctx context.Context, snap snapshot, sel selector.Resolved,
	slice []selector.Predicate) ([]string, bool, query.Cost, error) {
	f, err := t.scopeFields(ctx, snap, sel)
	if err != nil {
		return nil, false, query.Cost{}, fmt.Errorf("explain_spike: %w", err)
	}
	preds, compileErr := plan.Compile(slice, sel.Window)
	held := compileErr != nil
	for _, footer := range f.plan.Footers() {
		held = held || !slices.ContainsFunc(preds, func(c plan.CompiledPredicate) bool { return !plan.MayMatchSegment(footer, c) })
	}
	var names []string
	for _, stat := range f.stats() {
		// A field the slice only bounds, as path != "/health", still splits its lines.
		fixed := slices.ContainsFunc(slice, func(p selector.Predicate) bool {
			return p.Field == stat.Name && p.Op == selector.OpEq
		})
		if stat.Type == "enum" && !fixed {
			names = append(names, stat.Name)
		}
	}
	slices.Sort(names)
	return names, held, f.cost, nil
}

// collapseSame folds each field whose shifts have the shares of an earlier one's, value by
// value, into that field's Also, since the two split the rows the same way.
func collapseSame(ranked []fieldShift) []fieldShift {
	var kept []fieldShift
	for _, fs := range ranked {
		i := slices.IndexFunc(kept, func(k fieldShift) bool {
			return slices.EqualFunc(k.shifts, fs.shifts, func(a, b Shift) bool {
				return a.SliceShare == b.SliceShare && a.BaselineShare == b.BaselineShare
			})
		})
		if i < 0 {
			kept = append(kept, fs)
			continue
		}
		for j, s := range fs.shifts {
			kept[i].shifts[j].Also = append(kept[i].shifts[j].Also, s.Field+"="+s.Value)
		}
	}
	return kept
}

// shiftsOf compares one field's slice counts against the scope's, reporting the values
// the slice over-represents by at least minShare and minRise, the largest rise first. It reports false when the two mixes match, when the field is
// a measurement, or when it restates the slice: one value in every row of it and in no row
// outside it, or a field no row outside it holds.
func shiftsOf(field string, slice, scope map[string]int64) (fieldShift, bool) {
	baseline := make(map[string]int64, len(scope))
	for v, n := range scope {
		baseline[v] = n - slice[v]
	}
	sliceTotal, baselineTotal := float64(totalOf(slice)), float64(totalOf(baseline))
	if sliceTotal == 0 || baselineTotal == float64(baseline[""]) || measured(scope) {
		return fieldShift{}, false
	}
	if only := slices.Collect(maps.Keys(slice)); len(only) == 1 && baseline[only[0]] == 0 {
		return fieldShift{}, false
	}
	fs := fieldShift{field: field, divergence: stats.Divergence(slice, baseline)}
	if fs.divergence < shiftThreshold {
		return fieldShift{}, false
	}
	for _, v := range slices.Sorted(maps.Keys(slice)) {
		s := Shift{Field: field, Value: v, SliceCount: slice[v], BaselineCount: baseline[v],
			SliceShare: float64(slice[v]) / sliceTotal, BaselineShare: float64(baseline[v]) / baselineTotal}
		// Absence is news only where the rest of the scope always holds the field.
		if s.SliceShare >= minShare && s.SliceShare-s.BaselineShare >= minRise && (v != "" || s.BaselineShare == 0) {
			fs.shifts = append(fs.shifts, s)
		}
	}
	slices.SortStableFunc(fs.shifts, func(a, b Shift) int {
		return cmp.Compare(b.SliceShare-b.BaselineShare, a.SliceShare-a.BaselineShare)
	})
	return fs, true
}

// measured reports whether a field's values are all numbers, more of them than codes run
// to. Logfmt's numbers reach the store as strings, so the footers type such a field enum.
func measured(scope map[string]int64) bool {
	numbers := 0
	for v := range scope {
		if _, err := strconv.ParseFloat(v, 64); err == nil {
			numbers++
		} else if v != "" {
			return false
		}
	}
	return numbers > maxCodes
}
