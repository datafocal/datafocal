package tools

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// sampling is how sampleTraces reads: at which snapshot, skipping which trace, and, when
// horizon is set, each trace only within horizon of its first line, so an id that lives
// for hours, a user's, costs what a request's does.
type sampling struct {
	snapshot, skip string
	horizon        time.Duration
}

// sampleTraces picks the earliest limit traces of field among the lines sel selects with
// Aggregate and reads them, all in one scan (readTraces); meta adds up what they cost.
func (t toolSet) sampleTraces(ctx context.Context, sel selector.Selector, field string, limit int, how sampling) ([][]Span, query.Meta, error) {
	sample, err := t.Aggregate(ctx, AggregateRequest{
		Selector: sel, GroupBy: field, Bins: "all", Measures: []selector.MeasureSpec{{Measure: "min:" + segment.ColTimestamp}},
		Sort: "0:asc", Limit: limit, Snapshot: how.snapshot,
	})
	if err != nil {
		return nil, query.Meta{}, err
	}
	meta := sample.Meta
	var values []string
	var times []selector.TimeSpec
	for _, s := range sample.Data.Series {
		if s.Key == how.skip || s.Key == "" { // "" groups the lines without the field, no trace
			continue
		}
		values, times = append(values, s.Key), append(times, traceTime(sel.Time, s, how.horizon))
	}
	traces, err := t.readTraces(ctx, &meta, field, values, times)
	return traces, meta, err
}

// readTraces is the spans of each of values under field, as GetTrace reads one over its own
// time, from one scan of the union of their times for any of them: a hundred sampled traces
// cost a read of the lines they share rather than a hundred reads of it.
func (t toolSet) readTraces(ctx context.Context, meta *query.Meta, field string, values []string, times []selector.TimeSpec) ([][]Span, error) {
	if len(values) == 0 {
		return nil, nil
	}
	snap, err := t.resolveSnapshot(ctx, nil, "get_trace", meta.Snapshot)
	if err != nil {
		return nil, err
	}
	windows, union, err := traceWindows(ctx, times)
	if err != nil {
		return nil, err
	}
	fields, joinCost, err := t.equivalents(ctx, snap, union, field, true)
	if err != nil {
		return nil, fmt.Errorf("sampled traces: %w", err)
	}
	match := make([]selector.Predicate, 0, len(values)*len(fields))
	for _, v := range values {
		for _, f := range fields {
			match = append(match, selector.Predicate{Field: f, Op: selector.OpEq, Value: v})
		}
	}
	p, err := t.traceItems(ctx, snap, fields, values, windows)
	if err != nil {
		return nil, err
	}
	pair := instrumentedPair(p.Items)
	read, rowGroups, cost, err := t.scanSpanRows(ctx, p, selector.ParsePredicates([]selector.Predicate{{Any: match}}), maxTraceSpans*len(values), &pair)
	if err != nil {
		return nil, fmt.Errorf("sampled traces: %w", err)
	}
	meta.Cost = addCost(meta.Cost, addCost(cost, joinCost))
	if len(p.Items) > 0 {
		meta.Coverage = min(meta.Coverage, float64(rowGroups)/float64(len(p.Items)))
	}
	byTrace := rowsByTrace(read, fields, values)
	footers := itemFooters(p.Items[:rowGroups])
	traces := make([][]Span, len(values))
	for i, rows := range byTrace {
		var approx bool
		traces[i], approx = traceSpans(rows, p.Items[:rowGroups], footers, fields, traceMatch(fields, values[i]))
		meta.Approx = meta.Approx || approx
	}
	return traces, nil
}

// traceWindows is each of times resolved and the window covering them all.
func traceWindows(ctx context.Context, times []selector.TimeSpec) ([]selector.Window, selector.Window, error) {
	windows := make([]selector.Window, len(times))
	var union selector.Window
	for i, tm := range times {
		resolved, err := validateSelector(ctx, "get_trace", selector.Selector{Time: tm})
		if err != nil {
			return nil, selector.Window{}, err
		}
		windows[i], union = resolved.Window, widen(union, resolved.Window, i == 0)
	}
	return windows, union, nil
}

// rowsByTrace is read's rows of each of values, a row going to the first value one of fields names.
func rowsByTrace(read []scan.Row, fields, values []string) [][]scan.Row {
	of := make(map[string]int, len(values))
	for i, v := range values {
		of[v] = i
	}
	byTrace := make([][]scan.Row, len(values))
	for _, r := range read {
		for _, f := range fields {
			v, _ := r.Text(f)
			if i, ok := of[v]; ok {
				byTrace[i] = append(byTrace[i], r)
				break
			}
		}
	}
	return byTrace
}

// traceItems is the row groups any of values' traces reads under fields in its own window,
// each once, in plan order: what reading them apart would read, with the row groups they share
// read once.
func (t toolSet) traceItems(ctx context.Context, snap snapshot, fields, values []string, windows []selector.Window) (plan.Plan, error) {
	var union plan.Plan
	seen := map[[2]any]bool{}
	for i, v := range values {
		p, err := t.Planner.Prune(ctx, snap.catalog, selector.Resolved{Window: windows[i], Where: []selector.Predicate{traceMatch(fields, v)}})
		if err != nil {
			return plan.Plan{}, err
		}
		union.Cost = addCost(union.Cost, p.Cost)
		for _, wi := range p.Items {
			if key := [2]any{wi.Segment.ID, wi.RowGroup}; !seen[key] {
				seen[key] = true
				union.Items = append(union.Items, wi)
			}
		}
	}
	return union, nil
}

// traceSpans is one trace's spans from its rows as GetTrace builds them, its link pair the
// instrumented one its own segments carry or else discovered over its rows, and whether they
// are approximate.
func traceSpans(rows []scan.Row, items []plan.WorkItem, footers map[string]segment.Footer, fields []string, match selector.Predicate) ([]Span, bool) {
	held := map[string]bool{}
	for _, r := range rows {
		held[r.Segment.ID] = true
	}
	own := slices.DeleteFunc(slices.Clone(items), func(wi plan.WorkItem) bool { return !held[wi.Segment.ID] })
	pair := instrumentedPair(own)
	byKey := spanRows{}
	byKey.addDiscovered(rows, own, footers, fields, &pair)
	spans, _, approx := buildSpans(byKey, maxTraceSpans, pair.id, match)
	return spans, approx
}

// widen is w grown to cover o, or o itself when first.
func widen(w, o selector.Window, first bool) selector.Window {
	if first {
		return o
	}
	if o.From.IsZero() || !w.From.IsZero() && o.From.Before(w.From) {
		w.From = o.From
	}
	if o.To.IsZero() || !w.To.IsZero() && o.To.After(w.To) {
		w.To = o.To
	}
	return w
}

// traceTime is horizon either side of s's first line, or window without a horizon or a first line.
func traceTime(window selector.TimeSpec, s Series, horizon time.Duration) selector.TimeSpec {
	first := s.Values
	if len(s.MeasureValues) > 0 {
		first = s.MeasureValues[0]
	}
	if horizon == 0 || len(first) == 0 || first[0] == nil {
		return window
	}
	at := time.UnixMilli(int64(*first[0])).UTC()
	return selector.TimeSpec{From: at.Add(-horizon).Format(time.RFC3339), To: at.Add(horizon + time.Second).Format(time.RFC3339)}
}

// readTrace is GetTrace's spans for value of field in time, at meta's snapshot, its cost
// and coverage added to meta.
func (t toolSet) readTrace(ctx context.Context, meta *query.Meta, field, value string, time selector.TimeSpec) ([]Span, error) {
	env, err := t.GetTrace(ctx, TraceRequest{Field: field, Value: value, Time: time, Limit: maxTraceSpans, Snapshot: meta.Snapshot})
	meta.Cost = addCost(meta.Cost, env.Meta.Cost)
	meta.Coverage = min(meta.Coverage, env.Meta.Coverage)
	meta.Approx = meta.Approx || env.Meta.Approx
	return env.Data.Spans, err
}
