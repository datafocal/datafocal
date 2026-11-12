package tools

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/topology"
	"github.com/datafocal/datafocal/internal/value"
)

// get_trace reconstructs a span waterfall from log events sharing a correlation
// id: logs are the only ingest and traces are derived. It reads span structure
// when the producer emitted it and derives the same shape from plain log lines
// when it did not, so an uninstrumented service still appears in the waterfall.
//
// A log line is written when the work it reports finishes, so an event with a
// duration covers [ts-duration, ts] and one without is a point at ts. That is the
// whole basis for deriving nesting: the db line for a query lands before the
// request's own line, so its interval falls inside the request's. Reading ts as a
// span start would put the callee before the caller and nothing would nest.
//
// Explicit span structure wins over derivation in three places — spanKey
// (identity), spanRow.durMs (duration), parentKeyOf (causality) — each a two-line
// fallback, so one pipeline handles both input shapes and a request crossing an
// instrumented and an uninstrumented service resolves per span.
// The id and parent need not be span_id/parent_span_id: a trace without span_id
// discovers its pair (discoverParentPair), such as a transcript's uuid/parentUuid.

// The span-structure attributes an instrumented producer may emit. trace_id and
// duration_ms are spelled segment.ColTraceID and segment.ColDurationMs — they
// are real columns the codec sketches and the planner filters on, not names
// private to this tool.
const (
	attrSpanID       = "span_id"
	attrParentSpanID = "parent_span_id"
)

// The two keyspaces spans are grouped under. Prefixes keep a service name from
// ever colliding with a span_id when a trace mixes instrumented and plain
// events.
const (
	spanKeyPrefix    = "span:"
	serviceKeyPrefix = "svc:"
	stepKeyPrefix    = "step:"
)

// defaultTraceSpans caps returned spans when the caller gives no limit,
// matching search_logs.
const defaultTraceSpans = 100

// maxTraceSpans caps the span identities read before grouping, so a request
// fanning out to thousands of calls is grouped whole rather than cut at limit.
const maxTraceSpans = 10000 //nolint:dupdefault // a trace span cap, unrelated to series' bin cap

// minGroupSize is the fewest siblings sharing a service and name that collapse
// into a group span, and groupSlowest how many of the slowest stay as its children.
const (
	minGroupSize = 5
	groupSlowest = 3
)

// Span is one node of the reconstructed trace waterfall.
// Parent is the index of this span's parent within the enclosing Spans slice, or
// -1 for a root span. StartMs is the span start as Unix epoch milliseconds and
// DurMs its duration in milliseconds. ID is the span's stable key, ParentMethod
// how its parent was found ("read", "contained", or "" for a root),
// DurationMethod how its duration was ("reported" or "observed"), Lines the
// events folded into it, and Where the predicates selecting exactly those events.
// SelfMs is DurMs less the time its children cover, Critical marks a span on the
// path that sets the trace's latency (criticalPath), and CriticalInferred one whose
// path from the root crosses a contained link. Group is set on a span standing
// for repeated siblings, whose slowest and erroring members are its children. Sums
// totals a derived span's numeric columns over its lines.
type Span struct {
	Name             string               `json:"name"`
	Service          string               `json:"service"`
	StartMs          int64                `json:"start_ms"`
	DurMs            int64                `json:"dur_ms"`
	Error            bool                 `json:"error"`
	Parent           int                  `json:"parent"`
	ID               string               `json:"id"`
	ParentMethod     string               `json:"parent_method"`
	DurationMethod   string               `json:"duration_method"`
	Lines            int                  `json:"lines"`
	Where            []selector.Predicate `json:"where"`
	SelfMs           int64                `json:"self_ms"`
	Critical         bool                 `json:"critical"`
	CriticalInferred bool                 `json:"critical_inferred"`
	Group            *SpanGroup           `json:"group,omitempty"`
	Sums             map[string]float64   `json:"sums,omitempty"`
}

// SpanGroup summarizes the siblings a group span stands for: how many, the
// nearest-rank p50 and p95 and the max of their durations, and how many errored.
type SpanGroup struct {
	Count  int   `json:"count"`
	P50Ms  int64 `json:"p50_ms"`
	P95Ms  int64 `json:"p95_ms"`
	MaxMs  int64 `json:"max_ms"`
	Errors int   `json:"errors"`
}

// The ways a span's parent and duration are found.
const (
	parentRead       = "read"
	parentContained  = "contained"
	durationReported = "reported"
	durationObserved = "observed"
)

// TraceRequest is the wire request for get_trace. Field names the correlation id
// to group events by — any high-cardinality id column (trace_id, request_id,
// user_id), defaulting to trace_id; Value is required; Snapshot pins
// reproducibility; Limit caps returned spans (default 100 when <= 0). Time is
// optional for an id, whose membership filter or zone map prunes the corpus, and
// required for any other field — see boundedTraceWindow. Fields, defaulting to the
// equivalents of Field in a bounded window, gathers the lines matching Value in any of
// them. IDField and ParentField,
// named together or not at all, are the columns holding a span's id and its
// parent's; when neither is named they are span_id/parent_span_id for a trace
// carrying span_id and discovered otherwise.
type TraceRequest struct {
	Field       string            `json:"field,omitempty"`
	Fields      []string          `json:"fields,omitempty"`
	Value       string            `json:"value"`
	Time        selector.TimeSpec `json:"time,omitempty"`
	Snapshot    string            `json:"snapshot,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	IDField     string            `json:"id_field,omitempty"`
	ParentField string            `json:"parent_field,omitempty"`
}

// TraceResponse is the wire response: the reconstructed span waterfall for one
// correlated request, wrapped in query.Envelope[TraceResponse]. Field and Value
// echo the correlation the spans were grouped by. IDField and ParentField are the
// columns the spans were linked by, when named or discovered: empty for
// span_id/parent_span_id and for links by containment only. Fields names the fields
// joined when there are several. Total counts the spans before grouping. Meta.Approx
// reports whether any span's shape was derived rather than read.
type TraceResponse struct {
	Field       string `json:"field"`
	Value       string `json:"value"`
	IDField     string `json:"id_field,omitempty"`
	ParentField string `json:"parent_field,omitempty"`
	Spans       []Span `json:"spans"`
	Total       int    `json:"total"`

	Fields []string `json:"fields,omitempty"`
}

// GetTrace reconstructs a span waterfall for one correlation id. The id
// predicate routes through xor-filter membership pruning, so only segments whose
// filter admits the value are fetched; the surviving events are grouped into
// spans and parent-linked. Returns *query.InvalidRequestError for bad input,
// *query.APIError for an unknown correlation field, and propagates any other
// *query.APIError verbatim.
func (t toolSet) GetTrace(ctx context.Context, req TraceRequest) (query.Envelope[TraceResponse], error) {
	var zero query.Envelope[TraceResponse]

	// No name check on field. Traces here are derived from unstructured logs, so
	// what correlates events is whatever the producer happened to stamp on them —
	// grouping is just "events sharing this column's value", and that works for
	// any column. Rejecting a field for not being spelled like an id would refuse
	// a question the data can answer. A field the corpus does not carry is caught
	// by collectSpanRows against the footers, which is ground truth rather than a
	// guess about naming.
	field := cmp.Or(req.Field, segment.ColTraceID)
	if req.Value == "" {
		return zero, query.Invalidf("get_trace: value is required")
	}
	if (req.IDField == "") != (req.ParentField == "") {
		return zero, query.Invalidf("get_trace: id_field and parent_field are named together or not at all")
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultTraceSpans
	}

	snap, err := t.resolveSnapshot(ctx, nil, "get_trace", req.Snapshot)
	if err != nil {
		return zero, err
	}

	fields := req.Fields
	if len(fields) == 0 {
		fields = []string{field}
	}
	sel, idCost, err := t.boundedTraceWindow(ctx, snap, req, fields)
	if err != nil {
		return zero, err
	}
	var joinCost query.Cost
	if len(req.Fields) == 0 {
		if fields, joinCost, err = t.equivalents(ctx, snap, sel.Window, field, true); err != nil {
			return zero, fmt.Errorf("get_trace: %w", err)
		}
		sel.Where = []selector.Predicate{traceMatch(fields, req.Value)}
	}

	p, err := t.Planner.Prune(ctx, snap.catalog, sel)
	if err != nil {
		return zero, err // propagate APIError verbatim
	}
	p.Cost = addCost(p.Cost, addCost(joinCost, idCost))

	pair := linkPair{id: req.IDField, parent: req.ParentField}
	if pair == (linkPair{}) {
		pair = instrumentedPair(p.Items)
	}
	byKey, cost, coverage, err := t.collectSpanRows(ctx, p, sel.Where, fields, maxTraceSpans, &pair)
	if err != nil {
		return zero, err
	}

	spans, total, approx := buildSpans(byKey, limit, pair.id, sel.Where[0])
	resp := TraceResponse{Field: field, Value: req.Value, Spans: spans, Total: total}
	if len(fields) > 1 {
		resp.Fields = fields
	}
	if pair != spanIDPairs[0] {
		resp.IDField, resp.ParentField = pair.id, pair.parent
	}
	return query.Envelope[TraceResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: coverage,
			Approx:   approx,
			Cost:     cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}, nil
}

// boundedTraceWindow builds the selector the trace is read through, requiring a
// time bound unless every field is an id by its name or its values, with what finding
// that out cost.
//
// An id needs no window: its membership filter, or in a segment with too few ids for
// one its zone map, rejects the segments that do not hold the value, so the read is
// already proportional to the trace. Every other field — the tool deliberately accepts
// any column — may prune nothing. With no window that is the tenant's whole history,
// and the limit does not stop it: unlabeled events key by service, so byKey holds a
// handful of entries and never reaches it. One mistyped field name is a full-corpus scan.
func (t toolSet) boundedTraceWindow(ctx context.Context, snap snapshot, req TraceRequest, fields []string) (selector.Resolved, query.Cost, error) {
	sel, err := validateSelector(ctx, "get_trace", selector.Selector{
		Time:  req.Time,
		Where: []selector.Predicate{traceMatch(fields, req.Value)},
	})
	if err != nil || sel.Window.Bounded() {
		return sel, query.Cost{}, err
	}
	var cost query.Cost
	for _, f := range fields {
		if segment.IsID(f) {
			continue
		}
		id, read, err := t.filteredID(ctx, snap, sel.Window, selector.Predicate{Field: f, Op: selector.OpEq, Value: req.Value})
		cost = addCost(cost, read)
		if err != nil {
			return selector.Resolved{}, cost, err
		}
		if !id {
			return selector.Resolved{}, cost, query.NewAPIError(query.ErrCodeUnboundedScan,
				fmt.Sprintf("get_trace: %q is not an id, so a bounded time range is required", f)).
				WithHint("set time.from and time.to, or group by an _id column")
		}
	}
	return sel, cost, nil
}

// traceMatch selects the lines holding value in one of fields.
func traceMatch(fields []string, value string) selector.Predicate {
	match := make([]selector.Predicate, len(fields))
	for i, f := range fields {
		match[i] = selector.Predicate{Field: f, Op: selector.OpEq, Value: value}
	}
	if len(match) == 1 {
		return match[0]
	}
	return selector.Predicate{Any: match}
}

// equivalents is field, then, when it is a correlation column, named so or, where byValues,
// an id by its values, and the window is bounded, the correlation columns sharing most of its
// values across the window's services: the names one entity goes by.
func (t toolSet) equivalents(ctx context.Context, snap snapshot, window selector.Window, field string, byValues bool) ([]string, query.Cost, error) {
	if !window.Bounded() || !byValues && !segment.IsCorrelation(field) {
		return []string{field}, query.Cost{}, nil
	}
	var cost query.Cost
	if !segment.IsCorrelation(field) {
		id, read, err := t.filteredID(ctx, snap, window, selector.Predicate{Field: field})
		if err != nil || !id {
			return []string{field}, read, err
		}
		cost = read
	}
	descs, err := t.Planner.Segments(ctx, snap.catalog, selector.Resolved{Window: window})
	if err != nil {
		return nil, cost, err
	}
	descs, names := overlapping(descs, field)
	p, err := t.footers(ctx, descs)
	cost = addCost(cost, p.Cost)
	if err != nil {
		return nil, cost, err
	}
	regs, err := correlationRegisters(p, names)
	if err != nil {
		return nil, cost, err
	}
	return sharing(regs, field), cost, nil
}

// sharing is field, then the columns of regs sharing most of their values with it, sorted.
func sharing(regs map[string][]byte, field string) []string {
	fields := []string{field}
	own, ok := regs[field]
	if !ok {
		return fields
	}
	a := sketch.UnionEstimate(own)
	for _, col := range slices.Sorted(maps.Keys(regs)) {
		b := sketch.UnionEstimate(regs[col])
		shared, ok := topology.SharedValues(a, b, sketch.UnionEstimate(own, regs[col]))
		if col != field && ok && 2*shared > min(a, b) {
			fields = append(fields, col)
		}
	}
	return fields
}

// carriesNone reports whether items were read and none of their segments has a column for
// one of fields.
func carriesNone(items []plan.WorkItem, fields []string) bool {
	return len(items) > 0 && !slices.ContainsFunc(items, func(wi plan.WorkItem) bool {
		return slices.ContainsFunc(fields, func(f string) bool { _, ok := wi.Footer.ZoneMaps[f]; return ok })
	})
}

// overlapping is descs holding field or one of the otherNames it may go by, and those names
// with field; none without any.
func overlapping(descs []segment.Descriptor, field string) ([]segment.Descriptor, []string) {
	names := otherNames(descs, field)
	if len(names) == 0 {
		return nil, nil
	}
	names = append(names, field)
	return slices.DeleteFunc(descs, func(d segment.Descriptor) bool {
		return !slices.ContainsFunc(names, func(col string) bool { _, ok := d.ZoneMaps[col]; return ok })
	}), names
}

// otherNames is the correlation and string columns, which may be ids by their values, of
// segments with rows lacking field whose bounds overlap field's: only a column other lines
// carry can name the same entity differently, and only one whose values could be field's is
// worth reading the footers for.
func otherNames(descs []segment.Descriptor, field string) []string {
	lo, hi := bounds(descs, field)
	var others []string
	for _, d := range descs {
		if zm, ok := d.ZoneMaps[field]; ok && zm.PresentRows >= d.Rows || hi == "" {
			continue
		}
		for col, zm := range d.ZoneMaps {
			if (segment.IsCorrelation(col) || zm.Type == value.TypeString) && zm.Max.String >= lo && zm.Min.String <= hi && !slices.Contains(others, col) {
				others = append(others, col)
			}
		}
	}
	return others
}

// bounds is the least and greatest of field's values across descs, both "" where none holds it.
func bounds(descs []segment.Descriptor, field string) (lo, hi string) {
	for _, d := range descs {
		if zm, ok := d.ZoneMaps[field]; ok {
			lo, hi = cmp.Or(min(lo, zm.Min.String), zm.Min.String), max(hi, zm.Max.String)
		}
	}
	return lo, hi
}

// correlationRegisters is each of names' values across p's services, as HLL registers.
func correlationRegisters(p plan.Plan, names []string) (map[string][]byte, error) {
	values := map[string]*sketch.HLL{}
	for _, footer := range p.Footers() {
		for _, service := range footer.Services() {
			if err := mergeColumns(values, footer.ForService(service).DistinctCounts, names); err != nil {
				return nil, err
			}
		}
	}
	regs := make(map[string][]byte, len(values))
	for col, hll := range values {
		regs[col], _ = hll.Registers() // registers always read
	}
	return regs, nil
}

// mergeColumns merges each of names' sketch in from into into's.
func mergeColumns(into, from map[string]*sketch.HLL, names []string) error {
	for _, col := range names {
		hll, ok := from[col]
		if !ok {
			continue
		}
		if into[col] == nil {
			into[col] = sketch.NewHLL()
		}
		if err := into[col].Merge(hll); err != nil {
			return fmt.Errorf("merge %s values: %w", col, err)
		}
	}
	return nil
}

// spanRow accumulates every observed log event for one span identity. start is
// the earliest moment any of the span's work began and end the write time of its
// last event (see the file comment on timestamps); reported records whether some
// event stated its own duration, which is what separates a read duration from a
// derived one. A group row also carries the members it stands for and their summary, a
// service's row its lines, and a step row its pattern and its lines' numeric sums.
type spanRow struct {
	key       string
	parentID  string
	parentKey string
	name      string
	service   string
	start     time.Time
	end       time.Time
	reported  bool
	isError   bool
	lines     int
	members   []*spanRow
	group     *SpanGroup
	steps     []*spanRow
	pattern   string
	sums      map[string]float64
}

// durMs is the span's duration: the extent of the work it covers. Because start
// already backdates each event by the duration that event reported, this single
// expression covers a producer-reported span duration, a duration parsed out of
// message text, and a multi-event span whose extent is only observable — with no
// branch on which one it was.
func (r *spanRow) durMs() int64 {
	return r.end.Sub(r.start).Milliseconds()
}

// derived reports whether r's shape was worked out rather than read. A span with
// no span_id had its identity and parent link inferred; one with no reported
// duration had DurMs inferred from the observed event extent. Either way the
// caller is told the waterfall is approximate. A group is derived when a member is.
func (r *spanRow) derived() bool {
	if r.members != nil {
		return slices.ContainsFunc(r.members, (*spanRow).derived)
	}
	return !strings.HasPrefix(r.key, spanKeyPrefix) || !r.reported || r.parentID != "" && r.parentKey != "" && !r.readParent()
}

// readParent reports whether r's parent is the one it names, not one its interval found.
func (r *spanRow) readParent() bool {
	return r.parentID != "" && r.parentKey == spanKeyPrefix+r.parentID
}

// linkPair is a span's id and parent id columns, zero for containment links only.
type linkPair struct{ id, parent string }

// spanIDPairs are the pairs instrumented producers emit: span_id/parent_span_id, then
// spanId/parentSpanId as an OTLP JSON export spells them.
var spanIDPairs = []linkPair{{id: attrSpanID, parent: attrParentSpanID}, {id: "spanId", parent: "parentSpanId"}}

// instrumentedPair is the first of spanIDPairs some item's segment has an id column of,
// zero for none, so collectSpanRows discovers one.
func instrumentedPair(items []plan.WorkItem) linkPair {
	for _, pair := range spanIDPairs {
		if slices.ContainsFunc(items, func(wi plan.WorkItem) bool { _, ok := wi.Footer.ZoneMaps[pair.id]; return ok }) {
			return pair
		}
	}
	return linkPair{}
}

// spanColumns are the columns get_trace reads beyond the correlation field and the
// link pair, in a fixed order so cost accounting is deterministic. ts and level
// anchor and colour a span; service, if present, keeps the row identity from the
// body rather than the descriptor; pattern_id names a span no column names.
var spanColumns = []string{segment.ColTimestamp, segment.ColLevel, segment.ColService, segment.ColDurationMs, segment.ColPatternID}

// collectSpanRows reads the span and *pair columns of rows matching the exact
// correlation value in one of fields (xor false-positive defense), stopping at limit span identities
// unless a zero *pair must first be discovered from every string column, and
// returns the rows keyed by identity, the read cost, and the share of work items read.
func (t toolSet) collectSpanRows(ctx context.Context, p plan.Plan, where []selector.Predicate, fields []string,
	limit int, pair *linkPair) (rows spanRows, cost query.Cost, coverage float64, err error) {
	footers := itemFooters(p.Items)
	read, rowGroups, cost, err := t.scanSpanRows(ctx, p, selector.ParsePredicates(where), limit, pair)
	if err != nil {
		return nil, query.Cost{}, 0, fmt.Errorf("get_trace: %w", err)
	}
	byKey := spanRows{}
	byKey.addDiscovered(read, p.Items[:rowGroups], footers, fields, pair)

	// Distinguish "this column exists and nothing matched" from "no such column",
	// which are the same empty result but different next actions for the caller.
	// The codec writes a zone map for every column it encodes, so the footers
	// already read answer this without another GET.
	if carriesNone(p.Items[:rowGroups], fields) {
		return nil, query.Cost{}, 0, query.NewAPIError(query.ErrCodeUnknownField,
			fmt.Sprintf("get_trace: no segment in range carries a %q column", fields[0])).
			WithHint("call rank_fields to see which columns this tenant emits; any of them can group a trace")
	}

	// Everything the plan admitted was read unless the limit cut the loop short;
	// pruned row groups are excluded work, not missing work, so they do not count
	// against coverage.
	coverage = 1.0
	if len(p.Items) > 0 {
		coverage = float64(rowGroups) / float64(len(p.Items))
	}
	return byKey, cost, coverage, nil
}

// itemFooters is each item's footer by its segment.
func itemFooters(items []plan.WorkItem) map[string]segment.Footer {
	footers := make(map[string]segment.Footer, len(items))
	for _, wi := range items {
		footers[wi.Segment.ID] = wi.Footer
	}
	return footers
}

// scanSpanRows reads the rows matching matches of p's items in order until limit span
// identities are read, a zero pair counting none, and is how many items it read.
func (t toolSet) scanSpanRows(ctx context.Context, p plan.Plan, matching []selector.ParsedPredicate, limit int,
	pair *linkPair) ([]scan.Row, int, query.Cost, error) {
	// The rows are kept to discover the name column and maybe the pair over them; ids
	// counts the span identities read, which a zero pair cannot yet tell apart.
	var read []scan.Row
	ids := map[[2]string]bool{}
	cost := p.Cost
	rowGroups := 0
	for i, wi := range p.Items {
		if len(ids) >= limit {
			break
		}
		q := scan.Query{Where: matching, Columns: spanQueryColumns(wi, *pair)}
		rows := t.Scanner.Scan(ctx, plan.Plan{Items: p.Items[i : i+1]}, q)
		for rows.Next() {
			r := rows.Row()
			read = append(read, r)
			if *pair != (linkPair{}) {
				id, _ := r.Text(pair.id)
				ids[[2]string{rowService(r), id}] = true
			}
		}
		cost = addCost(cost, rows.Cost())
		if err := rows.Err(); err != nil {
			return nil, 0, query.Cost{}, err
		}
		rowGroups++
	}
	return read, rowGroups, cost, nil
}

// spanQueryColumns is what get_trace reads from wi: the span columns, pair, columns that
// may name a span, the message, whose being empty frees a row's name from its pattern
// (discoverNameColumn), and numeric columns a step sums, or every string column when the
// pair is to be discovered.
func spanQueryColumns(wi plan.WorkItem, pair linkPair) []string {
	cols := slices.Concat(spanColumns, []string{pair.id, pair.parent, segment.ColMessage}, namingColumns(wi))
	if pair == (linkPair{}) {
		cols = slices.Concat(spanColumns, stringColumns(wi))
	}
	for _, col := range slices.Sorted(maps.Keys(wi.Footer.ZoneMaps)) {
		if summed(col, wi.Footer.ZoneMaps[col]) {
			cols = append(cols, col)
		}
	}
	return cols
}

// addDiscovered folds rows, read from items, in by *pair (discovered when zero) and the name
// column discovered over them, neither one of the fields gathering them nor the name a
// producer field or stream label, which say where a line came from rather than what it did.
func (byKey spanRows) addDiscovered(rows []scan.Row, items []plan.WorkItem, footers map[string]segment.Footer, fields []string, pair *linkPair) {
	var all, naming, labels []string
	seen := map[string]bool{} // a segment's row groups share its footer and labels
	for _, wi := range items {
		if seen[wi.Segment.ID] {
			continue
		}
		seen[wi.Segment.ID] = true
		all, naming = append(all, stringColumns(wi)...), append(naming, namingColumns(wi)...)
		labels = slices.AppendSeq(labels, maps.Keys(wi.Segment.Stream.Labels))
	}
	others := func(cols []string, not ...string) []string {
		cols = slices.DeleteFunc(cols, func(c string) bool { return slices.Contains(fields, c) || slices.Contains(not, c) })
		slices.Sort(cols)
		return slices.Compact(cols)
	}
	if *pair == (linkPair{}) {
		*pair = discoverParentPair(rows, others(all))
	}
	name := discoverNameColumn(rows, others(naming, slices.Concat(spanColumns, producerFields, labels, []string{segment.ColMessage, pair.id, pair.parent})...))
	for _, r := range rows {
		byKey.add(r, *pair, footers, name)
	}
}

// stringColumns is every scalar string column of wi's segment, sorted.
func stringColumns(wi plan.WorkItem) []string {
	var cols []string
	for col, zm := range wi.Footer.ZoneMaps {
		if zm.Type == value.TypeString {
			cols = append(cols, col)
		}
	}
	slices.Sort(cols)
	return cols
}

// namingColumns is every string column of wi's segment that may name a span: one its
// footer counts the values of, unlike an id or free text, one value fills, read free, or
// one whose name says it names an operation, which a segment of many services' lines
// holds too many values of to count.
func namingColumns(wi plan.WorkItem) []string {
	cols := summary.CountedColumns(wi.Footer)
	for _, col := range stringColumns(wi) {
		zm := wi.Footer.ZoneMaps[col]
		if (zm.Min.String == zm.Max.String || operationName.MatchString(col)) && !slices.Contains(cols, col) {
			cols = append(cols, col)
		}
	}
	return cols
}

// operationName is a column whose name says it names an operation: OpenTelemetry's span
// name, an operation, a method or a route.
var operationName = regexp.MustCompile(`(?i)(^|[._])(name|operation|op|method|route)$`)

// discoverNameColumn is the column of cols every row holds with one value across a service's
// rows of a pattern, holding the most values and at least two (one value names no operation
// apart), so "Charge ok" and "Charge error" lines share the name Charge; "" when none does.
// A row with no message, as an OpenTelemetry span is, has a pattern named by a
// few of its values, its kind, rather than templated from text, which says nothing a name
// must agree with, so only its holding a value counts: otherwise a service's every client
// call shares one pattern, and only a column naming where it ran, its pod, agrees across them.
func discoverNameColumn(rows []scan.Row, cols []string) string {
	best, most := "", 1
	for _, col := range cols {
		values := nameValues(rows, col)
		if len(values) > most {
			best, most = col, len(values)
		}
	}
	return best
}

// nameValues is the values col names rows' operations by, nil when a row holds none or two
// rows of a service's pattern with a message hold different ones.
func nameValues(rows []scan.Row, col string) map[string]bool {
	names := map[segment.ServicePattern]string{}
	values := map[string]bool{}
	for _, r := range rows {
		v, _ := r.Text(col)
		if v == "" {
			return nil
		}
		values[v] = true
		pattern, _ := r.Text(segment.ColPatternID)
		if message, _ := r.Text(segment.ColMessage); message == "" {
			continue
		}
		at := segment.ServicePattern{Service: rowService(r), Pattern: pattern}
		if named, ok := names[at]; ok && named != v {
			return nil
		}
		names[at] = v
	}
	return values
}

// discoverParentPair is the sorted cols pair (A, B) whose non-empty B values name another
// row's A on the most rows, and at least half the time, where excluding a row's own A keeps
// columns that merely repeat each other, like a message's type and role, from passing for a
// parent link. Counting rows rather than the share keeps a pair two rows happen to link
// from beating the one linking the whole trace. A parent is logged no later than its child,
// so at least half a pair's links must name an earlier row: a chain links as well read
// backwards, uuid naming parentUuid, and only time tells the two apart. An id column holds
// a value on the rows it links, so A must be on at least half as many rows as B links:
// otherwise a column every row repeats, a working directory, would link every row to the
// one row whose A happens to hold the same value. A pair no row's B repeats chains each line
// to one other and branches nowhere, which says no more than time order, so it is no pair:
// the lines are read as their service's steps instead.
func discoverParentPair(rows []scan.Row, cols []string) linkPair {
	values, ids := readColumns(rows, cols)
	best := bestPair(rows, cols, values, ids)
	if best != (linkPair{}) && !repeats(values[best.parent].vals) {
		return linkPair{}
	}
	return best
}

// bestPair is the first pair in cols order naming the most distinct parents, then linking
// the most rows, among those that pass: a value many rows repeat, a model copied onto every
// line, links them all to one row, which no real pair loses to. A pair names no more parents nor links
// more rows than its parent column holds values, nor twice the rows its id is on, so the
// columns are tried most held first and a pair that cannot reach the best is never counted.
func bestPair(rows []scan.Row, cols []string, values map[string]heldColumn, ids map[string]map[string]time.Time) linkPair {
	p, byHeld := newPairSearch(rows, cols, values, ids)
	for _, b := range byHeld {
		if p.set[b] == 0 || p.set[b] < p.most {
			break
		}
		p.share(b)
		for _, a := range byHeld {
			if 2*p.set[a] < p.most || p.set[a] == 0 {
				break
			}
			if a != b && p.shares[a] == b+1 {
				p.try(a, b)
			}
		}
	}
	if p.first[0] < 0 {
		return linkPair{}
	}
	return linkPair{id: cols[p.first[0]], parent: cols[p.first[1]]}
}

// pairSearch is bestPair's state: the columns and their values, how many rows hold each, and
// the best pair so far, by index into cols, with the parents it names and the rows it links.
type pairSearch struct {
	rows         []scan.Row
	cols         []string
	values       map[string]heldColumn
	ids          map[string]map[string]time.Time
	set          []int
	most, linked int
	first        [2]int
	// holders is the columns holding each value, and shares is b+1 for each column holding a
	// value parent column b does, the only ones b can link to.
	holders map[string][]int
	shares  []int
}

// newPairSearch is a search of cols, and their indexes most held first.
func newPairSearch(rows []scan.Row, cols []string, values map[string]heldColumn, ids map[string]map[string]time.Time) (*pairSearch, []int) {
	p := &pairSearch{rows: rows, cols: cols, values: values, ids: ids, set: make([]int, len(cols)), first: [2]int{-1, -1},
		holders: map[string][]int{}, shares: make([]int, len(cols))}
	byHeld := make([]int, len(cols))
	for i, c := range cols {
		p.set[i], byHeld[i] = len(values[c].rows), i
		for v := range ids[c] {
			p.holders[v] = append(p.holders[v], i)
		}
	}
	slices.SortStableFunc(byHeld, func(x, y int) int { return cmp.Compare(p.set[y], p.set[x]) })
	return p, byHeld
}

// share marks the columns holding a value of parent column b.
func (p *pairSearch) share(b int) {
	for v := range p.ids[p.cols[b]] {
		for _, a := range p.holders[v] {
			p.shares[a] = b + 1
		}
	}
}

// try counts the pair of id column a and parent column b, keeping it when it passes and
// beats the best so far, or ties it and comes first in cols order.
func (p *pairSearch) try(a, b int) {
	l := linkCount(p.rows, p.values[p.cols[a]], p.ids[p.cols[a]], p.values[p.cols[b]])
	if 2*l.linked < l.named || 2*l.earlier < l.linked || 2*p.set[a] < l.linked || l.linked == 0 {
		return
	}
	better := cmp.Or(cmp.Compare(l.parents, p.most), cmp.Compare(l.linked, p.linked))
	first := p.first[0] < 0 || a < p.first[0] || a == p.first[0] && b < p.first[1]
	if better > 0 || better == 0 && first {
		p.most, p.linked, p.first = l.parents, l.linked, [2]int{a, b}
	}
}

// repeats reports whether a set value of values occurs twice.
func repeats(values []string) bool {
	seen := map[string]bool{}
	for _, v := range values {
		if v != "" && seen[v] {
			return true
		}
		seen[v] = true
	}
	return false
}

// heldColumn is a column's set values in a trace's rows, by ascending row index.
type heldColumn struct {
	rows []int
	vals []string
}

// readColumns is each of cols' set values in rows, and when each was first logged, read row
// by row from the columns each holds, since a trace's rows each hold a few of thousands.
func readColumns(rows []scan.Row, cols []string) (map[string]heldColumn, map[string]map[string]time.Time) {
	values, ids := map[string]heldColumn{}, make(map[string]map[string]time.Time, len(cols))
	for _, col := range cols {
		ids[col] = map[string]time.Time{}
	}
	for i, r := range rows {
		r.Each(func(col string, _ []byte) {
			first, ok := ids[col]
			if !ok {
				return
			}
			v, _ := r.Text(col)
			if v == "" {
				return
			}
			held := values[col]
			held.rows, held.vals = append(held.rows, i), append(held.vals, v)
			values[col] = held
			if at, seen := first[v]; !seen || r.Time.Before(at) {
				first[v] = r.Time
			}
		})
	}
	return values, ids
}

// links counts a candidate pair's parent links: the rows naming a parent, those whose
// parent is another row, those whose parent row was logged no later than they were, and the
// distinct parents they name.
type links struct{ named, linked, earlier, parents int }

// linkCount counts the links parents' values make to the ids rows hold, each row's own id
// excepted, given when each id was first logged.
func linkCount(rows []scan.Row, own heldColumn, ids map[string]time.Time, parents heldColumn) links {
	var l links
	named := map[string]bool{}
	j := 0
	for k, i := range parents.rows {
		v := parents.vals[k]
		l.named++
		for j < len(own.rows) && own.rows[j] < i {
			j++
		}
		at, ok := ids[v]
		if !ok || j < len(own.rows) && own.rows[j] == i && own.vals[j] == v {
			continue
		}
		l.linked++
		named[v] = true
		if !at.After(rows[i].Time) {
			l.earlier++
		}
	}
	l.parents = len(named)
	return l
}

// spanRows keys the spans by identity as their events are read.
type spanRows map[string]*spanRow

// add folds one event into its span identity (spanKey): start and end widen to
// the observed extremes, isError ORs across every event, and name/parentID come from
// the event that starts earliest, its name column or else its pattern's text.
func (byKey spanRows) add(r scan.Row, pair linkPair, footers map[string]segment.Footer, nameCol string) {
	service := rowService(r)
	end := r.Time
	reportedMs, reported := parseMillis(cell(r, segment.ColDurationMs))
	start := end.Add(-time.Duration(reportedMs) * time.Millisecond)
	key := spanKey(r, service, pair.id)

	prev, ok := byKey[key]
	if !ok {
		prev = &spanRow{key: key, service: service}
		byKey[key] = prev
	}
	line := &spanRow{key: stepKeyPrefix + service, parentKey: key, name: cmp.Or(cell(r, nameCol), spanName(r, footers[r.Segment.ID], service)),
		service: service, start: start, end: end, reported: reported, isError: isErrorLevel(r), lines: 1}
	if !strings.HasPrefix(key, spanKeyPrefix) {
		line.pattern, line.sums = cell(r, segment.ColPatternID), numericCells(r, footers[r.Segment.ID])
		prev.steps = append(prev.steps, line)
	}
	if !ok || start.Before(prev.start) {
		prev.parentID = cell(r, pair.parent)
		prev.name = line.name
	}
	prev.widen(line)
}

// widen folds o's lines into r: its extent, and whether one reported a duration or errored.
func (r *spanRow) widen(o *spanRow) {
	if r.lines == 0 || o.start.Before(r.start) {
		r.start = o.start
	}
	if r.lines == 0 || o.end.After(r.end) {
		r.end = o.end
	}
	r.lines += o.lines
	r.reported = r.reported || o.reported
	r.isError = r.isError || o.isError
	for col, v := range o.sums {
		if r.sums == nil {
			r.sums = map[string]float64{}
		}
		r.sums[col] += v
	}
}

// summed reports whether a step sums column col: a number, but not the duration its extent
// already says nor a number masked out of a message.
func summed(col string, zm segment.ZoneMap) bool {
	return zm.Type.Numeric() && col != segment.ColDurationMs && !strings.HasPrefix(col, segment.ParamPrefix)
}

// numericCells is r's value in each column of its segment's footer f a step sums.
func numericCells(r scan.Row, f segment.Footer) map[string]float64 {
	nums := map[string]float64{}
	for col, zm := range f.ZoneMaps {
		if !summed(col, zm) {
			continue
		}
		if v, err := strconv.ParseFloat(cell(r, col), 64); err == nil {
			nums[col] = v
		}
	}
	return nums
}

// splitSteps names r after the line covering the most of it, the earliest of those that tie,
// and is r's lines in time order as steps, each its service's consecutive lines of one pattern
// with no gap between them over the p95 of its gaps, nested by containment, or none when they
// make one.
func (r *spanRow) splitSteps() []*spanRow {
	lines := r.steps
	slices.SortStableFunc(lines, func(a, b *spanRow) int { return a.end.Compare(b.end) })
	if len(lines) > 0 {
		r.name = slices.MinFunc(lines, func(a, b *spanRow) int {
			return cmp.Or(cmp.Compare(b.durMs(), a.durMs()), a.start.Compare(b.start))
		}).name
	}
	gaps := make([]int64, 0, len(lines))
	for i := 1; i < len(lines); i++ {
		gaps = append(gaps, lines[i].end.Sub(lines[i-1].end).Milliseconds())
	}
	slices.Sort(gaps)
	p95 := nearestRank(gaps, 0.95)
	var steps []*spanRow
	for i, l := range lines {
		if i == 0 || l.pattern != lines[i-1].pattern || l.end.Sub(lines[i-1].end).Milliseconds() > p95 {
			steps = append(steps, &spanRow{key: fmt.Sprintf("%s:%d", l.key, len(steps)), parentKey: r.key, name: l.name, service: l.service, pattern: l.pattern})
		}
		steps[len(steps)-1].widen(l)
	}
	if len(steps) < 2 {
		return nil
	}
	for _, s := range steps {
		s.parentKey = cmp.Or(innermostContaining(s, steps), r.key)
	}
	return r.fold(steps)
}

// fold is steps less r's only outermost step when it is one line spanning all of r, its steps moved up to r.
func (r *spanRow) fold(steps []*spanRow) []*spanRow {
	top := slices.DeleteFunc(slices.Clone(steps), func(s *spanRow) bool { return s.parentKey != r.key })
	if len(top) != 1 || top[0].lines != 1 || !top[0].start.Equal(r.start) || !top[0].end.Equal(r.end) {
		return steps
	}
	for _, s := range steps {
		if s.parentKey == top[0].key {
			s.parentKey = r.key
		}
	}
	return slices.DeleteFunc(steps, func(s *spanRow) bool { return s == top[0] })
}

// spanKey is the identity an event's span is grouped under: the producer's
// span id in idField when the event carries one, and otherwise the emitting service — one
// span per service's work on this correlated request, which is the only span
// boundary a plain log stream reveals. This single fallback is the difference
// between reading an instrumented trace and deriving one; everything downstream
// treats both alike.
func spanKey(r scan.Row, service, idField string) string {
	if id := cell(r, idField); idField != "" && id != "" {
		return spanKeyPrefix + id
	}
	return serviceKeyPrefix + service
}

// isErrorLevel reports whether r is an error-level event.
func isErrorLevel(r scan.Row) bool {
	return cell(r, segment.ColLevel) == "error"
}

// buildSpans resolves every span's parent, orders the result so a span always
// precedes any span naming it as parent, groups repeated siblings, caps the result at limit, and reports
// the span count before the cap and whether any returned span's shape was derived (which the caller surfaces as
// Meta.Approx). Each span's Where selects its events by idField or service, and corr.
//
// Truncating after the topological order is what makes limit safe to apply here: a
// parent always sits at a lower index than its children, so dropping the tail can
// only remove descendants and never orphans a span that survives. Parents are
// resolved over the full set first, so a link is decided by the whole trace rather
// than by whichever spans happened to fit.
func buildSpans(byKey map[string]*spanRow, limit int, idField string, corr selector.Predicate) ([]Span, int, bool) {
	rows := make([]*spanRow, 0, len(byKey))
	for _, r := range byKey {
		rows = append(rows, r)
	}
	// Sorted before parent resolution so innermostContaining breaks ties
	// deterministically, and so the topological pass starts from a stable order.
	slices.SortFunc(rows, compareSpanRows)

	children := namedChildren(rows)
	for _, r := range rows {
		r.parentKey = parentKeyOf(r, rows, byKey, children)
	}
	// Steps nest under their service's span by construction, so they are split only now.
	for _, r := range slices.Clone(rows) {
		rows = append(rows, r.splitSteps()...)
	}
	slices.SortFunc(rows, compareSpanRows)

	rows = groupSiblings(topoOrder(rows))
	total := len(rows)
	rows = rows[:min(total, limit)]

	indexOf := make(map[string]int, len(rows))
	for i, r := range rows {
		indexOf[r.key] = i
	}

	spans := make([]Span, len(rows))
	approx := false
	for i, r := range rows {
		parent := -1
		if idx, ok := indexOf[r.parentKey]; ok {
			parent = idx
		}
		spans[i] = Span{
			Name:    r.name,
			Service: r.service,
			StartMs: r.start.UnixMilli(),
			// Whole milliseconds between the start's and the end's, so StartMs plus DurMs is the
			// end's millisecond, as aggregate's max:ts reads it, rather than one short of it.
			DurMs:          r.end.UnixMilli() - r.start.UnixMilli(),
			Error:          r.isError,
			Parent:         parent,
			ID:             r.key,
			ParentMethod:   parentMethod(r, parent),
			DurationMethod: durationObserved,
			Lines:          r.lines,
			Where:          r.where(idField, corr),
			Group:          r.group,
			Sums:           r.sums,
		}
		if r.reported {
			spans[i].DurationMethod = durationReported
		}
		approx = approx || r.derived()
	}
	criticalPath(spans)
	return spans, total, approx
}

// criticalPath sets each span's SelfMs and marks the spans that set the trace's
// latency, starting from the root that ends last.
func criticalPath(spans []Span) {
	children := make([][]int, len(spans))
	var roots []int
	for i, s := range spans {
		if s.Parent < 0 {
			roots = append(roots, i)
		} else {
			children[s.Parent] = append(children[s.Parent], i)
		}
	}
	for i := range spans {
		spans[i].SelfMs = selfMs(spans, i, children[i])
	}
	if len(roots) == 0 {
		return
	}
	last := slices.MaxFunc(roots, func(a, b int) int { return cmp.Compare(spanEnd(spans[a]), spanEnd(spans[b])) })
	markCritical(spans, children, last, false)
}

// markCritical marks span i critical, then walks back from its end taking each
// child that ends last at or before the cursor and moving the cursor to that
// child's start. inferred carries whether a contained link lies above i.
func markCritical(spans []Span, children [][]int, i int, inferred bool) {
	inferred = inferred || spans[i].ParentMethod == parentContained
	spans[i].Critical, spans[i].CriticalInferred = true, inferred
	byEnd := slices.Clone(children[i])
	slices.SortStableFunc(byEnd, func(a, b int) int { return cmp.Compare(spanEnd(spans[b]), spanEnd(spans[a])) })
	cursor := spanEnd(spans[i])
	for _, c := range byEnd {
		if spanEnd(spans[c]) <= cursor {
			cursor = spans[c].StartMs
			markCritical(spans, children, c, inferred)
		}
	}
}

// selfMs is span i's duration less the union of its children's intervals,
// clipped to its own, since a read parent link need not nest in time.
func selfMs(spans []Span, i int, children []int) int64 {
	s := spans[i]
	byStart := slices.Clone(children)
	slices.SortFunc(byStart, func(a, b int) int { return cmp.Compare(spans[a].StartMs, spans[b].StartMs) })
	self, covered := s.DurMs, s.StartMs
	for _, c := range byStart {
		start, end := max(spans[c].StartMs, covered), min(spanEnd(spans[c]), spanEnd(s))
		if end > start {
			self -= end - start
			covered = end
		}
	}
	return self
}

// spanEnd is the epoch millisecond s ends at.
func spanEnd(s Span) int64 {
	return s.StartMs + s.DurMs
}

// parentMethod is how r's parent, at index parent, was found: read from the
// parent id r names, contained when derived from intervals, or "" for a root.
func parentMethod(r *spanRow, parent int) string {
	switch {
	case parent < 0:
		return ""
	case r.readParent():
		return parentRead
	default:
		return parentContained
	}
}

// where is the predicates selecting r's events: its span id, or else its
// service, or for a step its pattern in its service, or for a group any of its members',
// alongside the trace's correlation predicate corr.
func (r *spanRow) where(idField string, corr selector.Predicate) []selector.Predicate {
	if r.members != nil {
		anyOf := make([]selector.Predicate, len(r.members))
		for i, m := range r.members {
			anyOf[i] = m.where(idField, corr)[0]
		}
		// Steps of one pattern are selected alike, so a group of them is selected as one is.
		if !slices.ContainsFunc(anyOf, func(p selector.Predicate) bool { return p.Field != anyOf[0].Field || p.Value != anyOf[0].Value }) {
			return r.members[0].where(idField, corr)
		}
		return []selector.Predicate{{Any: anyOf}, corr}
	}
	own := selector.Predicate{Field: segment.ColService, Op: selector.OpEq, Value: r.service}
	if id, ok := strings.CutPrefix(r.key, spanKeyPrefix); ok {
		own = selector.Predicate{Field: idField, Op: selector.OpEq, Value: id}
	}
	if strings.HasPrefix(r.key, stepKeyPrefix) {
		return []selector.Predicate{{Field: segment.ColPatternID, Op: selector.OpEq, Value: r.pattern}, own, corr}
	}
	return []selector.Predicate{own, corr}
}

// parentKeyOf is a span's causal parent: the span_id the producer named, when it
// named one in this trace, and otherwise the innermost span whose interval encloses
// this one, not among its own descendants. A named parent missing from the trace is a
// hop that wrote no span, as a proxy that traces nothing is, so the span is linked
// through it to the caller enclosing it. Returns "" for a root — a span with no
// parent or enclosing span, or a span naming itself.
func parentKeyOf(r *spanRow, rows []*spanRow, byKey map[string]*spanRow, children map[string][]*spanRow) string {
	if r.parentID != "" {
		key := spanKeyPrefix + r.parentID
		if key == r.key {
			return ""
		}
		if _, ok := byKey[key]; ok {
			return key
		}
		below := namedDescendants(r.key, children)
		rows = slices.DeleteFunc(slices.Clone(rows), func(c *spanRow) bool { return below[c] })
	}
	return innermostContaining(r, rows)
}

// namedChildren is the rows naming each span key their parent.
func namedChildren(rows []*spanRow) map[string][]*spanRow {
	children := map[string][]*spanRow{}
	for _, c := range rows {
		if c.parentID != "" {
			children[spanKeyPrefix+c.parentID] = append(children[spanKeyPrefix+c.parentID], c)
		}
	}
	return children
}

// namedDescendants is the rows whose named parents lead, span by span, to key, so linking
// key under one would make a cycle; one walk down from key rather than one up from each row.
func namedDescendants(key string, children map[string][]*spanRow) map[*spanRow]bool {
	below := map[*spanRow]bool{}
	for next := slices.Clone(children[key]); len(next) > 0; {
		c := next[len(next)-1]
		if next = next[:len(next)-1]; !below[c] {
			below[c] = true
			next = append(next, children[c.key]...)
		}
	}
	return below
}

// innermostContaining returns the key of the narrowest span whose interval
// encloses r's, or "" when none does. Narrowest makes the link the nearest
// enclosing caller rather than the outermost one, which is what a waterfall's
// nesting means. rows must be pre-sorted so equal-extent candidates resolve to
// the same one every time.
func innermostContaining(r *spanRow, rows []*spanRow) string {
	best := ""
	var bestExtent time.Duration
	for _, c := range rows {
		if c.key == r.key || !encloses(c, r) {
			continue
		}
		if extent := c.end.Sub(c.start); best == "" || extent < bestExtent {
			best, bestExtent = c.key, extent
		}
	}
	return best
}

// encloses reports whether outer's interval contains inner's with at least one
// endpoint strictly outside. The strictness matters: two spans with identical
// intervals would otherwise each claim the other as parent, which is a cycle.
func encloses(outer, inner *spanRow) bool {
	if outer.start.After(inner.start) || outer.end.Before(inner.end) {
		return false
	}
	return outer.start.Before(inner.start) || outer.end.After(inner.end)
}

// compareSpanRows orders spans by start, then by the wider span first (a
// container precedes what it contains), then by key so the result is total.
func compareSpanRows(a, b *spanRow) int {
	return cmp.Or(
		a.start.Compare(b.start),
		b.end.Compare(a.end),
		cmp.Compare(a.key, b.key),
	)
}

// topoOrder reorders pre-sorted rows so a span always precedes any span that
// names it as parent, holding back a span that sorts before its parent until the parent is placed.
func topoOrder(rows []*spanRow) []*spanRow {
	waiting := map[string][]*spanRow{}
	placed := make(map[string]bool, len(rows))
	order := make([]*spanRow, 0, len(rows))
	var place func(r *spanRow)
	place = func(r *spanRow) {
		if placed[r.key] {
			return
		}
		placed[r.key] = true
		order = append(order, r)
		for _, c := range waiting[r.key] {
			place(c)
		}
	}
	for _, r := range rows {
		if r.parentKey == "" || placed[r.parentKey] {
			place(r)
		} else {
			waiting[r.parentKey] = append(waiting[r.parentKey], r)
		}
	}
	for _, r := range rows {
		place(r) // a parent cycle in bad client data readies none of its spans
	}
	return order
}

// groupSiblings replaces each set of at least minGroupSize siblings sharing a service and name
// with a group row placed where the first stood, dropping unkept members' descendants so the order stays topological.
func groupSiblings(rows []*spanRow) []*spanRow {
	type siblings struct{ parent, service, name string }
	bySiblings := map[siblings][]*spanRow{}
	for _, r := range rows {
		k := siblings{r.parentKey, r.service, r.name}
		bySiblings[k] = append(bySiblings[k], r)
	}
	groupBefore := map[*spanRow]*spanRow{}
	dropped := map[string]bool{}
	for _, members := range bySiblings {
		if len(members) >= minGroupSize {
			groupBefore[members[0]] = newGroupRow(members, dropped)
		}
	}

	out := make([]*spanRow, 0, len(rows))
	keep := func(r *spanRow) {
		if dropped[r.key] || dropped[r.parentKey] {
			dropped[r.key] = true
			return
		}
		out = append(out, r)
	}
	for _, r := range rows {
		if g := groupBefore[r]; g != nil {
			keep(g)
		}
		keep(r)
	}
	return out
}

// newGroupRow is the row standing for members, reparenting the groupSlowest slowest and
// every erroring member under it and marking the rest dropped.
func newGroupRow(members []*spanRow, dropped map[string]bool) *spanRow {
	first := members[0]
	g := &spanRow{
		key:       "group:" + first.key,
		parentID:  first.parentID,
		parentKey: first.parentKey,
		name:      first.name,
		service:   first.service,
		members:   members,
		group:     &SpanGroup{Count: len(members)},
	}
	for _, m := range members {
		g.widen(m)
		if m.isError {
			g.group.Errors++
		}
	}

	slowest := slices.Clone(members)
	slices.SortStableFunc(slowest, func(a, b *spanRow) int { return cmp.Compare(b.durMs(), a.durMs()) })
	for i, m := range slowest {
		if i < groupSlowest || m.isError {
			m.parentKey = g.key
		} else {
			dropped[m.key] = true
		}
	}
	// slowest is descending, so the nearest-rank quantile q sits ceil(q*n) from its end.
	n := len(slowest)
	g.group.P50Ms = slowest[n-int(math.Ceil(0.5*float64(n)))].durMs()
	g.group.P95Ms = slowest[n-int(math.Ceil(0.95*float64(n)))].durMs()
	g.group.MaxMs = slowest[0].durMs()
	return g
}

// cell is r's value for column as an owned string, or "" when the row has none.
func cell(r scan.Row, column string) string {
	v, _ := r.Value(column)
	return string(v)
}

// rowService is the service r was logged by: its service column, else its stream's.
func rowService(r scan.Row) string {
	service, _ := r.Text(segment.ColService)
	return cmp.Or(service, r.Segment.Stream.Service)
}

// spanName is the name of a span no column names: the text of r's pattern in its
// segment's footer f, falling back to the span's service.
func spanName(r scan.Row, f segment.Footer, service string) string {
	text, _ := f.PatternText(cell(r, segment.ColPatternID))
	return cmp.Or(text, service)
}

// parseMillis reads a millisecond duration value — a duration_ms attribute, or the
// param the collector names duration_ms when it parses one out of message text —
// and reports whether it found a usable one.
//
// Floating point, not integer: the collector names the param ahead of an "ms" token
// duration_ms, so "query took 12.5 ms" yields "12.5", which an integer parse read
// as zero.
//
// The value is untrusted log text, so a duration that cannot describe an extent
// (negative, NaN) is reported absent — it would put a span's start after its end,
// corrupting the containment checks every derived parent link depends on.
func parseMillis(s string) (int64, bool) {
	ms, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(ms) || math.IsInf(ms, 0) || ms < 0 {
		return 0, false
	}
	return int64(math.Round(ms)), true
}
