package series

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
)

// fromRows reads descs' rows for a measure neither the summary nor a footer sketch
// answers, exactly except for distinct, whose sketch still estimates however its rows
// were chosen.
func (r reader) fromRows(ctx context.Context, descs []segment.Descriptor, req Request) (Result, error) {
	if req.Measure.Kind == KindQuantile {
		return r.quantileFromRows(ctx, descs, req)
	}
	key, field := newBinKey(req), req.Measure.Field
	m := ordered(req, key, rowMeasure(key, req.Measure))
	var columns []string
	if field != "" {
		columns = []string{field}
	}
	cost, err := r.scanRows(ctx, descs, req, m, columns...)
	if err == nil && field != "" {
		err = r.uncarried(descs, field, "for "+req.Measure.Kind.label())
	}
	res := m.result()
	res.Cost, res.Coverage, res.Multiplied = cost, 1, *key.multiplied
	return res, err
}

// quantileFromRows plans before it builds its query, because a bare quantile takes its
// column from the planned footers.
func (r reader) quantileFromRows(ctx context.Context, descs []segment.Descriptor, req Request) (Result, error) {
	m := &binQuantiles{binKey: newBinKey(req), length: req.Measure.Length, quantile: req.Measure.Quantile, values: map[string]map[int64][]float64{}}
	o := ordered(req, m.binKey, m)
	p, q, err := r.planRows(ctx, descs, req, o)
	if err != nil {
		return Result{Cost: p.Cost}, err
	}
	if m.field, err = scanQuantileField(p, descs, req.Measure.Field); err != nil {
		return Result{Cost: p.Cost}, err
	}
	q.Columns = append(q.Columns, m.field)
	read, err := r.fold(ctx, p, q, o)
	if err == nil {
		err = r.uncarried(descs, m.field, "for a scanned quantile")
	}
	res := m.result()
	res.Cost, res.Coverage, res.Multiplied = addCost(p.Cost, read), 1, *m.multiplied
	return res, err
}

// eachFromRows reads req.Measures in one scan, each over the rows its own predicates match.
func (r reader) eachFromRows(ctx context.Context, req Request) (Result, error) {
	descs, err := r.segments(ctx, req)
	if err != nil {
		return Result{}, err
	}
	key := newBinKey(req)
	each := filteredMeasures{each: make([]whereMeasure, len(req.Measures)), counted: map[string][]bool{}}
	var columns []string
	for i, f := range req.Measures {
		k := key
		if f.GroupBy != "" {
			k.column = f.GroupBy
			columns = append(columns, f.GroupBy)
		}
		m := &each.each[i]
		m.where, m.measure = parsedOnce(req.Measures[:i], each.each[:i], f.Where), distinct(rowMeasure(k, f.Measure), f.DistinctBy)
		columns = append(append(columns, f.Field), scan.Fields(m.where)...)
		if f.DistinctBy != "" {
			columns = append(columns, f.DistinctBy)
		}
	}
	cost, err := r.scanRows(ctx, descs, req, ordered(req, key, each), columns...)
	for _, f := range req.Measures {
		if err == nil && f.Field != "" {
			err = r.uncarried(descs, f.Field, "for "+f.Kind.label())
		}
	}
	res := each.result()
	res.Cost, res.Coverage, res.Multiplied = cost, 1, *key.multiplied
	return res, err
}

// parsedOnce is where parsed, or the parse of an earlier measure's equal predicates, so
// filteredMeasures matches a row against them once.
func parsedOnce(earlier []FilteredMeasure, parsed []whereMeasure, where []selector.Predicate) []selector.ParsedPredicate {
	for i, e := range earlier {
		if len(where) > 0 && reflect.DeepEqual(e.Where, where) {
			return parsed[i].where
		}
	}
	return selector.ParsePredicates(where)
}

// rowMeasure folds m's rows by key; a quantile names its field.
func rowMeasure(key binKey, m Measure) measure {
	switch m.Kind {
	case KindDistinct:
		return binDistinct{binKey: key, field: m.Field, sketches: map[string]map[int64]*sketch.HLL{}}
	case KindQuantile:
		return &binQuantiles{binKey: key, field: m.Field, length: m.Length, quantile: m.Quantile, values: map[string]map[int64][]float64{}}
	case KindFirst, KindLast, KindMin, KindMax, KindMaxRun, KindChanges, KindSpan:
		return &binSequences{binKey: key, kind: m.Kind, field: m.Field, length: m.Length, samples: map[string]map[int64][]sample{}}
	}
	return binTotals{binKey: key, field: m.Field, length: m.Length, rate: m.Kind == KindRate, all: Values{}}
}

// distinctRows passes its measure the first row read with each value of column, and every
// row without one, so a value a producer repeats on several lines counts once. The scan
// yields rows in a fixed order, so which row is first is the same on every read.
type distinctRows struct {
	measure
	column string
	seen   map[string]struct{}
}

// distinct is m counting each value of column once, or m when column is empty.
func distinct(m measure, column string) measure {
	if column == "" {
		return m
	}
	return &distinctRows{measure: m, column: column, seen: map[string]struct{}{}}
}

func (d *distinctRows) observe(r scan.Row) error {
	v, ok := r.Text(d.column)
	if ok && v != "" {
		if _, dup := d.seen[v]; dup {
			return nil
		}
		if int64(len(d.seen)) >= maxCollectedValues {
			return query.NewAPIError(query.ErrCodeScanTooLarge,
				fmt.Sprintf("aggregate: distinct_by %q over more than %d values does not fit in memory", d.column, maxCollectedValues)).
				WithHint("narrow the time window or the selector")
		}
		d.seen[strings.Clone(v)] = struct{}{}
	}
	return d.measure.observe(r)
}

// filteredMeasures folds each row into every measure whose own predicates it matches, but
// for the measures its segment's footer counted.
type filteredMeasures struct {
	each []whereMeasure
	// counted is each scanned segment's measures its footer counted.
	counted map[string][]bool
}

type whereMeasure struct {
	where []selector.ParsedPredicate
	measure
}

func (ms filteredMeasures) observe(row scan.Row) error {
	var seen matchedOnce
	var counted []bool
	if len(ms.counted) > 0 {
		counted = ms.counted[row.Segment.ID]
	}
	for i, m := range ms.each {
		if counted != nil && counted[i] || !seen.matches(row, m.where) {
			continue
		}
		if err := m.observe(row); err != nil {
			return err
		}
	}
	return nil
}

// matchedOnce is a row's matches of the first few predicates its measures share a parse of,
// so a row is matched once for each rather than once a measure; past them it is matched each time.
type matchedOnce struct {
	where [4]*selector.ParsedPredicate
	ok    [4]bool
	held  int
}

func (s *matchedOnce) matches(row scan.Row, where []selector.ParsedPredicate) bool {
	if len(where) == 0 {
		return true
	}
	for i := range s.held {
		if s.where[i] == &where[0] {
			return s.ok[i]
		}
	}
	ok := row.Matches(where)
	if s.held < len(s.where) {
		s.where[s.held], s.ok[s.held] = &where[0], ok
		s.held++
	}
	return ok
}

// countFooters counts each measure off each planned segment's footer where it answers it,
// leaving to scan the segments it answers only some measures of.
func (ms filteredMeasures) countFooters(p plan.Plan, req Request, where []selector.ParsedPredicate) []plan.WorkItem {
	counted := map[string]bool{}
	for d, footer := range p.Footers() {
		counted[d.ID] = ms.countFooter(d, footer, req, where)
	}
	return slices.DeleteFunc(p.Items, func(wi plan.WorkItem) bool { return counted[wi.Segment.ID] })
}

func (ms filteredMeasures) countFooter(d segment.Descriptor, footer segment.Footer, req Request, where []selector.ParsedPredicate) bool {
	counted := make([]bool, len(ms.each))
	for i, m := range ms.each {
		t, ok := m.measure.(binTotals)
		counted[i] = excludes(footer, m.where) || ok && t.field == "" && t.matched == nil && t.countFooter(d, footer, req, slices.Concat(where, m.where))
	}
	if !slices.Contains(counted, false) {
		return true
	}
	if slices.Contains(counted, true) {
		ms.counted[d.ID] = counted
	}
	return false
}

// excludes reports whether footer, which views every service its rows name, has no view of
// a service where names, so a measure over where reads none of its rows.
func excludes(footer segment.Footer, where []selector.ParsedPredicate) bool {
	return slices.ContainsFunc(where, func(p selector.ParsedPredicate) bool {
		return p.Field == segment.ColService && p.Op == selector.OpEq && footer.ForService(p.Value).Rows == 0 && len(footer.Services()) > 0
	})
}

func (ms filteredMeasures) result() Result {
	res := Result{Measures: make([]Result, len(ms.each))}
	for i, m := range ms.each {
		res.Measures[i] = m.result()
		res.Approx = res.Approx || res.Measures[i].Approx
	}
	return res
}

// scanQuantileField picks the value column a scanned quantile reads: an explicit field
// as-is, and a bare quantile the sole sketch column of the first planned footer
// carrying one.
func scanQuantileField(p plan.Plan, descs []segment.Descriptor, field string) (string, error) {
	if field != "" || len(p.Items) == 0 {
		return field, nil
	}
	for _, footer := range p.Footers() {
		if column, ok, err := soleDistribution(footer); ok || err != nil {
			return column, err
		}
	}
	return "", query.NewAPIError(query.ErrCodeUnknownField,
		"aggregate: no segment in the window carries a quantile sketch to resolve the scan column").
		WithSuggestedFields(columnsAcross(descs))
}

// measure folds the rows a scan matches into per-(key, bin) values, which result reduces
// once the scan is done.
type measure interface {
	observe(scan.Row) error
	result() Result
}

// scanRows folds the rows of descs into m by (key, bin), reading columns beside ts and
// the group_by column.
func (r reader) scanRows(ctx context.Context, descs []segment.Descriptor, req Request, m measure, columns ...string) (query.Cost, error) {
	p, q, err := r.planRows(ctx, descs, req, m, columns...)
	if err != nil {
		return query.Cost{}, err
	}
	cost, err := r.fold(ctx, p, q, m)
	return addCost(p.Cost, cost), err
}

// fold passes every row of p that q matches to m, stopping at the first error.
func (r reader) fold(ctx context.Context, p plan.Plan, q scan.Query, m measure) (query.Cost, error) {
	rows := r.scanner.Scan(ctx, p, q)
	for rows.Next() {
		if err := m.observe(rows.Row()); err != nil {
			return rows.Cost(), err
		}
	}
	if o, ok := m.(*ordinal); ok && rows.Err() == nil {
		return rows.Cost(), o.replay()
	}
	return rows.Cost(), rows.Err()
}

// planRows refuses past the bound before anything is read, counts what m can off the
// descriptors, and plans the rest with the query that reads its rows.
func (r reader) planRows(ctx context.Context, descs []segment.Descriptor, req Request, m measure,
	columns ...string) (plan.Plan, scan.Query, error) {
	if err := req.Bound.Check(descs, "aggregate: "+req.Measure.Kind.label()); err != nil {
		return plan.Plan{}, scan.Query{}, err
	}
	column := groupByColumn(req.GroupBy)
	q := scan.Query{
		Window:  req.Window,
		Where:   selector.ParsePredicates(req.Selector.Where),
		Columns: append([]string{segment.ColTimestamp, column}, columns...),
	}
	if req.OrdinalOf != "" {
		q.Columns = append(append(q.Columns, req.OrdinalOf), scan.Fields(selector.ParsePredicates(req.OrdinalWhere))...)
	}
	// Only a total binned by time can count a whole segment's rows off its descriptor, or
	// else off its footer's value counts.
	totals, counts := m.(binTotals)
	byFooter, footers := m.(footerCounter)
	if NeedsGroupScan(descs, req.GroupBy) {
		q.Require, counts = required(req, column), false
	}
	rest := make([]segment.Descriptor, 0, len(descs))
	for _, d := range descs {
		if counts && countable(d, req.Window, req.Bins, q.Where) {
			key, _ := plan.SegmentValue(d, req.GroupBy)
			if totals.countSegment(d, key, req.Bins.BinStart(d.TimeRange.Min)) {
				continue
			}
		}
		rest = append(rest, d)
	}
	p, err := r.planner.Plan(ctx, rest, selector.Resolved{Window: req.Window, Where: req.Selector.Where})
	if err == nil && !req.Window.Bounded() {
		err = unboundedGroups(p, column)
	}
	if err == nil && footers {
		p.Items = byFooter.countFooters(p, req, q.Where)
	}
	return p, q, err
}

// required is what a row group must carry to be scanned when the segments do not settle
// the group_by column: that column, unless a measure keys by its own and so counts every
// row.
func required(req Request, column string) []string {
	if slices.ContainsFunc(req.Measures, func(f FilteredMeasure) bool { return f.GroupBy != "" }) {
		return nil
	}
	return []string{column}
}

// footerCounter is a measure that can count some planned segments off their footers.
type footerCounter interface {
	// countFooters counts each segment its footer answers, returning the work items left
	// to scan.
	countFooters(p plan.Plan, req Request, where []selector.ParsedPredicate) []plan.WorkItem
}

// unboundedGroups refuses grouping by an id a planned segment holds too many values of
// to count, since without a time range there is no bound on how many groups it makes.
func unboundedGroups(p plan.Plan, column string) error {
	for _, footer := range p.Footers() {
		if _, counted := footer.ValueCounts(column); footer.HasFilter(column) && !counted {
			return query.NewAPIError(query.ErrCodeUnboundedScan,
				fmt.Sprintf("aggregate: grouping by high-cardinality %q requires a bounded time range", column)).
				WithHint("set selector.time.from and selector.time.to")
		}
	}
	return nil
}

// countFooters counts each planned segment its footer's value counts answer, returning
// the work items left to scan.
func (m binTotals) countFooters(p plan.Plan, req Request, where []selector.ParsedPredicate) []plan.WorkItem {
	if m.field != "" || m.matched != nil {
		return p.Items
	}
	counted := map[string]bool{}
	for d, footer := range p.Footers() {
		counted[d.ID] = m.countFooter(d, footer, req, where)
	}
	return slices.DeleteFunc(p.Items, func(wi plan.WorkItem) bool { return counted[wi.Segment.ID] })
}

// countFooter adds d's rows by value from its footer, keying a row without the column
// "" as a scan does, or reports false when the footer cannot answer.
func (m binTotals) countFooter(d segment.Descriptor, footer segment.Footer, req Request, where []selector.ParsedPredicate) bool {
	counts, rows, ok := footerCounts(d, footer, m.column, req, where)
	if !ok {
		return false
	}
	bin := req.Bins.BinStart(d.TimeRange.Min)
	for v, n := range counts {
		addTo(m.all, v, bin, float64(n))
		rows -= int(n)
	}
	if rows > 0 {
		addTo(m.all, "", bin, float64(rows))
	}
	return true
}

// footerCounts is d's rows per value of column, read from the view of the service where
// names, and how many rows that view holds; false unless d is countable by the rest of
// where and the footer counts the column.
func footerCounts(d segment.Descriptor, footer segment.Footer, column string, req Request,
	where []selector.ParsedPredicate) (map[string]uint64, int, bool) {
	rest := where
	if i := slices.IndexFunc(where, func(p selector.ParsedPredicate) bool {
		return p.Field == segment.ColService && p.Op == selector.OpEq
	}); i >= 0 {
		footer, rest = footer.ForService(where[i].Value), slices.Delete(slices.Clone(where), i, i+1)
	}
	if !countable(d, req.Window, req.Bins, rest) {
		return nil, 0, false
	}
	counts, ok := footer.ValueCounts(column)
	return counts, footer.Rows, ok
}

// countable reports whether every row of d is inside w and one bin and passes where as
// d's descriptor settles it, so the descriptor's row count is the answer.
func countable(d segment.Descriptor, w selector.Window, bins Bins, where []selector.ParsedPredicate) bool {
	if !w.Contains(d.TimeRange.Min) || !w.Contains(d.TimeRange.Max) || bins.BinStart(d.TimeRange.Min) != bins.BinStart(d.TimeRange.Max) {
		return false
	}
	for _, p := range where {
		if v, ok := plan.SegmentValue(d, p.Field); !ok || !p.Matches(v, true) {
			return false
		}
	}
	return true
}

// Bound is what one row scan may read before it refuses with scan_too_large; zero on
// either limit is uncapped.
type Bound struct {
	Segments int
	Bytes    int64
}

// Exceeds reports whether reading descs is more than b allows by either limit.
func (b Bound) Exceeds(descs []segment.Descriptor) bool {
	return (b.Segments > 0 && len(descs) > b.Segments) || (b.Bytes > 0 && estimatedBytes(descs) > b.Bytes)
}

// Check refuses with scan_too_large when reading descs is more than b allows, naming the
// limit that tripped in a message that starts with what.
func (b Bound) Check(descs []segment.Descriptor, what string) error {
	tooLarge := func(format string, args ...any) error {
		return query.NewAPIError(query.ErrCodeScanTooLarge, fmt.Sprintf(format, args...)).
			WithHint("narrow the time window or the selector, or raise the scan bound for this call")
	}
	if b.Segments > 0 && len(descs) > b.Segments {
		return tooLarge("%s would scan more than %d segments (%d in scope)", what, b.Segments, len(descs))
	}
	if total := estimatedBytes(descs); b.Bytes > 0 && total > b.Bytes {
		return tooLarge("%s would scan about %d MB, over the %d MB bound (%d segments in scope)",
			what, total>>20, b.Bytes>>20, len(descs))
	}
	return nil
}

// uncarried refuses a measure over a field that is not derived and no segment of descs
// holds as a column or a stream label, since an empty series would read as a column that
// held nothing; the error names the columns that are there.
func (r reader) uncarried(descs []segment.Descriptor, field, purpose string) error {
	if len(descs) == 0 || r.planner.Derived().Has(field) || slices.ContainsFunc(descs, func(d segment.Descriptor) bool {
		_, column := d.ZoneMaps[field]
		_, label := d.Stream.Labels[field]
		return column || label
	}) {
		return nil
	}
	return query.NewAPIError(query.ErrCodeUnknownField,
		fmt.Sprintf("aggregate: no segment in the window carries a %q column %s", field, purpose)).
		WithSuggestedFields(columnsAcross(descs))
}

// columnsAcross lists, sorted, the columns the descriptors' zone maps name, so a
// suggestion costs no read.
func columnsAcross(descs []segment.Descriptor) []string {
	seen := map[string]struct{}{}
	for _, d := range descs {
		for col := range d.ZoneMaps {
			seen[col] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// estimatedBytes totals what reading descs would transfer, from the sizes the catalog
// already carries — so a bound can be checked before anything is opened.
func estimatedBytes(descs []segment.Descriptor) int64 {
	var total int64
	for _, d := range descs {
		total += d.EstimatedSize
	}
	return total
}

// maxCollectedValues caps the values an exact quantile holds to sort, near 256MB, and
// is a var only so a test can lower it.
var maxCollectedValues int64 = 32 << 20
