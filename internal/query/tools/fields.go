package tools

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// FieldsRequest is the wire request for rank_fields and describe_schema, whose time window
// defaults to open bounds so a caller can discover the schema without pinning a range.
type FieldsRequest struct {
	Selector selector.Selector `json:"selector"`
	Snapshot string            `json:"snapshot,omitempty"`
}

// FieldStat is the per-field metadata rank_fields returns. Type is one of
// str|num|enum|id. Cardinality is the HLL estimate merged across the pruned
// segments. Present is the fraction of segments the field appears in (0..1), Lines
// the fraction of matched rows holding a value (0..1).
// Signal is presenceCardinalityScore; HighSignal marks the fields worth surfacing
// for group-by and filter predicates.
type FieldStat struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Cardinality int64   `json:"cardinality"`
	Present     float64 `json:"present"`
	Lines       float64 `json:"lines"`
	Signal      float64 `json:"signal"`
	HighSignal  bool    `json:"high_signal"`
}

// FieldsResponse wraps the ranked fields, ids last and each part sorted by Signal descending.
type FieldsResponse struct {
	Fields []FieldStat `json:"fields"`
}

const (
	// enumMaxCardinality is the distinct-count ceiling below which a non-id,
	// non-numeric column is treated as an enum rather than free text.
	enumMaxCardinality = 50
	// signalNorm scales the raw present×log2(1+cardinality) product into [0,1];
	// log2 of a corpus with ~1M distinct values (~20 bits) maps to signal 1.0.
	signalNorm = 20.0
	// highSignalThreshold is the signal above which a field is high-signal. A
	// constant field (cardinality 1, always present) sits exactly at the floor
	// (log2(2)/20 = 0.05) and is deliberately excluded.
	highSignalThreshold = 0.05
)

// RankFields ranks the fields of the rows the selector matches by signal, merging the
// footers of the segments whose descriptors settle the predicates and scanning the rest's
// matched rows.
func (t toolSet) RankFields(ctx context.Context, req FieldsRequest) (query.Envelope[FieldsResponse], error) {
	var zero query.Envelope[FieldsResponse]

	sel, err := validateSelector(ctx, "rank_fields", req.Selector)
	if err != nil {
		return zero, err
	}

	snap, err := t.resolveSnapshot(ctx, nil, "rank_fields", req.Snapshot)
	if err != nil {
		return zero, err // resolveSnapshot classifies client vs infra errors
	}
	f, err := t.scopeFields(ctx, snap, sel)
	if err != nil {
		return zero, fmt.Errorf("rank_fields: %w", err)
	}
	resp := FieldsResponse{Fields: withDerived(f.stats(), f.accs, f.segments, f.rows, t.Planner.Derived())}

	return query.Envelope[FieldsResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: 1.0,  // every planned segment was merged or scanned
			Approx:   true, // cardinality comes from HLL sketches ⇒ bounded error
			Cost:     f.cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}, nil
}

// fields are the stats of the fields of a selector's rows, over the segments it planned.
type fields struct {
	plan           plan.Plan
	accs           map[string]*fieldAcc
	segments, rows int
	cost           query.Cost
}

func (f fields) stats() []FieldStat { return buildFieldStats(f.accs, f.segments, f.rows) }

// scopeFields gathers the fields of sel's rows alone: a segment's footer where sel settles
// every row of it, its service's view where sel names a service and settles the rest, and
// a scan of its matched rows otherwise, so a segment shared with other services adds none
// of theirs. It logs the step with what each way read.
func (t toolSet) scopeFields(ctx context.Context, snap snapshot, sel selector.Resolved) (fields, error) {
	start := time.Now()
	p, err := t.Planner.Prune(ctx, snap.catalog, sel)
	if err != nil {
		return fields{}, fmt.Errorf("prune: %w", err)
	}
	f := fields{plan: p, accs: map[string]*fieldAcc{}}
	where := selector.ParsePredicates(sel.Where)
	read := map[string]bool{} // the segments a footer or a view answered
	for d, footer := range p.Footers() {
		whole := settles(d, where)
		view, viewed := scopeView(d, footer, where)
		switch {
		case whole:
			err = mergeFooterStats(f.accs, footer)
		case viewed:
			err = mergeViewStats(f.accs, view)
			footer = view
		default:
			continue
		}
		if err != nil {
			return fields{}, fmt.Errorf("merge %s: %w", d.ID, err)
		}
		read[d.ID] = true
		f.segments, f.rows = f.segments+1, f.rows+footer.Rows
	}
	rest := p
	rest.Items = slices.DeleteFunc(slices.Clone(p.Items), func(wi plan.WorkItem) bool { return read[wi.Segment.ID] })
	scanned, err := t.scanFieldStats(ctx, rest, sel, f.accs)
	if err != nil {
		return fields{}, err
	}
	f.segments, f.rows, f.cost = f.segments+scanned.segments, f.rows+scanned.rows, addCost(p.Cost, scanned.cost)
	slog.InfoContext(ctx, "scope fields", "duration_ms", float64(time.Since(start).Microseconds())/1000,
		"from_footers", len(read), "scanned", scanned.scanned, "cached", scanned.cached,
		"gets", f.cost.GETs, "bytes_read", f.cost.BytesRead)
	return f, nil
}

// scopeView is the view of the service where names in footer, when where names one and
// d's descriptor settles the rest of it, and the view keeps field stats.
func scopeView(d segment.Descriptor, footer segment.Footer, where []selector.ParsedPredicate) (segment.Footer, bool) {
	i := slices.IndexFunc(where, func(p selector.ParsedPredicate) bool {
		return p.Field == segment.ColService && p.Op == selector.OpEq && len(p.Any) == 0
	})
	if i < 0 || !settles(d, slices.Delete(slices.Clone(where), i, i+1)) {
		return segment.Footer{}, false
	}
	view := footer.ForService(where[i].Value)
	_, ok := view.Fields()
	return view, ok
}

// mergeViewStats folds a service view's field stats into the per-column accumulators, as
// mergeFooterStats does a whole footer's.
func mergeViewStats(accs map[string]*fieldAcc, view segment.Footer) error {
	stats, _ := view.Fields()
	for name, s := range stats {
		a := accFor(accs, name)
		a.present++
		a.lines += min(s.Values, view.Rows) // a list column's count is of elements
		if a.hll == nil {
			a.hll = sketch.NewHLL()
		}
		if err := a.hll.Merge(s.Distinct); err != nil {
			return fmt.Errorf("hll %q: %w", name, err)
		}
		if a.valueType == 0 {
			a.valueType = s.Type.Scalar()
		}
		a.noteFilter(s.ID())
	}
	for name := range view.Distributions {
		accFor(accs, name).hasDDSketch = true
	}
	return nil
}

// DescribeSchemaResponse is every column the pruned segments hold, sorted by name, and the
// pattern profiles that wrote them, sorted by profile, then version.
type DescribeSchemaResponse struct {
	Columns   []SchemaColumn `json:"columns"`
	Producers []Producer     `json:"producers"`
}

// SchemaColumn is one column: its stored type, its kind as rank_fields types it (str, num,
// enum or id) and its distinct count merged across the segments.
type SchemaColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Kind        string `json:"kind"`
	Cardinality uint64 `json:"cardinality"`
}

// Producer is one version of a pattern profile, the highest degradation (none,
// column_names or catch_all) any of its segments reached, and how many segments it wrote.
type Producer struct {
	Profile     string `json:"profile"`
	Version     int    `json:"version"`
	Degradation string `json:"degradation"`
	Segments    int    `json:"segments"`
}

// degradations names the pattern package's degradation levels.
var degradations = [...]string{pattern.LevelProfile: "none", pattern.LevelColumnNames: "column_names", pattern.LevelCatchAll: "catch_all"}

// typeNames names the stored scalar types.
var typeNames = map[value.Type]string{
	value.TypeString: "string", value.TypeInt: "int", value.TypeFloat: "float",
	value.TypeBool: "bool", value.TypeTimestamp: "timestamp", value.TypeJSON: "json",
}

// DescribeSchema describes the columns of the segments the selector prunes to and the
// profiles that produced them, from their footers alone.
func (t toolSet) DescribeSchema(ctx context.Context, req FieldsRequest) (query.Envelope[DescribeSchemaResponse], error) {
	var zero query.Envelope[DescribeSchemaResponse]

	sel, err := validateSelector(ctx, "describe_schema", req.Selector)
	if err != nil {
		return zero, err
	}
	snap, err := t.resolveSnapshot(ctx, nil, "describe_schema", req.Snapshot)
	if err != nil {
		return zero, err
	}
	p, err := t.Planner.Prune(ctx, snap.catalog, sel)
	if err != nil {
		return zero, fmt.Errorf("describe_schema: prune: %w", err)
	}

	type version struct {
		profile string
		version int
	}
	accs, segments, degradation := map[string]*fieldAcc{}, map[version]int{}, map[version]int{}
	for d, footer := range p.Footers() {
		if err := mergeFooterStats(accs, footer); err != nil {
			return zero, fmt.Errorf("describe_schema: merge %s: %w", d.ID, err)
		}
		k := version{footer.PatternProfile.Name, footer.PatternProfile.Version}
		segments[k]++
		degradation[k] = max(degradation[k], footer.PatternProfile.Degradation)
	}

	resp := DescribeSchemaResponse{Columns: []SchemaColumn{}, Producers: []Producer{}}
	for _, name := range slices.Sorted(maps.Keys(accs)) {
		if internalColumn(name) {
			continue
		}
		a := accs[name]
		var card int64
		if a.hll != nil {
			card = int64(math.Round(a.hll.Estimate()))
		}
		resp.Columns = append(resp.Columns, SchemaColumn{
			Name:        name,
			Type:        typeNames[a.valueType],
			Kind:        inferFieldType(name, idByFilters(a, card), card, a.valueType, a.hasDDSketch, card <= int64(a.lines/2)),
			Cardinality: uint64(card),
		})
	}
	for _, k := range slices.SortedFunc(maps.Keys(segments), func(a, b version) int {
		return cmp.Or(strings.Compare(a.profile, b.profile), cmp.Compare(a.version, b.version))
	}) {
		resp.Producers = append(resp.Producers, Producer{k.profile, k.version, degradations[degradation[k]], segments[k]})
	}

	return query.Envelope[DescribeSchemaResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: 1.0,
			Approx:   true, // cardinality comes from HLL sketches
			Cost:     p.Cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}, nil
}

// settles reports whether d's descriptor gives every predicate's field, each alternative's
// for a group, so that planning d already decided every row of it matches.
func settles(d segment.Descriptor, where []selector.ParsedPredicate) bool {
	return !slices.ContainsFunc(where, func(p selector.ParsedPredicate) bool {
		if len(p.Any) > 0 {
			return !settles(d, p.Any)
		}
		_, ok := plan.SegmentValue(d, p.Field)
		return !ok
	})
}

// scanned is what scanFieldStats read: the segments holding a matched row and the rows
// matched, of the segments it scanned and the ones its cache answered.
type scanned struct {
	segments, rows, scanned, cached int
	cost                            query.Cost
}

// scanFieldStats folds the non-empty values of the rows sel matches in p into accs,
// refusing past the scan bound before any row is read. A segment wholly inside sel's window
// is read once a selector: its stats are kept, as a segment never changes.
func (t toolSet) scanFieldStats(ctx context.Context, p plan.Plan, sel selector.Resolved, accs map[string]*fieldAcc) (scanned, error) {
	key := segmentFieldsKey(sel, t.Planner.Derived().Version())
	var out scanned
	hits := t.cachedFields(p, sel, key, accs, &out)
	rest := p
	rest.Items = slices.DeleteFunc(slices.Clone(p.Items), func(wi plan.WorkItem) bool { return hits[wi.Segment.ID] })
	descs, columns := scanColumns(rest)
	if err := (series.Bound{Segments: defaultMaxScanSegments, Bytes: defaultMaxScanBytes}).Check(descs, "rank_fields: this ranking"); err != nil {
		return scanned{}, err
	}
	bySegment, cost, err := t.scanBySegment(ctx, rest, sel, columns)
	if err != nil {
		return scanned{}, err
	}
	for d, footer := range rest.Footers() {
		sf := bySegment[d.ID]
		if sf.accs == nil {
			sf.accs = map[string]*fieldAcc{}
		}
		for name := range sf.held {
			sf.accs[name].present++
		}
		footerTypes(sf.accs, footer)
		if wholly(sel.Window, d) {
			t.fieldsCache.Add(fieldsCacheKey{d.ID, key}, sf)
		}
		out.scanned++
		out.add(sf, accs)
	}
	out.cost = cost
	return out, nil
}

// cachedFields folds into accs and out the stats the cache keeps of p's segments wholly
// inside sel's window, and returns the segments it answered.
func (t toolSet) cachedFields(p plan.Plan, sel selector.Resolved, key string, accs map[string]*fieldAcc, out *scanned) map[string]bool {
	hits := map[string]bool{}
	for d := range p.Footers() {
		if sf, ok := t.fieldsCache.Get(fieldsCacheKey{d.ID, key}); ok && wholly(sel.Window, d) {
			hits[d.ID] = true
			out.cached++
			out.add(sf, accs)
		}
	}
	return hits
}

// scanBySegment is each segment's stats of the fields of the rows sel matches in p.
func (t toolSet) scanBySegment(ctx context.Context, p plan.Plan, sel selector.Resolved, columns []string) (map[string]segmentFields, query.Cost, error) {
	bySegment := map[string]segmentFields{}
	rows := t.Scanner.Scan(ctx, p, scan.Query{Window: sel.Window, Where: selector.ParsePredicates(sel.Where), Columns: columns})
	for rows.Next() {
		row := rows.Row()
		sf, ok := bySegment[row.Segment.ID]
		if !ok {
			sf = segmentFields{accs: map[string]*fieldAcc{}, held: map[string]bool{}}
		}
		sf.rows++
		observeFields(row, columns, sf.held, sf.accs)
		bySegment[row.Segment.ID] = sf
	}
	if err := rows.Err(); err != nil {
		return nil, rows.Cost(), fmt.Errorf("rank_fields: %w", err)
	}
	return bySegment, rows.Cost(), nil
}

// add folds one segment's stats into accs and what was read.
func (s *scanned) add(sf segmentFields, accs map[string]*fieldAcc) {
	if sf.rows > 0 {
		s.segments++
	}
	s.rows += sf.rows
	mergeAccs(accs, sf.accs)
}

// scanColumns is p's segments and every column their footers name but the left-out ones, sorted.
func scanColumns(p plan.Plan) ([]segment.Descriptor, []string) {
	var descs []segment.Descriptor
	columns := map[string]struct{}{}
	for d, footer := range p.Footers() {
		descs = append(descs, d)
		for name := range footer.ZoneMaps {
			if !internalColumn(name) {
				columns[name] = struct{}{}
			}
		}
	}
	return descs, slices.Sorted(maps.Keys(columns))
}

// observeFields adds each of row's non-empty values among columns to its column's
// distinct-count sketch and lines, and marks the column held.
func observeFields(row scan.Row, columns []string, held map[string]bool, accs map[string]*fieldAcc) {
	for _, name := range columns {
		v, ok := row.Value(name)
		if !ok || len(v) == 0 {
			continue
		}
		held[name] = true
		a := accFor(accs, name)
		a.lines++
		if a.hll == nil {
			a.hll = sketch.NewHLL()
		}
		a.hll.Add(v)
	}
}

// footerTypes records the types footer gives the columns accs already holds, and their filters.
func footerTypes(accs map[string]*fieldAcc, footer segment.Footer) {
	for name := range footer.Distributions {
		if a := accs[name]; a != nil {
			a.hasDDSketch = true
		}
	}
	for name, zm := range footer.ZoneMaps {
		a := accs[name]
		if a == nil {
			continue
		}
		if a.valueType == 0 && zm.Type != 0 {
			a.valueType = zm.Type.Scalar()
		}
		a.noteFilter(footer.HasFilter(name))
	}
}

// internalColumn reports whether a column is one the system writes for its own reads:
// ts, pattern_id, a masked token's param_<n> or a string column's masked copy.
func internalColumn(name string) bool {
	n, param := strings.CutPrefix(name, segment.ParamPrefix)
	_, err := strconv.Atoi(n)
	return name == segment.ColTimestamp || name == segment.ColPatternID ||
		strings.HasSuffix(name, segment.PatternSuffix) || (param && err == nil)
}

// fieldAcc accumulates one column's cross-segment state: the merged distinct-count
// sketch, the count of segments the column was present in and of rows holding it, and
// whether any segment carried a numeric (DDSketch) index for it, and whether each holding it
// carried a membership filter for it.
type fieldAcc struct {
	hll         *sketch.HLL
	present     int
	lines       int
	hasDDSketch bool
	filtered    bool
	unfiltered  bool
	valueType   value.Type
}

// noteFilter records whether a segment holding the column carried its membership filter.
func (a *fieldAcc) noteFilter(has bool) {
	a.filtered, a.unfiltered = a.filtered || has, a.unfiltered || !has
}

// hold counts footer's segment as holding the column a accumulates, and its rows the
// column's zone map counts, all of them for a column without one.
func (a *fieldAcc) hold(footer segment.Footer, name string) {
	a.present++
	zm, ok := footer.ZoneMaps[name]
	if !ok {
		a.lines += footer.Rows
		return
	}
	a.lines += min(zm.PresentRows, footer.Rows) // a list column's count is of elements
}

// mergeFooterStats folds one footer into the per-column accumulators: it merges
// each column's HLL, marks columns present, and records numeric columns.
func mergeFooterStats(accs map[string]*fieldAcc, footer segment.Footer) error {
	for name, hll := range footer.DistinctCounts {
		a := accFor(accs, name)
		a.hold(footer, name)
		if a.hll == nil {
			a.hll = sketch.NewHLL()
		}
		if err := a.hll.Merge(hll); err != nil {
			return fmt.Errorf("hll %q: %w", name, err)
		}
	}
	for name := range footer.Distributions {
		accFor(accs, name).hasDDSketch = true
	}
	for name, zm := range footer.ZoneMaps {
		if zm.Type == 0 {
			continue
		}
		a := accFor(accs, name)
		// A typed zone map marks a column present unless its HLL already did.
		if _, counted := footer.DistinctCounts[name]; !counted {
			a.hold(footer, name)
		}
		if a.valueType == 0 {
			a.valueType = zm.Type.Scalar()
		}
		a.noteFilter(footer.HasFilter(name))
	}
	return nil
}

// accFor returns the accumulator for a column, creating it on first sight.
func accFor(accs map[string]*fieldAcc, name string) *fieldAcc {
	a := accs[name]
	if a == nil {
		a = &fieldAcc{}
		accs[name] = a
	}
	return a
}

// buildFieldStats materializes the sorted FieldStat slice from the accumulators.
// total is the number of pruned segments (the presence denominator), rows the number
// of matched rows (the lines one).
func buildFieldStats(accs map[string]*fieldAcc, total, rows int) []FieldStat {
	fields := make([]FieldStat, 0, len(accs))
	for name, a := range accs {
		if internalColumn(name) {
			continue
		}
		var card int64
		if a.hll != nil {
			card = int64(math.Round(a.hll.Estimate()))
		}
		present := ratio(a.present, total)
		signal := presenceCardinalityScore(present, card)
		fields = append(fields, FieldStat{
			Name:        name,
			Type:        inferFieldType(name, idByFilters(a, card), card, a.valueType, a.hasDDSketch, card <= int64(a.lines/2)),
			Cardinality: card,
			Present:     present,
			Lines:       ratio(a.lines, rows),
			Signal:      signal,
			HighSignal:  signal > highSignalThreshold,
		})
	}
	sortFieldStats(fields)
	return fields
}

// withDerived adds to fields each derived column reading a column the window holds, in place
// of any stored column it shadows. Its presence and lines are those of the most present
// column it reads, and no sketch describes its values, so its cardinality and signal are zero.
func withDerived(fields []FieldStat, accs map[string]*fieldAcc, total, rows int, derived plan.Derived) []FieldStat {
	if total == 0 || len(derived.Fields()) == 0 {
		return fields
	}
	fields = slices.DeleteFunc(fields, func(f FieldStat) bool { return derived.Has(f.Name) })
	for _, d := range derived.Fields() {
		present, lines := 0, 0
		for _, col := range d.Reads {
			if a := accs[col]; a != nil {
				present, lines = max(present, a.present), max(lines, a.lines)
			}
		}
		if present == 0 {
			continue
		}
		typ := "str"
		if d.Numeric {
			typ = "num"
		}
		fields = append(fields, FieldStat{Name: d.Name, Type: typ, Present: float64(present) / float64(total), Lines: ratio(lines, rows)})
	}
	sortFieldStats(fields)
	return fields
}

// sortFieldStats sorts ids last, since their high cardinality marks no use for group-by or
// filtering, then by signal descending, name ascending as a deterministic tiebreak so a
// repeated call against the same snapshot returns byte-identical Data.
func sortFieldStats(fields []FieldStat) {
	sort.SliceStable(fields, func(i, j int) bool {
		if id := fields[i].Type == "id"; id != (fields[j].Type == "id") {
			return !id
		}
		if fields[i].Signal != fields[j].Signal {
			return fields[i].Signal > fields[j].Signal
		}
		return fields[i].Name < fields[j].Name
	})
}

// idByFilters reports whether every segment holding a's column filters it and, since a
// segment's filter judges its whole column, other services' rows too, the selected rows'
// own card values over a.lines are an id's as well.
func idByFilters(a *fieldAcc, card int64) bool {
	return a.filtered && !a.unfiltered && segment.IDByValues(int(card), a.lines)
}

// inferFieldType classifies a column: columns named as ids first — reported as "id"
// whatever their measured cardinality, since a segment holding one trace still
// describes a column that identifies traces — then numeric physical types or
// quantile sketches, then columns every segment holding them filters, ids by their values, then
// low-cardinality enums whose values repeat, at most half as many as the rows holding
// them, else free string.
func inferFieldType(name string, filtered bool, cardinality int64, valueType value.Type, hasDDSketch, repeats bool) string {
	switch {
	case segment.IsID(name):
		return "id"
	case hasDDSketch, valueType.Numeric(), valueType.Scalar() == value.TypeTimestamp:
		return "num"
	case filtered:
		return "id"
	case repeats && cardinality <= enumMaxCardinality:
		return "enum"
	default:
		return "str"
	}
}

// ratio is n of total, 0 when total is.
func ratio(n, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) / float64(total)
}

// presenceCardinalityScore is present × log2(1+cardinality), normalized to [0,1]: it
// rewards fields that are both present and high-cardinality.
func presenceCardinalityScore(present float64, cardinality int64) float64 {
	return min(present*math.Log2(1+float64(cardinality))/signalNorm, 1)
}
