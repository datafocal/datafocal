package tools

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// SearchRequest is the wire request for the search_logs tool. Selector is the
// shared query Lens. Limit caps returned lines (default 100 when <= 0).
// Follow requests the live tail. Snapshot pins reproducibility. Around
// returns the selector's lines around a line instead of searching. Columns, when given,
// are the attributes each line carries; the fixed fields always come back.
type SearchRequest struct {
	Selector selector.Selector `json:"selector"`
	Limit    int               `json:"limit,omitempty"`
	Follow   bool              `json:"follow,omitempty"`
	Snapshot string            `json:"snapshot,omitempty"`
	Around   *LineRef          `json:"around,omitempty"`
	// MaxScanSegments is aggregate's bound of the same name.
	MaxScanSegments int      `json:"max_scan_segments,omitempty" jsonschema:"cap on segments one search may read; defaults to 2048, negative means uncapped"`
	Columns         []string `json:"columns,omitempty" jsonschema:"attributes each line carries; defaults to all"`
	// After is an earlier answer's Next, whose page this reads on from.
	After *LineCursor `json:"after,omitempty" jsonschema:"an earlier answer's next, to read the lines after its last"`
}

// LineCursor is a page's last line, where the next page of the same search begins: its
// time, segment and row, the order search returns lines in.
type LineCursor struct {
	Time    string `json:"time"`
	Segment string `json:"segment"`
	Row     int    `json:"row"`
}

// LineRef is a returned line's Segment and Row, with how many lines of the anchor's
// producer to read before and after it (default 20, max 200).
type LineRef struct {
	Segment string `json:"segment"`
	Row     int    `json:"row"`
	Before  int    `json:"before,omitempty"`
	After   int    `json:"after,omitempty"`
}

// LogLine is one decoded row, referenced by its Segment and Row, with every column
// beyond the fixed ones, derived ones included, in Attributes.
type LogLine struct {
	Timestamp  string            `json:"timestamp"`
	Level      string            `json:"level"`
	Message    string            `json:"message"`
	PatternID  string            `json:"pattern_id"`
	Service    string            `json:"service,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Segment    string            `json:"segment,omitempty"`
	Row        int               `json:"row"`
}

// SearchResponse is the wire response, wrapped in query.Envelope[SearchResponse].
// Mode is "needle", "scan", "around" or a follow's "tail". Lines are newest first, except
// around's and tail's, which are in time order.
type SearchResponse struct {
	Mode  string    `json:"mode"`
	Lines []LogLine `json:"lines,omitempty"`
	// Next is the last line of a full page, to pass as after for the next, nil when the
	// lines ran out first.
	Next *LineCursor `json:"next,omitempty"`
}

// SearchLogs reads the lines a selector matches: those of an id equality wherever they
// are ("needle"), any other selector's over a bounded window ("scan"), or the lines of
// a line's producer either side of it ("around").
func (t toolSet) SearchLogs(ctx context.Context, req SearchRequest) (query.Envelope[SearchResponse], error) {
	var zero query.Envelope[SearchResponse]

	sel, err := validateSelector(ctx, "search_logs", req.Selector)
	if err != nil {
		return zero, err
	}

	// Honor snapshot pin or take current snapshot
	snap, err := t.resolveSnapshot(ctx, nil, "search_logs", req.Snapshot)
	if err != nil {
		return zero, err
	}

	// Default limit to 100
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	bound := series.Bound{Segments: cmp.Or(req.MaxScanSegments, defaultMaxScanSegments), Bytes: defaultMaxScanBytes}

	var resp SearchResponse
	var envCost query.Cost
	// A search asking for some columns reads only those, rather than decoding every column
	// of every row it keeps and dropping the rest.
	t.columns = req.Columns
	if req.Around != nil && req.After != nil {
		return zero, query.Invalidf("search_logs: around reads one line's neighbours, not a page after another")
	}
	if req.Around != nil {
		resp, envCost, err = t.searchAround(ctx, snap, sel, bound, *req.Around)
	} else {
		resp, envCost, err = t.search(ctx, snap, sel, limit, bound, req.After)
	}
	if err != nil {
		return zero, err
	}
	if req.Columns != nil {
		for _, l := range resp.Lines {
			maps.DeleteFunc(l.Attributes, func(k, _ string) bool { return !slices.Contains(req.Columns, k) })
		}
	}

	return query.Envelope[SearchResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: 1.0,
			Cost:     envCost,
			Tokens:   searchTokens(resp),
		},
	}, nil
}

// search reads sel's newest lines: anywhere for an id equality, within a bounded window
// for any other selector.
func (t toolSet) search(ctx context.Context, snap snapshot, sel selector.Resolved, limit int, bound series.Bound,
	after *LineCursor) (SearchResponse, query.Cost, error) {
	mode := "scan"
	field, value, ok := classifyNeedle(sel)
	var idCost query.Cost
	if !ok && !sel.Window.Bounded() {
		needle, read, err := t.filteredNeedle(ctx, snap, sel)
		if err != nil {
			return SearchResponse{}, query.Cost{}, err
		}
		field, value, ok, idCost = needle.Field, needle.Value, needle.Field != "", read
	}
	if ok {
		if err := needleValueRequired(field, value); err != nil {
			return SearchResponse{}, query.Cost{}, err
		}
		mode, bound = "needle", series.Bound{}
	} else if !sel.Window.Bounded() {
		return SearchResponse{}, query.Cost{}, query.NewAPIError(query.ErrCodeUnboundedScan,
			"search_logs: this search reads every row; give it a time range or an id equality such as trace_id")
	}
	past, cursor, err := pastCursor(sel, after)
	if err != nil {
		return SearchResponse{}, query.Cost{}, err
	}
	kept, cost, err := t.searchLines(ctx, snap, past, limit, readOrder{newest: true, after: cursor}, bound)
	if err != nil {
		return SearchResponse{}, query.Cost{}, err
	}
	resp := SearchResponse{Mode: mode, Lines: linesOf(kept)}
	if len(kept) == limit {
		last := kept[len(kept)-1]
		resp.Next = &LineCursor{Time: last.at.UTC().Format(time.RFC3339Nano), Segment: last.Segment, Row: last.Row}
	}
	return resp, addCost(cost, idCost), nil
}

// filteredNeedle is sel's first equality on a column that is an id by its values, which reads
// its lines without a time range as an id named so does, zero if none, and what finding it cost.
func (t toolSet) filteredNeedle(ctx context.Context, snap snapshot, sel selector.Resolved) (selector.Predicate, query.Cost, error) {
	var cost query.Cost
	for _, pred := range sel.Where {
		if pred.Op != selector.OpEq || pred.Value == "" {
			continue
		}
		id, read, err := t.filteredID(ctx, snap, sel.Window, pred)
		cost = addCost(cost, read)
		if err != nil {
			return selector.Predicate{}, cost, err
		}
		if id {
			return pred, cost, nil
		}
	}
	return selector.Predicate{}, cost, nil
}

// filteredID reports whether pred's field is an id by its values to pred, an equality or, with
// no value, any lookup in window: some segment holds the field and each whose descriptor does
// not rule pred out has its membership filter, read in batches doubling from one so a field an
// early one does not filter costs little.
func (t toolSet) filteredID(ctx context.Context, snap snapshot, window selector.Window, pred selector.Predicate) (bool, query.Cost, error) {
	sel := selector.Resolved{Window: window}
	if pred.Value != "" {
		sel.Where = []selector.Predicate{pred}
	}
	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return false, query.Cost{}, err
	}
	field := pred.Field
	held := slices.DeleteFunc(slices.Clone(descs), func(d segment.Descriptor) bool {
		_, ok := d.ZoneMaps[field]
		return !ok
	})
	var cost query.Cost
	for lo, n := 0, 1; lo < len(held); lo, n = lo+n, 2*n {
		p, err := t.footers(ctx, held[lo:min(lo+n, len(held))])
		// The footers read, not the segments, which the lookup bills as it reads them.
		cost = addCost(cost, query.Cost{GETs: p.Cost.GETs, BytesRead: p.Cost.BytesRead, FootersDecoded: p.Cost.FootersDecoded})
		if err != nil {
			return false, cost, err
		}
		for _, footer := range p.Footers() {
			if !footer.HasFilter(field) {
				return false, cost, nil
			}
		}
	}
	return len(held) > 0, cost, nil
}

// classifyNeedle reports whether the selector carries an exact equality
// predicate on a correlation id, which reads its lines without a time range because an
// id's membership filter or zone map throws most row groups away first.
//
// An empty value is not a needle either — see needleValueRequired.
func classifyNeedle(sel selector.Resolved) (field, value string, ok bool) {
	for _, pred := range sel.Where {
		if pred.Op == selector.OpEq && segment.IsID(pred.Field) {
			return pred.Field, pred.Value, true
		}
	}
	return "", "", false
}

// needleValueRequired rejects an equality predicate on an id with no value.
//
// Predicate.Value is omitempty and nothing upstream requires it, so
// `{field:"trace_id", op:"eq"}` reaches here as a needle for the empty string, which
// a row group without the column reads for every row. get_trace already
// requires its value; this is the same guard on the same shape.
func needleValueRequired(field, value string) error {
	if value != "" {
		return nil
	}
	return query.Invalidf("search_logs: %s is required to have a value — an equality predicate with no value matches nothing", field)
}

// pastCursor is sel ending with the line after names, which a page of the newest lines
// reads up to, and that line to read past; nil after leaves sel as it is.
func pastCursor(sel selector.Resolved, after *LineCursor) (selector.Resolved, *keptLine, error) {
	if after == nil {
		return sel, nil, nil
	}
	at, err := time.Parse(time.RFC3339Nano, after.Time)
	if err != nil {
		return sel, nil, query.Invalidf("search_logs: after.time %q is not an RFC 3339 time: %v", after.Time, err)
	}
	if end := at.Add(time.Nanosecond); sel.Window.To.IsZero() || end.Before(sel.Window.To) {
		sel.Window.To = end
	}
	return sel, &keptLine{LogLine: LogLine{Segment: after.Segment, Row: after.Row}, at: at}, nil
}

// readOrder is the order a search reads lines in, newest or oldest first, and the line it
// reads past, nil for none.
type readOrder struct {
	newest bool
	after  *keptLine
}

// searchLines reads the first limit rows sel matches in the order by names, returning those
// read so far with the scan_too_large that refuses the segment past bound.
func (t toolSet) searchLines(ctx context.Context, snap snapshot, sel selector.Resolved, limit int, by readOrder,
	bound series.Bound) ([]keptLine, query.Cost, error) {
	newest, after := by.newest, by.after
	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return nil, query.Cost{}, fmt.Errorf("search: %w", err)
	}
	// Oldest first orders by a segment's first row, newest first by its last, descending.
	sign, edge := 1, func(d segment.Descriptor) time.Time { return d.TimeRange.Min }
	if newest {
		sign, edge = -1, func(d segment.Descriptor) time.Time { return d.TimeRange.Max }
	}
	slices.SortFunc(descs, func(a, b segment.Descriptor) int {
		return cmp.Or(sign*edge(a).Compare(edge(b)), cmp.Compare(a.ID, b.ID))
	})
	order := func(a, b keptLine) int {
		return sign * cmp.Or(a.at.Compare(b.at), cmp.Compare(a.Segment, b.Segment), cmp.Compare(a.Row, b.Row))
	}
	var (
		held []keptLine
		cost query.Cost
	)
	// An id's lines rarely fill the limit, so a needle reads ahead, billing what it read.
	ahead, merged := 0, 0
	if _, _, needle := classifyNeedle(sel); needle {
		ahead = segmentsAhead
	}
	reads := segmentReads{descs: descs, bound: bound, read: func(d segment.Descriptor) chan segmentRead {
		done := make(chan segmentRead, 1)
		go func() {
			rows, cost, err := t.segmentRows(ctx, d, sel)
			done <- segmentRead{rows, cost, err}
		}()
		return done
	}}
	for i, d := range descs {
		if len(held) == limit && sign*edge(d).Compare(held[limit-1].at) > 0 {
			break
		}
		if err := bound.Check(descs[:i+1], "search_logs: this search"); err != nil {
			return held, cost, err
		}
		end := i + 1
		if len(held) < limit {
			end += ahead
		}
		reads.to(end)
		read := <-reads.started[i]
		if read.err != nil {
			return nil, query.Cost{}, read.err
		}
		cost = addCost(cost, read.cost)
		rows := read.rows
		if after != nil { // a line at the cursor's time is past it only after it in order
			rows = slices.DeleteFunc(slices.Clone(rows), func(k keptLine) bool { return order(k, *after) <= 0 })
		}
		// A segment's rows are in service order, so each one read is read whole.
		held = append(held, rows...)
		slices.SortFunc(held, order)
		held, merged = held[:min(len(held), limit)], i+1
	}
	return held, addCost(cost, reads.unmerged(merged)), nil
}

// segmentsAhead is how many segments a needle reads ahead of the one it merges while it
// holds fewer than its limit, fixed so a call bills the same reads on every machine.
const segmentsAhead = 4

// segmentReads is a search's segment reads, started in order as it reaches or reads
// ahead to each segment the bound admits.
type segmentReads struct {
	descs   []segment.Descriptor
	bound   series.Bound
	read    func(d segment.Descriptor) chan segmentRead
	started []chan segmentRead
}

// segmentRead is one segment's matching rows and what reading them cost.
type segmentRead struct {
	rows []keptLine
	cost query.Cost
	err  error
}

// to starts the reads of the segments before end.
func (r *segmentReads) to(end int) {
	for n := len(r.started); n < min(end, len(r.descs)) && r.bound.Check(r.descs[:n+1], "") == nil; n++ {
		r.started = append(r.started, r.read(r.descs[n]))
	}
}

// unmerged is what the reads started past the first merged cost, which an early stop
// leaves made all the same.
func (r *segmentReads) unmerged(merged int) query.Cost {
	var cost query.Cost
	for _, done := range r.started[merged:] {
		cost = addCost(cost, (<-done).cost)
	}
	return cost
}

// segmentRows is every row of d that sel matches, as its line, built from the row group as
// the scan reads it rather than from a copy of the row kept until the search ends.
func (t toolSet) segmentRows(ctx context.Context, d segment.Descriptor, sel selector.Resolved) ([]keptLine, query.Cost, error) {
	p, err := t.Planner.Plan(ctx, []segment.Descriptor{d}, sel)
	if err != nil {
		return nil, query.Cost{}, err
	}
	var matched []keptLine
	rows := t.Scanner.Scan(ctx, p, scan.Query{Window: sel.Window, Where: selector.ParsePredicates(sel.Where), Columns: t.lineColumns(d)})
	for rows.Next() {
		matched = append(matched, keep(rows.Row()))
	}
	if err := rows.Err(); err != nil {
		return nil, query.Cost{}, fmt.Errorf("search: %w", err)
	}
	return matched, addCost(p.Cost, rows.Cost()), nil
}

// searchTokens is query.EstimateTokens of resp, its lines encoded one at a time: a session
// read whole is tens of MB, which one encoding buffers twice over as it grows.
func searchTokens(resp SearchResponse) int {
	size := query.EncodedSize(SearchResponse{Mode: resp.Mode, Next: resp.Next})
	if len(resp.Lines) > 0 { // {"mode":…,"lines":[line,…],"next":…}
		size += len(`,"lines":[]`) + len(resp.Lines) - 1
		for _, l := range resp.Lines {
			size += query.EncodedSize(l)
		}
	}
	return query.TokensOf(size)
}

// lineColumns is every column d holds and every derived column, sorted, or of them only a
// line's own fields and the columns asked for.
func (t toolSet) lineColumns(d segment.Descriptor) []string {
	columns := slices.Collect(maps.Keys(d.ZoneMaps))
	for _, f := range t.Planner.Derived().Fields() {
		columns = append(columns, f.Name)
	}
	if t.columns != nil {
		columns = slices.DeleteFunc(columns, func(c string) bool {
			return !slices.Contains(lineFields, c) && !slices.Contains(t.columns, c)
		})
	}
	slices.Sort(columns)
	return columns
}

// lineFields are the columns a LogLine holds outside its Attributes.
var lineFields = []string{segment.ColTimestamp, segment.ColLevel, segment.ColMessage, segment.ColPatternID, segment.ColService}

// keptLine is a matched row as the LogLine search returns, and the time it sorts by.
type keptLine struct {
	LogLine
	at time.Time
}

// keep is r as its line, with every other column it holds in Attributes, each value copied.
func keep(r scan.Row) keptLine {
	line := logLine(r)
	r.Each(func(col string, v []byte) {
		if slices.Contains(lineFields, col) {
			return
		}
		if line.Attributes == nil {
			line.Attributes = map[string]string{}
		}
		line.Attributes[col] = string(v)
	})
	return keptLine{LogLine: line, at: r.Time}
}

func linesOf(kept []keptLine) []LogLine {
	lines := make([]LogLine, len(kept))
	for i, k := range kept {
		lines[i] = k.LogLine
	}
	return lines
}

// logLine is a matched row as the LogLine search returns, without its Attributes.
func logLine(r scan.Row) LogLine {
	service := cell(r, segment.ColService)
	if service == "" {
		service = r.Segment.Stream.Service
	}
	return LogLine{
		Timestamp: r.Time.Format(time.RFC3339Nano),
		Level:     cell(r, segment.ColLevel),
		Message:   cell(r, segment.ColMessage),
		PatternID: cell(r, segment.ColPatternID),
		Service:   service,
		Segment:   r.Segment.ID,
		Row:       r.Index(),
	}
}

// producerFields name the producer of a line: Context keeps the lines that agree with
// the anchor's on each it has.
var producerFields = []string{segment.ColService, segment.ColNamespace, segment.ColPod, segment.ColContainer}

// searchAround returns, in time order, the referenced line's producer's lines either side
// of it, counting one at the same instant as after it.
func (t toolSet) searchAround(ctx context.Context, snap snapshot, sel selector.Resolved, bound series.Bound,
	ref LineRef) (SearchResponse, query.Cost, error) {
	anchor, cost, err := t.anchor(ctx, snap, ref)
	if err != nil {
		return SearchResponse{}, query.Cost{}, err
	}
	sel.Where = slices.Clone(sel.Where)
	for _, f := range producerFields {
		if v, _ := anchor.Value(f); v != nil {
			sel.Where = append(sel.Where, selector.Predicate{Field: f, Op: selector.OpEq, Value: string(v)})
		}
	}
	before, after := sel, sel
	before.Window.To, after.Window.From = anchor.Time, anchor.Time
	older, olderCost, err := t.searchLines(ctx, snap, before, cmp.Or(ref.Before, 20), readOrder{newest: true}, bound)
	if err = boundEndsWalk(err); err != nil {
		return SearchResponse{}, query.Cost{}, err
	}
	// One more, for the referenced line, which the after walk reads too.
	n := cmp.Or(ref.After, 20)
	newer, newerCost, err := t.searchLines(ctx, snap, after, n+1, readOrder{}, bound)
	if err = boundEndsWalk(err); err != nil {
		return SearchResponse{}, query.Cost{}, err
	}
	newer = slices.DeleteFunc(newer, func(k keptLine) bool {
		return k.Segment == anchor.Segment.ID && k.Row == anchor.Index()
	})
	slices.Reverse(older)
	lines := linesOf(slices.Concat(older, []keptLine{keep(anchor)}, newer[:min(len(newer), n)]))
	return SearchResponse{Mode: "around", Lines: lines}, addCost(cost, olderCost, newerCost), nil
}

// boundEndsWalk drops scan_too_large, since a Context walk that reaches the bound keeps
// the lines it has rather than refusing.
func boundEndsWalk(err error) error {
	var apiErr *query.APIError
	if errors.As(err, &apiErr) && apiErr.Code == query.ErrCodeScanTooLarge {
		return nil
	}
	return err
}

// anchor reads the row ref names, refusing a reference to no row or a before or after
// outside 0 to 200.
func (t toolSet) anchor(ctx context.Context, snap snapshot, ref LineRef) (scan.Row, query.Cost, error) {
	if ref.Before < 0 || ref.Before > 200 || ref.After < 0 || ref.After > 200 {
		return scan.Row{}, query.Cost{}, query.Invalidf("search_logs: around.before and around.after must be within 0 to 200")
	}
	d, err := t.anchorSegment(ctx, snap, ref)
	if err != nil {
		return scan.Row{}, query.Cost{}, err
	}
	p, err := t.Planner.Plan(ctx, []segment.Descriptor{d}, selector.Resolved{})
	if err != nil {
		return scan.Row{}, query.Cost{}, err
	}
	rows := t.Scanner.Scan(ctx, p, scan.Query{Columns: slices.Concat(t.lineColumns(d), producerFields)})
	for rows.Next() {
		if rows.Row().Index() == ref.Row {
			row, cost := rows.Row().Detach(), addCost(p.Cost, rows.Cost())
			rows.Close()
			return row, cost, nil
		}
	}
	if err := rows.Err(); err != nil {
		return scan.Row{}, query.Cost{}, fmt.Errorf("search: around: %w", err)
	}
	return scan.Row{}, query.Cost{}, query.Invalidf("search_logs: segment %q has no row %d", ref.Segment, ref.Row)
}

// anchorSegment is the segment ref names, refusing one that has no row ref.Row.
func (t toolSet) anchorSegment(ctx context.Context, snap snapshot, ref LineRef) (segment.Descriptor, error) {
	descs, err := t.Planner.Segments(ctx, snap.catalog, selector.Resolved{})
	if err != nil {
		return segment.Descriptor{}, fmt.Errorf("search: around: %w", err)
	}
	i := slices.IndexFunc(descs, func(d segment.Descriptor) bool { return d.ID == ref.Segment })
	if i < 0 || ref.Row < 0 || ref.Row >= descs[i].Rows {
		return segment.Descriptor{}, query.Invalidf("search_logs: segment %q has no row %d", ref.Segment, ref.Row)
	}
	return descs[i], nil
}

// FollowSearch streams the querier's live tail, an event with no lines once attached and then
// every log line as it lands in time order, until ctx is done; req is not read, since the tail
// is all logs.
func (t toolSet) FollowSearch(ctx context.Context, _ SearchRequest, emit func(query.Envelope[SearchResponse]) error) error {
	return t.tail.follow(ctx, func(lines []LogLine) error {
		resp := SearchResponse{Mode: "tail", Lines: lines}
		return emit(query.Envelope[SearchResponse]{Data: resp, Meta: query.Meta{Coverage: 1.0, Tokens: query.EstimateTokens(resp)}})
	})
}
