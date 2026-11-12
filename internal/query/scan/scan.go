// Package scan reads the rows of the row groups a plan kept.
package scan

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"
	"unsafe"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// Scanner reads the rows of the row groups a plan kept.
type Scanner interface {
	// Scan returns a cursor over the rows of p that q matches, reading up to readAhead
	// row groups ahead of the one the caller is in.
	Scan(ctx context.Context, p plan.Plan, q Query) *Rows
}

// Query is what Scan reads from each row group: the predicate columns first, then the
// rest only for a row group with a matching row, and never a column the segment's
// descriptor settles to one value, except Require.
type Query struct {
	// Window filters the rows by their ts, which is read only for a window with a bound
	// or a query naming ts among Columns.
	Window  selector.Window
	Where   []selector.ParsedPredicate
	Columns []string
	// Require skips a row group not carrying all of these before anything is read.
	Require []string
}

// timed reports whether q reads each row's ts.
func (q Query) timed() bool {
	return !q.Window.From.IsZero() || !q.Window.To.IsZero() || slices.Contains(q.Columns, segment.ColTimestamp)
}

type scanner struct {
	rd      segment.Reader
	derived plan.Derived
}

// New returns a Scanner reading column chunks through rd and evaluating derived's columns
// on the rows they match.
func New(rd segment.Reader, derived plan.Derived) Scanner {
	return scanner{rd: rd, derived: derived}
}

func (s scanner) Scan(ctx context.Context, p plan.Plan, q Query) *Rows {
	return &Rows{ctx: ctx, rd: s.rd, derived: s.derived, q: q, items: p.Items}
}

// Rows is a cursor over the rows a scan matches, yielded in row-group order while the
// next row groups are read concurrently, and a row group's columns released once its
// rows are passed:
//
//	rows := s.Scan(ctx, p, q)
//	for rows.Next() {
//		use(rows.Row())
//	}
//	err := rows.Err()
type Rows struct {
	ctx     context.Context
	rd      segment.Reader
	derived plan.Derived
	q       Query
	items   []plan.WorkItem
	// ahead holds a result per row group already handed to a reader, in item order.
	ahead []chan groupRead
	batch []Row
	// held is the row group batch is from, released once batch is passed.
	held *rowGroup
	// working counts the cursor among those that hold memory they will give back, nil
	// while Next waits on a read.
	working   func()
	mu        sync.Mutex
	waitingOn chan groupRead
	row       Row
	cost      query.Cost
	err       error
	// done, when set, is told what the scan read once its last row is passed.
	done func(query.Cost, error)
}

// readAhead is how many row groups a scan reads ahead of the one it yields from, fixed
// rather than the core count so a call bills the same reads on every machine.
const readAhead = 16

// groupRead is one row group's matching rows and what reading them cost.
type groupRead struct {
	item *plan.WorkItem
	rows []Row
	cost segment.Cost
	err  error
	// working counts the cursor waiting on the read among those that hold memory they
	// will give back, from before the read's turn ends.
	working func()
}

// Next advances to the next matching row, reading the next row group that has one, and
// reports false once there is none or a read failed.
func (r *Rows) Next() bool {
	for len(r.batch) == 0 {
		r.release()
		if r.err != nil || (len(r.ahead) == 0 && len(r.items) == 0) {
			r.stopWorking()
			if r.done != nil {
				r.done(r.cost, r.err)
				r.done = nil
			}
			return false
		}
		if r.working == nil {
			r.working = query.Working(r.ctx)
		}
		r.readAheadGroups()
		read := r.receive(r.ahead[0])
		r.ahead = r.ahead[1:]
		r.bill(read.cost)
		r.batch = read.rows
		if read.err != nil {
			r.err = fmt.Errorf("scan %s row group %d: %w", read.item.Segment.ID, read.item.RowGroup, read.err)
			r.batch = nil
		}
		if len(r.batch) > 0 {
			r.held = r.batch[0].group
		}
	}
	r.row, r.batch = r.batch[0], r.batch[1:]
	return true
}

// readAheadGroups keeps readAhead row-group reads started, each sending to its own
// buffered channel so one the caller abandons exits rather than blocking.
func (r *Rows) readAheadGroups() {
	for len(r.ahead) < readAhead && len(r.items) > 0 {
		wi := &r.items[0]
		r.items = r.items[1:]
		done := make(chan groupRead, 1)
		r.ahead = append(r.ahead, done)
		ctx, end := query.Turn(r.ctx)
		go func() {
			read := groupRead{item: wi}
			read.rows, read.cost, read.err = r.readGroup(ctx, wi)
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.waitingOn == done {
				read.working = query.Working(r.ctx)
			}
			end()
			done <- read
		}()
	}
}

// receive is the read done sends, which a cursor waiting on counts in its stead.
func (r *Rows) receive(done chan groupRead) groupRead {
	r.mu.Lock()
	select {
	case read := <-done:
		r.mu.Unlock()
		return read
	default:
	}
	r.waitingOn = done
	r.stopWorking()
	r.mu.Unlock()
	read := <-done
	r.mu.Lock()
	r.waitingOn = nil
	r.mu.Unlock()
	r.working = read.working
	return read
}

func (r *Rows) stopWorking() {
	if r.working != nil {
		r.working()
		r.working = nil
	}
}

// release gives back the row group the caller has passed.
func (r *Rows) release() {
	if r.held != nil {
		r.held.release(r.ctx)
		r.held = nil
	}
}

// Close gives back what the scan holds, for a caller that stops before Next reports
// false and has called Cost.
func (r *Rows) Close() {
	for _, done := range r.ahead {
		if read := <-done; len(read.rows) > 0 {
			read.rows[0].group.release(r.ctx)
		}
	}
	r.ahead = nil
}

func (r *Rows) bill(cost segment.Cost) {
	r.cost.GETs += cost.GETs
	r.cost.BytesRead += cost.Bytes
}

// Row is the row Next advanced to.
func (r *Rows) Row() Row { return r.row }

// Err is the read failure that stopped Next, nil when the rows ran out.
func (r *Rows) Err() error { return r.err }

// Cost is what the row groups read so far cost, including those read ahead and not yet
// yielded, which it waits for: their GETs were made whether or not the caller reads on.
// The row the caller is on stays readable, but no longer counts against the budget, so
// a read ahead waiting for room can finish.
func (r *Rows) Cost() query.Cost {
	r.release()
	r.stopWorking()
	for i, done := range r.ahead {
		read := <-done
		r.bill(read.cost)
		read.cost = segment.Cost{}
		next := make(chan groupRead, 1)
		next <- read
		r.ahead[i] = next
	}
	return r.cost
}

// readGroup reads the columns deciding which of wi's rows match, then the rest only if
// one did; with nothing to filter on, everything is one read. It runs on its own
// goroutine, so it reads r and writes only what it returns.
func (r *Rows) readGroup(ctx context.Context, wi *plan.WorkItem) ([]Row, segment.Cost, error) {
	var cost segment.Cost
	read := func(g *rowGroup, rows []int, columns []string) error {
		c, err := g.read(ctx, r.rd, rows, columns)
		cost.GETs += c.GETs
		cost.Bytes += c.Bytes
		return err
	}
	if !carries(wi, r.q.Require, r.derived) {
		return nil, cost, nil
	}
	g := &rowGroup{item: wi, cells: map[string]segment.ColumnChunk{}, settled: settle(wi.Segment, r.q, r.derived), derived: r.derived}
	for _, zm := range wi.Footer.RowGroupZoneMaps[:min(wi.RowGroup, len(wi.Footer.RowGroupZoneMaps))] {
		g.first += zm[segment.ColTimestamp].PresentRows // every row has a ts
	}
	timed := r.q.timed()
	first := r.derived.Stored(Fields(r.q.Where))
	if timed {
		first = append(first, segment.ColTimestamp)
	}
	rest := r.derived.Stored(slices.Concat(r.q.Require, r.q.Columns))
	if len(first) == 0 {
		first = rest
	}
	if err := read(g, nil, first); err != nil {
		return nil, cost, err
	}
	// Every column read is settled or absent: ts is the one left to count the rows by.
	if len(g.cells) == 0 {
		if err := read(g, nil, []string{segment.ColTimestamp}); err != nil {
			return nil, cost, err
		}
	}
	g.derive(Fields(r.q.Where))
	rows, err := g.match(r.q, timed)
	if err != nil || len(rows) == 0 {
		g.release(ctx)
		return nil, cost, err
	}
	err = read(g, picks(rows, g.count()), rest)
	g.derive(r.q.Require, r.q.Columns)
	return rows, cost, err
}

// picks is the indexes of rows within their row group of n, nil when that is every row.
func picks(rows []Row, n int) []int {
	if len(rows) == n {
		return nil
	}
	at := make([]int, len(rows))
	for k, row := range rows {
		at[k] = row.i
	}
	return at
}

// carries reports whether wi carries every stored column of columns.
func carries(wi *plan.WorkItem, columns []string, derived plan.Derived) bool {
	for _, col := range columns {
		if _, ok := wi.Footer.Offsets.Get(wi.RowGroup, col); !ok && !derived.Has(col) {
			return false
		}
	}
	return true
}

// settle collects the values d's descriptor settles for q's columns, the same for every row.
func settle(d segment.Descriptor, q Query, derived plan.Derived) map[string][]byte {
	settled := map[string][]byte{}
	for _, col := range derived.Stored(slices.Concat(Fields(q.Where), q.Columns)) {
		if col == segment.ColTimestamp || slices.Contains(q.Require, col) {
			continue
		}
		if v, ok := plan.SegmentValue(d, col); ok {
			settled[col] = []byte(v)
		}
	}
	return settled
}

// Fields is the columns where reads, a group's alternatives' included.
func Fields(where []selector.ParsedPredicate) []string {
	out := make([]string, 0, len(where))
	for _, p := range where {
		if len(p.Any) > 0 {
			out = append(out, Fields(p.Any)...)
			continue
		}
		out = append(out, p.Field)
	}
	return out
}

// rowGroup is one row group's columns as read so far.
type rowGroup struct {
	item    *plan.WorkItem
	cells   map[string]segment.ColumnChunk
	settled map[string][]byte
	// detached is a detached row's values, which replace cells, nil for a settled column it holds null.
	detached map[string][]byte
	derived  plan.Derived
	first    int // the segment row index of the row group's first row
}

// read fetches the columns not yet read or settled in one Columns call, decoding only
// rows of them or every row for nil; one the row group does not carry costs nothing to
// ask for again.
func (g *rowGroup) read(ctx context.Context, rd segment.Reader, rows []int, columns []string) (segment.Cost, error) {
	var want []string
	for _, col := range columns {
		_, read := g.cells[col]
		if _, settled := g.settled[col]; col != "" && !read && !settled && !slices.Contains(want, col) {
			want = append(want, col)
		}
	}
	if len(want) == 0 {
		return segment.Cost{}, nil
	}
	got, cost, err := rd.Columns(ctx, g.item.Segment.Object, g.item.Footer, g.item.RowGroup, rows, want...)
	for col, values := range got {
		g.cells[col] = values
	}
	return cost, err
}

// release gives back the memory budget charged for the columns g read.
func (g *rowGroup) release(ctx context.Context) {
	for col, values := range g.cells {
		if !g.derived.Has(col) {
			values.Release(ctx)
		}
	}
}

// match returns the rows q keeps, timed by their ts when the query reads it.
func (g *rowGroup) match(q Query, timed bool) ([]Row, error) {
	var rows []Row
	from, to, where := g.serviceRun(q.Where)
	for i := from; i < to; i++ {
		row := Row{Segment: &g.item.Segment, group: g, i: i}
		if !row.Matches(where) {
			continue
		}
		if timed {
			t, ok := g.cells[segment.ColTimestamp].Time(i)
			if !ok {
				return nil, fmt.Errorf("unparseable ts %q", g.cells[segment.ColTimestamp].Text(i))
			}
			if row.Time = t; !q.Window.Contains(t) {
				continue
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// serviceRun is the rows [from, to) an equality on service can match, found by binary
// search since a row group's rows are in service order, and where's other predicates.
func (g *rowGroup) serviceRun(where []selector.ParsedPredicate) (from, to int, rest []selector.ParsedPredicate) {
	services, read := g.cells[segment.ColService]
	eq := slices.IndexFunc(where, func(p selector.ParsedPredicate) bool {
		return p.Field == segment.ColService && p.Op == selector.OpEq && len(p.Any) == 0
	})
	if !read || eq < 0 {
		return 0, g.count(), where
	}
	service := where[eq].Value
	at := func(i int) string { return bytesString(services.Text(i)) }
	from = sort.Search(services.Len(), func(i int) bool { return at(i) >= service })
	to = sort.Search(services.Len(), func(i int) bool { return at(i) > service })
	return from, to, slices.Delete(slices.Clone(where), eq, eq+1)
}

// derive computes each derived column of columns on every row, from the stored columns
// read for it.
func (g *rowGroup) derive(columns ...[]string) {
	for _, col := range slices.Concat(columns...) {
		if _, done := g.cells[col]; done {
			continue
		}
		if values, derived := g.derived.Column(col, g.count(), g.value); derived {
			g.cells[col] = segment.TextChunk(values)
		}
	}
}

// count is the row group's rows, as many as any column read holds.
func (g *rowGroup) count() int {
	for _, values := range g.cells {
		return values.Len()
	}
	return 0
}

func (g *rowGroup) value(column string, i int) ([]byte, bool) {
	if v, ok := g.detached[column]; ok {
		return v, v != nil
	}
	if values, ok := g.cells[column]; ok && i < values.Len() {
		v := values.Text(i)
		return v, v != nil
	}
	v, ok := g.settled[column]
	return v, ok
}

func (g *rowGroup) number(column string, i int) (float64, bool) {
	if values, ok := g.cells[column]; ok && i < values.Len() {
		return values.Number(i)
	}
	return 0, false
}

// Row is one row a query matched.
type Row struct {
	Segment *segment.Descriptor
	Time    time.Time
	group   *rowGroup
	i       int
}

// NewRow is a row of seg at t holding values, for a Fake scan to yield.
func NewRow(seg *segment.Descriptor, t time.Time, values map[string][]byte) Row {
	cells := make(map[string]segment.ColumnChunk, len(values))
	for col, v := range values {
		cells[col] = segment.TextChunk([][]byte{v})
	}
	return Row{Segment: seg, Time: t, group: &rowGroup{cells: cells}}
}

// Index is the row's index within its segment.
func (r Row) Index() int { return r.group.first + r.i }

// Detach is the row holding its own copy of each value, so a caller keeping it past Next
// does not keep its row group's columns, which the scan releases once it passes them. A
// column the row is null in is left out, as an absent one reads null, unless the segment
// settles it: a row of a segment with thousands of columns keeps only what it holds.
func (r Row) Detach() Row {
	g := *r.group
	g.cells, g.detached, g.first = nil, map[string][]byte{}, r.Index()
	size := 0
	for col, values := range r.group.cells {
		v := values.Text(r.i)
		if _, settled := g.settled[col]; v == nil && !settled {
			continue
		}
		g.detached[col], size = v, size+len(v)
	}
	// One copy of the row's values rather than one a value.
	buf := make([]byte, 0, size)
	for col, v := range g.detached {
		if v != nil {
			buf = append(buf, v...)
			g.detached[col] = buf[len(buf)-len(v) : len(buf) : len(buf)]
		}
	}
	r.group, r.i = &g, 0
	return r
}

// Each calls fn with each column the row holds a value of, read, settled by the segment's
// descriptor or derived, in no order, as Value reads each; value is the row's, valid until
// the scan passes it.
func (r Row) Each(fn func(column string, value []byte)) {
	g := r.group
	for col, v := range g.detached {
		if v != nil {
			fn(col, v)
		}
	}
	for col, values := range g.cells {
		if r.i >= values.Len() {
			continue
		}
		if v := values.Text(r.i); v != nil {
			fn(col, v)
		}
	}
	for col, v := range g.settled {
		if !r.holds(col) {
			fn(col, v)
		}
	}
}

// holds reports whether the row reads col from its own cells, so not from its settled values.
func (r Row) holds(col string) bool {
	if r.group.detached != nil {
		_, ok := r.group.detached[col]
		return ok
	}
	values, ok := r.group.cells[col]
	return ok && r.i < values.Len()
}

// Value is the row's value for column, read, settled by the segment's descriptor, or derived.
func (r Row) Value(column string) ([]byte, bool) {
	return r.group.value(column, r.i)
}

// Number is the row's value of column as a finite number, false where it holds none,
// read unformatted from a numeric column.
func (r Row) Number(column string) (float64, bool) {
	f, ok := r.group.number(column, r.i)
	if !ok {
		text, _ := r.Text(column)
		var err error
		f, err = strconv.ParseFloat(text, 64)
		ok = err == nil
	}
	return f, ok && !math.IsInf(f, 0) && !math.IsNaN(f)
}

// Text is Value as a string sharing its bytes, so it allocates nothing.
func (r Row) Text(column string) (string, bool) {
	v, ok := r.Value(column)
	return bytesString(v), ok
}

// List is the row's elements of column, and whether the column is a list; a null list
// has none.
func (r Row) List(column string) ([]string, bool) {
	if r.group.item == nil || !r.group.item.Footer.ZoneMaps[column].Type.Repeated() {
		return nil, false
	}
	var elems []string
	if v, _ := r.Value(column); v != nil {
		_ = json.Unmarshal(v, &elems) // a list cell decodes to a JSON array of strings
	}
	return elems, true
}

// Values is the row's elements of column, or its one value when column is not a list,
// and whether it is a list; a row without the column has none.
func (r Row) Values(column string) ([]string, bool) {
	if elems, list := r.List(column); list {
		return elems, true
	}
	if text, ok := r.Text(column); ok {
		return []string{text}, false
	}
	return nil, false
}

// Keys is what the row groups under by column: its value, or a list column's distinct
// elements in order and "" for a list with none, as for a row without the column.
func (r Row) Keys(column string) []string {
	elems, list := r.List(column)
	if !list {
		text, _ := r.Text(column)
		return []string{text}
	}
	if len(elems) == 0 {
		return []string{""}
	}
	slices.Sort(elems)
	return slices.Compact(elems)
}

// Matches reports whether the row satisfies every predicate in where.
func (r Row) Matches(where []selector.ParsedPredicate) bool {
	for _, p := range where {
		if !r.matches(p) {
			return false
		}
	}
	return true
}

// matches reports whether the row satisfies p, or any of a group's alternatives.
func (r Row) matches(p selector.ParsedPredicate) bool {
	if p.Equal != nil {
		return r.matchesEqual(p.Equal)
	}
	if len(p.Any) > 0 {
		return slices.ContainsFunc(p.Any, r.matches)
	}
	v, ok := r.Text(p.Field)
	if elems, list := r.List(p.Field); list && ok {
		return p.MatchesList(elems)
	}
	return p.Matches(v, ok)
}

// matchesEqual is matches of a group of equalities, each field's values: a field's value, or
// one of its list's elements, among them, "" for a row without the field.
func (r Row) matchesEqual(equal map[string]map[string]struct{}) bool {
	for field, values := range equal {
		v, ok := r.Text(field)
		if elems, list := r.List(field); list && ok {
			if slices.ContainsFunc(elems, func(e string) bool { _, in := values[e]; return in }) {
				return true
			}
			continue
		}
		if _, in := values[v]; in {
			return true
		}
	}
	return false
}

// bytesString aliases raw instead of copying it, which is safe because nothing mutates
// a decoded column chunk or a settled value once built.
func bytesString(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return unsafe.String(&raw[0], len(raw))
}
