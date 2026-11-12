package segment

import (
	"bytes"
	"cmp"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"time"
	"unsafe"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
)

// Cost is the object-store requests one Reader call made and the column bytes they
// transferred, which a caller adds to the cost it reports.
type Cost struct {
	GETs  int
	Bytes int64
}

// Reader reads segment objects with ranged GETs, so pruning happens with zero data
// GETs.
type Reader interface {
	// Grams reads the row groups' message filters, nil for a segment without them.
	Grams(ctx context.Context, loc ObjectLocation, footer Footer) (Grams, Cost, error)
	// Footer returns the segment footer and what reading it cost, which is nothing when
	// a cache served it.
	Footer(ctx context.Context, loc ObjectLocation) (Footer, Cost, error)
	// Columns range-GETs the named columns of one row group using an already-resolved
	// footer, leaving out any the row group does not carry, and decodes only rows, ascending,
	// or every row for nil.
	Columns(ctx context.Context, loc ObjectLocation, footer Footer, rowGroup int, rows []int, cols ...string) (map[string]ColumnChunk, Cost, error)
	// MayContain reads column's membership filter section to report whether the segment
	// may hold value; a column without a filter is not filtered and may hold any value.
	MayContain(ctx context.Context, loc ObjectLocation, footer Footer, column, value string) (filtered, may bool, cost Cost, err error)
	// Records reads the footer and every row, each row as the record it was sealed as: each
	// column in its stored type and left out of a row where it is null.
	Records(ctx context.Context, loc ObjectLocation) ([]Record, Footer, Cost, error)
	// RowGroupRecords is Records for one row group of footer's segment, so a reader of a
	// whole segment holds one group's rows at a time.
	RowGroupRecords(ctx context.Context, loc ObjectLocation, footer Footer, rowGroup int) ([]Record, Cost, error)
	// Rollup reads a merged segment's rollup object whole, for the summary package to decode.
	Rollup(ctx context.Context, loc ObjectLocation) ([]byte, Cost, error)
	// Filters reads a merged segment's filters object, each filtered column's membership filter.
	Filters(ctx context.Context, loc ObjectLocation) (map[string]*sketch.XorFilter, Cost, error)
}

type storeReader struct {
	store storage.Store
}

// NewReader returns a Reader over the segment objects in store.
func NewReader(store storage.Store) Reader {
	return storeReader{store: store}
}

// Footer suffix-reads the trailer, then the footer region it gives the length of, and
// bills the pair as the one footer GET the cost model counts.
func (r storeReader) Footer(ctx context.Context, loc ObjectLocation) (Footer, Cost, error) {
	cost := Cost{GETs: 1}
	for want := int64(trailerLen); ; {
		tail, err := r.store.GetSuffix(ctx, loc.Key, want)
		if err != nil {
			return Footer{}, cost, fmt.Errorf("segment: read footer: %w", err)
		}
		need, err := footerRegionLen(tail, want)
		if err != nil {
			return Footer{}, cost, err
		}
		if need <= int64(len(tail)) {
			footer, err := footerFromTail(tail)
			if err == nil {
				err = query.ChargeMem(ctx, footer.size)
			}
			return footer, cost, err
		}
		want = need
	}
}

// Columns bills each chunk its encoded byte range, which encodings make smaller than
// the decoded values, and charges ctx's memory budget the decoded values.
// A row group's chunks lie side by side, so the columns asked for are read in as few GETs as
// their spans allow, and a wide row group read whole costs one GET rather than one per column.
func (r storeReader) Columns(ctx context.Context, loc ObjectLocation, footer Footer, rowGroup int, rows []int, cols ...string) (map[string]ColumnChunk, Cost, error) {
	values := make(map[string]ColumnChunk, len(cols))
	var cost Cost
	for _, s := range spans(footer, rowGroup, cols) {
		buf, err := r.store.GetRange(ctx, loc.Key, s.Offset, s.Length)
		cost.GETs++
		if err != nil {
			return nil, cost, fmt.Errorf("segment: read column %s/%d: %w", s.chunks[0].col, rowGroup, err)
		}
		cost.Bytes += s.Length
		for _, c := range s.chunks {
			at := c.Offset - s.Offset
			if values[c.col], err = decodeColumnChunk(buf[at:at+c.Length], rows); err != nil {
				return nil, cost, fmt.Errorf("segment: decode column %s/%d: %w", c.col, rowGroup, err)
			}
			if err := query.ChargeMem(ctx, values[c.col].size()); err != nil {
				return nil, cost, err
			}
		}
	}
	return values, cost, nil
}

// maxSpanGap is the most bytes no column asked for that one GET reads through rather than
// splitting in two, since a round trip to the store costs more than reading that much.
const maxSpanGap = 64 << 10

// span is one byte range Columns reads and the column chunks inside it.
type span struct {
	ByteRange
	chunks []spanChunk
}

type spanChunk struct {
	col string
	ByteRange
}

// spans groups the chunks of cols that rowGroup carries into ranges, in file order, each
// reaching to the next chunk no more than maxSpanGap past its end.
func spans(footer Footer, rowGroup int, cols []string) []span {
	var chunks []spanChunk
	for _, col := range cols {
		if br, ok := footer.Offsets.Get(rowGroup, col); ok {
			chunks = append(chunks, spanChunk{col: col, ByteRange: br})
		}
	}
	slices.SortFunc(chunks, func(a, b spanChunk) int { return cmp.Compare(a.Offset, b.Offset) })
	var out []span
	for _, c := range chunks {
		if n := len(out); n > 0 && c.Offset <= out[n-1].Offset+out[n-1].Length+maxSpanGap {
			last := &out[n-1]
			last.Length = max(last.Length, c.Offset+c.Length-last.Offset)
			last.chunks = append(last.chunks, c)
			continue
		}
		out = append(out, span{ByteRange: c.ByteRange, chunks: []spanChunk{c}})
	}
	return out
}

func (r storeReader) MayContain(ctx context.Context, loc ObjectLocation, footer Footer, column, value string) (filtered, may bool, cost Cost, err error) {
	filter, cost, err := r.filter(ctx, loc, footer, column)
	return filter != nil, filter == nil || filter.MayContain(value), cost, err
}

// filter range-GETs and decodes column's filter section, nil when the footer locates none.
func (r storeReader) filter(ctx context.Context, loc ObjectLocation, footer Footer, column string) (*sketch.XorFilter, Cost, error) {
	br, ok := footer.filters[column]
	if !ok {
		return nil, Cost{}, nil
	}
	buf, err := r.store.GetRange(ctx, loc.Key, br.Offset, br.Length)
	cost := Cost{GETs: 1}
	if err != nil {
		return nil, cost, fmt.Errorf("segment: read filter %s: %w", column, err)
	}
	cost.Bytes = br.Length
	filter := &sketch.XorFilter{}
	if err := filter.UnmarshalBinary(buf); err != nil {
		return nil, cost, fmt.Errorf("segment: decode filter %s: %w", column, err)
	}
	return filter, cost, nil
}

func (r storeReader) Records(ctx context.Context, loc ObjectLocation) ([]Record, Footer, Cost, error) {
	footer, cost, err := r.Footer(ctx, loc)
	if err != nil {
		return nil, Footer{}, cost, err
	}
	var records []Record
	for g := range footer.RowGroups {
		group, read, err := r.RowGroupRecords(ctx, loc, footer, g)
		cost.GETs, cost.Bytes = cost.GETs+read.GETs, cost.Bytes+read.Bytes
		if err != nil {
			return nil, Footer{}, cost, err
		}
		records = append(records, group...)
	}
	return records, footer, cost, nil
}

func (r storeReader) Rollup(ctx context.Context, loc ObjectLocation) ([]byte, Cost, error) {
	return r.whole(ctx, loc, "rollup")
}

func (r storeReader) Filters(ctx context.Context, loc ObjectLocation) (map[string]*sketch.XorFilter, Cost, error) {
	body, cost, err := r.whole(ctx, loc, "filters")
	if err != nil {
		return nil, cost, err
	}
	var filters map[string]*sketch.XorFilter
	if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&filters); err != nil {
		return nil, cost, fmt.Errorf("segment: decode filters: %w", err)
	}
	return filters, cost, nil
}

// whole reads the object at loc in one GET, naming it what in an error.
func (r storeReader) whole(ctx context.Context, loc ObjectLocation, what string) ([]byte, Cost, error) {
	body, err := r.store.GetSuffix(ctx, loc.Key, math.MaxInt64)
	if err != nil {
		return nil, Cost{GETs: 1}, fmt.Errorf("segment: read %s: %w", what, err)
	}
	return body, Cost{GETs: 1, Bytes: int64(len(body))}, nil
}

// EncodeFilters is the filters object of a merged segment whose filters are filters.
func EncodeFilters(filters map[string]*sketch.XorFilter) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(filters); err != nil {
		return nil, fmt.Errorf("segment: encode filters: %w", err)
	}
	return buf.Bytes(), nil
}

// RowGroupRecords reads every column row group g carries.
func (r storeReader) RowGroupRecords(ctx context.Context, loc ObjectLocation, footer Footer, g int) ([]Record, Cost, error) {
	var cols []string
	for key := range footer.Offsets {
		if key.RowGroup == g {
			cols = append(cols, key.Column)
		}
	}
	slices.Sort(cols)
	chunks, cost, err := r.Columns(ctx, loc, footer, g, nil, cols...)
	if err != nil {
		return nil, cost, err
	}
	records := make([]Record, chunks[ColTimestamp].Len())
	for i := range records {
		records[i].Timestamp, _ = chunks[ColTimestamp].Time(i)
	}
	for _, name := range cols {
		if name == ColTimestamp {
			continue
		}
		if err := chunks[name].appendTo(records, name); err != nil {
			return nil, cost, fmt.Errorf("segment: read column %s/%d: %w", name, g, err)
		}
	}
	return records, cost, nil
}

// ColumnChunk is one column of one row group, holding a number, bool or timestamp as its
// value and formatting it only when a caller asks for its text.
type ColumnChunk struct {
	text   [][]byte
	typ    value.Type
	values []value.Value
	null   []bool // by row, for a value column with a null row
	// rows, when set, are the n rows' indexes the chunk holds, every other row read as null.
	rows []int
	n    int
}

// TextChunk is a chunk of rows already in their canonical text, nil for a null row.
func TextChunk(rows [][]byte) ColumnChunk { return ColumnChunk{text: rows} } //nolint:passthrough // the field is unexported so a chunk holds one form, and callers outside segment cannot write the literal

func (c ColumnChunk) holding(rows []int, n int) ColumnChunk {
	c.rows, c.n = rows, n
	return c
}

// at is where c holds row i, false for a row it was not decoded for.
func (c ColumnChunk) at(i int) (int, bool) {
	if c.rows == nil {
		return i, true
	}
	return slices.BinarySearch(c.rows, i)
}

// Len is the chunk's rows.
func (c ColumnChunk) Len() int {
	if c.rows != nil {
		return c.n
	}
	if c.text != nil {
		return len(c.text)
	}
	return len(c.values)
}

// Text is row i's canonical text, nil for a null row; a value row is formatted on each
// call, so a caller reading it repeatedly keeps it.
func (c ColumnChunk) Text(i int) []byte {
	i, ok := c.at(i)
	if !ok {
		return nil
	}
	if c.text != nil {
		return c.text[i]
	}
	if c.null != nil && c.null[i] {
		return nil
	}
	return value.AppendFormat(nil, c.typ, c.values[i])
}

// Time is row i's instant, a text row's parsed as RFC3339; false for a null row, a
// value row of another type, or text that is no timestamp.
func (c ColumnChunk) Time(i int) (time.Time, bool) {
	i, ok := c.at(i)
	if !ok {
		return time.Time{}, false
	}
	if c.text != nil {
		t, err := time.Parse(time.RFC3339Nano, string(c.text[i]))
		return t, err == nil
	}
	if c.typ != value.TypeTimestamp || (c.null != nil && c.null[i]) {
		return time.Time{}, false
	}
	return c.values[i].Time, true
}

// Number is row i's value unformatted, false unless it is an int or a float.
func (c ColumnChunk) Number(i int) (float64, bool) {
	i, ok := c.at(i)
	if !ok || c.text != nil || (c.null != nil && c.null[i]) {
		return 0, false
	}
	switch c.typ {
	case value.TypeInt:
		return float64(c.values[i].Int), true
	case value.TypeFloat:
		return c.values[i].Float, true
	}
	return 0, false
}

// Rows is every row's Text.
func (c ColumnChunk) Rows() [][]byte {
	if c.text != nil && c.rows == nil {
		return c.text
	}
	rows := make([][]byte, c.Len())
	for i := range rows {
		rows[i] = c.Text(i)
	}
	return rows
}

// size is the bytes c holds, a row sharing a dictionary entry counted as if its own.
func (c ColumnChunk) size() int64 {
	n := int64(len(c.text))*int64(unsafe.Sizeof([]byte(nil))) + int64(len(c.values))*int64(unsafe.Sizeof(value.Value{})) + int64(len(c.null))
	for _, row := range c.text {
		n += int64(len(row))
	}
	return n
}

// Release gives back to ctx's memory budget what Columns charged it for c, once the
// caller no longer holds c.
func (c ColumnChunk) Release(ctx context.Context) { query.ReleaseMem(ctx, c.size()) }

// appendTo appends each row's value to that record's columns as column name, leaving out a
// null row.
func (c ColumnChunk) appendTo(records []Record, name string) error {
	for i := range records {
		v, ok, err := c.value(i)
		if err != nil {
			return err
		}
		if ok {
			records[i].Columns = append(records[i].Columns, Column{Name: name, Type: c.typ, Value: v})
		}
	}
	return nil
}

// value is row i's value in the chunk's type, false for a null row; a list's text is the
// JSON array of its elements' text.
func (c ColumnChunk) value(i int) (value.Value, bool, error) {
	i, ok := c.at(i)
	if !ok {
		return value.Value{}, false, nil
	}
	if c.text == nil {
		return c.values[i], c.null == nil || !c.null[i], nil
	}
	text := c.text[i]
	if text == nil {
		return value.Value{}, false, nil
	}
	if !c.typ.Repeated() {
		return textValue(c.typ, text), true, nil
	}
	var elems []string
	if err := json.Unmarshal(text, &elems); err != nil {
		return value.Value{}, false, err
	}
	list := make([]value.Value, len(elems))
	for j, elem := range elems {
		var err error
		if list[j], err = value.Parse(c.typ.Scalar(), elem); err != nil {
			return value.Value{}, false, err
		}
	}
	return value.Value{List: list}, true, nil
}

// Tier is the key prefix a segment is written under, so a bucket lifecycle rule can
// expire each tier on its own schedule.
type Tier string

const (
	// TierRaw is the collector's segments, which compaction supersedes.
	TierRaw Tier = "raw"
	// TierHour is the compactor's merged stream-hours.
	TierHour Tier = "hour"
	// TierWindow is the compactor's merged windows of an hour not yet complete, which the
	// hour's merge supersedes.
	TierWindow Tier = "window"
)

// BuildSegmentKey is the tenant-prefixed object key a sealed segment of tier is written to.
func BuildSegmentKey(tenant string, tier Tier, descriptor Descriptor) string {
	ts := descriptor.TimeRange.Min.UTC()
	return fmt.Sprintf("%s/%s/%s/%02d/%02d/%02d/%s/%s.seg",
		tenant,
		tier,
		ts.Format("2006"),
		int(ts.Month()),
		ts.Day(),
		ts.Hour(),
		descriptor.Stream.Fingerprint(),
		descriptor.ID,
	)
}
