package segment

import (
	"bytes"
	"cmp"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"

	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// The on-disk segment object. All multi-byte integers are
// big-endian.
//
//	BODY:    row groups of self-describing column chunks, located by the
//	         footer's offset table so a reader range-GETs one chunk.
//	FILTERS: one serialized xor filter per filtered column, sorted by column.
//	FOOTER:  version, row-group count, zone maps, pattern_id present-set and texts,
//	         pattern profile, filter byte ranges, sketches, column-chunk
//	         offset table.
//	TRAILER: footerLen uint64 | magic "INSG1SEG" (16 bytes)
//
// A reader suffix-GETs the trailer, which validates the object and locates the
// footer immediately preceding it.
//
// A row group holds a chunk of each column one of its rows holds, and of ts.
// A column chunk is chunkVersion | value type | encoding tag, then a zstd frame of
// uint32 n, n × uvarint (0 for a null row, else its value count + 1), then the values.
// A list row's counts are its offsets buffer, delta-coded.
//
//	tag 1 (raw):   per value, uint32 len, bytes
//	tag 2 (dict):  per value, uvarint id; an id one past the dictionary is
//	               followed by uint32 len, bytes, its new entry
//	tag 3 (delta): per value, zigzag-varint delta of its int64 form
//
// zstd then removes what each encoding leaves, the dictionary's repeated ids included.

var segmentMagic = [8]byte{'I', 'N', 'S', 'G', '1', 'S', 'E', 'G'}

const (
	trailerLen    = 16 // footerLen(8) + magic(8)
	footerVersion = 11
	// fieldsVersion is the first footer version whose service views keep field stats; a
	// reader still decodes the one before it, whose views report none.
	fieldsVersion  = 11
	chunkVersion   = 1
	chunkHeaderLen = 3

	chunkTagRaw   = 1
	chunkTagDict  = 2
	chunkTagDelta = 3

	// targetRowGroupBytes is the encoded size a row group is cut at; doubling lands a cut
	// below 2×.
	targetRowGroupBytes = 8 << 20
	firstGroupRows      = 64
	// MaxGroupCells is the most rows times columns a row group holds a chunk of however small
	// they encode, since a reader decodes a group whole and each cell costs it tens of bytes.
	MaxGroupCells = 1 << 20
	// maxChunkBytes bounds a chunk's decompressed size, since its frame comes off
	// disk untrusted.
	maxChunkBytes = 1 << 30
)

// VersionError reports a chunk or footer in an encoding version this build does not
// read; there is one supported version of each and no compat path.
type VersionError struct {
	Object  string
	Version byte
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("segment: unsupported %s version %d", e.Object, e.Version)
}

var (
	zstdEncoder, _ = zstd.NewWriter(nil)
	// footerEncoder is one encoder for footers, since the history a large footer grows an
	// encoder to is kept: each encoder of a pool would come to hold its own.
	footerEncoder, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	zstdDecoder, _   = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0), zstd.WithDecoderMaxMemory(maxChunkBytes))
)

// Encode serializes a sealed segment into byte-sized row groups of column chunks and
// a footer whose indexes are computed from the same rows.
func (s Sealed) Encode() ([]byte, error) {
	if s.table != nil {
		return encodeTable(s.table, s.Descriptor.ZoneMaps, s.PatternProfile, s.patterns, targetRowGroupBytes)
	}
	return encodeTable(tableOf(s.Records), nil, s.PatternProfile, s.patterns, targetRowGroupBytes)
}

// encodeTable cuts row groups at groupBytes, which tests shrink to get many groups;
// zm is the rows' zone maps, computed here when nil.
func encodeTable(t *table, zm ZoneMaps, profile PatternProfile, patterns map[string]string, groupBytes int) ([]byte, error) {
	if zm == nil {
		zm = t.zoneMaps(0, len(t.ts))
	}
	cols := t.columns()
	footer, filters, err := buildFooter(cols, t, textsOf(cols, t), zm, patterns)
	if err != nil {
		return nil, err
	}
	footer.PatternProfile = profile
	return sealBody(appendRowGroups(nil, &footer, cols, t, zm, groupBytes), footer, filters)
}

// appendRowGroups appends t's rows to body as row groups cut at groupBytes, adding each
// one's chunk offsets and zone maps to f; zm is the rows' zone maps.
func appendRowGroups(body []byte, f *Footer, cols []column, t *table, zm ZoneMaps, groupBytes int) []byte {
	if f.Offsets == nil {
		f.Offsets = make(OffsetTable)
	}
	var messages columnTexts
	for _, c := range cols {
		if c.name == ColMessage {
			messages = textsOf([]column{c}, t)[0]
		}
	}
	// An empty segment still gets one empty row group, keeping the layout uniform.
	for lo := 0; lo < len(t.ts) || f.RowGroups == 0; f.RowGroups++ {
		hi, chunks := cutRowGroup(cols, t, lo, groupBytes)
		for ci, chunk := range chunks {
			if chunk == nil {
				continue
			}
			f.Offsets.Set(f.RowGroups, cols[ci].name, ByteRange{Offset: int64(len(body)), Length: int64(len(chunk))})
			body = append(body, chunk...)
		}
		groupZM := zm
		if lo > 0 || hi < len(t.ts) { // a group short of the rows has its own
			groupZM = t.zoneMaps(lo, hi)
		}
		f.RowGroupZoneMaps = append(f.RowGroupZoneMaps, groupZM)
		filter := buildGrams(messages.span(lo, hi))
		f.grams = append(f.grams, filter)
		lo = hi
	}
	return body
}

// sealBody appends each filter section, then footer locating them, then the trailer to body.
func sealBody(body []byte, footer Footer, filters map[string][]byte) ([]byte, error) {
	filters[GramsSection] = encodeGrams(footer.grams)
	footer.filters = make(map[string]ByteRange, len(filters))
	for _, name := range slices.Sorted(maps.Keys(filters)) {
		footer.filters[name] = ByteRange{Offset: int64(len(body)), Length: int64(len(filters[name]))}
		body = append(body, filters[name]...)
	}
	raw, err := footer.MarshalBinary()
	if err != nil {
		return nil, err
	}
	// Compressed because its sketches are mostly empty registers and counters, which
	// would otherwise make the footer most of a small segment.
	footerBytes := footerEncoder.EncodeAll(raw, nil)

	body = append(body, footerBytes...)
	body = binary.BigEndian.AppendUint64(body, uint64(len(footerBytes)))
	return append(body, segmentMagic[:]...), nil
}

// cutRowGroup encodes the row group starting at lo, doubling its rows until its
// chunks reach groupBytes or the rows run out (invariant S2). The rest of the
// rows is tried first: under groupBytes, every shorter group the doubling would try
// is too, since a group's chunks grow with its rows, so it is the group.
func cutRowGroup(cols []column, t *table, lo, groupBytes int) (int, [][]byte) {
	if hi, full := cellCap(cols, t, lo, len(t.ts)); !full {
		if chunks, size := encodeRowGroup(cols, t, lo, hi); size < groupBytes {
			return hi, chunks
		}
	}
	for rows := firstGroupRows; ; rows *= 2 {
		hi, full := cellCap(cols, t, lo, min(lo+rows, len(t.ts)))
		chunks, size := encodeRowGroup(cols, t, lo, hi)
		if size >= groupBytes || hi == len(t.ts) || full {
			return hi, chunks
		}
	}
}

// cellCap is hi, or where short of it the group from lo reaches MaxGroupCells rows times the
// columns it holds a chunk of, and whether it does: rows that compress well decode to many
// times their encoded size, and a reader decodes a group whole.
func cellCap(cols []column, t *table, lo, hi int) (int, bool) {
	cells := func(hi int) int {
		held := 0
		for _, c := range cols {
			if c.name == ColTimestamp || len(cellsIn(t.cols[c.name], lo, hi)) > 0 {
				held++
			}
		}
		return (hi - lo) * held
	}
	if cells(hi) <= MaxGroupCells {
		return hi, false
	}
	// A group's cells only grow with its rows, so the last row under the cap is searched for.
	past := lo + 1 + sort.Search(hi-lo-1, func(n int) bool { return cells(lo+1+n) > MaxGroupCells })
	return max(lo+1, past-1), true
}

// footerFromTail parses a segment footer from its trailing bytes. tail must end
// with the object's trailer and contain the whole footer region (as returned by
// a suffix range GET large enough to cover the footer); it may include arbitrary
// preceding body bytes, which are ignored.
func footerFromTail(tail []byte) (Footer, error) {
	if len(tail) < trailerLen {
		return Footer{}, fmt.Errorf("segment: footer tail too short (%d bytes)", len(tail))
	}
	if !bytes.Equal(tail[len(tail)-8:], segmentMagic[:]) {
		return Footer{}, errors.New("segment: bad magic; not a segment object")
	}
	footerLen := binary.BigEndian.Uint64(tail[len(tail)-trailerLen : len(tail)-8])
	if uint64(len(tail)) < uint64(trailerLen)+footerLen {
		return Footer{}, fmt.Errorf("segment: footer truncated: have %d bytes, need %d", len(tail), uint64(trailerLen)+footerLen)
	}
	start := len(tail) - trailerLen - int(footerLen)
	raw, err := zstdDecoder.DecodeAll(tail[start:len(tail)-trailerLen], nil)
	if err != nil {
		return Footer{}, fmt.Errorf("segment: decompress footer: %w", err)
	}
	return decodeFooter(raw)
}

// footerRegionLen returns how many trailing bytes hold the footer and trailer, read
// from the trailer at the end of tail, a suffix read of up to want bytes.
func footerRegionLen(tail []byte, want int64) (int64, error) {
	if len(tail) < trailerLen {
		return 0, fmt.Errorf("segment: object too small for a footer (%d bytes)", len(tail))
	}
	footerLen := binary.BigEndian.Uint64(tail[len(tail)-trailerLen : len(tail)-8])
	// A suffix shorter than asked for is the whole object, which bounds the footer; the
	// check also keeps a corrupt near-MaxUint64 length from overflowing.
	if footerLen > uint64(len(tail)-trailerLen) && int64(len(tail)) < want {
		return 0, fmt.Errorf("segment: footer length %d exceeds object size %d", footerLen, len(tail))
	}
	if footerLen > math.MaxInt64-trailerLen {
		return 0, fmt.Errorf("segment: footer length %d out of range", footerLen)
	}
	return int64(footerLen) + trailerLen, nil
}

// decodeColumnChunk parses one column chunk, range-read using a ByteRange from the
// footer's OffsetTable, into a ColumnChunk holding rows, ascending, or every row for nil.
func decodeColumnChunk(chunk []byte, rows []int) (ColumnChunk, error) {
	if len(chunk) < chunkHeaderLen {
		return ColumnChunk{}, fmt.Errorf("segment: column chunk too short (%d bytes)", len(chunk))
	}
	if chunk[0] != chunkVersion {
		return ColumnChunk{}, &VersionError{Object: "chunk", Version: chunk[0]}
	}
	typ, tag := value.Type(chunk[1]), chunk[2]
	pooled := payloads.Get().(*[]byte)
	payload, err := zstdDecoder.DecodeAll(chunk[chunkHeaderLen:], (*pooled)[:0])
	if err != nil {
		return ColumnChunk{}, fmt.Errorf("segment: decompress column chunk: %w", err)
	}
	c, err := decodePayload(payload, typ, tag, rows)
	if err == nil && ownPayload(c, payload) {
		*pooled = payload[:0]
		if cap(payload) <= maxPooledPayload {
			payloads.Put(pooled)
		}
	}
	return c, err
}

// payloads are buffers a chunk decompresses into, given back when its decoded chunk keeps
// no slice of it: a number's values are decoded, a list's rows formatted, and the few values
// a mostly null column holds copied out, so most chunks a wide read decodes reuse one.
var payloads = sync.Pool{New: func() any { return new([]byte) }}

// maxPooledPayload bounds a buffer the pool keeps, so one wide chunk does not stay held.
const maxPooledPayload = 4 << 20

// ownPayload reports whether c holds none of payload, copying out its text when what it
// keeps is under a quarter of the payload; a chunk that keeps more aliases it rather than
// copying most of it.
func ownPayload(c ColumnChunk, payload []byte) bool {
	if c.text == nil || c.typ.Repeated() {
		return true
	}
	kept := 0
	for _, v := range c.text {
		kept += len(v)
	}
	if 4*kept >= len(payload) {
		return false
	}
	own := make([]byte, 0, kept)
	for i, v := range c.text {
		if v != nil {
			own = append(own, v...)
			c.text[i] = own[len(own)-len(v) : len(own) : len(own)]
		}
	}
	return true
}

// decodePayload is decodeColumnChunk of the chunk's decompressed payload.
func decodePayload(payload []byte, typ value.Type, tag byte, rows []int) (ColumnChunk, error) {
	r := &cursor{b: payload}
	held, n, err := decodeCounts(r, typ)
	if err != nil {
		return ColumnChunk{}, err
	}
	read, err := held.pick(rows)
	if err != nil {
		return ColumnChunk{}, err
	}
	if tag == chunkTagDelta {
		leaves, err := decodeNumbers(r, typ, n, read.keep)
		if err != nil {
			return ColumnChunk{}, err
		}
		return valueChunk(typ, read.counts, leaves).holding(read.rows, held.total), nil
	}
	leaves, err := decodeTexts(r, tag, n, read.keep)
	if err != nil {
		return ColumnChunk{}, err
	}
	return textChunk(typ, read.counts, leaves).holding(read.rows, held.total), nil
}

// heldRows is a chunk's rows' value counts: every row's (-1 for null) in perRow when at least
// half hold a value, else the rows holding one, ascending, and their counts, so a column few of
// thousands of rows carry costs those few and one most rows carry costs what it did.
type heldRows struct {
	perRow       []int
	rows, counts []int
	total        int
}

// chunkRead is the rows a chunk is decoded for, nil for all, each one's value count (-1 for
// null), and which of the chunk's values they hold, nil for all.
type chunkRead struct {
	rows, counts []int
	keep         []bool
}

// each calls fn with each row holding a value, ascending, and its value count.
func (h heldRows) each(fn func(row, n int)) {
	if h.perRow == nil {
		for j, row := range h.rows {
			fn(row, h.counts[j])
		}
		return
	}
	for row, n := range h.perRow {
		if n >= 0 {
			fn(row, n)
		}
	}
}

// pick is the read of rows, every row for nil, which keeps only those holding a value when
// fewer than half of them do, every other row then reading as null.
func (h heldRows) pick(rows []int) (chunkRead, error) {
	if rows == nil {
		if h.perRow == nil {
			return chunkRead{rows: h.rows, counts: h.counts}, nil
		}
		return chunkRead{counts: h.perRow}, nil
	}
	for k, row := range rows {
		if row >= h.total || k > 0 && row <= rows[k-1] {
			return chunkRead{}, fmt.Errorf("segment: %d rows not ascending within %d", len(rows), h.total)
		}
	}
	read := h.within(rows)
	if 2*len(read.rows) >= len(rows) {
		read.rows, read.counts = rows, h.expand(rows)
	}
	return read, nil
}

// within is the read of the rows of rows, ascending, that hold a value.
func (h heldRows) within(rows []int) chunkRead {
	read, next := chunkRead{rows: []int{}, keep: []bool{}}, 0
	h.each(func(row, n int) {
		for next < len(rows) && rows[next] < row {
			next++
		}
		in := next < len(rows) && rows[next] == row
		if in {
			read.rows, read.counts = append(read.rows, row), append(read.counts, n)
		}
		for range n {
			read.keep = append(read.keep, in)
		}
	})
	return read
}

// expand is each of rows' value count, -1 for a null one.
func (h heldRows) expand(rows []int) []int {
	counts := make([]int, len(rows))
	j := 0
	for k, row := range rows {
		if h.perRow != nil {
			counts[k] = h.perRow[row]
			continue
		}
		for j < len(h.rows) && h.rows[j] < row {
			j++
		}
		counts[k] = -1
		if j < len(h.rows) && h.rows[j] == row {
			counts[k] = h.counts[j]
		}
	}
	return counts
}

// valueChunk keeps a scalar column's values unformatted, and formats a list column's
// rows now, since a list's text is a JSON array its readers decode whole.
func valueChunk(typ value.Type, counts []int, leaves []value.Value) ColumnChunk {
	if typ.Repeated() {
		rows := make([][]byte, len(counts))
		for i, n := range counts {
			if n >= 0 {
				rows[i] = []byte(value.Format(typ, value.Value{List: leaves[:n]}))
			}
			leaves = leaves[max(n, 0):]
		}
		return ColumnChunk{text: rows, typ: typ}
	}
	if len(leaves) == len(counts) { // no null row, so each row is its one leaf
		return ColumnChunk{typ: typ, values: leaves}
	}
	c := ColumnChunk{typ: typ, values: make([]value.Value, len(counts))}
	for i, n := range counts {
		if n < 0 {
			if c.null == nil {
				c.null = make([]bool, len(counts))
			}
			c.null[i] = true
			continue
		}
		c.values[i], leaves = leaves[0], leaves[1:]
	}
	return c
}

// textChunk aliases each scalar row's text in the decoded payload rather than copying
// it, which is safe because nothing mutates a decoded chunk.
func textChunk(typ value.Type, counts []int, leaves [][]byte) ColumnChunk {
	if !typ.Repeated() && len(leaves) == len(counts) { // no null row, so each row is its one leaf
		return ColumnChunk{text: leaves, typ: typ}
	}
	rows := make([][]byte, len(counts))
	for i, n := range counts {
		switch {
		case n < 0: // a null row stays nil
		case typ.Repeated():
			rows[i] = listText(typ, leaves[:n])
		default:
			rows[i] = leaves[0]
		}
		leaves = leaves[max(n, 0):]
	}
	return ColumnChunk{text: rows, typ: typ}
}

// listText is a list row's text from its elements', which for a string or JSON element is
// its canonical text, so only another type's are made values to format.
func listText(typ value.Type, leaves [][]byte) []byte {
	elems := make([]string, len(leaves))
	for j, text := range leaves {
		elems[j] = string(text)
		if s := typ.Scalar(); s != value.TypeString && s != value.TypeJSON {
			elems[j] = value.Format(s, textValue(s, text))
		}
	}
	return value.ListText(elems)
}

// decodeCounts reads each row's value count and their total, bounding both by the payload
// length since every row and value consumes at least one byte. It reads them twice, first to
// count the rows holding a value and so allocate only for the form heldRows keeps.
func decodeCounts(r *cursor, typ value.Type) (heldRows, int, error) {
	total := r.count()
	start := r.off
	held, values := 0, 0
	for i := range total {
		n := int(r.uvarint()) - 1
		scalarOK := typ.Repeated() || n == -1 || n == 1
		if n < -1 || n > len(r.b) || !scalarOK {
			return heldRows{}, 0, fmt.Errorf("segment: column chunk row %d holds %d values", i, n)
		}
		if n >= 0 {
			held, values = held+1, values+n
		}
	}
	if values > len(r.b) {
		return heldRows{}, 0, fmt.Errorf("segment: column chunk value count %d out of range", values)
	}
	r.off = start
	h := heldRows{total: total}
	if 2*held >= total {
		h.perRow = make([]int, total)
	} else {
		h.rows, h.counts = make([]int, 0, held), make([]int, 0, held)
	}
	for i := range total {
		n := int(r.uvarint()) - 1
		if h.perRow != nil {
			h.perRow[i] = n
		} else if n >= 0 {
			h.rows, h.counts = append(h.rows, i), append(h.counts, n)
		}
	}
	return h, values, r.err
}

// decodeNumbers returns the values keep marks, every one for nil keep.
func decodeNumbers(r *cursor, typ value.Type, n int, keep []bool) ([]value.Value, error) {
	values := make([]value.Value, 0, leafCap(n, keep))
	var prev int64
	for i := range n {
		prev += zigzagDecode(r.uvarint())
		if keep == nil || keep[i] {
			values = append(values, fromInt64(typ, prev))
		}
	}
	return values, r.err
}

// decodeTexts returns the text of each value keep marks, every one for nil keep, as a
// slice of the payload, a dictionary entry's shared by every value naming it.
func decodeTexts(r *cursor, tag byte, n int, keep []bool) ([][]byte, error) {
	texts := make([][]byte, 0, leafCap(n, keep))
	var dict [][]byte
	for i := range n {
		var text []byte
		switch tag {
		case chunkTagRaw:
			text = r.bytes()
		case chunkTagDict:
			id := int(r.uvarint())
			if id == len(dict) {
				dict = append(dict, r.bytes())
			}
			if id < 0 || id >= len(dict) {
				return nil, fmt.Errorf("segment: dict id %d out of range %d", id, len(dict))
			}
			text = dict[id]
		default:
			return nil, fmt.Errorf("segment: unknown column chunk tag %d", tag)
		}
		if keep == nil || keep[i] {
			texts = append(texts, text)
		}
	}
	return texts, r.err
}

// leafCap is the capacity a decode keeping keep of n values starts with, growing for a pick.
func leafCap(n int, keep []bool) int {
	if keep == nil {
		return n
	}
	return 0
}

func textValue(typ value.Type, text []byte) value.Value {
	if typ.Scalar() == value.TypeJSON {
		return value.JSON(text)
	}
	return value.Value{String: string(text)}
}

// column is one physical column: its name, value type and whether some service's
// view counts its values.
type column struct {
	name    string
	typ     value.Type
	counted bool
}

// columnTexts is one column's canonical text, every leaf in row order with the row holding
// it, so each value is formatted once for all the footer's indexes and every service's view
// of it, and a column few rows hold costs those few.
type columnTexts struct {
	texts []string
	at    []int32 // the row of each leaf
}

// textsOf is each column's texts, the columns concurrently.
func textsOf(cols []column, t *table) []columnTexts {
	out := make([]columnTexts, len(cols))
	parallel(len(cols), func(ci int) {
		c, cells := cols[ci], t.cols[cols[ci].name]
		// Sized for a scalar's one leaf a cell, which a list's grow past.
		out[ci] = columnTexts{texts: make([]string, 0, len(cells)), at: make([]int32, 0, len(cells))}
		for _, cell := range cells {
			for j := range leafCount(c.typ, cell.val, true) {
				out[ci].texts = append(out[ci].texts, value.Format(c.typ.Scalar(), leafAt(c.typ, cell.val, j)))
				out[ci].at = append(out[ci].at, cell.row)
			}
		}
	})
	return out
}

// span is the leaves of rows [lo, hi).
func (c columnTexts) span(lo, hi int) []string {
	from, _ := slices.BinarySearch(c.at, int32(lo))
	to, _ := slices.BinarySearch(c.at, int32(hi))
	return c.texts[from:to]
}

// row is row i's leaves.
func (c columnTexts) row(i int) []string { return c.span(i, i+1) }

// rows is the leaves of the given rows, ascending.
func (c columnTexts) rows(rows []int) []string {
	var out []string
	next := 0
	for j, row := range c.at {
		for next < len(rows) && int32(rows[next]) < row {
			next++
		}
		if next == len(rows) {
			break
		}
		if int32(rows[next]) == row {
			out = append(out, c.texts[j])
		}
	}
	return out
}

// BuildZoneMaps is for callers holding records that have not been through a Builder.
func BuildZoneMaps(records []Record) ZoneMaps {
	return tableOf(records).zoneMaps(0, len(records))
}

// leafCount is how many values a cell holds: none when absent, a list's elements,
// or the one scalar.
func leafCount(typ value.Type, v value.Value, present bool) int {
	switch {
	case !present:
		return 0
	case typ.Repeated():
		return len(v.List)
	default:
		return 1
	}
}

func leafAt(typ value.Type, v value.Value, i int) value.Value {
	if typ.Repeated() {
		return v.List[i]
	}
	return v
}

// encodeRowGroup encodes and compresses each column of rows [lo, hi), the columns
// concurrently, returning the chunks and their total size.
func encodeRowGroup(cols []column, t *table, lo, hi int) ([][]byte, int) {
	chunks := make([][]byte, len(cols))
	parallel(len(cols), func(ci int) {
		// A column no row of the group holds has no chunk, nil here, which a reader reads as
		// null: a group of an agent's flattened transcript holds a few hundred of its thousands.
		if cells := cellsIn(t.cols[cols[ci].name], lo, hi); len(cells) > 0 || cols[ci].name == ColTimestamp {
			chunks[ci] = encodeColumnChunk(cols[ci], cells, lo, hi)
		}
	})
	size := 0
	for _, chunk := range chunks {
		size += len(chunk)
	}
	return chunks, size
}

// encodeColumnChunk encodes column c of rows [lo, hi), whose cells are cells.
func encodeColumnChunk(c column, cells []cell, lo, hi int) []byte {
	enc := valueEncoder{tag: chunkTag(c), typ: c.typ}
	var counts, values []byte
	for row := int32(lo); row < int32(hi); row++ {
		if len(cells) == 0 || cells[0].row != row {
			counts = append(counts, 0) // a null row
			continue
		}
		n := leafCount(c.typ, cells[0].val, true)
		counts = binary.AppendUvarint(counts, uint64(n)+1)
		for j := range n {
			values = enc.append(values, leafAt(c.typ, cells[0].val, j))
		}
		cells = cells[1:]
	}
	payload := make([]byte, 0, 4+len(counts)+len(values))
	payload = binary.BigEndian.AppendUint32(payload, uint32(hi-lo))
	payload = append(append(payload, counts...), values...)
	return zstdEncoder.EncodeAll(payload, []byte{chunkVersion, byte(c.typ), enc.tag})
}

// parallel runs fn(i) for every i in [0, n) on at most GOMAXPROCS goroutines; fn writes
// only what is its own i's, so the result does not depend on the schedule.
func parallel(n int, fn func(i int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(n, runtime.GOMAXPROCS(0)) {
		wg.Go(func() {
			for i := int(next.Add(1)) - 1; i < n; i = int(next.Add(1)) - 1 {
				fn(i)
			}
		})
	}
	wg.Wait()
}

// chunkTag deltas numbers and timestamps, and dictionaries the counted columns, whose
// values are few.
func chunkTag(c column) byte {
	switch c.typ.Scalar() {
	case value.TypeInt, value.TypeFloat, value.TypeBool, value.TypeTimestamp:
		return chunkTagDelta
	case value.TypeString:
		if c.counted {
			return chunkTagDict
		}
	}
	return chunkTagRaw
}

type valueEncoder struct {
	tag  byte
	typ  value.Type
	prev int64
	ids  map[string]uint64
}

func (e *valueEncoder) append(buf []byte, v value.Value) []byte {
	switch e.tag {
	case chunkTagDelta:
		x := toInt64(e.typ, v)
		buf = binary.AppendUvarint(buf, zigzagEncode(x-e.prev))
		e.prev = x
		return buf
	case chunkTagDict:
		if id, ok := e.ids[v.String]; ok {
			return binary.AppendUvarint(buf, id)
		}
		if e.ids == nil {
			e.ids = make(map[string]uint64)
		}
		e.ids[v.String] = uint64(len(e.ids))
		buf = binary.AppendUvarint(buf, e.ids[v.String])
	}
	if e.typ.Scalar() == value.TypeJSON {
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v.Bytes)))
		return append(buf, v.Bytes...)
	}
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(v.String)))
	return append(buf, v.String...)
}

// toInt64 maps a delta-encoded value onto the int64 the chunk stores, and fromInt64
// maps it back.
func toInt64(typ value.Type, v value.Value) int64 {
	switch typ.Scalar() {
	case value.TypeFloat:
		return int64(math.Float64bits(v.Float))
	case value.TypeBool:
		if v.Bool {
			return 1
		}
		return 0
	case value.TypeTimestamp:
		return v.Time.UnixNano()
	default:
		return v.Int
	}
}

func fromInt64(typ value.Type, x int64) value.Value {
	switch typ.Scalar() {
	case value.TypeFloat:
		return value.Value{Float: math.Float64frombits(uint64(x))}
	case value.TypeBool:
		return value.Value{Bool: x != 0}
	case value.TypeTimestamp:
		return value.Value{Time: time.Unix(0, x).UTC()}
	default:
		return value.Value{Int: x}
	}
}

func truncatePatternText(text string) string {
	if len(text) <= 512 {
		return text
	}
	end := 509
	for !utf8.RuneStart(text[end]) {
		end--
	}
	return text[:end] + "…"
}

// buildFooter computes every footer index over t's rows, whose column texts are texts,
// and each filtered column's filter section, leaving the row groups and the sections' offsets to the caller.
func buildFooter(cols []column, t *table, texts []columnTexts, zm ZoneMaps, patterns map[string]string) (Footer, map[string][]byte, error) {
	f := Footer{
		Rows:           len(t.ts),
		ZoneMaps:       zm,
		Patterns:       NewPresentSet(),
		patternTexts:   make(map[string]string),
		DistinctCounts: make(map[string]*sketch.HLL),
		Distributions:  make(map[string]*sketch.DDSketch),
	}
	filters := map[string][]byte{}

	ids := t.text(ColPatternID)
	for _, id := range ids {
		if id != "" {
			f.Patterns.Add(id)
		}
	}

	for id := range f.Patterns {
		f.patternTexts[id] = truncatePatternText(patterns[id])
	}

	f.valueCounts = map[string]map[string]uint64{}
	indexes := make([]columnIndex, len(cols))
	parallel(len(cols), func(i int) { indexes[i] = indexColumn(cols[i], texts[i].texts) })
	for i, ix := range indexes {
		if ix.err != nil {
			return Footer{}, nil, ix.err
		}
		maps.Copy(f.DistinctCounts, ix.sketches.DistinctCounts)
		maps.Copy(f.Distributions, ix.sketches.Distributions)
		if ix.filter != nil {
			filters[cols[i].name] = ix.filter
		}
		if ix.counts != nil {
			f.valueCounts[cols[i].name], cols[i].counted = ix.counts, true
		}
	}
	f.services, f.ServicePatternCounts = serviceViews(cols, texts, t.text(ColService), ids, f.valueCounts)
	return f, filters, nil
}

// columnIndex is what the footer keeps of one column: its sketches, filter section and
// value counts, built apart from every other column's.
type columnIndex struct {
	sketches Footer
	filter   []byte
	counts   map[string]uint64
	err      error
}

func indexColumn(c column, vals []string) columnIndex {
	ix := columnIndex{sketches: Footer{DistinctCounts: map[string]*sketch.HLL{}, Distributions: map[string]*sketch.DDSketch{}}}
	buildColumnIndex(&ix.sketches, c, vals)
	ix.filter, ix.err = filterSection(c, vals)
	// Counts of a column with fewer than two rows per value, an id, would copy the
	// column into the footer and group nothing.
	if c.typ == value.TypeString {
		ix.counts = countValues(vals, len(vals)/2)
	}
	return ix
}

// serviceViews splits by the rows' services the summaries a reader keeps per service:
// each one's sketches, value counts and rows of each of ids, the rows' pattern_ids.
func serviceViews(cols []column, texts []columnTexts, services, ids []string, whole map[string]map[string]uint64) (map[string]Footer, map[ServicePattern]int) {
	views, counts := map[string]Footer{}, map[ServicePattern]int{}
	if !slices.ContainsFunc(cols, func(c column) bool { return c.name == ColService }) {
		return views, counts
	}
	byService := map[string][]int{}
	for r, service := range services {
		if service == "" {
			continue
		}
		byService[service] = append(byService[service], r)
		if id := ids[r]; id != "" {
			counts[ServicePattern{Service: service, Pattern: id}]++
		}
	}
	names := slices.Sorted(maps.Keys(byService))
	built := make([]serviceFooter, len(names))
	parallel(len(names), func(i int) { built[i] = serviceView(cols, texts, byService[names[i]]) })
	for i, service := range names {
		views[service] = built[i].view
		for _, name := range built[i].uncountable {
			delete(whole, name)
		}
	}
	return views, counts
}

// serviceFooter is one service's view and the counted columns whose values in its rows
// are too many to count, which the segment's footer then stops counting.
type serviceFooter struct {
	view        Footer
	uncountable []string
}

// serviceView is one service's row count, a distribution per number column, its
// correlation sketches and the value counts of each counted column it holds, dropping from
// whole a column whose values in these rows are too many to count.
func serviceView(cols []column, texts []columnTexts, rows []int) serviceFooter {
	v := Footer{Rows: len(rows), DistinctCounts: map[string]*sketch.HLL{}, Distributions: map[string]*sketch.DDSketch{},
		valueCounts: map[string]map[string]uint64{}, fields: map[string]FieldStats{}}
	var uncountable []string
	for ci, c := range cols {
		vals := texts[ci].rows(rows)
		if c.counted && len(vals) > 0 {
			if counts := countValues(vals, maxCountedValues); counts != nil {
				v.valueCounts[c.name] = counts
			} else {
				uncountable = append(uncountable, c.name)
			}
		}
		var id bool
		if c.typ == value.TypeString {
			keys, rows := keysOf(nil, vals)
			id = idByValues(c.name, keys, rows)
		}
		IndexServiceColumn(&v, c.name, c.typ, id, vals)
		// A row's time is its own, distinct on nearly every row, and ranks no field.
		if stats, ok := fieldStats(c.typ, vals, v.DistinctCounts[c.name]); ok && c.name != ColTimestamp {
			v.fields[c.name] = stats
		}
	}
	v.valuePairs = valuePairs(cols, texts, rows, v.valueCounts)
	return serviceFooter{view: v, uncountable: uncountable}
}

// fieldStats is a column's stats over vals, one service's values of it, and false when they
// hold none; distinct is the view's sketch of the same values when it keeps one, which the
// stats then share rather than hold twice.
func fieldStats(typ value.Type, vals []string, distinct *sketch.HLL) (FieldStats, bool) {
	stats := FieldStats{Type: typ, Distinct: distinct}
	if distinct == nil {
		stats.Distinct = sketch.NewHLL()
	}
	for _, v := range vals {
		if v == "" {
			continue
		}
		stats.Values++
		if distinct == nil {
			stats.Distinct.Add([]byte(v))
		}
		stats.Text = stats.Text || strings.Contains(v, " ")
	}
	return stats, stats.Values > 0
}

// maxPairColumns bounds the columns a service's value pairs are drawn from, as each row
// costs a count per two of them.
const maxPairColumns = 12

// valuePairs counts the rows holding each pair of values of two of the counted string
// columns that may name an endpoint, leaving out a column pair past maxCountedValues pairs.
func valuePairs(cols []column, texts []columnTexts, rows []int, counted map[string]map[string]uint64) map[ValuePair]uint64 {
	var pairCols []int
	for ci, c := range cols {
		if c.typ == value.TypeString && counted[c.name] != nil && EndpointColumn(c.name) && len(pairCols) < maxPairColumns {
			pairCols = append(pairCols, ci)
		}
	}
	if len(pairCols) < 2 {
		return nil
	}
	byColumns := map[[2]string]map[[2]string]uint64{}
	values := make([]string, len(pairCols))
	for _, r := range rows {
		for i, ci := range pairCols {
			values[i] = endpointText(texts[ci].row(r))
		}
		countRowPairs(byColumns, cols, pairCols, values)
	}
	var pairs map[ValuePair]uint64
	for names, counts := range byColumns {
		if len(counts) > maxCountedValues {
			continue
		}
		if pairs == nil {
			pairs = map[ValuePair]uint64{}
		}
		for vals, n := range counts {
			pairs[ValuePair{Columns: names, Values: vals}] = n
		}
	}
	return pairs
}

// countRowPairs counts one row's values, those of pairCols, two at a time, names sorted
// as cols are.
func countRowPairs(byColumns map[[2]string]map[[2]string]uint64, cols []column, pairCols []int, values []string) {
	for i, a := range values {
		if a == "" {
			continue
		}
		for j := i + 1; j < len(values); j++ {
			if values[j] == "" {
				continue
			}
			names := [2]string{cols[pairCols[i]].name, cols[pairCols[j]].name}
			if byColumns[names] == nil {
				byColumns[names] = map[[2]string]uint64{}
			}
			byColumns[names][[2]string{a, values[j]}]++
		}
	}
}

// EndpointColumn reports whether a column may name one end of a call: not one naming the
// line's own producer, its text or level, nor an id.
func EndpointColumn(name string) bool {
	switch name {
	case ColService, ColHost, ColPod, ColNamespace, ColContainer, ColMessage, ColLevel, ColPatternID, ColTimestamp:
		return false
	}
	return !IsID(name) && !IsCorrelation(name) && !strings.HasSuffix(name, PatternSuffix)
}

// endpointText is a row's one value of a column, empty for a list or a value holding a
// space, which no endpoint name does.
func endpointText(leaves []string) string {
	if len(leaves) != 1 || strings.ContainsAny(leaves[0], " \t") {
		return ""
	}
	return leaves[0]
}

// IndexServiceColumn folds correlation sketches and numeric distributions into a service view,
// a correlation sketch for a column named as one or, where id is true, an id by its values.
func IndexServiceColumn(v *Footer, name string, typ value.Type, id bool, vals []string) {
	numeric := typ.Numeric()
	correlation := (typ.Scalar() == value.TypeString || typ.Scalar() == value.TypeInt) && (id || IsCorrelation(name))
	for _, val := range vals {
		if correlation && val != "" {
			addDistinctValue(v, name, val)
		}
		if numeric && val != "" {
			numeric = addQuantileValue(v, name, val)
		}
	}
}

// IsCorrelation reports whether a column's values each gather one unit of work: an id, or
// a word naming one.
func IsCorrelation(name string) bool {
	switch strings.ToLower(name[strings.LastIndex(name, ".")+1:]) {
	case "trace", "session", "transaction", "correlation", "conversation", "task", "job", "run":
		return true
	}
	return IsID(name)
}

// IsID reports whether a column other than pattern_id names one thing by its id, so
// equality on it is a needle lookup.
func IsID(name string) bool {
	if name == ColPatternID {
		return false
	}
	leaf := name[strings.LastIndex(name, ".")+1:]
	if leaf == "id" || strings.HasSuffix(leaf, "_id") || strings.HasSuffix(leaf, "-id") {
		return true
	}
	if len(leaf) < 3 || !strings.HasSuffix(leaf, "Id") {
		return false
	}
	c := leaf[len(leaf)-3]
	return strings.IndexByte("abcdefghijklmnopqrstuvwxyz0123456789", c) >= 0
}

// countValues counts each of vals, nil past limit distinct ones.
func countValues(vals []string, limit int) map[string]uint64 {
	counts := map[string]uint64{}
	for _, v := range vals {
		if counts[v]++; len(counts) > limit {
			return nil
		}
	}
	return counts
}

// buildColumnIndex folds one column's values, a list's elements one by one, into
// whichever sketches its identity calls for (distinct, quantile).
func buildColumnIndex(f *Footer, c column, vals []string) {
	numeric := c.typ.Numeric()
	for _, v := range vals {
		if v != "" {
			addDistinctValue(f, c.name, v)
		}
		if numeric && v != "" {
			numeric = addQuantileValue(f, c.name, v)
		}
	}
}

// minIDRows is the fewest rows a string column is on to be an id by its values, and
// minIDValues the fewest distinct values it holds, as a few lines of an enum (an event's
// reason, a level) hold a value each yet name no unit of work.
const (
	minIDRows   = 8
	minIDValues = 8
)

// idByValues reports whether string column name, whose distinct values' hashes are keys, nil if one
// holds a space, over the rows rows holding one, is an id by its values whatever its name
// other than pattern_id: it is on at least minIDRows rows and its distinct values are at
// least a quarter of them, so each gathers a handful of rows, as a request's id does, and
// at least minIDValues of them.
func idByValues(name string, keys *hashSet, rows int) bool {
	return name != ColPatternID && keys != nil && IDByValues(len(keys.distinct()), rows)
}

// IDByValues reports whether a string column with distinct values over the rows holding
// one is an id by its values, as the writer judges a column it gives a membership filter.
func IDByValues(distinct, rows int) bool {
	return rows >= minIDRows && distinct >= minIDValues && 4*distinct >= rows
}

// filterSection is the serialized membership filter over a string column, nil for any other.
func filterSection(c column, vals []string) ([]byte, error) {
	if c.typ != value.TypeString {
		return nil, nil
	}
	keys, rows := keysOf(nil, vals)
	return keysFilter(c.name, keys, rows)
}

// keysFilter is the serialized membership filter over keys, the hashes of the distinct values
// of string column name over the rows rows holding one, nil unless the column is an id by its
// values or has too many values to count; nil keys, free text whose values hold spaces, gets none.
func keysFilter(name string, keys *hashSet, rows int) ([]byte, error) {
	if keys == nil || len(keys.distinct()) <= maxCountedValues && !idByValues(name, keys, rows) {
		return nil, nil
	}
	xf, err := sketch.NewXorFilterOfHashes(keys.distinct())
	if err != nil {
		return nil, fmt.Errorf("segment: build filter for %q: %w", name, err)
	}
	return xf.MarshalBinary()
}

// addDistinctValue folds one non-empty value into the column's distinct-count sketch,
// which every column gets because rank_fields needs cardinality
// for any column; empty strings are skipped so they don't bias estimates.
func addDistinctValue(f *Footer, name, v string) {
	hll := f.DistinctCounts[name]
	if hll == nil {
		hll = sketch.NewHLL()
		f.DistinctCounts[name] = hll
	}
	hll.Add([]byte(v))
}

// addQuantileValue folds one value into the column's quantile sketch, reporting
// whether the column is still numeric. ParseFloat accepts "inf"/"nan" (err==nil)
// but DDSketch cannot represent them, so such a column drops its sketch rather
// than silently omit those rows and under-report the tail.
func addQuantileValue(f *Footer, name, v string) bool {
	n, err := strconv.ParseFloat(v, 64)
	if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
		delete(f.Distributions, name)
		return false
	}
	dd := f.Distributions[name]
	if dd == nil {
		dd = sketch.NewDDSketch(sketch.QuantileAccuracy)
		f.Distributions[name] = dd
	}
	dd.Add(n)
	return true
}

// MarshalBinary encodes the footer as a segment holds it, before compression.
func (f Footer) MarshalBinary() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte(footerVersion)
	putU32(&buf, uint32(f.RowGroups))
	putU32(&buf, uint32(f.Rows))

	putZoneMaps(&buf, f.ZoneMaps)
	putU32(&buf, uint32(len(f.RowGroupZoneMaps)))
	for _, zm := range f.RowGroupZoneMaps {
		putZoneMaps(&buf, zm)
	}

	// Pattern present-set.
	tmpls := slices.Sorted(maps.Keys(f.Patterns))
	putU32(&buf, uint32(len(tmpls)))
	for _, t := range tmpls {
		putStr(&buf, t)
	}

	for _, id := range tmpls {
		putStr(&buf, f.patternTexts[id])
	}

	putStr(&buf, f.PatternProfile.Name)
	putU32(&buf, uint32(f.PatternProfile.Version))
	putU32(&buf, uint32(len(f.PatternProfile.Components)))
	for _, c := range f.PatternProfile.Components {
		putStr(&buf, c)
	}
	buf.WriteByte(byte(f.PatternProfile.Degradation))

	putFilterRanges(&buf, f.filters)

	// Sketches, one length-prefixed section per type.
	if err := putSketchMap(&buf, slices.Sorted(maps.Keys(f.DistinctCounts)), func(c string) ([]byte, error) {
		return f.DistinctCounts[c].MarshalBinary()
	}); err != nil {
		return nil, err
	}
	if err := putSketchMap(&buf, slices.Sorted(maps.Keys(f.Distributions)), func(c string) ([]byte, error) {
		return f.Distributions[c].MarshalBinary()
	}); err != nil {
		return nil, err
	}
	putValueCounts(&buf, f.valueCounts)

	putOffsets(&buf, f.Offsets)
	if err := putServiceViews(&buf, f, tmpls); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// putFilterRanges writes each filtered column's filter section range, sorted by column.
func putFilterRanges(buf *bytes.Buffer, filters map[string]ByteRange) {
	putU32(buf, uint32(len(filters)))
	for _, c := range slices.Sorted(maps.Keys(filters)) {
		putStr(buf, c)
		putU64(buf, uint64(filters[c].Offset))
		putU64(buf, uint64(filters[c].Length))
	}
}

// putOffsets writes the chunk offset table, sorted by row group then column.
func putOffsets(buf *bytes.Buffer, offsets OffsetTable) {
	keys := slices.SortedFunc(maps.Keys(offsets), func(a, b ChunkKey) int {
		return cmp.Or(cmp.Compare(a.RowGroup, b.RowGroup), cmp.Compare(a.Column, b.Column))
	})
	putU32(buf, uint32(len(keys)))
	for _, k := range keys {
		br := offsets[k]
		putU32(buf, uint32(k.RowGroup))
		putStr(buf, k.Column)
		putU64(buf, uint64(br.Offset))
		putU64(buf, uint64(br.Length))
	}
}

// putServiceViews writes each service's view and pattern counts, naming a pattern by its
// index in tmpls, the footer's sorted pattern_ids, since a service's are a subset of them.
func putServiceViews(buf *bytes.Buffer, f Footer, tmpls []string) error {
	index := make(map[string]int, len(tmpls))
	for i, t := range tmpls {
		index[t] = i
	}
	ids := map[string][]string{}
	for k := range f.ServicePatternCounts {
		ids[k.Service] = append(ids[k.Service], k.Pattern)
	}
	services := f.Services()
	putU32(buf, uint32(len(services)))
	for _, service := range services {
		v := f.services[service]
		putStr(buf, service)
		putU32(buf, uint32(v.Rows))
		slices.Sort(ids[service])
		putU32(buf, uint32(len(ids[service])))
		for _, id := range ids[service] {
			putU32(buf, uint32(index[id]))
			putU32(buf, uint32(f.ServicePatternCounts[ServicePattern{Service: service, Pattern: id}]))
		}
		if err := putSketchMap(buf, slices.Sorted(maps.Keys(v.DistinctCounts)), func(c string) ([]byte, error) {
			return v.DistinctCounts[c].MarshalBinary()
		}); err != nil {
			return err
		}
		if err := putSketchMap(buf, slices.Sorted(maps.Keys(v.Distributions)), func(c string) ([]byte, error) {
			return v.Distributions[c].MarshalBinary()
		}); err != nil {
			return err
		}
		putValueCounts(buf, v.valueCounts)
		putValuePairs(buf, v.valuePairs)
		if err := putFieldStats(buf, v.fields, v.DistinctCounts); err != nil {
			return err
		}
	}
	return nil
}

// putFieldStats writes a view's field stats sorted by column, a sketch the view's distinct
// counts already hold as none.
func putFieldStats(buf *bytes.Buffer, fields map[string]FieldStats, shared map[string]*sketch.HLL) error {
	putU32(buf, uint32(len(fields)))
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		s := fields[name]
		var distinct []byte
		if shared[name] == nil {
			var err error
			if distinct, err = s.Distinct.MarshalBinary(); err != nil {
				return fmt.Errorf("segment: encode field stats of %s: %w", name, err)
			}
		}
		putStr(buf, name)
		buf.WriteByte(byte(s.Type))
		putU32(buf, uint32(s.Values))
		text := byte(0)
		if s.Text {
			text = 1
		}
		buf.WriteByte(text)
		putBytes(buf, distinct)
	}
	return nil
}

// readFieldStats reads what putFieldStats wrote, a sketch it wrote as none the one shared
// holds, adding the sketches as held to size.
func readFieldStats(r *cursor, shared map[string]*sketch.HLL, size *int64) (map[string]FieldStats, error) {
	fields := map[string]FieldStats{}
	for n := r.count(); n > 0; n-- {
		name := r.str()
		s := FieldStats{Type: value.Type(r.byte()), Values: int(r.u32()), Text: r.byte() == 1, Distinct: sketch.NewHLL()}
		data := r.bytes()
		if r.err != nil {
			return nil, r.err
		}
		if len(data) == 0 && shared[name] != nil {
			s.Distinct = shared[name]
			fields[name] = s
			continue
		}
		if err := s.Distinct.UnmarshalBinary(data); err != nil {
			return nil, fmt.Errorf("segment: decode field stats of %s: %w", name, err)
		}
		*size += s.Distinct.Size() - int64(len(data))
		fields[name] = s
	}
	return fields, nil
}

// putValuePairs writes each value pair's count, sorted by columns then values.
func putValuePairs(buf *bytes.Buffer, pairs map[ValuePair]uint64) {
	keys := slices.SortedFunc(maps.Keys(pairs), func(a, b ValuePair) int {
		return cmp.Or(cmp.Compare(a.Columns[0], b.Columns[0]), cmp.Compare(a.Columns[1], b.Columns[1]),
			cmp.Compare(a.Values[0], b.Values[0]), cmp.Compare(a.Values[1], b.Values[1]))
	})
	putU32(buf, uint32(len(keys)))
	for _, k := range keys {
		putStr(buf, k.Columns[0])
		putStr(buf, k.Columns[1])
		putStr(buf, k.Values[0])
		putStr(buf, k.Values[1])
		putU64(buf, pairs[k])
	}
}

// readValuePairs reads what putValuePairs wrote, nil for none as valuePairs builds.
func readValuePairs(r *cursor) map[ValuePair]uint64 {
	n := r.count()
	if n == 0 {
		return nil
	}
	pairs := make(map[ValuePair]uint64, n)
	for ; n > 0; n-- {
		k := ValuePair{Columns: [2]string{r.str(), r.str()}, Values: [2]string{r.str(), r.str()}}
		pairs[k] = r.u64()
	}
	return pairs
}

// putValueCounts writes each counted column's counts, sorted by value.
func putValueCounts(buf *bytes.Buffer, counts map[string]map[string]uint64) {
	putU32(buf, uint32(len(counts)))
	for _, c := range slices.Sorted(maps.Keys(counts)) {
		putStr(buf, c)
		putU32(buf, uint32(len(counts[c])))
		for _, v := range slices.Sorted(maps.Keys(counts[c])) {
			putStr(buf, v)
			putU64(buf, counts[c][v])
		}
	}
}

func readValueCounts(r *cursor) map[string]map[string]uint64 {
	counts := map[string]map[string]uint64{}
	for n := r.count(); n > 0; n-- {
		c := r.str()
		counts[c] = map[string]uint64{}
		for m := r.count(); m > 0; m-- {
			counts[c][r.str()] = r.u64()
		}
	}
	return counts
}

// readFilterRanges reads what putFilterRanges wrote.
func readFilterRanges(r *cursor) map[string]ByteRange {
	filters := map[string]ByteRange{}
	for n := r.count(); n > 0; n-- {
		filters[r.str()] = ByteRange{Offset: int64(r.u64()), Length: int64(r.u64())}
	}
	return filters
}

// readOffsets reads what putOffsets wrote.
func readOffsets(r *cursor, offsets OffsetTable) {
	for n := r.count(); n > 0; n-- {
		rowGroup, column := int(r.u32()), r.str()
		offsets.Set(rowGroup, column, ByteRange{Offset: int64(r.u64()), Length: int64(r.u64())})
	}
}

// decodeFooter parses the footer payload (without the trailer).
func decodeFooter(b []byte) (Footer, error) {
	// A footer repeats its columns and values across views and pairs, so each is held once.
	r := &cursor{b: b, strs: map[string]string{}}
	version := r.byte()
	if version != footerVersion && version != fieldsVersion-1 {
		return Footer{}, &VersionError{Object: "footer", Version: version}
	}
	f := Footer{
		size:           int64(len(b)),
		RowGroups:      int(r.u32()),
		Rows:           int(r.u32()),
		Patterns:       NewPresentSet(),
		patternTexts:   make(map[string]string),
		DistinctCounts: make(map[string]*sketch.HLL),
		Distributions:  make(map[string]*sketch.DDSketch),
		Offsets:        make(OffsetTable),
	}

	var err error
	if f.ZoneMaps, err = readZoneMaps(r); err != nil {
		return Footer{}, err
	}
	for n := r.count(); n > 0; n-- {
		zm, err := readZoneMaps(r)
		if err != nil {
			return Footer{}, err
		}
		if f.RowGroups == 1 { // a lone row group's zone maps are the segment's, so share them
			zm = f.ZoneMaps
		}
		f.RowGroupZoneMaps = append(f.RowGroupZoneMaps, zm)
	}

	var tmpls []string
	for n := r.count(); n > 0; n-- {
		tmpls = append(tmpls, r.str())
		f.Patterns.Add(tmpls[len(tmpls)-1])
	}

	for _, id := range tmpls {
		f.patternTexts[id] = r.str()
	}

	f.PatternProfile.Name = r.str()
	f.PatternProfile.Version = int(r.u32())
	for n := r.count(); n > 0; n-- {
		f.PatternProfile.Components = append(f.PatternProfile.Components, r.str())
	}
	f.PatternProfile.Degradation = int(r.byte())

	f.filters = readFilterRanges(r)

	if err := decodeDistinctCounts(r, f.DistinctCounts, &f.size); err != nil {
		return Footer{}, err
	}
	if err := decodeSketchMap(r, f.Distributions, func() *sketch.DDSketch { return &sketch.DDSketch{} }); err != nil {
		return Footer{}, err
	}
	f.valueCounts = readValueCounts(r)

	readOffsets(r, f.Offsets)

	if f.services, f.ServicePatternCounts, err = readServiceViews(r, tmpls, version >= fieldsVersion, &f.size); err != nil {
		return Footer{}, err
	}
	if r.err != nil {
		return Footer{}, r.err
	}
	return f, validateFooter(f)
}

func readServiceViews(r *cursor, tmpls []string, withFields bool, size *int64) (map[string]Footer, map[ServicePattern]int, error) {
	views, counts := map[string]Footer{}, map[ServicePattern]int{}
	for n := r.count(); n > 0; n-- {
		service := r.str()
		v := Footer{Rows: int(r.u32()), DistinctCounts: map[string]*sketch.HLL{}, Distributions: map[string]*sketch.DDSketch{}}
		for m := r.count(); m > 0; m-- {
			i, count := int(r.u32()), int(r.u32())
			if i >= len(tmpls) {
				return nil, nil, fmt.Errorf("segment: service %q names pattern %d of %d", service, i, len(tmpls))
			}
			counts[ServicePattern{Service: service, Pattern: tmpls[i]}] = count
		}
		if err := decodeDistinctCounts(r, v.DistinctCounts, size); err != nil {
			return nil, nil, err
		}
		if err := decodeSketchMap(r, v.Distributions, func() *sketch.DDSketch { return &sketch.DDSketch{} }); err != nil {
			return nil, nil, err
		}
		v.valueCounts = readValueCounts(r)
		v.valuePairs = readValuePairs(r)
		if withFields {
			fields, err := readFieldStats(r, v.DistinctCounts, size)
			if err != nil {
				return nil, nil, err
			}
			v.fields = fields
		}
		views[service] = v
	}
	return views, counts, nil
}

// putZoneMaps writes zone maps sorted by column for determinism, each bound in its
// scalar's canonical text since a list column's bounds are over its elements.
func putZoneMaps(buf *bytes.Buffer, zms ZoneMaps) {
	cols := slices.Sorted(maps.Keys(zms))
	putU32(buf, uint32(len(cols)))
	for _, c := range cols {
		zm := zms[c]
		putStr(buf, c)
		buf.WriteByte(byte(zm.Type))
		putStr(buf, value.Format(zm.Type.Scalar(), zm.Min))
		putStr(buf, value.Format(zm.Type.Scalar(), zm.Max))
		putU32(buf, uint32(zm.PresentRows))
	}
}

func readZoneMaps(r *cursor) (ZoneMaps, error) {
	zms := make(ZoneMaps)
	for n := r.count(); n > 0; n-- {
		col := r.str()
		typ := value.Type(r.byte())
		min, err := value.Parse(typ, r.str())
		if err != nil {
			return nil, fmt.Errorf("segment: decode zone map %q min: %w", col, err)
		}
		max, err := value.Parse(typ, r.str())
		if err != nil {
			return nil, fmt.Errorf("segment: decode zone map %q max: %w", col, err)
		}
		zms[col] = ZoneMap{Type: typ, Min: min, Max: max, PresentRows: int(r.u32())}
	}
	return zms, nil
}

// validateFooter rejects a corrupt footer at decode, so every Footer a Reader returns
// has a row group and only in-range chunk offsets.
func validateFooter(f Footer) error {
	if f.RowGroups < 1 {
		return fmt.Errorf("segment: footer has %d row groups", f.RowGroups)
	}
	for k, br := range f.Offsets {
		if k.RowGroup < 0 || k.RowGroup >= f.RowGroups || br.Offset < 0 || br.Length < 0 {
			return fmt.Errorf("segment: invalid chunk range %d+%d for column %q row group %d", br.Offset, br.Length, k.Column, k.RowGroup)
		}
	}
	for column, br := range f.filters {
		if br.Offset < 0 || br.Length < 0 {
			return fmt.Errorf("segment: invalid filter range %d+%d for column %q", br.Offset, br.Length, column)
		}
	}
	return nil
}

// decodeDistinctCounts reads a distinct-count sketch section into dst and moves size from
// the section's encoded bytes to the bytes its sketches hold decoded.
func decodeDistinctCounts(r *cursor, dst map[string]*sketch.HLL, size *int64) error {
	start := r.off
	if err := decodeSketchMap(r, dst, sketch.NewHLL); err != nil {
		return err
	}
	*size -= int64(r.off - start)
	for _, h := range dst {
		*size += h.Size()
	}
	return nil
}

// decodeSketchMap reads one length-prefixed sketch section into dst. newSketch
// supplies the zero value to unmarshal into, which is the only thing that differs
// between the three sections.
func decodeSketchMap[S encoding.BinaryUnmarshaler](r *cursor, dst map[string]S, newSketch func() S) error {
	for n := r.count(); n > 0; n-- {
		col := r.str()
		data := r.bytes()
		if r.err != nil {
			return r.err
		}
		s := newSketch()
		if err := s.UnmarshalBinary(data); err != nil {
			return err
		}
		dst[col] = s
	}
	return nil
}

func putSketchMap(buf *bytes.Buffer, cols []string, marshal func(string) ([]byte, error)) error {
	putU32(buf, uint32(len(cols)))
	for _, c := range cols {
		b, err := marshal(c)
		if err != nil {
			return err
		}
		putStr(buf, c)
		putBytes(buf, b)
	}
	return nil
}

// --- little-endian-free write helpers over a bytes.Buffer ---

func putU32(buf *bytes.Buffer, v uint32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], v)
	buf.Write(tmp[:])
}

func putU64(buf *bytes.Buffer, v uint64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], v)
	buf.Write(tmp[:])
}

func putBytes(buf *bytes.Buffer, b []byte) {
	putU32(buf, uint32(len(b)))
	buf.Write(b)
}

func putStr(buf *bytes.Buffer, s string) {
	putU32(buf, uint32(len(s)))
	buf.WriteString(s)
}

// cursor is a bounds-checked, error-latching reader over a byte slice.
type cursor struct {
	b   []byte
	off int
	err error
	// strs, when set, is every string str has returned, so a repeated one is not copied again.
	strs map[string]string
}

func (c *cursor) byte() byte {
	if c.err != nil || c.off+1 > len(c.b) {
		c.fail()
		return 0
	}
	v := c.b[c.off]
	c.off++
	return v
}

func (c *cursor) u32() uint32 {
	if c.err != nil || c.off+4 > len(c.b) {
		c.fail()
		return 0
	}
	v := binary.BigEndian.Uint32(c.b[c.off:])
	c.off += 4
	return v
}

func (c *cursor) u64() uint64 {
	if c.err != nil || c.off+8 > len(c.b) {
		c.fail()
		return 0
	}
	v := binary.BigEndian.Uint64(c.b[c.off:])
	c.off += 8
	return v
}

// count reads a section element-count and bounds it against the bytes that
// remain: every element consumes at least one byte, so a legitimate count can
// never exceed the remaining length. This stops a corrupt count (e.g. ~4e9) from
// spinning a decode loop billions of times before the inner reads finally latch
// an error.
func (c *cursor) count() int {
	n := int(c.u32())
	if c.err != nil || n < 0 || n > len(c.b)-c.off {
		c.fail()
		return 0
	}
	return n
}

func (c *cursor) bytes() []byte {
	n := int(c.u32())
	if c.err != nil || n < 0 || c.off+n > len(c.b) {
		c.fail()
		return nil
	}
	v := c.b[c.off : c.off+n]
	c.off += n
	return v
}

func (c *cursor) str() string {
	b := c.bytes()
	if c.strs == nil {
		return string(b)
	}
	if s, ok := c.strs[string(b)]; ok {
		return s
	}
	s := string(b)
	c.strs[s] = s
	return s
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.off:])
	if n <= 0 {
		c.fail()
		return 0
	}
	c.off += n
	return v
}

func (c *cursor) fail() {
	if c.err == nil {
		c.err = errors.New("segment: malformed or truncated segment bytes")
	}
}

func zigzagEncode(v int64) uint64 { return uint64((v << 1) ^ (v >> 63)) }
func zigzagDecode(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }
