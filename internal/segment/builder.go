package segment

import (
	"cmp"
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/datafocal/datafocal/internal/value"
)

type BuilderConfig struct {
	Tenant    string
	Stream    Stream
	CreatedAt time.Time
}

type Builder struct {
	cfg      BuilderConfig
	rows     table
	stats    Stats
	patterns map[string]string
}

func NewBuilder(cfg BuilderConfig) *Builder {
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = time.Now()
	}
	return &Builder{
		cfg:      cfg,
		patterns: make(map[string]string),
		stats: Stats{
			CreatedAt: cfg.CreatedAt,
		},
	}
}

// Append adds service and pattern_id as conventions rather than fields, so an empty one
// adds no column, sparing every metric and span columns of "".
func (b *Builder) Append(record Record, parsed ParsedPattern) error {
	if record.Timestamp.IsZero() {
		return errors.New("record timestamp is required")
	}
	b.rows.ts = append(b.rows.ts, record.Timestamp)
	// First, so a row's own service, such as a per-host collector's container, wins.
	if b.cfg.Stream.Service != "" {
		b.stats.EstimatedBytes += b.rows.set(stringColumn(ColService, b.cfg.Stream.Service))
	}
	for _, col := range record.Columns {
		b.stats.EstimatedBytes += b.rows.set(col)
	}
	for _, col := range parsed.Columns {
		b.stats.EstimatedBytes += b.rows.set(col)
	}
	if parsed.PatternID != "" {
		b.stats.EstimatedBytes += b.rows.set(stringColumn(ColPatternID, parsed.PatternID))
	}
	b.patterns[parsed.PatternID] = parsed.Pattern
	b.stats.Rows++
	b.stats.TimeRange.Add(record.Timestamp)
	return nil
}

func (b *Builder) Stats() Stats {
	return b.stats
}

// Seal is idempotent, so a batch whose flush failed can be sealed again.
func (b *Builder) Seal() Sealed {
	t := &b.rows
	t.finish(nil, true)
	services := map[string]bool{}
	for _, s := range t.text(ColService) {
		if s != "" {
			services[s] = true
		}
	}
	descriptor := b.descriptor(t.zoneMaps(0, len(t.ts)), slices.Sorted(maps.Keys(services)))
	return Sealed{Descriptor: descriptor, table: t, patterns: b.patterns}
}

// descriptor describes the rows appended, whose zone maps are zm and services services.
func (b *Builder) descriptor(zm ZoneMaps, services []string) Descriptor {
	return Descriptor{
		ID:            NewID(b.cfg.Tenant, b.cfg.Stream, b.stats.TimeRange, b.stats.Rows, b.cfg.CreatedAt),
		Stream:        b.cfg.Stream,
		TimeRange:     b.stats.TimeRange,
		Rows:          b.stats.Rows,
		PatternIDs:    slices.Sorted(maps.Keys(b.patterns)),
		Services:      services,
		ZoneMaps:      zm,
		CreatedAt:     b.cfg.CreatedAt,
		EstimatedSize: b.stats.EstimatedBytes,
	}
}

// table holds rows a column at a time, each column's cells in row order, so sealing reads
// a column without visiting every row and no row's columns are ever sorted.
type table struct {
	ts      []time.Time
	cols    map[string][]cell
	scratch []byte
}

// cell is one row's value of a column, in its own type until finish gives it the column's.
type cell struct {
	row int32
	typ value.Type
	val value.Value
}

// tableOf holds records as a Builder does, without its conventional columns or ordering.
func tableOf(records []Record) *table {
	t := &table{}
	for _, r := range records {
		t.ts = append(t.ts, r.Timestamp)
		for _, col := range r.Columns {
			t.set(col)
		}
	}
	t.finish(nil, false)
	return t
}

// set gives the last row col, a later column overriding an earlier one of its name, and
// returns how much that grows the payload estimate, each column's name and canonical text.
func (t *table) set(col Column) int64 {
	if col.Name == "" {
		return 0
	}
	if t.cols == nil {
		t.cols = map[string][]cell{}
	}
	row, cells, size := int32(len(t.ts)-1), t.cols[col.Name], 0
	if n := len(cells); n > 0 && cells[n-1].row == row {
		size -= t.estimate(col.Name, cells[n-1])
		cells = cells[:n-1]
	}
	c := cell{row: row, typ: col.Type, val: col.Value}
	t.cols[col.Name] = append(cells, c)
	return int64(size + t.estimate(col.Name, c))
}

// estimate predicts neither the encoded nor the compressed size.
func (t *table) estimate(name string, c cell) int {
	t.scratch = value.AppendFormat(t.scratch[:0], c.typ, c.val)
	return len(name) + len(t.scratch)
}

// finish gives every column one physical type, types' or else its cells' promoted,
// orders the rows by service when byService, and adds the
// rows' timestamps as ColTimestamp, which shadows a column of that name.
func (t *table) finish(types map[string]value.Type, byService bool) {
	if types == nil {
		types = map[string]value.Type{}
		for name, cells := range t.cols {
			for _, c := range cells {
				types[name] = value.Promote(types[name], c.typ)
			}
		}
	}
	for name, cells := range t.cols {
		for i, c := range cells {
			if c.typ != types[name] {
				cells[i].typ, cells[i].val = types[name], value.Coerce(c.typ, c.val, types[name])
			}
		}
	}
	if byService {
		t.sortByService()
	}
	ts := make([]cell, len(t.ts))
	for i, at := range t.ts {
		ts[i] = cell{row: int32(i), typ: value.TypeTimestamp, val: value.Value{Time: at.UTC()}}
	}
	if t.cols == nil {
		t.cols = map[string][]cell{}
	}
	t.cols[ColTimestamp] = ts
}

// sortByService orders rows by service, then time, so each row group holds about one
// service, which its zone map then lets a service predicate skip.
func (t *table) sortByService() {
	services := t.text(ColService)
	order := make([]int32, len(t.ts))
	for i := range order {
		order[i] = int32(i)
	}
	slices.SortStableFunc(order, func(a, b int32) int {
		return cmp.Or(cmp.Compare(services[a], services[b]), t.ts[a].Compare(t.ts[b]))
	})
	if slices.IsSorted(order) {
		return
	}
	at, ts := make([]int32, len(order)), make([]time.Time, len(order))
	for i, row := range order {
		at[row], ts[i] = int32(i), t.ts[row]
	}
	t.ts = ts
	for name, cells := range t.cols {
		// Sorted as a row and an index packed in a word, which moves far less than a cell.
		keys := make([]uint64, len(cells))
		for i, c := range cells {
			keys[i] = uint64(at[c.row])<<32 | uint64(i)
		}
		slices.Sort(keys)
		sorted := make([]cell, len(cells))
		for i, key := range keys {
			sorted[i] = cells[uint32(key)]
			sorted[i].row = int32(key >> 32)
		}
		t.cols[name] = sorted
	}
}

// text is each row's value of column name as its canonical text, "" where it has none.
func (t *table) text(name string) []string {
	out := make([]string, len(t.ts))
	for _, c := range t.cols[name] {
		out[c.row] = value.Format(c.typ, c.val)
	}
	return out
}

// columns is the table's columns in name order, ColTimestamp among them.
func (t *table) columns() []column {
	cols := make([]column, 0, len(t.cols))
	for _, name := range slices.Sorted(maps.Keys(t.cols)) {
		typ := value.TypeTimestamp
		if name != ColTimestamp {
			typ = t.cols[name][0].typ
		}
		cols = append(cols, column{name: name, typ: typ})
	}
	return cols
}

// zoneMaps folds the cells of rows [lo, hi).
func (t *table) zoneMaps(lo, hi int) ZoneMaps {
	zm := make(ZoneMaps)
	for name, cells := range t.cols {
		for _, c := range cellsIn(cells, lo, hi) {
			n := leafCount(c.typ, c.val, true)
			if n == 0 {
				zm.declare(name, c.typ) // an empty list
			}
			for j := range n {
				zm.Update(name, c.typ, leafAt(c.typ, c.val, j))
			}
		}
	}
	for name, z := range zm {
		zm[name] = z.bounded()
	}
	return zm
}

// cellsIn is the cells of rows [lo, hi).
func cellsIn(cells []cell, lo, hi int) []cell {
	byRow := func(c cell, row int32) int { return cmp.Compare(c.row, row) }
	i, _ := slices.BinarySearchFunc(cells, int32(lo), byRow)
	j, _ := slices.BinarySearchFunc(cells, int32(hi), byRow)
	return cells[i:j]
}

// LogRecord is a log line's record from its level, message and string attributes.
// level and message are conventions rather than fields, so an empty one adds no column.
func LogRecord(ts time.Time, level, message string, attrs map[string]string) Record {
	cols := make([]Column, 0, len(attrs)+2)
	for k, v := range attrs {
		cols = append(cols, stringColumn(k, v))
	}
	cols = appendConventional(cols, stringColumn(ColLevel, level), stringColumn(ColMessage, message))
	return Record{Timestamp: ts, Columns: cols}
}

func appendConventional(cols []Column, conventional ...Column) []Column {
	for _, col := range conventional {
		if col.Value.String != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

func stringColumn(name, s string) Column {
	return Column{Name: name, Type: value.TypeString, Value: value.Value{String: s}}
}
