package segment

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rowsOf is t's rows as records, each one's columns in name order.
func rowsOf(t *table) []Record {
	records := make([]Record, len(t.ts))
	for i, ts := range t.ts {
		records[i].Timestamp = ts
	}
	for _, name := range slices.Sorted(maps.Keys(t.cols)) {
		for _, c := range t.cols[name] {
			if name != ColTimestamp {
				records[c.row].Columns = append(records[c.row].Columns, Column{Name: name, Type: c.typ, Value: c.val})
			}
		}
	}
	return records
}

// buildTestSegment constructs a Sealed segment of n rows spanning two+ row
// groups, with a mix of fixed columns, string attributes (trace_id), and a
// numeric param (duration_ms).
func buildTestSegment(t *testing.T, n int) (Sealed, []Record) {
	t.Helper()
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{
		Tenant:    "acme",
		Stream:    Stream{Service: "checkout"},
		CreatedAt: base,
	})
	for i := range n {
		level := "info"
		if i%5 == 0 {
			level = "error"
		}
		tmpl := fmt.Sprintf("tmpl-%d", i%7)
		ev := LogRecord(base.Add(time.Duration(i)*time.Millisecond), level, fmt.Sprintf("request %d completed", i), map[string]string{
			"trace_id": fmt.Sprintf("trace-%08d", i),
			"service":  "checkout",
		})
		parsed := ParsedPattern{
			PatternID: tmpl,
			Pattern:   "request <*> completed",
			Columns: []Column{
				{Name: "duration_ms", Type: value.TypeInt, Value: value.Value{Int: int64(1 + i%250)}},
			},
		}
		if err := b.Append(ev, parsed); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	sealed := b.Seal()
	return sealed, rowsOf(sealed.table)
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	const n = 1000
	sealed, rows := buildTestSegment(t, n)

	obj, err := encodeTable(sealed.table, nil, sealed.PatternProfile, sealed.patterns, 1<<10)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	footer, err := footerFromTail(obj)
	if err != nil {
		t.Fatalf("decode footer: %v", err)
	}

	if footer.RowGroups < 2 {
		t.Fatalf("row groups = %d, want several", footer.RowGroups)
	}

	// Reassemble each column from all row groups and compare to the originals.
	columns := []string{ColTimestamp, ColLevel, ColMessage, ColPatternID, "trace_id", "service", "duration_ms"}
	for _, col := range columns {
		var got [][]byte
		for g := range footer.RowGroups {
			vals, err := readColumn(obj, footer, g, col)
			if err != nil {
				t.Fatalf("read column %q group %d: %v", col, g, err)
			}
			got = append(got, vals...)
		}
		if len(got) != n {
			t.Fatalf("column %q: got %d rows, want %d", col, len(got), n)
		}
		for i := range n {
			want := expectedCell(col, rows[i])
			if string(got[i]) != want {
				t.Fatalf("column %q row %d: got %q, want %q", col, i, got[i], want)
			}
		}
	}
}

// TestRecordRoundTripWithoutLogColumns is the metric-shaped record: no level, no
// message, no service, no pattern. It goes through Append and Seal rather than a
// hand-built Sealed, because the round trip the contract promises is the one the
// collector's own path produces.
func TestRecordRoundTripWithoutLogColumns(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 123, time.UTC)
	columns := []Column{
		{Name: "count", Type: value.TypeInt, Value: value.Value{Int: 10}},
		{Name: "ok", Type: value.TypeBool, Value: value.Value{Bool: true}},
		{Name: "payload", Type: value.TypeJSON, Value: value.JSON([]byte(`{"a":1}`))},
	}

	// No Stream.Service, and a Record carrying nothing but its timestamp.
	b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: ts})
	require.NoError(t, b.Append(LogRecord(ts, "", "", nil), ParsedPattern{Columns: columns}))
	sealed := b.Seal()

	record := Record{Timestamp: ts, Columns: columns}
	require.Equal(t, []Record{record}, rowsOf(sealed.table), "seal must add no conventional column")

	obj, err := sealed.Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)

	got := Record{Timestamp: ts}
	for _, name := range []string{"count", "ok", "payload"} {
		values, err := readColumn(obj, footer, 0, name)
		require.NoError(t, err)
		require.Len(t, values, 1)
		got.Columns = append(got.Columns, Column{
			Name:  name,
			Type:  footer.ZoneMaps[name].Type,
			Value: mustParseValue(t, footer.ZoneMaps[name].Type, values[0]),
		})
	}
	assert.Equal(t, record, got)
}

// Rows of two services arriving interleaved are sealed sorted by service, and the
// footer keeps each one's own counts, latencies and trace sketch.
func TestSealSplitsTheFooterByService(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", Stream: Stream{Service: "node-1"}, CreatedAt: ts})
	for i, service := range []string{"nginx", "checkout", "nginx", ""} {
		r := LogRecord(ts.Add(time.Duration(i)*time.Second), "info", "ok", map[string]string{ColService: service, ColTraceID: fmt.Sprint("t", i)})
		if service == "" {
			r = LogRecord(r.Timestamp, "info", "ok", nil)
		}
		parsed := ParsedPattern{PatternID: fmt.Sprint("tmpl-", i%2), Columns: []Column{{Name: ColDurationMs, Type: value.TypeInt, Value: value.Value{Int: int64(10 * (i + 1))}}}}
		require.NoError(t, b.Append(r, parsed))
	}
	sealed := b.Seal()
	assert.Equal(t, []string{"checkout", "nginx", "node-1"}, sealed.Descriptor.Services)
	var order []string
	for _, r := range rowsOf(sealed.table) {
		order = append(order, r.Text(ColService))
	}
	assert.Equal(t, []string{"checkout", "nginx", "nginx", "node-1"}, order)

	obj, err := sealed.Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)
	assert.Equal(t, []string{"checkout", "nginx", "node-1"}, footer.Services())
	nginx := footer.ForService("nginx")
	assert.Equal(t, 2, nginx.Rows)
	assert.InDelta(t, 2, nginx.DistinctCounts[ColTraceID].Estimate(), 0.1)
	assert.InDelta(t, 30, nginx.Distributions[ColDurationMs].Quantile(1), 1)
	assert.Equal(t, map[ServicePattern]int{{"checkout", "tmpl-1"}: 1, {"nginx", "tmpl-0"}: 2, {"node-1", "tmpl-1"}: 1}, footer.ServicePatternCounts)
	assert.Nil(t, footer.ForService("node-1").DistinctCounts[ColTraceID], "a service without trace ids has no trace sketch")
	fields, ok := nginx.Fields()
	require.True(t, ok)
	str, num := value.TypeString, value.TypeInt
	assert.Equal(t, map[string]string{ColTraceID: fmt.Sprintf("%d 2 2 false", str), ColLevel: fmt.Sprintf("%d 2 1 false", str),
		ColMessage: fmt.Sprintf("%d 2 1 false", str), ColDurationMs: fmt.Sprintf("%d 2 2 false", num),
		ColService: fmt.Sprintf("%d 2 1 false", str), ColPatternID: fmt.Sprintf("%d 2 1 false", str)}, describeFields(fields))
	_, whole := footer.Fields()
	assert.False(t, whole, "a whole footer keeps no field stats of its own")
}

// describeFields is each field's type, values, distinct estimate and whether it is text.
func describeFields(fields map[string]FieldStats) map[string]string {
	out := map[string]string{}
	for name, s := range fields {
		out[name] = fmt.Sprintf("%d %d %.0f %t", s.Type, s.Values, s.Distinct.Estimate(), s.Text)
	}
	return out
}

// The data picks the counted columns: a service's 5 regions are counted and
// dictionary-coded, its 50,000 user ids are neither, and both survive a round trip.
func TestValueCountsFollowTheData(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: ts})
	const n = 50_000
	for i := range n {
		r := LogRecord(ts.Add(time.Duration(i)*time.Millisecond), "info", "ok",
			map[string]string{ColService: "api", "region": fmt.Sprint("r", i%5), "user.id": fmt.Sprint("u", i)})
		require.NoError(t, b.Append(r, ParsedPattern{}))
	}
	obj, err := b.Seal().Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)

	want := map[string]uint64{"r0": n / 5, "r1": n / 5, "r2": n / 5, "r3": n / 5, "r4": n / 5}
	for _, view := range []Footer{footer.ForService("api"), footer} {
		got, ok := view.ValueCounts("region")
		assert.True(t, ok)
		assert.Equal(t, want, got)
		_, ok = view.ValueCounts("user.id")
		assert.False(t, ok, "a column over the threshold has no counts")
	}
	for column, tag := range map[string]byte{"region": chunkTagDict, "user.id": chunkTagRaw} {
		br, _ := footer.Offsets.Get(0, column)
		assert.Equal(t, tag, obj[br.Offset+2], column)
	}
}

func TestValuePairsCountWhatALineNamesTogether(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: ts})
	for i := range 40 {
		dst := []string{"cart", "checkout"}[i%2]
		r := LogRecord(ts.Add(time.Duration(i)*time.Millisecond), "info", "connection complete", map[string]string{
			ColService: "ztunnel", "src.workload": "web", "dst.workload": dst, ColPod: "ztunnel-x", "request_id": fmt.Sprint("r", i),
		})
		require.NoError(t, b.Append(r, ParsedPattern{}))
	}
	obj, err := b.Seal().Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)

	cols := [2]string{"dst.workload", "src.workload"}
	assert.Equal(t, map[ValuePair]uint64{
		{Columns: cols, Values: [2]string{"cart", "web"}}:     20,
		{Columns: cols, Values: [2]string{"checkout", "web"}}: 20,
	}, footer.ForService("ztunnel").ValuePairs(), "the producer's own columns and an id are not endpoints")
}

// The counts of 30 services' 20 small string columns add under 3% of the segment to
// its footer.
func TestValueCountsFooterShare(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: ts})
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 30_000 {
		attrs := map[string]string{ColService: fmt.Sprint("svc-", i%30), ColTraceID: fmt.Sprintf("%032x", rng.Uint64())}
		for c := range 20 {
			attrs[fmt.Sprint("col_", c)] = fmt.Sprint("v", rng.IntN(c+2))
		}
		r := LogRecord(ts.Add(time.Duration(i)*time.Millisecond), "info", fmt.Sprintf("request %x served", rng.Uint64()), attrs)
		require.NoError(t, b.Append(r, ParsedPattern{}))
	}
	obj, err := b.Seal().Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)
	with, err := footer.MarshalBinary()
	require.NoError(t, err)
	footer.valueCounts = nil
	for service, v := range footer.services {
		v.valueCounts = nil
		footer.services[service] = v
	}
	without, err := footer.MarshalBinary()
	require.NoError(t, err)
	added := len(zstdEncoder.EncodeAll(with, nil)) - len(zstdEncoder.EncodeAll(without, nil))
	assert.Less(t, float64(added)/float64(len(obj)), 0.03)
}

// Sorting by service at seal leaves each row group holding about one, so a service
// predicate can skip the others by their zone maps.
func TestRowGroupsHoldAboutOneService(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: ts})
	for i := range 3000 {
		r := LogRecord(ts.Add(time.Duration(i)*time.Millisecond), "info", fmt.Sprint("request ", i), map[string]string{ColService: fmt.Sprint("svc-", i%3)})
		require.NoError(t, b.Append(r, ParsedPattern{}))
	}
	sealed := b.Seal()
	obj, err := encodeTable(sealed.table, nil, sealed.PatternProfile, sealed.patterns, 256)
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)
	require.Greater(t, footer.RowGroups, 6)
	mixed := 0
	for _, zm := range footer.RowGroupZoneMaps {
		if _, sole := zm[ColService].SoleValue(zm[ColService].PresentRows); !sole {
			mixed++
		}
	}
	assert.LessOrEqual(t, mixed, 2, "only a group straddling two services holds both")
}

// Rows that compress to well under a group's bytes still end a group at MaxGroupCells rows
// times the columns it holds, so decoding one costs what its cells do, not what its bytes do,
// and a group of columns that change over its rows, as a transcript's do, is not cut for the
// segment's other columns.
func TestRowGroupsEndAtGroupCells(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		attrs func(row int) map[string]string
		// changing is whether the columns change over the rows.
		changing bool
	}{
		{"every row every column", func(int) map[string]string {
			attrs := map[string]string{}
			for c := range 400 {
				attrs[fmt.Sprint("field_", c)] = "same"
			}
			return attrs
		}, false},
		{"ten rows a column", func(row int) map[string]string { return map[string]string{fmt.Sprint("field_", row/10): "same"} }, true},
	} {
		b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: ts})
		for i := range 22_000 {
			require.NoError(t, b.Append(LogRecord(ts.Add(time.Duration(i)*time.Millisecond), "info", "same", tc.attrs(i)), ParsedPattern{}))
		}
		sealed := b.Seal()
		obj, err := encodeTable(sealed.table, nil, sealed.PatternProfile, sealed.patterns, targetRowGroupBytes)
		require.NoError(t, err)
		footer, err := footerFromTail(obj)
		require.NoError(t, err)
		assert.Greater(t, footer.RowGroups, 1, tc.name)
		assert.LessOrEqual(t, footer.LargestGroupCells(), MaxGroupCells, tc.name)
		if tc.changing { // fewer groups than half what a cap on every column of the segment cuts
			assert.Less(t, footer.RowGroups, 22_000/(MaxGroupCells/len(footer.ZoneMaps))/2, tc.name)
		}
	}
}

func TestFooterRoundTripsZoneMapValueTypeRepeatedness(t *testing.T) {
	footer := Footer{
		RowGroups: 1,
		ZoneMaps: ZoneMaps{
			"values": {
				Type: value.TypeInt | value.TypeRepeated,
				Min:  value.Value{Int: 2},
				Max:  value.Value{Int: 10},
			},
		},
		Offsets: make(OffsetTable),
	}

	data, err := footer.MarshalBinary()
	require.NoError(t, err)
	got, err := decodeFooter(data)
	require.NoError(t, err)

	assert.Equal(t, footer.ZoneMaps, got.ZoneMaps)
	assert.True(t, got.ZoneMaps["values"].Type.Repeated())
	assert.Equal(t, value.TypeInt, got.ZoneMaps["values"].Type.Scalar())
}

// TestTextZoneMapBounds: a long text column's bounds are cut short yet still bound every value,
// a cut landing inside a multi-byte rune or on the last rune included, and a sole long value
// stays whole as the column's value.
func TestTextZoneMapBounds(t *testing.T) {
	long := func(prefix string, tail byte) string { return prefix + strings.Repeat("x", 200) + string(tail) }
	for _, tc := range []struct {
		name   string
		values []string
		// sole is whether the bounds name the one value; short whether they are cut, which a
		// maximum of nothing but the last rune, with nothing above its prefix, is not.
		sole, short bool
	}{
		{"ascii", []string{long("a", 'a'), long("a", 'b'), long("b", 'c'), "c"}, false, true},
		{"a rune across the cut", []string{strings.Repeat("é", 40), strings.Repeat("é", 39) + "f"}, false, true},
		{"the last rune", []string{strings.Repeat("\U0010FFFF", 30), strings.Repeat("\U0010FFFF", 29) + "a"}, false, false},
		{"one long value", []string{long("same", 'z'), long("same", 'z')}, true, false},
	} {
		var records []Record
		for i, v := range tc.values {
			records = append(records, Record{Timestamp: time.Unix(int64(i), 0), Columns: []Column{{Name: "body", Type: value.TypeString, Value: value.Value{String: v}}}})
		}
		zm := BuildZoneMaps(records)["body"]
		_, sole := zm.SoleValue(len(records))
		assert.Equal(t, tc.sole, sole, tc.name)
		if tc.short {
			assert.LessOrEqual(t, len(zm.Min.String)+len(zm.Max.String), 2*(maxBoundBytes+4), tc.name)
		}
		for _, v := range tc.values {
			assert.True(t, zm.Min.String <= v && v <= zm.Max.String && zm.MayContainString(v), "%s: %q", tc.name, v)
		}
		assert.True(t, utf8.ValidString(zm.Min.String) && utf8.ValidString(zm.Max.String), tc.name)
	}
}

func TestNumericZoneMapComparesNumerically(t *testing.T) {
	zm := ZoneMaps{}
	zm.Update("duration_ms", value.TypeInt, value.Value{Int: 2})
	zm.Update("duration_ms", value.TypeInt, value.Value{Int: 10})

	// Lexicographically "10" sorts below "2"; the bound must order the other way.
	assert.Equal(t, ZoneMap{Type: value.TypeInt, Min: value.Value{Int: 2}, Max: value.Value{Int: 10}, PresentRows: 2}, zm["duration_ms"])

	minimum, maximum, ok := zm["duration_ms"].NumericBounds()
	require.True(t, ok)
	assert.Equal(t, [2]float64{2, 10}, [2]float64{minimum, maximum})
}

func TestBuilderSealSortsRecordColumns(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{
		Tenant:    "acme",
		Stream:    Stream{Service: "checkout"},
		CreatedAt: ts,
	})
	event := LogRecord(ts, "info", "ok", map[string]string{
		"z":       "last",
		"service": "nginx", // the row's own service wins over the stream's
		"a":       "first",
	})
	parsed := ParsedPattern{
		PatternID: "tmpl",
		Pattern:   "ok",
		Columns: []Column{
			{Name: "m", Type: value.TypeString, Value: value.Value{String: "middle"}},
		},
	}

	require.NoError(t, b.Append(event, parsed))
	sealed := b.Seal()
	records := rowsOf(sealed.table)
	require.Len(t, records, 1)

	assert.Equal(t, []Column{
		{Name: "a", Type: value.TypeString, Value: value.Value{String: "first"}},
		{Name: ColLevel, Type: value.TypeString, Value: value.Value{String: "info"}},
		{Name: "m", Type: value.TypeString, Value: value.Value{String: "middle"}},
		{Name: ColMessage, Type: value.TypeString, Value: value.Value{String: "ok"}},
		{Name: ColPatternID, Type: value.TypeString, Value: value.Value{String: "tmpl"}},
		{Name: ColService, Type: value.TypeString, Value: value.Value{String: "nginx"}},
		{Name: "z", Type: value.TypeString, Value: value.Value{String: "last"}},
	}, records[0].Columns)
}

// TestSealUnifiesColumnTypes pins the invariant the footer depends on: a sealed
// segment carries one physical type per column, so records that disagree are
// promoted and their values coerced rather than left to contradict the footer.
func TestSealUnifiesColumnTypes(t *testing.T) {
	tests := []struct {
		name string
		a, b Column
		want []Column
	}{
		{
			name: "int meets float widens to float",
			a:    Column{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 42}},
			b:    Column{Name: "n", Type: value.TypeFloat, Value: value.Value{Float: 42.5}},
			want: []Column{
				{Name: "n", Type: value.TypeFloat, Value: value.Value{Float: 42}},
				{Name: "n", Type: value.TypeFloat, Value: value.Value{Float: 42.5}},
			},
		},
		{
			name: "int meets string falls back to string",
			a:    Column{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 42}},
			b:    Column{Name: "n", Type: value.TypeString, Value: value.Value{String: "abc"}},
			want: []Column{
				{Name: "n", Type: value.TypeString, Value: value.Value{String: "42"}},
				{Name: "n", Type: value.TypeString, Value: value.Value{String: "abc"}},
			},
		},
		{
			name: "bool meets int falls back to string",
			a:    Column{Name: "n", Type: value.TypeBool, Value: value.Value{Bool: true}},
			b:    Column{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 0}},
			want: []Column{
				{Name: "n", Type: value.TypeString, Value: value.Value{String: "true"}},
				{Name: "n", Type: value.TypeString, Value: value.Value{String: "0"}},
			},
		},
		{
			name: "int meets float list widens to a float list",
			a:    Column{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 1}},
			b:    Column{Name: "n", Type: value.TypeRepeated | value.TypeFloat, Value: value.Value{List: []value.Value{{Float: 2.5}}}},
			want: []Column{
				{Name: "n", Type: value.TypeRepeated | value.TypeFloat, Value: value.Value{List: []value.Value{{Float: 1}}}},
				{Name: "n", Type: value.TypeRepeated | value.TypeFloat, Value: value.Value{List: []value.Value{{Float: 2.5}}}},
			},
		},
		{
			name: "agreeing records keep their type",
			a:    Column{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 1}},
			b:    Column{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 2}},
			want: []Column{
				{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 1}},
				{Name: "n", Type: value.TypeInt, Value: value.Value{Int: 2}},
			},
		},
	}

	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuilder(BuilderConfig{Tenant: "acme", Stream: Stream{Service: "checkout"}, CreatedAt: ts})
			for i, col := range []Column{tc.a, tc.b} {
				event := LogRecord(ts.Add(time.Duration(i)*time.Second), "info", "ok", nil)
				parsed := ParsedPattern{PatternID: "tmpl", Pattern: "ok", Columns: []Column{col}}
				require.NoError(t, b.Append(event, parsed))
			}
			sealed := b.Seal()
			records := rowsOf(sealed.table)
			require.Len(t, records, 2)

			got := []Column{columnNamed(t, records[0], "n"), columnNamed(t, records[1], "n")}
			assert.Equal(t, tc.want, got)
			// The zone map is built from the unified records, so it types the column
			// the same way and its bounds cover every row.
			assert.Equal(t, tc.want[0].Type, sealed.Descriptor.ZoneMaps["n"].Type)
		})
	}
}

// TestZoneMapDropsBoundOnTypeConflict covers the exported entry point: a caller
// that mixes types must get no bound rather than one that omits rows, because
// every reader treats a missing zone map as unknown.
func TestZoneMapDropsBoundOnTypeConflict(t *testing.T) {
	zm := ZoneMaps{}
	zm.Update("n", value.TypeInt, value.Value{Int: 42})
	zm.Update("n", value.TypeString, value.Value{String: "abc"})

	_, ok := zm["n"]
	assert.False(t, ok, "a type conflict must leave no bound")
}

func columnNamed(t *testing.T, record Record, name string) Column {
	t.Helper()
	for _, col := range record.Columns {
		if col.Name == name {
			return col
		}
	}
	t.Fatalf("record has no column %q", name)
	return Column{}
}

func expectedCell(col string, record Record) string {
	if col == ColTimestamp {
		return record.Timestamp.UTC().Format(value.TimeLayoutFixedNano)
	}
	for _, c := range record.Columns {
		if c.Name == col {
			return value.Format(c.Type, c.Value)
		}
	}
	return ""
}

func mustParseValue(t *testing.T, typ value.Type, raw []byte) value.Value {
	t.Helper()
	v, err := value.Parse(typ, string(raw))
	require.NoError(t, err)
	return v
}

func TestFooterIndexesCorrect(t *testing.T) {
	const n = 1000
	sealed, rows := buildTestSegment(t, n)
	obj, err := sealed.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	footer, err := footerFromTail(obj)
	if err != nil {
		t.Fatalf("decode footer: %v", err)
	}

	// Present-set: every pattern_id present, and only those.
	wantPatterns := map[string]bool{}
	for _, r := range rows {
		wantPatterns[expectedCell(ColPatternID, r)] = true
	}
	for tmpl := range wantPatterns {
		if !footer.Patterns.Contains(tmpl) {
			t.Fatalf("present-set missing pattern %q", tmpl)
		}
	}
	if got := len(footer.Patterns); got != len(wantPatterns) {
		t.Fatalf("present-set size = %d, want %d", got, len(wantPatterns))
	}
	if footer.Patterns.Contains("tmpl-999") {
		t.Fatalf("present-set contains absent pattern")
	}

	// Zone maps: ts min/max match the extremes.
	zm, ok := footer.ZoneMaps[ColTimestamp]
	if !ok {
		t.Fatalf("no zone map for ts")
	}
	wantMin := value.Value{Time: rows[0].Timestamp.UTC()}
	wantMax := value.Value{Time: rows[n-1].Timestamp.UTC()}
	if zm.Type != value.TypeTimestamp || !assert.Equal(t, wantMin, zm.Min) || !assert.Equal(t, wantMax, zm.Max) {
		t.Fatalf("ts zone map = [%s,%s], want [%s,%s]", value.Format(zm.Type, zm.Min), value.Format(zm.Type, zm.Max), value.Format(value.TypeTimestamp, wantMin), value.Format(value.TypeTimestamp, wantMax))
	}
	// level zone map is [error, info].
	if lz := footer.ZoneMaps[ColLevel]; lz.Type != value.TypeString || lz.Min.String != "error" || lz.Max.String != "info" {
		t.Fatalf("level zone map = [%s,%s], want [error,info]", lz.Min.String, lz.Max.String)
	}

	// HLL: trace_id is unique per row, so estimate ~= n (within HLL error).
	hll, ok := footer.DistinctCounts["trace_id"]
	if !ok {
		t.Fatalf("no HLL for trace_id")
	}
	est := hll.Estimate()
	if math.Abs(est-float64(n))/float64(n) > 0.05 {
		t.Fatalf("trace_id distinct estimate = %.0f, want ~%d (>5%% off)", est, n)
	}

	// DDSketch: duration_ms median is near the true median of 1..250 cycling.
	dd, ok := footer.Distributions["duration_ms"]
	if !ok {
		t.Fatalf("no DDSketch for duration_ms")
	}
	q50 := dd.Quantile(0.5)
	if q50 < 100 || q50 > 150 {
		t.Fatalf("duration_ms p50 = %.1f, want ~125", q50)
	}

	// Value counts: "error" appears n/5 times (every 5th row), counted exactly.
	levels, ok := footer.ValueCounts(ColLevel)
	if !ok {
		t.Fatalf("no value counts for level")
	}
	if got, want := levels["error"], uint64((n+4)/5); got != want {
		t.Fatalf("level=error counted %d, want %d", got, want)
	}
}

func TestFooterFromTailRejectsBadInput(t *testing.T) {
	if _, err := footerFromTail([]byte("too short")); err == nil {
		t.Fatalf("expected error on short input")
	}
	bad := make([]byte, 64) // valid length, wrong magic
	if _, err := footerFromTail(bad); err == nil {
		t.Fatalf("expected error on bad magic")
	}
}

func TestDecodeFooterRejectsCorruptLayout(t *testing.T) {
	tests := []struct {
		name   string
		footer Footer
	}{
		{name: "no row groups", footer: Footer{}},
		{name: "negative offset", footer: Footer{RowGroups: 1, Offsets: OffsetTable{{RowGroup: 0, Column: "a"}: {Offset: -1, Length: 4}}}},
		{name: "negative length", footer: Footer{RowGroups: 1, Offsets: OffsetTable{{RowGroup: 0, Column: "a"}: {Offset: 0, Length: -4}}}},
		{name: "row group out of range", footer: Footer{RowGroups: 1, Offsets: OffsetTable{{RowGroup: 1, Column: "a"}: {Offset: 0, Length: 4}}}},
		{name: "negative filter length", footer: Footer{RowGroups: 1, filters: map[string]ByteRange{"a": {Offset: 0, Length: -4}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := tc.footer.MarshalBinary()
			require.NoError(t, err)
			_, err = decodeFooter(data)
			assert.Error(t, err)
		})
	}
}

// TestUniversalHLLCoversAllColumns is the pkg_codec universal-HLL contract:
// every column with at least one non-empty value gets a per-column HLL, not
// just the hard-coded highCardColumns. This is the prerequisite that lets
// rank_fields estimate cardinality for any column (e.g. host, deploy.version).
func TestUniversalHLLCoversAllColumns(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{
		Tenant:    "acme",
		Stream:    Stream{Service: "checkout"},
		CreatedAt: base,
	})
	hosts := []string{"host-a", "host-b", "host-c"}
	for i := range 9 {
		ev := LogRecord(base.Add(time.Duration(i)*time.Millisecond), "info", "ok", map[string]string{
			"host": hosts[i%len(hosts)],
		})
		if err := b.Append(ev, ParsedPattern{PatternID: "t", Pattern: "ok"}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	sealed := b.Seal()
	obj, err := sealed.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	footer, err := footerFromTail(obj)
	if err != nil {
		t.Fatalf("decode footer: %v", err)
	}

	hll, ok := footer.DistinctCounts["host"]
	if !ok || hll == nil {
		t.Fatalf("expected footer.DistinctCounts[\"host\"] to be non-nil (universal HLL)")
	}
	if est := hll.Estimate(); est < 3.0 {
		t.Fatalf("host HLL estimate = %.1f, want >= 3.0 (3 distinct values)", est)
	}
}

func TestEncodeEmptySegment(t *testing.T) {
	b := NewBuilder(BuilderConfig{Tenant: "t", Stream: Stream{Service: "s"}})
	sealed := b.Seal()
	obj, err := sealed.Encode()
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}
	footer, err := footerFromTail(obj)
	if err != nil {
		t.Fatalf("decode empty footer: %v", err)
	}
	vals, err := readColumn(obj, footer, 0, ColTimestamp)
	if err != nil {
		t.Fatalf("read empty column: %v", err)
	}
	if len(vals) != 0 {
		t.Fatalf("empty segment ts column = %d rows, want 0", len(vals))
	}
}

// readColumn returns the decoded per-row values of one column chunk sliced out of the
// full object bytes, the in-memory oracle the package's tests check Reader against.
func readColumn(object []byte, f Footer, rowGroup int, column string) ([][]byte, error) {
	br, ok := f.Offsets.Get(rowGroup, column)
	if !ok {
		return nil, fmt.Errorf("segment: no chunk for column %q in row group %d", column, rowGroup)
	}
	if br.Offset < 0 || br.Length < 0 || br.Offset+br.Length > int64(len(object)) {
		return nil, fmt.Errorf("segment: chunk range %d+%d out of object bounds %d", br.Offset, br.Length, len(object))
	}
	chunk, err := decodeColumnChunk(object[br.Offset:br.Offset+br.Length], nil)
	return chunk.Rows(), err
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	chunk := encodeColumnChunk(column{name: "n", typ: value.TypeInt}, nil, 0, 0)
	chunk[0]++
	footer, err := Footer{RowGroups: 1}.MarshalBinary()
	require.NoError(t, err)
	footer[0] = 8

	_, chunkErr := decodeColumnChunk(chunk, nil)
	_, footerErr := decodeFooter(footer)
	assert.Equal(t, []error{
		&VersionError{Object: "chunk", Version: chunkVersion + 1},
		&VersionError{Object: "footer", Version: 8},
	}, []error{chunkErr, footerErr})
}

// TestListColumns: a List<Struct> (one repeated column per field, named for columns
// with a filter and a distribution) keeps null and empty lists distinct and indexes
// its elements as a scalar column holding them would be.
func TestListColumns(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	lists, scalars := NewBuilder(BuilderConfig{CreatedAt: ts}), NewBuilder(BuilderConfig{CreatedAt: ts})
	for i, traces := range [][]string{{"a", "b"}, nil, {}, {"c"}} {
		var traceList, msList []value.Value
		for j, trace := range traces {
			traceList = append(traceList, value.Value{String: trace})
			msList = append(msList, value.Value{Int: int64(10*i + j)})
			require.NoError(t, scalars.Append(LogRecord(ts, "", "", nil), ParsedPattern{Columns: []Column{
				{Name: ColTraceID, Type: value.TypeString, Value: traceList[j]},
				{Name: ColDurationMs, Type: value.TypeInt, Value: msList[j]},
			}}))
		}
		var cols []Column
		if traces != nil {
			cols = []Column{
				{Name: ColTraceID, Type: value.TypeRepeated | value.TypeString, Value: value.Value{List: append([]value.Value{}, traceList...)}},
				{Name: ColDurationMs, Type: value.TypeRepeated | value.TypeInt, Value: value.Value{List: append([]value.Value{}, msList...)}},
			}
		}
		require.NoError(t, lists.Append(LogRecord(ts, "", "", nil), ParsedPattern{Columns: cols}))
	}
	obj, err := lists.Seal().Encode()
	require.NoError(t, err)
	got, err := footerFromTail(obj)
	require.NoError(t, err)
	scalarObj, err := scalars.Seal().Encode()
	require.NoError(t, err)
	want, err := footerFromTail(scalarObj)
	require.NoError(t, err)

	traceRows, err := readColumn(obj, got, 0, ColTraceID)
	require.NoError(t, err)
	msRows, err := readColumn(obj, got, 0, ColDurationMs)
	require.NoError(t, err)
	assert.Equal(t, [][]byte{[]byte(`["a","b"]`), nil, []byte(`[]`), []byte(`["c"]`)}, traceRows)
	assert.Equal(t, [][]byte{[]byte(`["0","1"]`), nil, []byte(`[]`), []byte(`["30"]`)}, msRows)
	br, _ := got.Offsets.Get(0, ColDurationMs)
	picked, err := decodeColumnChunk(obj[br.Offset:br.Offset+br.Length], []int{2, 3})
	require.NoError(t, err)
	assert.Equal(t, [][]byte{nil, nil, []byte(`[]`), []byte(`["30"]`)}, picked.Rows(), "a list's leaves are skipped by its row's count")
	for _, col := range []string{ColTraceID, ColDurationMs} {
		zm := want.ZoneMaps[col]
		zm.Type |= value.TypeRepeated
		assert.Equal(t, zm, got.ZoneMaps[col], col)
		assert.Equal(t, want.DistinctCounts[col], got.DistinctCounts[col], col)
	}
	assert.Equal(t, want.Distributions[ColDurationMs], got.Distributions[ColDurationMs])
}

// TestEncodeRowGroupAllocations: every encoding appends into per-column buffers that
// zstd compresses whole, so encoding allocates as its buffers grow, not per row.
func TestEncodeRowGroupAllocations(t *testing.T) {
	allocs := func(rows int) float64 {
		sealed, _ := buildTestSegment(t, rows)
		cols := sealed.table.columns()
		return testing.AllocsPerRun(3, func() { encodeRowGroup(cols, sealed.table, 0, rows) })
	}
	small, large := allocs(10000), allocs(100000)
	assert.Less(t, large-small, 90000/100.0, "90,000 more rows cost %.0f more allocations", large-small)
}

// TestDecodeColumnChunkAllocations: a decoded row aliases the chunk's payload or keeps its
// value unformatted, so decoding allocates per chunk, not per row, for every encoding.
func TestDecodeColumnChunkAllocations(t *testing.T) {
	allocs := func(rows int) map[string]float64 {
		sealed, _ := buildTestSegment(t, rows)
		cols := sealed.table.columns()
		chunks, _ := encodeRowGroup(cols, sealed.table, 0, rows)
		out := map[string]float64{}
		for i, c := range cols {
			out[c.name] = testing.AllocsPerRun(3, func() {
				_, err := decodeColumnChunk(chunks[i], nil)
				require.NoError(t, err)
			})
		}
		return out
	}
	small, large := allocs(1000), allocs(10000)
	for col, n := range large {
		assert.Lessf(t, n-small[col], 20.0, "column %s: 9,000 more rows cost %.0f more allocations", col, n-small[col])
	}
}

// TestDecodeColumnChunkRows: a chunk decoded for some rows reads them as the whole chunk
// does and every other row as null, in every encoding, holding a fraction of its bytes.
func TestDecodeColumnChunkRows(t *testing.T) {
	sealed, _ := buildTestSegment(t, 1000)
	cols := sealed.table.columns()
	chunks, _ := encodeRowGroup(cols, sealed.table, 0, 1000)
	rows := []int{3, 500, 999}
	for i, c := range cols {
		whole, err := decodeColumnChunk(chunks[i], nil)
		require.NoError(t, err)
		picked, err := decodeColumnChunk(chunks[i], rows)
		require.NoError(t, err)
		want := make([][]byte, whole.Len())
		for _, row := range rows {
			want[row] = whole.Text(row)
		}
		assert.Equal(t, want, picked.Rows(), c.name)
		assert.Less(t, 100*picked.size(), whole.size(), c.name)
	}
	_, err := decodeColumnChunk(chunks[0], []int{1000})
	assert.ErrorContains(t, err, "not ascending")
}

func TestPatternTextRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want string
	}{
		{"short", "took <*> ms", "took <*> ms"},
		{"limit", strings.Repeat("x", 512), strings.Repeat("x", 512)},
		{"long", strings.Repeat("x", 600), strings.Repeat("x", 509) + "…"},
		{"rune", strings.Repeat("x", 508) + "界" + strings.Repeat("x", 89), strings.Repeat("x", 508) + "…"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBuilder(BuilderConfig{})
			for _, message := range []string{"took 12 ms", "took 30 ms"} {
				require.NoError(t, b.Append(LogRecord(time.Unix(1, 0), "", message, nil), ParsedPattern{PatternID: "id", Pattern: tc.text}))
			}
			sealed := b.Seal()
			for _, handBuilt := range []bool{false, true} {
				if handBuilt {
					sealed = Sealed{Records: rowsOf(sealed.table)}
				}
				object, err := sealed.Encode()
				require.NoError(t, err)
				footer, err := footerFromTail(object)
				require.NoError(t, err)
				text, ok := footer.PatternText("id")
				require.True(t, ok)
				want := tc.want
				if handBuilt {
					want = ""
				}
				assert.Equal(t, want, text)
				assert.True(t, utf8.ValidString(text))
				_, ok = footer.PatternText("absent")
				assert.False(t, ok)
			}
		})
	}
}

func TestCorrelationNames(t *testing.T) {
	ids := []string{"id", "trace_id", "order_id", "request" + "Id", "trace" + "Id", "request-id", "http.request.id", "trace.id", "connection.id"}
	for _, name := range ids {
		assert.True(t, IsID(name), name)
		assert.True(t, IsCorrelation(name), name)
	}
	for _, name := range []string{"session", "Transaction", "correlation", "conversation", "task", "job", "run"} {
		assert.False(t, IsID(name), name)
		assert.True(t, IsCorrelation(name), name)
	}
	for _, name := range []string{"pattern_id", "grid", "invalid", "requestID", "host", "service", "message", "duration_ms"} {
		assert.False(t, IsID(name), name)
		assert.False(t, IsCorrelation(name), name)
	}
}

// TestIDByValues: a string column is an id by its values whatever its name, and a named one
// stays an id whatever its values: an id by values gets a filter section, and either gets
// its service's correlation sketch. A few lines of an enum, a value each, are not an id.
func TestIDByValues(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	records := make([]Record, 40)
	for i := range records {
		records[i] = LogRecord(base.Add(time.Duration(i)*time.Second), []string{"info", "warn", "error"}[i%3], "", map[string]string{
			ColService: "api",
			"req":      fmt.Sprintf("r-%d", i/3),
			ColTraceID: fmt.Sprintf("%016x", i/2),
			"session":  fmt.Sprintf("s-%d", i%2),
			"user_id":  "u-1",
			"note":     fmt.Sprintf("note %d", i),
		})
		records[i].Columns = append(records[i].Columns, Column{Name: ColDurationMs, Type: value.TypeInt, Value: value.Value{Int: int64(i)}})
		if i < 5 {
			records[i].Columns = append(records[i].Columns, stringColumn("x", fmt.Sprintf("x-%d", i)))
		}
		if i < 12 {
			records[i].Columns = append(records[i].Columns, stringColumn("reason", []string{"Scheduled", "Pulled", "Created", "Started"}[i%4]))
		}
	}
	obj, err := Sealed{Records: records}.Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)

	view := footer.ForService("api")
	for column, want := range map[string][2]bool{ // filter section, correlation sketch
		"req": {true, true}, ColTraceID: {true, true}, "session": {false, true}, "user_id": {false, true},
		ColLevel: {false, false}, ColDurationMs: {false, false}, "note": {false, false}, "x": {false, false},
		"reason": {false, false},
	} {
		_, sketched := view.DistinctCounts[column]
		assert.Equal(t, want, [2]bool{footer.HasFilter(column), sketched}, column)
	}
}
