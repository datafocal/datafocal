package segment

import (
	"flag"
	"fmt"
	"maps"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "update golden files")

const goldenSegmentPath = "testdata/segment.golden"

// denseSketchSegmentPath is the golden segment as written before footers kept small
// distinct-count sketches sparse, which every reader must still decode.
const denseSketchSegmentPath = "testdata/segment_dense_sketches.golden"

// v10SegmentPath is the golden segment as written before service views kept field stats,
// which every reader must still decode, its views reporting none.
const v10SegmentPath = "testdata/segment_v10.golden"

// goldenSegment names a service so its view carries value counts and its counted
// columns dict chunks.
func goldenSegment(t *testing.T) Sealed {
	t.Helper()
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", CreatedAt: base})
	for i := range 8 {
		event := LogRecord(base.Add(time.Duration(i*1500)*time.Microsecond), "", fmt.Sprintf("charge %d settled", i), map[string]string{
			ColService: "checkout",
			ColTraceID: fmt.Sprintf("trace-%04d", i),
			ColUserID:  fmt.Sprintf("user-%d", i%3),
		})
		parsed := ParsedPattern{
			PatternID: fmt.Sprintf("tmpl-%d", i%2),
			Pattern:   "charge <*> settled",
			Columns: []Column{
				{Name: ColDurationMs, Type: value.TypeInt, Value: value.Value{Int: int64(10 + i*7)}},
				{Name: "ratio", Type: value.TypeFloat, Value: value.Value{Float: float64(i) / 4}},
				{Name: "retried", Type: value.TypeBool, Value: value.Value{Bool: i%2 == 1}},
			},
		}
		require.NoError(t, b.Append(event, parsed))
	}
	sealed := b.Seal()
	sealed.PatternProfile = PatternProfile{Name: "logs.json", Version: 1, Components: []string{"fieldset", "extract(msg)"}, Degradation: 1}
	return sealed
}

func TestSegmentGolden(t *testing.T) {
	sealed := goldenSegment(t)
	got, err := sealed.Encode()
	require.NoError(t, err)

	if *updateGolden {
		require.NoError(t, os.WriteFile(goldenSegmentPath, got, 0o644))
		t.Log("golden file updated; rerun without -update to validate")
		return
	}

	golden, err := os.ReadFile(goldenSegmentPath)
	require.NoError(t, err, "read golden (run with -update to create)")
	assert.Equal(t, golden, got, "encoding changed (rerun with -update if intentional)")

	dense, err := os.ReadFile(denseSketchSegmentPath)
	require.NoError(t, err)
	v10, err := os.ReadFile(v10SegmentPath)
	require.NoError(t, err)
	for _, object := range [][]byte{golden, dense, v10} {
		footer, err := footerFromTail(object)
		require.NoError(t, err)
		want, filters := wantGoldenFooter(t, sealed, footer.Offsets)
		if &object[0] == &v10[0] {
			for service, view := range want.services {
				view.fields = nil
				want.services[service] = view
			}
		}
		if footer.HasFilter(GramsSection) {
			filter := buildGrams(textsOf([]column{{name: ColMessage, typ: value.TypeString}}, sealed.table)[0].texts)
			filters[GramsSection] = encodeGrams(Grams{filter})
			var end int64
			for _, br := range footer.Offsets {
				end = max(end, br.Offset+br.Length)
			}
			for _, name := range slices.Sorted(maps.Keys(filters)) {
				want.filters[name] = ByteRange{Offset: end, Length: int64(len(filters[name]))}
				end += int64(len(filters[name]))
			}
		}
		want.size = footer.size // TestFooterCacheByteBudget pins it
		assert.Equal(t, want, footer)
		assert.Equal(t, rowsOf(sealed.table), decodeGoldenRecords(t, object, footer))
	}
}

// wantGoldenFooter takes offsets as decoded, because the byte comparison pins them
// and decodeGoldenRecords proves they locate every chunk, and returns the filter sections
// the footer locates.
func wantGoldenFooter(t *testing.T, sealed Sealed, offsets OffsetTable) (Footer, map[string][]byte) {
	t.Helper()
	rows := sealed.table
	cols := rows.columns()
	want, filters, err := buildFooter(cols, rows, textsOf(cols, rows), rows.zoneMaps(0, len(rows.ts)), sealed.patterns)
	require.NoError(t, err)
	want.RowGroups, want.RowGroupZoneMaps, want.Offsets = 1, []ZoneMaps{rows.zoneMaps(0, len(rows.ts))}, offsets
	want.filters = map[string]ByteRange{}
	want.PatternProfile = sealed.PatternProfile
	return want, filters
}

// decodeGoldenRecords rebuilds the records from the golden's column chunks, typing
// each column by its zone map.
func decodeGoldenRecords(t *testing.T, object []byte, footer Footer) []Record {
	t.Helper()
	timestamps, err := readColumn(object, footer, 0, ColTimestamp)
	require.NoError(t, err)
	records := make([]Record, len(timestamps))
	for i, ts := range timestamps {
		records[i].Timestamp = mustParseValue(t, value.TypeTimestamp, ts).Time
	}
	for _, name := range slices.Sorted(maps.Keys(footer.ZoneMaps)) {
		if name == ColTimestamp {
			continue
		}
		values, err := readColumn(object, footer, 0, name)
		require.NoError(t, err)
		require.Len(t, values, len(records), "column %q", name)
		typ := footer.ZoneMaps[name].Type
		for i, v := range values {
			records[i].Columns = append(records[i].Columns, Column{Name: name, Type: typ, Value: mustParseValue(t, typ, v)})
		}
	}
	return records
}
