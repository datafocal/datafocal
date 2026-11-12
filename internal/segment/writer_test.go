package segment

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
)

// TestWriterAnswersAsSealing: rows written over several row groups read back, and their
// footer answers, as sealing the same rows at once does.
func TestWriterAnswersAsSealing(t *testing.T) {
	cfg := BuilderConfig{Tenant: "acme", Stream: Stream{Service: "api"}, CreatedAt: time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)}
	b := NewBuilder(cfg)
	var rows []Record
	for i := range 3000 {
		r := LogRecord(cfg.CreatedAt.Add(-time.Duration(i)*time.Second), []string{"info", "warn", "error"}[i%3], fmt.Sprintf("request %d done", i),
			map[string]string{ColService: []string{"api", "db"}[i%2], ColTraceID: fmt.Sprint("t-", i),
				"src": fmt.Sprint("web-", i%3), "dst": fmt.Sprint("db-", i%5, ":5432")})
		r.Columns = append(r.Columns, Column{Name: ColDurationMs, Type: value.TypeInt, Value: value.Value{Int: int64(i % 97)}})
		rows = append(rows, r)
		require.NoError(t, b.Append(r, ParsedPattern{PatternID: "p1", Pattern: "request <*> done"}))
	}
	sealed := b.Seal()
	whole, err := sealed.Encode()
	require.NoError(t, err)

	types := map[string]value.Type{}
	for name, zm := range sealed.Descriptor.ZoneMaps {
		types[name] = zm.Type
	}
	w := NewWriter(cfg, types)
	w.(*streamWriter).cells = 1000 * len(types) // three groups of a thousand rows
	for _, r := range rows {
		require.NoError(t, w.Append(r, ParsedPattern{PatternID: "p1", Pattern: "request <*> done"}))
	}
	streamed, desc, err := w.Close(PatternProfile{})
	require.NoError(t, err)

	assert.Equal(t, sealed.Descriptor, desc)
	got, want := answersOf(t, streamed), answersOf(t, whole)
	assert.Equal(t, 3, got.rowGroups)
	got.rowGroups = want.rowGroups
	assert.Equal(t, want, got)
}

// writerAnswers is what a segment's rows and footer tell a query.
type writerAnswers struct {
	rowGroups      int
	records        map[string]int
	zoneMaps       ZoneMaps
	levels         map[string]uint64
	dbLevels       map[string]uint64
	traces         float64
	p95, dbP95     float64
	filtered, may  bool
	patternsByServ map[ServicePattern]int
	pairs          map[ValuePair]uint64
	apiFields      map[string]string
}

func answersOf(t *testing.T, object []byte) writerAnswers {
	t.Helper()
	store := storage.NewFS(t.TempDir())
	require.NoError(t, store.Put(context.Background(), "seg", object))
	rd, loc := NewReader(store), ObjectLocation{Key: "seg"}
	records, footer, _, err := rd.Records(context.Background(), loc)
	require.NoError(t, err)
	a := writerAnswers{rowGroups: footer.RowGroups, records: map[string]int{}, zoneMaps: footer.ZoneMaps,
		traces: footer.DistinctCounts[ColTraceID].Estimate(), p95: footer.Distributions[ColDurationMs].Quantile(0.95),
		dbP95: footer.ForService("db").Distributions[ColDurationMs].Quantile(0.95), patternsByServ: footer.ServicePatternCounts,
		pairs: footer.ForService("api").ValuePairs()}
	for _, r := range records { // the order of rows is the writer's to choose
		a.records[fmt.Sprint(r)]++
	}
	a.levels, _ = footer.ValueCounts(ColLevel)
	a.dbLevels, _ = footer.ForService("db").ValueCounts(ColLevel)
	fields, _ := footer.ForService("api").Fields()
	a.apiFields = describeFields(fields)
	a.filtered, a.may, _, err = rd.MayContain(context.Background(), loc, footer, ColTraceID, "t-42")
	require.NoError(t, err)
	return a
}

// TestWriterHeapPerValue bounds what a Writer holds for each further distinct value it is
// written, since a merged hour of request ids is millions of values: it measured 21 bytes
// a value holding their hashes in a slice, 26 in a map, and 85 holding the values, which ran
// a compactor out of 1 GiB.
func TestWriterHeapPerValue(t *testing.T) {
	const values = 400_000
	cfg := BuilderConfig{Tenant: "acme", Stream: Stream{Service: "api"}, CreatedAt: time.Date(2026, 7, 18, 13, 0, 0, 0, time.UTC)}
	row := func(i int) Record {
		return LogRecord(cfg.CreatedAt.Add(-time.Duration(i)*time.Millisecond), "info", "request done",
			map[string]string{ColService: "api", "request_id": fmt.Sprintf("%08x-1111-2222-3333-%012x", i, i)})
	}
	types := map[string]value.Type{}
	for _, c := range row(0).Columns {
		types[c.Name] = c.Type
	}
	w := NewWriter(cfg, types)
	w.(*streamWriter).cells = 1000 * len(types) // groups too small for the rows held to count
	held := func(from int) int64 {              // after writing values more, less the rows encoded
		for i := from; i < from+values; i++ {
			require.NoError(t, w.Append(row(i), ParsedPattern{PatternID: "p1", Pattern: "request done"}))
		}
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return int64(m.HeapAlloc) - int64(len(w.(*streamWriter).body))
	}
	first := held(0)
	perValue := (held(values) - first) / values
	t.Logf("%d bytes a value", perValue)
	assert.LessOrEqual(t, perValue, int64(24))
}
