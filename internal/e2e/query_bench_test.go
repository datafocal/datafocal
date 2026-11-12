package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

var queryCopies = flag.Int("query.copies", 5, "hour-apart copies of the seeded logs BenchmarkQuery queries")

// BenchmarkQuery runs the stack benchmark's query archetypes, warm, through the querier's
// HTTP handler over -query.copies copies collected once into the fs store, and the wide
// archetypes over the wide scenario: the read path without docker, MinIO or a process per
// query, for iterating on it.
func BenchmarkQuery(b *testing.B) {
	dir := b.TempDir()
	corpus := collectCorpus(b, dir, *queryCopies)
	benchArchetypes(b, storage.NewFS(filepath.Join(dir, "store")), Archetypes(corpus))
	wide := filepath.Join(dir, "wide")
	_, err := seedcorpus.Build(context.Background(), wide, seedcorpus.ScenarioWide)
	require.NoError(b, err)
	benchArchetypes(b, storage.NewFS(wide), wideArchetypes())
}

func benchArchetypes(b *testing.B, store storage.Store, archetypes []Archetype) {
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store, Summary: &summary.Config{}})
	require.NoError(b, app.Refresh(context.Background()))
	handler := app.Handler()
	for _, a := range archetypes {
		body, err := json.Marshal(a.Request)
		require.NoError(b, err)
		b.Run(a.Name, func(b *testing.B) {
			b.ReportAllocs()
			var meta query.Meta
			for b.Loop() {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+a.Tool, bytes.NewReader(body)))
				var env query.Envelope[json.RawMessage]
				require.NoError(b, json.Unmarshal(rec.Body.Bytes(), &env))
				require.NotEmptyf(b, env.Meta.Snapshot, "%s refused: %s", a.Tool, rec.Body.Bytes())
				meta = env.Meta
			}
			b.ReportMetric(float64(meta.Cost.GETs), "GETs/op")
			b.ReportMetric(float64(meta.Tokens), "tokens/op")
			b.ReportMetric(float64(meta.Cost.BytesRead), "bytes-read/op")
		})
	}
}

// wideArchetypes is a rank_fields, an aggregate and a query over the whole wide scenario,
// the calls whose memory followed its column count rather than its values.
func wideArchetypes() []Archetype {
	start := seedcorpus.WideStart()
	all := selector.TimeSpec{From: rfc3339(start), To: rfc3339(start.Add(seedcorpus.WideSegments * 10 * time.Second))}
	return []Archetype{
		{"wide rank_fields: every field", "rank_fields", tools.FieldsRequest{Selector: selector.Selector{Time: all}}},
		{"wide aggregate: count by service, 1m bins", selector.StageAggregate, tools.AggregateRequest{
			Selector: selector.Selector{Time: all}, Measure: "count", GroupBy: segment.ColService, Bins: "1m",
		}},
		{"wide query: every line", "query", tools.QueryRequest{Q: `{service="agent"}`, Time: all}},
	}
}

// collectCorpus collects copies of the seeded logs, ending at the last whole hour as the
// stack benchmark's do, into dir's fs store, taking a thousand lines from each file in
// turn so segments mix services as a collector tailing them all writes them.
func collectCorpus(b testing.TB, dir string, copies int) Corpus {
	b.Helper()
	start, _ := seedcorpus.IncidentWindow()
	end := time.Now().UTC().Truncate(time.Hour)
	shift := end.Add(-time.Duration(copies) * time.Hour).Sub(start)
	logs := filepath.Join(dir, "logs")
	_, err := seedcorpus.WriteLogs(logs, seedcorpus.ScenarioAll, copies, shift)
	require.NoError(b, err)

	store := storage.NewFS(filepath.Join(dir, "store"))
	cfg := collector.DefaultConfig()
	cfg.Tenant = tenant
	c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, tenant)})
	require.NoError(b, err)
	resolver := identity.New("bench-node")
	paths, err := filepath.Glob(filepath.Join(logs, "*.log"))
	require.NoError(b, err)
	corpus := Corpus{Start: start.Add(shift), Copies: copies}
	ctx := context.Background()
	files := make([]*bufio.Scanner, len(paths))
	parsers := make([]logformat.Parser, len(paths))
	for i, path := range paths {
		f, err := os.Open(path)
		require.NoError(b, err)
		b.Cleanup(func() { _ = f.Close() }) // read only
		files[i] = bufio.NewScanner(f)
		parsers[i], err = logformat.New(logformat.Config{Format: "json"})
		require.NoError(b, err)
	}
	for more := true; more; {
		more = false
		for i, f := range files {
			for n := 0; n < 1000 && f.Scan(); n++ {
				more = true
				r, ok := parsers[i].Parse(f.Text())
				require.True(b, ok, "a seeded line is one JSON record")
				noteQueryInputs(&corpus, r)
				_, err := c.Collect(ctx, resolver.Resolve(paths[i], r))
				require.NoError(b, err)
			}
			require.NoError(b, f.Err())
		}
	}
	_, err = c.Flush(ctx)
	require.NoError(b, err)
	return corpus
}

// noteQueryInputs takes the corpus's first gateway trace_id and its symptom's pattern
// from the records as they pass.
func noteQueryInputs(c *Corpus, r segment.Record) {
	values := map[string]string{}
	for _, col := range r.Columns {
		values[col.Name] = col.Value.String
	}
	if c.TraceID == "" && values[segment.ColService] == "gateway" && values[segment.ColTraceID] != "" {
		c.TraceID = values[segment.ColTraceID]
	}
	if msg := values[segment.ColMessage]; c.SymptomPattern == "" && strings.HasPrefix(msg, "slow query ") {
		c.SymptomPattern = pattern.ID(pattern.Mask(msg))
	}
}
