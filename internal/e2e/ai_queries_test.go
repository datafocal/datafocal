package e2e

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/llm"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
)

// The same queries read each source through the shared ingest columns.
func TestAIViewQueries(t *testing.T) {
	for _, source := range []struct {
		name                                                                       string
		input, output, reasoning, cache, write, spend, tools, runs, skills, memory float64
	}{
		{"claude-code", 9, 27, 0, 34650, 5161, 0.175713, 2, 3, 0, 0}, {"codex", 3, 149, 0, 0, 14438, 0.037591, 0, 4, 0, 0}, {"pi", 2775, 141, 0, 0, 0, 0.0174, 1, 3, 0, 0},
		{"gemini-cli", 23407, 386, 269, 16286, 0, 0.0136758, 5, 3, 0, 0}, {"gateway", 10, 20, 0, 0, 0, 0.000225, 0, 1, 0, 0}, {"otel", 0, 0, 0, 0, 0, 0, 0, 8, 0, 0}, {"copilot-cli", 8363, 1221, 1088, 20608, 0, 0.00504795, 3, 9, 1, 0},
		{"otel-api", 18, 169, 0, 10974, 11160, 0.0242804, 0, 2, 0, 0},
		{"otel-spans", 6, 206, 0, 0, 0, 0.004144, 0, 2, 0, 0},
		{"claude-code-memory", 4, 100, 0, 4100, 150, 0.003586, 2, 3, 0, 3},
		{"claude-code-compaction", 6, 1721, 0, 430958, 34353, 0.2924006, 0, 5, 0, 0},
		{"otel-strands", 109, 158, 0, 0, 0, 0.00011687, 0, 1, 0, 0},
	} {
		t.Run(source.name, func(t *testing.T) {
			dir := ingestAIFixture(t, source.name)
			for _, q := range []struct {
				measure, group string
				want           float64
				exact          bool
			}{
				{"sum:input_tokens", "model", source.input, true}, {"sum:output_tokens", "model", source.output, true}, {"sum:reasoning_tokens", "model", source.reasoning, true}, {"sum:cache_read_tokens", "model", source.cache, true}, {"sum:cache_write_tokens", "model", source.write, true},
				{"sum:cost_usd", "model", source.spend, true}, {"count", "tool_name", source.tools, true}, {"distinct:tool_call_id", "skill_name", source.skills, true}, {"count", "memory_op", source.memory, true},
				{"sum:content_chars", "run_id", 0, false}, {"changes:tools_hash", "run_id", 0, true}, {"count", "run_id", source.runs, true},
			} {
				t.Run(q.measure+"/"+q.group, func(t *testing.T) {
					if strings.HasPrefix(source.name, "otel-") && source.name != "otel-strands" {
						q.exact = true
					}
					got := aggregate(t, dir, tools.AggregateRequest{Measure: q.measure, GroupBy: q.group, Bins: "all", Selector: selector.Selector{Time: selector.TimeSpec{From: "2020-01-01T00:00:00Z", To: "2030-01-01T00:00:00Z"}, Where: []selector.Predicate{{Field: q.group, Op: selector.OpExists}}}})
					if !q.exact {
						require.NotEmpty(t, got)
						require.NotNil(t, got[0].Values[0])
						assert.Positive(t, *got[0].Values[0])
					}
					if q.exact {
						total := seriesTotal(got)
						assert.InDelta(t, q.want, total, 1e-9)
					}
				})
			}
		})
	}
}

// Each source's quota reads through the shared quota columns: the highest share of each
// window, and the lines hitting a limit.
func TestAIQuotaQueries(t *testing.T) {
	for _, source := range []struct {
		name string
		want map[string]float64
	}{
		{"copilot-cli", map[string]float64{"max:quota_chat_used_pct": 9.5, "max:quota_completions_used_pct": 2, "count:quota_limited": 0}},
		{"codex-quota", map[string]float64{"max:quota_5h_used_pct": 100, "max:quota_7d_used_pct": 46, "count:quota_limited": 1, "last:credits_remaining": 0}},
		{"claude-code-statusline", map[string]float64{"max:quota_5h_used_pct": 24, "max:quota_7d_used_pct": 41.3, "count:quota_limited": 0}},
	} {
		t.Run(source.name, func(t *testing.T) {
			dir := ingestAIFixture(t, source.name)
			got := map[string]float64{}
			for measure := range source.want {
				m, field, _ := strings.Cut(measure, ":")
				req := tools.AggregateRequest{Measure: m + ":" + field, GroupBy: "service", Bins: "all", Selector: selector.Selector{Time: selector.TimeSpec{From: "2020-01-01T00:00:00Z", To: "2030-01-01T00:00:00Z"}}}
				if m == "count" {
					req.Measure, req.Selector.Where = "count", []selector.Predicate{{Field: field, Op: selector.OpExists}}
				}
				got[measure] = seriesTotal(aggregate(t, dir, req))
			}
			assert.Equal(t, source.want, got)
		})
	}
}

// A compaction reads through the shared columns: the tokens it removed, and the first call
// after it with the cache it wrote again.
func TestAICompactionQueries(t *testing.T) {
	dir := ingestAIFixture(t, "claude-code-compaction")
	got := map[string]float64{}
	for _, measure := range []string{"sum:compaction_tokens", "count", "sum:cache_write_tokens"} {
		req := tools.AggregateRequest{Measure: measure, GroupBy: "run_id", Bins: "all", Selector: selector.Selector{Time: selector.TimeSpec{From: "2020-01-01T00:00:00Z", To: "2030-01-01T00:00:00Z"}}}
		if measure != "sum:compaction_tokens" {
			req.Selector.Where = []selector.Predicate{{Field: "after_compaction", Op: selector.OpExists}}
		}
		got[measure] = seriesTotal(aggregate(t, dir, req))
	}
	assert.Equal(t, map[string]float64{"sum:compaction_tokens": 346732, "count": 1, "sum:cache_write_tokens": 30056}, got)
}

func TestDiscoveredLLMColumns(t *testing.T) {
	tables := map[string]map[string]string{"price_in": {"gpt-5.1-codex": "2"}, "price_out": {"gpt-5.1-codex": "4"}, "price_cache_write": {}, "price_cache_read": {"gpt-5.1-codex": "0.2"}}
	for _, tc := range []struct {
		name, want string
		row        map[string]string
	}{
		{"model by value", "0.0002", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100"}},
		{"unpriced usage", "", map[string]string{"model": "unknown", "input_tokens": "100"}},
		{"a bare word is no model", "", map[string]string{"trigger": "auto", "input_tokens": "100"}},
		{"logged cost wins", "8", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100", "cost_usd": "8"}},
		{"zero USD cost wins", "0", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100", "cost_usd": "0"}},
		{"unitless cost is not USD", "0.0002", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100", "data.cost": "1"}},
		{"inclusive cache", "0.000184", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100", "cached_input_tokens": "20", "output_tokens": "5"}},
		{"exclusive cache", "0.000224", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100", "cache_read_input_tokens": "20", "output_tokens": "5"}},
		{"cache only", "0.000004", map[string]string{"deployment": "gpt-5.1-codex", "cached_input_tokens": "20"}},
		{"reasoning only output", "0.00022", map[string]string{"deployment": "gpt-5.1-codex", "input_tokens": "100", "reasoning_tokens": "5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defs, _ := querier.DefaultDerived()
			d, err := plan.NewDerived(defs, tables)
			require.NoError(t, err)
			var record segment.Record
			for key, text := range tc.row {
				col := segment.Column{Name: key, Type: value.TypeString, Value: value.Value{String: text}}
				if n, err := strconv.ParseFloat(text, 64); err == nil {
					col.Type = value.TypeFloat
					col.Value = value.Value{Float: n}
				}
				record.Columns = append(record.Columns, col)
			}
			record = llm.Normalize(record)
			got, ok := d.Column("cost_usd", 1, func(name string, _ int) ([]byte, bool) {
				for _, col := range record.Columns {
					if col.Name == name {
						return []byte(value.Format(col.Type, col.Value)), true
					}
				}
				return nil, false
			})
			assert.True(t, ok)
			want := []byte(tc.want)
			if tc.want == "" {
				want = nil
			}
			assert.Equal(t, [][]byte{want}, got)
		})
	}
}

func BenchmarkAISpend(b *testing.B) {
	dir := b.TempDir()
	_, err := seedcorpus.Build(context.Background(), dir, seedcorpus.ScenarioFactory)
	require.NoError(b, err)
	derived, err := plan.NewDerived(querier.DefaultDerived())
	require.NoError(b, err)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Store: store, Catalog: catalog.New(store, tenant), Derived: derived})
	from, to := seedcorpus.FactoryWindow()
	req := tools.AggregateRequest{Measure: "sum:cost_usd", GroupBy: "service", Bins: "all", Selector: selector.Selector{Time: selector.TimeSpec{From: rfc3339(from), To: rfc3339(to)}}}
	_, err = app.Tools().Aggregate(context.Background(), req)
	require.NoError(b, err)
	b.ReportAllocs()
	for b.Loop() {
		got, err := app.Tools().Aggregate(context.Background(), req)
		require.NoError(b, err)
		require.NotEmpty(b, got.Data.Series)
	}
}

func ingestAIFixture(t *testing.T, name string) string {
	t.Helper()
	dir := t.TempDir()
	data, err := os.ReadFile(filepath.Join("testdata", "ai", name+".jsonl"))
	require.NoError(t, err)
	logs := filepath.Join(dir, "source.jsonl")
	require.NoError(t, os.WriteFile(logs, data, 0o644))
	store := storage.NewFS(dir)
	cfg := collector.DefaultConfig()
	cfg.Tenant = tenant
	c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, tenant), Clock: fixedClock{}})
	require.NoError(t, err)
	in, err := ingest.New(ingest.Config{StateDir: filepath.Join(dir, "state"), FlushInterval: time.Hour, Sources: []ingest.Source{{Path: logs, Parse: logformat.Config{Format: "json"}}}}, c)
	require.NoError(t, err)
	require.NoError(t, in.Drain(context.Background()))
	return dir
}

// ingestAIFixtures ingests every source in testdata/ai into one corpus, each from a file of
// its own as an agent writes it.
func ingestAIFixtures(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	paths, err := filepath.Glob(filepath.Join("testdata", "ai", "*.jsonl"))
	require.NoError(t, err)
	var sources []ingest.Source
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		logs := filepath.Join(dir, "logs", filepath.Base(path))
		require.NoError(t, os.MkdirAll(filepath.Dir(logs), 0o755))
		require.NoError(t, os.WriteFile(logs, data, 0o644))
		sources = append(sources, ingest.Source{Path: logs, Parse: logformat.Config{Format: "json"}})
	}
	store := storage.NewFS(dir)
	cfg := collector.DefaultConfig()
	cfg.Tenant = tenant
	c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, tenant), Clock: fixedClock{}})
	require.NoError(t, err)
	// Named, since the fixture this builds would otherwise hold whichever machine ran it.
	in, err := ingest.New(ingest.Config{StateDir: filepath.Join(dir, "state"), Host: "framework", FlushInterval: time.Hour, Sources: sources}, c)
	require.NoError(t, err)
	require.NoError(t, in.Drain(context.Background()))
	return dir
}

func seriesTotal(series []tools.Series) float64 {
	total := 0.0
	for _, s := range series {
		for _, v := range s.Values {
			if v != nil {
				total += *v
			}
		}
	}
	return total
}
