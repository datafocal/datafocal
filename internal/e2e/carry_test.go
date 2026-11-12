package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// aggregate answers req on the corpus in dir with the default derived columns.
func aggregate(t *testing.T, dir string, req tools.AggregateRequest) []tools.Series {
	t.Helper()
	derived, err := plan.NewDerived(querier.DefaultDerived())
	require.NoError(t, err)
	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store, Derived: derived})
	body, err := json.Marshal(req)
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/aggregate", bytes.NewReader(body)))
	var env query.Envelope[tools.AggregateResponse]
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env), rec.Body.String())
	return env.Data.Series
}

// A Codex session's usage lines take their turn's model from its turn_context line, so the
// default cost_usd prices them, and never sums a token_count event's running usage.
func TestCodexUsageIsPricedByItsTurnsModel(t *testing.T) {
	dir := t.TempDir()
	at := time.Now().UTC().Add(-time.Hour).Format("2006-01-02T15:04")
	var lines strings.Builder
	for _, l := range []string{
		`"type":"turn_context","payload":{"turn_id":"t1","model":"gpt-5.1-codex","cwd":"/workspace"}`,
		`"type":"token_usage_record","payload":{"turn_id":"t1","usage":{"input_tokens":12000,"cached_input_tokens":8000,"output_tokens":500}}`,
		`"type":"event_msg","payload":{"type":"token_count","turn_id":"t1","info":{"last_token_usage":{"input_tokens":12000,"cached_input_tokens":8000,"output_tokens":500},"total_token_usage":{"input_tokens":12000,"cached_input_tokens":8000,"output_tokens":500}}}`,
		`"type":"token_usage_record","payload":{"turn_id":"t1","usage":{"input_tokens":3000,"output_tokens":200}}`,
		`"type":"turn_context","payload":{"turn_id":"t2","model":"gpt-5.1-codex","cwd":"/workspace"}`,
		`"type":"token_usage_record","payload":{"turn_id":"t2","usage":{"input_tokens":20000,"cached_input_tokens":15000,"output_tokens":1000}}`,
		`"type":"event_msg","payload":{"type":"token_count","turn_id":"t2","info":{"last_token_usage":{"input_tokens":20000,"cached_input_tokens":15000,"output_tokens":1000},"total_token_usage":{"input_tokens":35000,"cached_input_tokens":23000,"output_tokens":1700}}}`,
	} {
		lines.WriteString(`{"timestamp":"` + at + `:00Z",` + l + "}\n")
	}
	logs := filepath.Join(dir, "rollout.jsonl")
	require.NoError(t, os.WriteFile(logs, []byte(lines.String()), 0o644))
	// Each usage line's uncached input, cached input and output at 1.25, 0.125 and 10 USD per million.
	want := ((12000-8000)*1.25 + 8000*0.125 + 500*10 + 3000*1.25 + 200*10 + (20000-15000)*1.25 + 15000*0.125 + 1000*10) / 1e6

	store := storage.NewFS(dir)
	cfg := collector.DefaultConfig()
	cfg.Tenant = tenant
	c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, tenant)})
	require.NoError(t, err)
	in, err := ingest.New(ingest.Config{StateDir: filepath.Join(dir, "state"), FlushInterval: time.Hour,
		Sources: []ingest.Source{{Path: logs, Parse: logformat.Config{Format: "json"}}}}, c)
	require.NoError(t, err)
	require.NoError(t, in.Drain(context.Background()))

	series := aggregate(t, dir, tools.AggregateRequest{Measure: "sum:cost_usd", GroupBy: "service", Bins: "all",
		Selector: selector.Selector{Time: selector.TimeSpec{From: rfc3339(time.Now().Add(-2 * time.Hour)), To: rfc3339(time.Now())}}})
	require.Len(t, series, 1)
	require.Len(t, series[0].Values, 1)
	assert.InDelta(t, want, *series[0].Values[0], 1e-12)
}

// Each factory session's failed tool results, counted by the tool name each result is
// carried from its call, are the generator's.
func TestToolErrorsPerToolAreTheFactorys(t *testing.T) {
	dir, built := factoryCorpus(t)
	from, to := seedcorpus.FactoryWindow()
	failed := int64(0)
	for session, truth := range built.Truth.Sessions {
		series := aggregate(t, dir, tools.AggregateRequest{Measure: "count", GroupBy: "message.content.name", Bins: "all",
			Selector: selector.Selector{Time: selector.TimeSpec{From: rfc3339(from), To: rfc3339(to)}, Where: []selector.Predicate{
				{Field: "message.content.is_error", Op: selector.OpEq, Value: "true"},
				{Field: seedcorpus.SessionColumn, Op: selector.OpEq, Value: session},
			}}})
		got := map[string]int64{}
		for _, s := range series {
			got[s.Key] = int64(*s.Values[0])
			failed += got[s.Key]
		}
		assert.Equal(t, truth.ToolErrors, got, fmt.Sprintf("session %s", session))
	}
	assert.NotZero(t, failed)
}

// The p95 and total of the factory's turns, each the span of the lines sharing a prompt id,
// and of its tool calls by tool, each the span of a call and its result, are the
// generator's.
func TestTurnAndToolLatencyAreTheFactorys(t *testing.T) {
	dir, built := factoryCorpus(t)
	from, to := seedcorpus.FactoryWindow()
	turn, call := seedcorpus.PromptColumn, "message.content.id"
	// ms holds each turn under "" and each call under its tool, as the rollups key them.
	ms := map[string]map[string][]float64{turn: {}, call: {}}
	for _, truth := range built.Truth.Sessions {
		for _, v := range truth.TurnMs {
			ms[turn][""] = append(ms[turn][""], float64(v))
		}
		for tool, vs := range truth.ToolMs {
			for _, v := range vs {
				ms[call][tool] = append(ms[call][tool], float64(v))
			}
		}
	}
	measures := map[string][]selector.MeasureSpec{turn: {{Measure: "span:" + turn}},
		call: {{Measure: "span:" + call}, {Measure: "first:message.content.name"}}}
	for by, each := range measures {
		for _, rollup := range []string{"p95", "sum"} {
			req := tools.AggregateRequest{GroupBy: by, Measures: each, Bins: "all", Limit: 10000,
				Selector: selector.Selector{Time: selector.TimeSpec{From: rfc3339(from), To: rfc3339(to)}},
				Rollup:   &selector.Rollup{Measure: rollup + ":values[0]", Bins: "all"}}
			if by == call {
				req.Rollup.GroupBy = "label[1]"
			}
			got, want := map[string]float64{}, map[string]float64{}
			for _, s := range aggregate(t, dir, req) {
				got[s.Key] = *s.Values[0]
			}
			for key, vs := range ms[by] {
				want[key] = series.QuantileOf(vs, 0.95)
				if rollup == "sum" {
					want[key] = 0
					for _, v := range vs {
						want[key] += v
					}
				}
			}
			assert.Equal(t, want, got, "%s of %s", rollup, by)
		}
	}
}
