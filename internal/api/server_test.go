package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
)

// TestServerBindsEachToolToItsMethod posts every route through a fake Tools: the body has
// to reach the matching method decoded, and that method's envelope has to come back.
func TestServerBindsEachToolToItsMethod(t *testing.T) {
	froms := map[string]string{}
	meta := func(tool, from string) query.Meta {
		froms[tool] = from
		return query.Meta{Snapshot: tool}
	}
	fake := tools.Fake{
		AggregateFn: func(r tools.AggregateRequest) (query.Envelope[tools.AggregateResponse], error) {
			return query.Envelope[tools.AggregateResponse]{Meta: meta("aggregate", r.Selector.Time.From)}, nil
		},
		AttributeWritesFn: func(r tools.AttributeWritesRequest) (query.Envelope[tools.AttributeWritesResponse], error) {
			return query.Envelope[tools.AttributeWritesResponse]{Meta: meta("attribute_writes", r.Selector.Time.From)}, nil
		},
		CorrelateFn: func(r tools.CorrelateRequest) (query.Envelope[tools.CorrelateResponse], error) {
			return query.Envelope[tools.CorrelateResponse]{Meta: meta("correlate", r.Selector.Time.From)}, nil
		},
		ExplainSpikeFn: func(r tools.ExplainSpikeRequest) (query.Envelope[tools.ExplainSpikeResponse], error) {
			return query.Envelope[tools.ExplainSpikeResponse]{Meta: meta("explain_spike", r.Selector.Time.From)}, nil
		},
		FindAnomaliesFn: func(r tools.FindAnomaliesRequest) (query.Envelope[tools.FindAnomaliesResponse], error) {
			return query.Envelope[tools.FindAnomaliesResponse]{Meta: meta("find_anomalies", r.Selector.Time.From)}, nil
		},
		FocusFn: func(r tools.FocusRequest) (query.Envelope[tools.FocusResponse], error) {
			return query.Envelope[tools.FocusResponse]{Meta: meta("focus", r.Selector.Time.From)}, nil
		},
		SearchLogsFn: func(r tools.SearchRequest) (query.Envelope[tools.SearchResponse], error) {
			return query.Envelope[tools.SearchResponse]{Meta: meta("search_logs", r.Selector.Time.From)}, nil
		},
		RankFieldsFn: func(r tools.FieldsRequest) (query.Envelope[tools.FieldsResponse], error) {
			return query.Envelope[tools.FieldsResponse]{Meta: meta("rank_fields", r.Selector.Time.From)}, nil
		},
		PatternsFn: func(r tools.PatternsRequest) (query.Envelope[tools.PatternsResponse], error) {
			return query.Envelope[tools.PatternsResponse]{Meta: meta("list_patterns", r.Selector.Time.From)}, nil
		},
		GetTraceFn: func(r tools.TraceRequest) (query.Envelope[tools.TraceResponse], error) {
			return query.Envelope[tools.TraceResponse]{Meta: meta("get_trace", r.Time.From)}, nil
		},
		ServiceGraphFn: func(r tools.ServiceGraphRequest) (query.Envelope[tools.ServiceGraphResponse], error) {
			return query.Envelope[tools.ServiceGraphResponse]{Meta: meta("service_graph", r.Selector.Time.From)}, nil
		},
		RCAWalkFn: func(r tools.RCAWalkRequest) (query.Envelope[tools.RCAWalkResponse], error) {
			return query.Envelope[tools.RCAWalkResponse]{Meta: meta("rca_walk", r.Selector.Time.From)}, nil
		},
	}
	srv := httptest.NewServer(NewServer(fake, Limits{}).Handler())
	defer srv.Close()

	for _, tool := range []string{"aggregate", "attribute_writes", "correlate", "explain_spike", "find_anomalies",
		"focus", "search_logs", "rank_fields", "list_patterns", "get_trace", "service_graph", "rca_walk"} {
		t.Run(tool, func(t *testing.T) {
			body := `{"selector":{"time":{"from":"now-1h"}}}`
			if tool == "get_trace" {
				body = `{"time":{"from":"now-1h"}}`
			}
			resp, err := http.Post(srv.URL+"/v1/"+tool, "application/json", strings.NewReader(body))
			require.NoError(t, err)
			defer func() { _ = resp.Body.Close() }()
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var env struct {
				Meta query.Meta `json:"meta"`
			}
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
			assert.Equal(t, tool, env.Meta.Snapshot)
			assert.Equal(t, "now-1h", froms[tool])
		})
	}
}

// TestQueryRequestLogNamesItsPanel: a view panel's query call logs its view, its panel and
// the names of its variables, never their values.
func TestQueryRequestLogNamesItsPanel(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	srv := httptest.NewServer(NewServer(tools.Fake{}, Limits{}).Handler())

	// The second call is answered from the cache, and logs the same.
	for range 2 {
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/query",
			strings.NewReader(`{"q":"{service=\"$service\", level=\"$level\"}","vars":{"service":"checkout","level":"*"}}`))
		require.NoError(t, err)
		req.Header.Set("X-DataFocal-View", "checkout/2")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	srv.Close() // waits for the handlers, so the lines are written

	dec := json.NewDecoder(&logs)
	for range 2 {
		var line map[string]any
		require.NoError(t, dec.Decode(&line))
		got := map[string]any{"view": line["view"], "panel": line["panel"], "vars": line["vars"], "vars_all": line["vars_all"]}
		assert.Equal(t, map[string]any{"view": "checkout", "panel": "2", "vars": []any{"service"}, "vars_all": []any{"level"}}, got, "cache %v", line["cache"])
	}
}
