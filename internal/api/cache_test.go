package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
)

// countingTools answers aggregate with the snapshot it pins, search_logs over a tenth of the
// answer cache and get_trace with a sixteenth of it, and fails list_patterns, counting each call.
func countingTools(calls *atomic.Int64) tools.Fake {
	return tools.Fake{
		SnapshotFn: func() (string, error) { return "e1.s1", nil },
		AggregateFn: func(req tools.AggregateRequest) (query.Envelope[tools.AggregateResponse], error) {
			calls.Add(1)
			return query.Envelope[tools.AggregateResponse]{Meta: query.Meta{Snapshot: req.Snapshot, Cost: query.Cost{GETs: 3}}}, nil
		},
		SearchLogsFn: func(tools.SearchRequest) (query.Envelope[tools.SearchResponse], error) {
			calls.Add(1)
			return query.Envelope[tools.SearchResponse]{Data: tools.SearchResponse{Mode: strings.Repeat("x", AnswerCacheBytes/10)}}, nil
		},
		GetTraceFn: func(req tools.TraceRequest) (query.Envelope[tools.TraceResponse], error) {
			calls.Add(1)
			return query.Envelope[tools.TraceResponse]{Data: tools.TraceResponse{Value: req.Value, Field: strings.Repeat("x", AnswerCacheBytes/16)}}, nil
		},
		PatternsFn: func(tools.PatternsRequest) (query.Envelope[tools.PatternsResponse], error) {
			calls.Add(1)
			return query.Envelope[tools.PatternsResponse]{}, errors.New("store unavailable")
		},
		ListSLOsFn: func(tools.ListSLOsRequest) (query.Envelope[tools.ListSLOsResponse], error) {
			calls.Add(1)
			return query.Envelope[tools.ListSLOsResponse]{}, nil
		},
	}
}

func post(h http.Handler, ctx context.Context, tool, args string) string {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/"+tool, strings.NewReader(args)))
	return rec.Body.String()
}

// TestAnswerCache: through either transport, a repeat at one snapshot is served from the cache,
// its data byte-identical, and the request line says how each call was served.
func TestAnswerCache(t *testing.T) {
	cases := []struct {
		name, tool string
		calls      [2]string
		want       []string
		computed   int64
		sameKey    bool
	}{
		{"a repeat is a hit", "aggregate", [2]string{`{"measure":"count"}`, `{"measure":"count"}`}, []string{"miss <nil>", "hit <nil>"}, 1, true},
		{"key order is a hit", "aggregate", [2]string{`{"measure":"count","bins":"5"}`, `{"bins":"5","measure":"count"}`}, []string{"miss <nil>", "hit <nil>"}, 1, true},
		{"other args miss", "aggregate", [2]string{`{"measure":"count"}`, `{"measure":"rate"}`}, []string{"miss <nil>", "miss <nil>"}, 2, false},
		{"another snapshot misses", "aggregate", [2]string{`{"snapshot":"e1.s1"}`, `{"snapshot":"e2.s2"}`}, []string{"miss <nil>", "miss <nil>"}, 2, false},
		{"a failure is not stored", "list_patterns", [2]string{`{}`, `{}`}, []string{"not_stored failed", "not_stored failed"}, 2, true},
		{"an oversize answer is not stored", "search_logs", [2]string{`{}`, `{}`}, []string{"not_stored oversize", "not_stored oversize"}, 2, true},
		{"a NoCache tool is off", "list_slos", [2]string{`{}`, `{}`}, []string{"off <nil>", "off <nil>"}, 2, false},
	}
	for _, leg := range []string{"http", "mcp"} {
		for _, tc := range cases {
			t.Run(leg+"/"+tc.name, func(t *testing.T) {
				var computed atomic.Int64
				srv := NewServer(countingTools(&computed), Limits{})
				var logs bytes.Buffer
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
				t.Cleanup(func() { slog.SetDefault(prev) })
				call := func(args string) string { return post(srv.Handler(), t.Context(), tc.tool, args) }
				if leg == "mcp" {
					cs := connectMCP(t, srv.Registry())
					call = func(args string) string {
						var m map[string]any
						require.NoError(t, json.Unmarshal([]byte(args), &m))
						text, _ := callTool(t, cs, tc.tool, m)
						return text
					}
				}
				answers := make([]query.Envelope[json.RawMessage], 2)
				for i, args := range tc.calls {
					_ = json.Unmarshal([]byte(call(args)), &answers[i])
				}
				var got, keys []string
				for dec := json.NewDecoder(&logs); dec.More(); {
					var line map[string]any
					require.NoError(t, dec.Decode(&line))
					got, keys = append(got, fmt.Sprint(line["cache"], " ", line["cache_reason"])), append(keys, fmt.Sprint(line["cache_key"]))
				}
				assert.Equal(t, tc.want, got)
				assert.Equal(t, tc.computed, computed.Load())
				assert.Equal(t, tc.sameKey, keys[0] == keys[1] && keys[0] != "<nil>", "cache_key is equal for one question")
				if tc.want[1] == "hit <nil>" {
					assert.Equal(t, string(answers[0].Data), string(answers[1].Data))
					answers[0].Meta.Cached, answers[0].Meta.Cost = true, query.Cost{}
					assert.Equal(t, answers[0].Meta, answers[1].Meta)
				}
			})
		}
	}
}

// TestAnswerCacheComputesOnce: 10 concurrent identical calls are one computation, and a waiter
// that gives up leaves the others their answer.
func TestAnswerCacheComputesOnce(t *testing.T) {
	entered, release := make(chan struct{}, 10), make(chan struct{})
	var computed atomic.Int64
	h := NewServer(tools.Fake{AggregateFn: func(tools.AggregateRequest) (query.Envelope[tools.AggregateResponse], error) {
		computed.Add(1)
		entered <- struct{}{}
		<-release
		return query.Envelope[tools.AggregateResponse]{Data: tools.AggregateResponse{Fields: []string{"service"}}}, nil
	}}, Limits{}).Handler()
	answers := make(chan string, 10)
	for range 10 {
		go func() { answers <- post(h, context.Background(), "aggregate", `{}`) }()
	}
	<-entered
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Contains(t, post(h, cancelled, "aggregate", `{}`), context.Canceled.Error())
	close(release)
	for range 10 {
		var env query.Envelope[tools.AggregateResponse]
		require.NoError(t, json.Unmarshal([]byte(<-answers), &env))
		assert.Equal(t, tools.AggregateResponse{Fields: []string{"service"}}, env.Data)
	}
	assert.Equal(t, int64(1), computed.Load())
}

// TestAnswerCacheStaysUnderItsCap: answers totalling twice the cap evict the oldest, keeping the
// bytes held, and the heap they grow, under it.
func TestAnswerCacheStaysUnderItsCap(t *testing.T) {
	var computed atomic.Int64
	srv := NewServer(countingTools(&computed), Limits{})
	live := func() int64 {
		runtime.GC() // twice, so the pooled encode buffers of the last answers are let go too
		runtime.GC()
		sample := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
		metrics.Read(sample)
		return int64(sample[0].Value.Uint64())
	}
	before := live()
	for i := range 32 {
		post(srv.Handler(), t.Context(), "get_trace", fmt.Sprintf(`{"value":"%d"}`, i))
	}
	assert.Less(t, live()-before, int64(AnswerCacheBytes)*11/10)
	stats := srv.Registry().AnswerCache()
	assert.LessOrEqual(t, stats.Bytes, int64(AnswerCacheBytes))
	assert.Equal(t, int64(32-stats.Entries), stats.Evictions)
	post(srv.Handler(), t.Context(), "get_trace", `{"value":"31"}`)
	post(srv.Handler(), t.Context(), "get_trace", `{"value":"0"}`)
	assert.Equal(t, int64(33), computed.Load(), "the newest is held and the oldest evicted")
}
