package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/config"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/web"
)

// sessionPath sends every request that already carries a session to path, so a
// session initialized on one MCP route is exercised on the other.
type sessionPath string

func (p sessionPath) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("Mcp-Session-Id") != "" {
		r = r.Clone(r.Context())
		r.URL.Path = string(p)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func TestMCPRoutesShareOneServer(t *testing.T) {
	dir := t.TempDir()
	ts := httptest.NewServer(querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), "local"), Store: storage.NewFS(dir)}).Handler())
	t.Cleanup(ts.Close)

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil).Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:             ts.URL + "/mcp",
		HTTPClient:           &http.Client{Transport: sessionPath("/mcp/")},
		DisableStandaloneSSE: true,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cs.Close() })

	_, err = cs.ListTools(ctx, nil)
	require.NoError(t, err)
}

func TestUIServedAtUI(t *testing.T) {
	dir := t.TempDir()
	h := querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), "local"), Store: storage.NewFS(dir)}).Handler()

	got := httptest.NewRecorder()
	h.ServeHTTP(got, httptest.NewRequest(http.MethodGet, "/ui/logs", nil))
	want := httptest.NewRecorder()
	web.Handler().ServeHTTP(want, httptest.NewRequest(http.MethodGet, "/logs", nil))
	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Body.String(), got.Body.String())
}

// TestRefreshLogsTheCaches: a call over an absolute window is kept and served again, one over a
// relative window never is, and a refresh logs one caches line counting the calls since the last.
func TestRefreshLogsTheCaches(t *testing.T) {
	dir := t.TempDir()
	app := querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), "local"), Store: storage.NewFS(dir)})
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	for _, window := range []string{"2026-07-18T14:00:00Z/2026-07-18T15:00:00Z", "2026-07-18T14:00:00Z/2026-07-18T15:00:00Z", "now-1h/now", "now-1h/now"} {
		from, to, _ := strings.Cut(window, "/")
		body := fmt.Sprintf(`{"selector":{"time":{"from":%q,"to":%q}},"measure":"count"}`, from, to)
		app.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/aggregate", strings.NewReader(body)))
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	refresh(ctx, app, time.Hour)
	served, caches := []string{}, []map[string]any{}
	var answerBytes any
	for dec := json.NewDecoder(&logs); dec.More(); {
		var line map[string]any
		require.NoError(t, dec.Decode(&line))
		delete(line, "time")
		switch line["msg"] {
		case "request":
			served = append(served, fmt.Sprint(line["cache"], " ", line["cache_reason"]))
			answerBytes = cmp.Or(answerBytes, line["answer_bytes"])
		case "caches":
			caches = append(caches, line)
		}
	}
	assert.Equal(t, []string{"miss <nil>", "hit <nil>", "not_stored clock", "not_stored clock"}, served)
	assert.Equal(t, []map[string]any{{"level": "INFO", "msg": "caches",
		"answer_entries": 1.0, "answer_bytes": answerBytes, "answer_cap_bytes": float64(api.AnswerCacheBytes), "answer_hits": 1.0,
		"answer_shared": 0.0, "answer_misses": 1.0, "answer_evictions": 0.0, "answer_not_stored": 2.0,
		"chunk_bytes": 0.0, "chunk_cap_bytes": float64(segment.ChunkCacheBytes), "chunk_hits": 0.0, "chunk_shared": 0.0,
		"chunk_reads": 0.0, "chunk_evictions": 0.0}}, caches)
}

// TestQuerierWritesNothing pins that the querier only reads, so any number of replicas may
// serve one tenant.
func TestQuerierWritesNothing(t *testing.T) {
	var puts []string
	put := func(key string, _ []byte) error { puts = append(puts, key); return nil }
	store := storage.Fake{PutFn: put, PutIfAbsentFn: put}
	cfg := summary.Config{}
	app := querier.Open(querier.Options{Catalog: catalog.New(store, "local"), Store: store, Summary: &cfg})
	require.NoError(t, app.Refresh(t.Context()))
	app.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/describe", nil))
	assert.Empty(t, puts)
}

// TestServerServesDebugVars: the server answers /debug/vars and /debug/pprof/heap itself, the
// process's memory with the heap its last collection left live, and every other path from its handler.
func TestServerServesDebugVars(t *testing.T) {
	srv := newServer(DefaultQuerierConfig(), http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	vars := httptest.NewRecorder()
	srv.Handler.ServeHTTP(vars, httptest.NewRequest(http.MethodGet, "/debug/vars", nil))
	var got map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(vars.Body.Bytes(), &got))
	assert.Contains(t, got, "heap_live_bytes")
	assert.Contains(t, got, "memstats")
	heap := httptest.NewRecorder()
	srv.Handler.ServeHTTP(heap, httptest.NewRequest(http.MethodGet, "/debug/pprof/heap?gc=1", nil))
	assert.Equal(t, http.StatusOK, heap.Code)
	other := httptest.NewRecorder()
	srv.Handler.ServeHTTP(other, httptest.NewRequest(http.MethodPost, "/v1/search_logs", nil))
	assert.Equal(t, http.StatusTeapot, other.Code)
}

// TestServerTakesTheConfiguredTimeouts: the server the querier starts sets the configured
// timeouts, and its deployment hands it the memory limit it keeps the heap under.
func TestServerTakesTheConfiguredTimeouts(t *testing.T) {
	cfg := DefaultQuerierConfig()
	cfg.ReadTimeout, cfg.WriteTimeout, cfg.IdleTimeout = config.Duration(time.Second), config.Duration(time.Minute), config.Duration(time.Hour)
	srv := newServer(cfg, http.NotFoundHandler())
	assert.Equal(t, []time.Duration{time.Second, time.Minute, time.Hour}, []time.Duration{srv.ReadTimeout, srv.WriteTimeout, srv.IdleTimeout})

	deployment, err := os.ReadFile("../../deploy/k8s/base/querier.yaml")
	require.NoError(t, err)
	assert.Contains(t, string(deployment), `
            - name: DATAFOCAL_MEMORY_LIMIT
              valueFrom:
                resourceFieldRef:
                  resource: limits.memory`)
}
