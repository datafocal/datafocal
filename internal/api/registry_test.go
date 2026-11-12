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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
)

func rawSchema() json.RawMessage {
	b, _ := json.Marshal(map[string]string{"type": "object"})
	return b
}

func TestRegistry_Register(t *testing.T) {
	reg := NewRegistry(nil)
	td := ToolDef{
		Name: "ping", Caching: NoCache,
		Doc:     "ping doc",
		Schema:  rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) { return "pong", nil },
	}
	reg.Register(td)

	tools := reg.Tools()
	if len(tools) != 1 {
		t.Fatalf("len(Tools()) = %d, want 1", len(tools))
	}
	if tools[0].Name != "ping" {
		t.Errorf("Tools()[0].Name = %q, want %q", tools[0].Name, "ping")
	}
}

func TestRegistry_Register_DuplicatePanics(t *testing.T) {
	assert.Panics(t, func() { NewRegistry(nil).Register(ToolDef{Name: "uncached"}) }, "a tool must say how it caches")
	reg := NewRegistry(nil)
	td := ToolDef{
		Name: "dup", Caching: NoCache,
		Doc:     "doc",
		Schema:  rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) { return nil, nil },
	}
	reg.Register(td)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate tool name, got none")
		}
	}()
	reg.Register(td)
}

func TestRegistry_Describe(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "mytool", Caching: NoCache,
		Doc:     "my doc",
		Schema:  rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) { return nil, nil },
	})

	desc := reg.Describe()
	if len(desc.Tools) != 1 {
		t.Fatalf("len(desc.Tools) = %d, want 1", len(desc.Tools))
	}
	if desc.Tools[0].Name != "mytool" {
		t.Errorf("desc.Tools[0].Name = %q, want %q", desc.Tools[0].Name, "mytool")
	}
	if desc.Tools[0].Doc != "my doc" {
		t.Errorf("desc.Tools[0].Doc = %q, want %q", desc.Tools[0].Doc, "my doc")
	}
}

func TestRegistry_HTTPHandler_ToolDispatch(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "calc", Caching: NoCache,
		Doc:    "calc doc",
		Schema: rawSchema(),
		Handler: func(_ context.Context, raw json.RawMessage) (any, error) {
			return map[string]string{"result": "42"}, nil
		},
	})

	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/calc", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["result"] != "42" {
		t.Errorf("body[result] = %q, want %q", body["result"], "42")
	}
}

func TestRegistry_HTTPHandler_Describe(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "foo", Caching: NoCache,
		Doc:     "foo doc",
		Schema:  rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) { return nil, nil },
	})

	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/describe")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var desc DescribeResult
	if err := json.NewDecoder(resp.Body).Decode(&desc); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(desc.Tools) != 1 || desc.Tools[0].Name != "foo" {
		t.Errorf("describe tools = %+v, want [{Name:foo ...}]", desc.Tools)
	}
}

func TestRegistry_HTTPHandler_APIError(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "failing", Caching: NoCache,
		Doc:    "doc",
		Schema: rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) {
			return nil, query.NewAPIError(query.ErrCodeUnboundedScan, "scan too wide")
		},
	})

	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/failing", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (APIError is in-body)", resp.StatusCode)
	}
	var apiErr query.APIError
	if err := json.NewDecoder(resp.Body).Decode(&apiErr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if apiErr.Code != query.ErrCodeUnboundedScan {
		t.Errorf("code = %q, want %q", apiErr.Code, query.ErrCodeUnboundedScan)
	}
}

func TestRegistry_HTTPHandler_InvalidRequestError(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "bad", Caching: NoCache,
		Doc:    "doc",
		Schema: rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) {
			return nil, query.Invalidf("bad input: %s", "missing field")
		},
	})

	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/bad", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestRegistry_HTTPHandler_InternalError(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "boom", Caching: NoCache,
		Doc:    "doc",
		Schema: rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) {
			return nil, fmt.Errorf("unexpected storage failure")
		},
	})

	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/boom", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

func TestRegistry_HTTPHandler_MethodNotAllowed(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "widget", Caching: NoCache,
		Doc:     "doc",
		Schema:  rawSchema(),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) { return nil, nil },
	})

	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/v1/widget")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestRegistry_HTTPHandler_Healthz(t *testing.T) {
	reg := NewRegistry(nil)

	healthz := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}

	ts := httptest.NewServer(reg.HTTPHandler(healthz))
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

// TestRequestLogCarriesTheOutcome checks a failed request's completion event carries its
// duration, its id and the error it answered with, at error since the server failed it, and
// that a line the tool logged while serving it carries the same id.
func TestRequestLogCarriesTheOutcome(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(LogHandler(slog.NewJSONHandler(&logs, nil))))
	t.Cleanup(func() { slog.SetDefault(prev) })
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "boom", Caching: NoCache,
		Doc:    "doc",
		Schema: rawSchema(),
		Handler: func(ctx context.Context, _ json.RawMessage) (any, error) {
			slog.InfoContext(ctx, "step")
			return nil, errors.New("unexpected storage failure")
		},
	})
	ts := httptest.NewServer(reg.HTTPHandler(func(w http.ResponseWriter, _ *http.Request) {}))

	resp, err := http.Post(ts.URL+"/v1/boom", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	ts.Close() // waits for the handler, so the event is written

	var step, event map[string]any
	dec := json.NewDecoder(&logs)
	require.NoError(t, dec.Decode(&step))
	require.NoError(t, dec.Decode(&event))
	assert.IsType(t, float64(0), event["duration_ms"])
	assert.NotEmpty(t, event["request_id"])
	assert.Equal(t, event["request_id"], step["request_id"], "the step is the request's")
	for _, key := range []string{"time", "msg", "duration_ms", "request_id"} {
		delete(event, key)
	}
	want := map[string]any{
		"level": "ERROR", "method": "POST", "path": "/v1/boom", "status": float64(500),
		"gets": float64(0), "bytes_read": float64(0), "footers_decoded": float64(0), "segments": float64(0),
		"error_code": query.ErrCodeInternal, "error": "unexpected storage failure",
		"cache": "off", "chunks_hit": float64(0), "chunks_shared": float64(0), "chunks_read": float64(0),
	}
	assert.Equal(t, want, event)
}

// TestLimitInFlight: with two requests held, a third is refused at once with 429 while
// /healthz still answers, and once one finishes the next is served.
func TestLimitInFlight(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	h := newRequests(Limits{MaxInFlight: 2}).middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			entered <- struct{}{}
			<-release
		}
		w.WriteHeader(http.StatusOK)
	}))
	held := make(chan int, 2)
	for range 2 {
		go func() {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/hold", nil))
			held <- rec.Code
		}()
		<-entered
	}

	refused := httptest.NewRecorder()
	h.ServeHTTP(refused, httptest.NewRequest(http.MethodPost, "/v1/aggregate", nil))
	var body query.APIError
	require.NoError(t, json.Unmarshal(refused.Body.Bytes(), &body))
	assert.Equal(t, http.StatusTooManyRequests, refused.Code)
	assert.Equal(t, "1", refused.Header().Get("Retry-After"))
	assert.Equal(t, query.APIError{Code: query.ErrCodeBusy, Message: "the querier is serving as many requests as it may at once", Retryable: true}, body)

	healthz := httptest.NewRecorder()
	h.ServeHTTP(healthz, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, healthz.Code)

	release <- struct{}{}
	assert.Equal(t, http.StatusOK, <-held)
	served := httptest.NewRecorder()
	h.ServeHTTP(served, httptest.NewRequest(http.MethodPost, "/v1/aggregate", nil))
	assert.Equal(t, http.StatusOK, served.Code)
	close(release)
	assert.Equal(t, http.StatusOK, <-held)
}
