package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
)

// ToolDef is one registered tool: name for routing, doc+schema for /v1/describe
// and MCP tools/list, handler for dispatch. Handler receives the raw JSON request
// body and returns either (result, nil) or (nil, error). Caching is required.
type ToolDef struct {
	Name    string
	Doc     string
	Schema  json.RawMessage
	Handler func(ctx context.Context, raw json.RawMessage) (any, error)
	Caching Caching
}

// ToolInfo is the wire representation of a tool in GET /v1/describe.
type ToolInfo struct {
	Name   string          `json:"name"`
	Doc    string          `json:"doc"`
	Schema json.RawMessage `json:"schema"`
}

// DescribeResult is the response body for GET /v1/describe.
type DescribeResult struct {
	Tools []ToolInfo     `json:"tools"`
	Enums map[string]any `json:"enums,omitempty"`
}

// Registry is the single source of truth for tool definitions. It generates
// HTTP routes, the /v1/describe endpoint, and tool lists for the MCP server.
type Registry struct {
	mu       sync.RWMutex
	tools    []ToolDef
	byName   map[string]int
	requests requests
	answers  *answerCache
}

// NewRegistry returns an empty Registry, whose CacheBySnapshot tools' answers are keyed on
// snapshot's for a call pinning none.
func NewRegistry(snapshot func(context.Context) (string, error)) *Registry {
	return &Registry{byName: make(map[string]int), answers: newAnswers(snapshot)}
}

// Register adds a tool, panicking if one with its name is registered or its Caching is unset.
func (r *Registry) Register(td ToolDef) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byName[td.Name]; exists {
		panic("registry: duplicate tool name: " + td.Name)
	}
	if td.Caching != CacheBySnapshot && td.Caching != NoCache {
		panic("registry: no caching set for tool: " + td.Name)
	}
	r.byName[td.Name] = len(r.tools)
	r.tools = append(r.tools, td)
}

// Tools returns a snapshot of all registered tools.
func (r *Registry) Tools() []ToolDef {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make([]ToolDef, len(r.tools))
	copy(cp, r.tools)
	return cp
}

// tool is the tool registered as name.
func (r *Registry) tool(name string) (ToolDef, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	i, ok := r.byName[name]
	if !ok {
		return ToolDef{}, false
	}
	return r.tools[i], true
}

// Describe builds the DescribeResult from registered tools.
func (r *Registry) Describe() DescribeResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	infos := make([]ToolInfo, len(r.tools))
	for i, td := range r.tools {
		infos[i] = ToolInfo{Name: td.Name, Doc: td.Doc, Schema: td.Schema}
	}
	return DescribeResult{Tools: infos}
}

// AnswerCache reports what the cache of the tools' answers holds and has counted.
func (r *Registry) AnswerCache() query.CacheStats { return r.answers.stats() }

// HTTPHandler returns a chi router with all registered tool routes, GET
// /v1/describe, and GET /healthz. The middleware chain adds panic recovery,
// request IDs, a 30-second timeout, and structured slog request logging.
func (r *Registry) HTTPHandler(healthz http.HandlerFunc) http.Handler {
	return r.HTTPHandlerWith(healthz, nil)
}

// HTTPHandlerWith is like HTTPHandler but also calls extra(mux) before returning,
// letting callers mount additional routes on the same mux without duplicating the
// middleware chain; they are outside the timeout, so a stream bounds itself. Pass nil
// for extra when no additional routes are needed.
func (r *Registry) HTTPHandlerWith(healthz http.HandlerFunc, extra func(chi.Router)) http.Handler {
	mux := chi.NewRouter()
	mux.Use(middleware.RequestID)
	mux.Use(r.requests.middleware)
	// Inside the request log, so a recovered panic still logs its request as a 500.
	mux.Use(middleware.Recoverer)

	mux.Group(func(timed chi.Router) {
		timed.Use(middleware.Timeout(30 * time.Second))
		timed.Get("/healthz", healthz)
		timed.Get("/v1/describe", r.handleDescribe)
		for _, td := range r.Tools() {
			timed.Post("/v1/"+td.Name, r.makeHandler(td))
		}
	})
	if extra != nil {
		extra(mux)
	}
	return mux
}

func (r *Registry) handleDescribe(w http.ResponseWriter, _ *http.Request) {
	writeRegistryJSON(w, http.StatusOK, r.Describe())
}

// makeHandler wraps a ToolDef as an http.HandlerFunc. It reads the request body,
// calls the handler, and maps the result or error to an HTTP response. Every
// failure body is a *query.APIError; only the status distinguishes them:
//   - *query.APIError            → 200, the error is re-plannable
//   - *query.InvalidRequestError → 400, the caller must fix the request
//   - any other error            → 500
func (reg *Registry) makeHandler(td ToolDef) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		raw, err := readBody(w, r)
		if err != nil {
			return
		}

		result, err := reg.answers.answer(r.Context(), td, raw)
		if err != nil {
			dispatchError(w, r, err)
			return
		}
		writeRegistryJSON(w, http.StatusOK, result)
	}
}

func readBody(w http.ResponseWriter, r *http.Request) (json.RawMessage, error) {
	var raw json.RawMessage
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&raw); err != nil {
		writeRegistryError(w, r, http.StatusBadRequest,
			query.NewAPIError(query.ErrCodeInvalidRequest, "decode request: "+err.Error()))
		return nil, err
	}
	return raw, nil
}

func dispatchError(w http.ResponseWriter, r *http.Request, err error) {
	apiErr, status := classifyError(err)
	writeRegistryError(w, r, status, apiErr)
}

// classifyError is the single mapping from a tool's error to what the caller
// receives: one APIError body, and the status that says whether the caller should
// re-plan (200), fix the request (400), or retry later (500). It is shared by
// every response path — including the streaming one, which cannot set a status
// after the first event and so relies on the body alone carrying the code.
func classifyError(err error) (*query.APIError, int) {
	var apiErr *query.APIError
	if errors.As(err, &apiErr) {
		return apiErr, http.StatusOK
	}
	var invalid *query.InvalidRequestError
	if errors.As(err, &invalid) {
		return query.NewAPIError(query.ErrCodeInvalidRequest, err.Error()), http.StatusBadRequest
	}
	return query.NewAPIError(query.ErrCodeInternal, err.Error()), http.StatusInternalServerError
}

func writeRegistryJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeRegistryError renders a failure. It takes an *query.APIError rather than a
// string so that the one error shape is not optional: a caller reading a 400 gets
// the same code/message/hint fields it reads off a 200 re-plannable error, and a
// new failure path cannot introduce a second spelling by passing a bare message.
func writeRegistryError(w http.ResponseWriter, r *http.Request, status int, apiErr *query.APIError) {
	outcomeOf(r.Context()).failure = apiErr
	writeRegistryJSON(w, status, apiErr)
}

// outcome is what a call's handler reports for the call's completion event: what the tool
// read, the error it answered with, and how the answer cache served it.
type outcome struct {
	cost    query.Cost
	failure *query.APIError
	// vars and varsAll are the variables a query substituted and set to All, by name.
	vars, varsAll []string
	call          *query.Call

	cache, cacheReason, cacheKey string
	answerBytes                  int
}

// noteVars records the variables a query call's raw request substitutes and sets to All, by
// name, never their values; a call served from the cache logs them too.
func (o *outcome) noteVars(raw json.RawMessage) {
	var req struct{ Vars map[string]string }
	_ = json.Unmarshal(raw, &req)
	for _, name := range slices.Sorted(maps.Keys(req.Vars)) {
		if req.Vars[name] == "*" {
			o.varsAll = append(o.varsAll, name)
		} else {
			o.vars = append(o.vars, name)
		}
	}
}

type outcomeKey struct{}

func withOutcome(ctx context.Context) (context.Context, *outcome) {
	o := &outcome{}
	ctx, o.call = query.WithCall(ctx)
	return context.WithValue(ctx, outcomeKey{}, o), o
}

// outcomeOf is ctx's outcome, or a discarded one for a handler called outside a transport.
func outcomeOf(ctx context.Context) *outcome {
	if o, ok := ctx.Value(outcomeKey{}).(*outcome); ok {
		return o
	}
	return &outcome{}
}

func (o *outcome) attrs() []any {
	attrs := []any{"gets", o.cost.GETs, "bytes_read", o.cost.BytesRead,
		"footers_decoded", o.cost.FootersDecoded, "segments", o.cost.Segments}
	if o.cache != "" {
		attrs = append(attrs, "cache", o.cache)
	}
	if o.cacheKey != "" {
		attrs = append(attrs, "cache_key", o.cacheKey, "answer_bytes", o.answerBytes)
	}
	if o.cacheReason != "" {
		attrs = append(attrs, "cache_reason", o.cacheReason)
	}
	attrs = append(attrs, "chunks_hit", o.call.ChunksHit.Load(), "chunks_shared", o.call.ChunksShared.Load(),
		"chunks_read", o.call.ChunksRead.Load())
	if o.failure != nil {
		attrs = append(attrs, "error_code", o.failure.Code, "error", o.failure.Message)
	}
	if len(o.vars)+len(o.varsAll) > 0 {
		attrs = append(attrs, "vars", o.vars, "vars_all", o.varsAll)
	}
	return attrs
}

// LogHandler stamps each record logged with a request's context with its request_id, so the
// lines a tool logs while serving it read back as that request's steps (get_trace by
// request_id).
func LogHandler(h slog.Handler) slog.Handler { return requestIDs{h} }

type requestIDs struct{ slog.Handler }

func (h requestIDs) Handle(ctx context.Context, r slog.Record) error {
	if id := middleware.GetReqID(ctx); id != "" {
		r.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h requestIDs) WithAttrs(attrs []slog.Attr) slog.Handler {
	return requestIDs{h.Handler.WithAttrs(attrs)}
}

func (h requestIDs) WithGroup(name string) slog.Handler { return requestIDs{h.Handler.WithGroup(name)} }

// Limits bound what the querier serves at once: MaxInFlight requests, past which one is refused
// busy, each holding at most MemBudget bytes decoded; 0 is any.
type Limits struct {
	MaxInFlight int
	MemBudget   int64
}

// requests serves each request under the Limits, so a burst waits in its callers rather than in
// the querier's heap.
type requests struct {
	slots     chan struct{} // nil serves any number at once
	memBudget int64
}

func newRequests(l Limits) requests {
	q := requests{memBudget: l.MemBudget}
	if l.MaxInFlight > 0 {
		q.slots = make(chan struct{}, l.MaxInFlight)
	}
	return q
}

// errBusy is a request refused for want of a slot, which the caller may retry in a second.
func errBusy() *query.APIError {
	apiErr := query.NewAPIError(query.ErrCodeBusy, "the querier is serving as many requests as it may at once")
	apiErr.Retryable = true
	return apiErr
}

// serve runs one request: holding one of the in-flight slots when held, or refusing busy when
// none is free; under its own memory budget, caching the chunks it reads whole once it ends; and
// logged as msg with the attrs run returns, its duration, what it read and its error.
func (q requests) serve(ctx context.Context, held bool, msg string, run func(context.Context) []any) *query.APIError {
	if held && q.slots != nil {
		select {
		case q.slots <- struct{}{}:
			defer func() { <-q.slots }()
		default:
			return errBusy()
		}
	}
	ctx, cacheChunks := segment.CacheChunksAfter(query.WithMemBudget(ctx, q.memBudget))
	defer cacheChunks()
	start := time.Now()
	ctx, o := withOutcome(ctx)
	attrs := run(ctx)
	level := slog.LevelInfo
	if o.failure != nil && o.failure.Code == query.ErrCodeInternal {
		level = slog.LevelError
	}
	attrs = append(attrs, "duration_ms", float64(time.Since(start).Microseconds())/1000)
	slog.Default().Log(ctx, level, msg, append(attrs, o.attrs()...)...) //nolint:sloglint // msg is each caller's constant request name
	return nil
}

// unheld are the routes no slot is held for: /healthz, so a full querier is not restarted for
// it, a follow, which reads nothing of its own for as long as it streams, and a batch, each of
// whose calls holds its own.
var unheld = map[string]bool{"/healthz": true, "/v1/search_logs/follow": true, "/v1/batch": true, "/v1/batch/stream": true}

// middleware serves each HTTP request as one request, logged with its method, path and status,
// at error when the server failed it; one refused for want of a slot is answered 429.
func (q requests) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		busy := q.serve(r.Context(), !unheld[r.URL.Path], "request", func(ctx context.Context) []any {
			next.ServeHTTP(ww, r.WithContext(ctx))
			if o := outcomeOf(ctx); o.failure == nil && ww.Status() >= http.StatusInternalServerError {
				o.failure = query.NewAPIError(query.ErrCodeInternal, http.StatusText(ww.Status()))
			}
			attrs := []any{"method", r.Method, "path", r.URL.Path, "status", ww.Status()}
			// A view's panel names itself, so a view's cost groups by panel.
			if view, panel, ok := strings.Cut(r.Header.Get("X-DataFocal-View"), "/"); ok {
				attrs = append(attrs, "view", view, "panel", panel)
			}
			return attrs
		})
		if busy != nil {
			w.Header().Set("Retry-After", "1")
			writeRegistryJSON(w, http.StatusTooManyRequests, busy)
		}
	})
}
