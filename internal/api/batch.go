package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
)

const (
	batchTool = "batch"
	// maxBatchCalls is the calls one batch makes at most.
	maxBatchCalls = 32
)

// BatchRequest is the calls a batch makes at once.
type BatchRequest struct {
	Calls []tools.ToolCall `json:"calls"`
}

// BatchResult is the answer to the call at Index: its envelope, or the error it failed with.
type BatchResult struct {
	Index    int             `json:"index"`
	Envelope any             `json:"envelope,omitempty"`
	Error    *query.APIError `json:"error,omitempty"`
}

// BatchResponse is each call's result, in request order.
type BatchResponse struct {
	Results []BatchResult `json:"results"`
}

const batchDoc = "Make up to 32 tool calls at once, each {tool, args} as the tool's own route takes it, and answer each call's envelope or error in request order. Each call is a request of its own: it runs in parallel with the rest, holds its own slot of the querier's requests in flight, failing busy alone when none is free, and its own memory budget; the same call twice is made once. A nested batch or an unknown tool fails that call only. POST /v1/batch/stream answers as SSE: an open event once the batch is accepted, then one {index, envelope | error} as each call finishes."

var batchSchema = inferSchema[BatchRequest]()

func (s *Server) batch(ctx context.Context, raw json.RawMessage) (any, error) {
	req, err := decodeBatch(raw)
	if err != nil {
		return nil, err
	}
	results := make([]BatchResult, len(req.Calls))
	s.runBatch(ctx, req, func(r BatchResult) { results[r.Index] = r })
	return BatchResponse{Results: results}, nil
}

// handleBatchStream handles POST /v1/batch/stream, sending each call's result as it finishes.
func (s *Server) handleBatchStream(w http.ResponseWriter, r *http.Request) {
	defer func() { _ = r.Body.Close() }()
	raw, err := readBody(w, r)
	if err != nil {
		return // readBody already wrote the 400
	}
	req, err := decodeBatch(raw)
	if err != nil {
		dispatchError(w, r, err)
		return
	}
	flusher, ok := startStream(w)
	if !ok {
		return
	}
	var mu sync.Mutex
	send := func(event string, v any) {
		data, _ := json.Marshal(v)
		mu.Lock()
		defer mu.Unlock()
		_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		flusher.Flush()
	}
	send("open", struct{}{})
	s.runBatch(r.Context(), req, func(r BatchResult) { send("result", r) })
}

// decodeBatch is raw as a BatchRequest, refusing a field it does not have and more than
// maxBatchCalls calls.
func decodeBatch(raw json.RawMessage) (BatchRequest, error) {
	var req BatchRequest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, query.Invalidf("decode request: %v", err)
	}
	if len(req.Calls) > maxBatchCalls {
		return req, query.Invalidf("batch: at most %d calls, got %d", maxBatchCalls, len(req.Calls))
	}
	return req, nil
}

// runBatch makes each distinct call of req at once, as a request of its own, and calls done
// with the result at each index it answers, from the goroutine that made it.
func (s *Server) runBatch(ctx context.Context, req BatchRequest, done func(BatchResult)) {
	same := map[string][]int{}
	keys := make([]string, len(req.Calls))
	for i, c := range req.Calls {
		keys[i] = callKey(c)
		same[keys[i]] = append(same[keys[i]], i)
	}
	var calls sync.WaitGroup
	for i, c := range req.Calls {
		if same[keys[i]][0] != i {
			continue
		}
		calls.Go(func() {
			r := s.call(ctx, c)
			for _, at := range same[keys[i]] {
				r.Index = at
				done(r)
			}
		})
	}
	calls.Wait()
}

// callKey is c's tool and its args with every object's keys sorted, equal for equal calls.
func callKey(c tools.ToolCall) string {
	var args any
	_ = json.Unmarshal(c.Args, &args) // decoded as JSON already; absent args key as null
	canonical, _ := json.Marshal(args)
	return c.Tool + string(canonical)
}

// call makes c as a request of its own, logged as a batch call.
func (s *Server) call(ctx context.Context, c tools.ToolCall) BatchResult {
	td, ok := s.registry.tool(c.Tool)
	if !ok || c.Tool == batchTool {
		return BatchResult{Error: query.NewAPIError(query.ErrCodeInvalidRequest, fmt.Sprintf("batch: no tool %q to call in a batch", c.Tool))}
	}
	var r BatchResult
	busy := s.registry.requests.serve(ctx, true, "batch call", func(ctx context.Context) []any {
		env, err := s.registry.answers.answer(ctx, td, c.Args)
		if err != nil {
			r.Error, _ = classifyError(err)
			outcomeOf(ctx).failure = r.Error
		}
		r.Envelope = env
		return []any{"tool", c.Tool}
	})
	if busy != nil {
		return BatchResult{Error: busy}
	}
	return r
}
