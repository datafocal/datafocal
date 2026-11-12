package api

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
)

// work is a call of the work tool, which answers id holding bytes and fails "bad"; "hold"
// waits for the test to release it.
func work(id string, bytes int) tools.ToolCall {
	return tools.ToolCall{Tool: "work", Args: json.RawMessage(fmt.Sprintf(`{"id":%q,"bytes":%d}`, id, bytes))}
}

// workServer serves the work tool, counting the calls it makes and signalling entered as each
// "hold" call starts waiting for release.
type workServer struct {
	srv     *Server
	url     string
	made    atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func batchServer(t *testing.T, l Limits) *workServer {
	t.Helper()
	w := &workServer{srv: NewServer(tools.Fake{}, l), entered: make(chan struct{}, maxBatchCalls), release: make(chan struct{})}
	w.srv.registry.Register(ToolDef{Name: "work", Schema: rawSchema(), Caching: NoCache, Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
		w.made.Add(1)
		var req struct {
			ID    string
			Bytes int64
		}
		assert.NoError(t, json.Unmarshal(raw, &req))
		if req.ID == "hold" {
			w.entered <- struct{}{}
			<-w.release
		}
		if err := query.ChargeMem(ctx, req.Bytes); err != nil || req.ID == "bad" {
			return nil, cmp.Or(err, query.Invalidf("bad failed"))
		}
		return query.Envelope[string]{Data: req.ID}, nil
	}})
	ts := httptest.NewServer(w.srv.Handler())
	t.Cleanup(ts.Close)
	w.url = ts.URL
	return w
}

// answers reads a batch's body as each call's data or "code: message" in request order, or
// as the batch's refusal.
func answers(t *testing.T, body []byte) []string {
	t.Helper()
	var resp struct {
		Results []BatchResult
		query.APIError
	}
	require.NoError(t, json.Unmarshal(body, &resp), string(body))
	if resp.Code != "" {
		return []string{resp.Code + ": " + resp.Message}
	}
	out := make([]string, len(resp.Results))
	for _, r := range resp.Results {
		out[r.Index] = answer(r)
	}
	return out
}

func answer(r BatchResult) string {
	if r.Envelope == nil && r.Error == nil {
		return ""
	}
	if r.Error != nil {
		return r.Error.Code + ": " + r.Error.Message
	}
	return fmt.Sprint(r.Envelope.(map[string]any)["data"])
}

// streamBatch posts req to /v1/batch/stream and returns the body of its refusal, or else its
// results in arrival order as one body, having checked open comes first.
func streamBatch(t *testing.T, url string, req BatchRequest) []byte {
	t.Helper()
	body, _ := json.Marshal(req)
	resp, err := http.Post(url+"/v1/batch/stream", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	lines := bufio.NewScanner(resp.Body)
	if resp.StatusCode != http.StatusOK {
		lines.Scan()
		return lines.Bytes()
	}
	assert.Equal(t, "no", resp.Header.Get("X-Accel-Buffering"))
	var events []string
	for lines.Scan() {
		if event, ok := strings.CutPrefix(lines.Text(), "event: "); ok && lines.Scan() {
			require.Equal(t, len(events) == 0, event == "open", "open comes first, and only first")
			events = append(events, strings.TrimPrefix(lines.Text(), "data: "))
		}
	}
	return []byte(`{"results":[` + strings.Join(events[1:], ",") + `]}`)
}

// batchVia are the ways a batch is sent: plain HTTP, MCP and the stream.
var batchVia = map[string]func(t *testing.T, srv *Server, url string, req BatchRequest) []byte{
	"http": func(t *testing.T, _ *Server, url string, req BatchRequest) []byte {
		body, _ := json.Marshal(req)
		resp, err := http.Post(url+"/v1/batch", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		var raw json.RawMessage
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&raw))
		return raw
	},
	"mcp": func(t *testing.T, srv *Server, _ string, req BatchRequest) []byte {
		var args map[string]any
		body, _ := json.Marshal(req)
		require.NoError(t, json.Unmarshal(body, &args))
		text, _ := callTool(t, connectMCP(t, srv.registry), batchTool, args)
		return []byte(text)
	},
	"stream": func(t *testing.T, _ *Server, url string, req BatchRequest) []byte { return streamBatch(t, url, req) },
}

// TestBatch drives each batch every way it is sent: the answers come back in request order,
// each call failing alone, and the work tool computes each distinct call once.
func TestBatch(t *testing.T) {
	busy := "busy: the querier is serving as many requests as it may at once"
	noTool := `invalid_request: batch: no tool %q to call in a batch`
	tests := []struct {
		name   string
		limits Limits
		calls  []tools.ToolCall
		want   []string
		made   int32
		// outside is the status of a request sent while two calls hold the batch's slots, when
		// one is sent; which calls got them is a race, so the answers are compared sorted.
		outside int
	}{
		{name: "order kept", calls: []tools.ToolCall{work("c", 0), work("b", 0), work("a", 0)},
			want: []string{"c", "b", "a"}, made: 3},
		{name: "duplicate calls computed once", calls: []tools.ToolCall{work("a", 0), {Tool: "work", Args: json.RawMessage(`{"bytes": 0, "id": "a"}`)}, work("b", 0)},
			want: []string{"a", "a", "b"}, made: 2},
		{name: "a failed call alongside successful ones", calls: []tools.ToolCall{work("a", 0), work("bad", 0), {Tool: "nope"}},
			want: []string{"a", "invalid_request: bad failed", fmt.Sprintf(noTool, "nope")}, made: 2},
		{name: "a nested batch refused", calls: []tools.ToolCall{{Tool: batchTool, Args: json.RawMessage(`{"calls":[]}`)}, work("a", 0)},
			want: []string{fmt.Sprintf(noTool, batchTool), "a"}, made: 1},
		{name: "more than 32 calls refused", calls: slices.Repeat([]tools.ToolCall{work("a", 0)}, 33),
			want: []string{"invalid_request: batch: at most 32 calls, got 33"}},
		{name: "a call over its memory budget fails alone", limits: Limits{MemBudget: 1 << 10}, calls: []tools.ToolCall{work("a", 1<<10), work("b", 2<<10)},
			want: []string{"a", "too_large: this request read more than it may hold in memory; narrow the time range or the selector"}, made: 2},
		{name: "past MaxInFlight a call is refused busy", limits: Limits{MaxInFlight: 2}, calls: []tools.ToolCall{work("hold", 0), work("hold", 1), work("hold", 2), work("hold", 3)},
			want: []string{busy, busy, "hold", "hold"}, made: 2, outside: http.StatusTooManyRequests},
	}
	for _, tt := range tests {
		for via, send := range batchVia {
			t.Run(tt.name+"/"+via, func(t *testing.T) {
				w := batchServer(t, tt.limits)
				if tt.outside == 0 {
					close(w.release)
				}
				answered := make(chan []string)
				go func() { answered <- answers(t, send(t, w.srv, w.url, BatchRequest{Calls: tt.calls})) }()
				if tt.outside != 0 {
					<-w.entered
					<-w.entered
					resp, err := http.Post(w.url+"/v1/work", "application/json", strings.NewReader(`{}`))
					require.NoError(t, err)
					_ = resp.Body.Close()
					assert.Equal(t, tt.outside, resp.StatusCode)
					close(w.release)
				}
				got := <-answered
				if tt.outside != 0 {
					slices.Sort(got)
				}
				assert.Equal(t, tt.want, got)
				assert.Equal(t, tt.made, w.made.Load())
			})
		}
	}
}

// TestBatchStreamsEachCallAsItFinishes: the five calls after one that holds arrive while it
// still holds, so each is sent as it finishes and none waits on the one before it.
func TestBatchStreamsEachCallAsItFinishes(t *testing.T) {
	w := batchServer(t, Limits{})
	calls := []tools.ToolCall{work("hold", 0)}
	for i := range 5 {
		calls = append(calls, work(fmt.Sprint(i), 0))
	}
	body, _ := json.Marshal(BatchRequest{Calls: calls})
	// A stream that held the five back would hang here rather than fail.
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(w.url+"/v1/batch/stream", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	lines := bufio.NewScanner(resp.Body)
	next := func() string {
		for lines.Scan() {
			if data, ok := strings.CutPrefix(lines.Text(), "data: "); ok {
				var r BatchResult
				require.NoError(t, json.Unmarshal([]byte(data), &r))
				return cmp.Or(answer(r), "open")
			}
		}
		return ""
	}
	got := []string{next()}
	for range 5 {
		got = append(got, next())
	}
	close(w.release)
	got = append(got, next())
	slices.Sort(got[1:6])
	assert.Equal(t, []string{"open", "0", "1", "2", "3", "4", "hold"}, got)
}
