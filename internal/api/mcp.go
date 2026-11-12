package api

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
)

// DefaultMCPResultTokens is the tokens an MCP result holds at most, under the 25,000 at
// which Claude Code truncates one.
const DefaultMCPResultTokens = 20000

// mcpInstructions is what an agent is told about the server when it connects.
const mcpInstructions = `DataFocal answers questions about anything running in the cluster from its logs.

Start with overview: one row per (namespace, service) over the last hour by default, with its lines, error lines, first and last seen, pods, and with cluster events, terminations and restarts, most errors first. Then narrow by namespace and service, and read lines with search_logs, or with query, which takes LogQL.

Every line carries the columns of its producer: namespace, pod and container from the Kubernetes container log it was read from, service from the line's own service field or else that pod's workload, and host from the node it ran on. level, message and pattern_id are on every line too, and other fields are columns as logged; describe_schema and rank_fields list them.

Kubernetes events and container lifecycle are lines under the pod they are about. An event line's reason is the event's (Killing, BackOff, FailedScheduling), and a lifecycle line reads like "container compactor terminated: OOMKilled (exit 137), restarts 3", with reason, exit_code and restart_count columns. To learn why a pod restarted, search its pod for lines with an exit_code or reason.

Times take RFC3339 or a relative time such as now-1h. An equality on an id column (one ending in _id, such as request_id or trace_id) needs no time range; any other selector needs one.

A result over the token budget keeps the head of its list and says so in meta.truncated, with a hint at how to narrow the call.`

// NewMCPServer creates an MCP server that exposes all registry tools via the
// MCP protocol. Each ToolDef is registered as an MCP tool with its Schema as
// InputSchema and a handler that delegates to ToolDef.Handler, its result cut to
// resultTokens, or DefaultMCPResultTokens when 0.
func NewMCPServer(reg *Registry, resultTokens int) *mcp.Server {
	srv := mcp.NewServer(&mcp.Implementation{
		Name:    "datafocal-querier",
		Version: "0.1.0",
	}, &mcp.ServerOptions{Instructions: mcpInstructions})
	budget := cmp.Or(resultTokens, DefaultMCPResultTokens)

	for _, td := range reg.Tools() {
		srv.AddTool(
			&mcp.Tool{
				Name:        td.Name,
				Description: td.Doc,
				InputSchema: td.Schema,
			},
			makeMCPToolHandler(td, reg, budget),
		)
	}
	return srv
}

// NewMCPHandler returns an http.Handler that serves the MCP streamable-HTTP
// transport. Mount it at a path (e.g. /mcp) on the main HTTP server. It is stateless,
// every tool being one request and its response, so any replica serves any call.
func NewMCPHandler(reg *Registry, resultTokens int) http.Handler {
	srv := NewMCPServer(reg, resultTokens)
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server {
		return srv
	}, &mcp.StreamableHTTPOptions{Stateless: true})
}

// makeMCPToolHandler serves each call as one request, as the HTTP middleware does, since the
// MCP routes are outside it; a batch holds no slot, each of its calls holding its own.
func makeMCPToolHandler(td ToolDef, reg *Registry, budget int) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var result *mcp.CallToolResult
		busy := reg.requests.serve(ctx, td.Name != batchTool, "mcp tool call", func(ctx context.Context) []any {
			result = callMCPTool(ctx, reg.answers, td, req, budget)
			return []any{"tool", td.Name, "session_id", req.Session.ID()}
		})
		if busy != nil {
			return mcpErrorResult(ctx, busy), nil
		}
		return result, nil
	}
}

// callMCPTool answers one MCP tool call, its result cut to budget tokens.
func callMCPTool(ctx context.Context, a *answerCache, td ToolDef, req *mcp.CallToolRequest, budget int) *mcp.CallToolResult {
	raw := req.Params.Arguments
	if raw == nil {
		raw = json.RawMessage(`{}`)
	}

	result, err := a.answer(ctx, td, raw)
	if err != nil {
		return mcpErrorResult(ctx, err)
	}

	data, err := json.Marshal(clipped(td.Name, result))
	if err == nil {
		data, err = fit(data, budget)
	}
	if err != nil {
		return mcpErrorResult(ctx, err)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
	}
}

// maxValueBytes is the longest attribute value an MCP search_logs line holds whole.
const maxValueBytes = 1024 //nolint:dupdefault // an attribute value's bytes, unrelated to carry's keys

// clipped is result with clipValues applied to a search_logs answer's lines, decoding a cached
// answer, which is its encoding, first.
func clipped(tool string, result any) any {
	if cached, ok := result.(json.RawMessage); ok && tool == "search_logs" {
		var env query.Envelope[tools.SearchResponse]
		if json.Unmarshal(cached, &env) == nil {
			result = env
		}
	}
	if env, ok := result.(query.Envelope[tools.SearchResponse]); ok {
		clipValues(env.Data.Lines)
	}
	return result
}

// clipValues cuts each attribute value over maxValueBytes to its first ones, on a rune
// boundary, and says how many bytes it left out.
func clipValues(lines []tools.LogLine) {
	for _, line := range lines {
		for k, v := range line.Attributes {
			if len(v) <= maxValueBytes {
				continue
			}
			n := maxValueBytes
			for n > 0 && !utf8.RuneStart(v[n]) {
				n--
			}
			line.Attributes[k] = fmt.Sprintf("%s…(+%d bytes)", v[:n], len(v)-n)
		}
	}
}

// mainLists are the keys of the list a result's data holds its answer in, the first one
// present being the one fit cuts.
var mainLists = []string{"lines", "fields", "series", "edges", "nodes", "spans"}

// truncated is what meta.truncated reports of the list fit cut.
type truncated struct {
	Kept  int    `json:"kept"`
	Total int    `json:"total"`
	Hint  string `json:"hint"`
}

// fit drops the tail of data's main list until data is at most budget tokens as
// query.EstimateTokens counts them, and reports the cut in meta.truncated; the query
// tool's list is in the envelope of the tool it called, and data with no list is
// returned as it is.
func fit(data []byte, budget int) ([]byte, error) {
	if tokens(data) <= budget {
		return data, nil
	}
	var env struct {
		Data map[string]json.RawMessage `json:"data"`
		Meta map[string]any             `json:"meta"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, err
	}
	if inner, ok := env.Data["result"]; ok {
		cut, err := fit(inner, budget-tokens(data)+tokens(inner))
		env.Data["result"] = cut
		if err != nil {
			return nil, err
		}
		return json.Marshal(env)
	}
	for _, key := range mainLists {
		var items []json.RawMessage
		if json.Unmarshal(env.Data[key], &items) == nil && len(items) > 0 {
			return cut(env.Data, env.Meta, key, items, len(data)-budget*4, budget)
		}
	}
	return data, nil
}

// cut keeps the head of items, the list under key in data, dropping what over bytes come
// to and then one more item at a time for what re-encoding adds, until data and meta
// encode to at most budget tokens.
func cut(data map[string]json.RawMessage, meta map[string]any, key string, items []json.RawMessage, over, budget int) ([]byte, error) {
	kept := len(items)
	for ; kept > 0 && over > 0; kept-- {
		over -= len(items[kept-1]) + 1
	}
	env := map[string]any{"data": data, "meta": meta}
	for ; ; kept-- {
		data[key], _ = json.Marshal(items[:kept])
		meta["truncated"] = truncated{Kept: kept, Total: len(items),
			Hint: "narrow the call: a tighter time range, fewer columns, a lower limit or another predicate"}
		out, err := json.Marshal(env)
		if err != nil {
			return nil, err
		}
		if kept == 0 || tokens(out) <= budget {
			meta["token_estimate"] = tokens(out)
			return json.Marshal(env)
		}
	}
}

// tokens is query.EstimateTokens of the JSON data.
func tokens(data []byte) int { return (len(data) + 3) / 4 }

// mcpErrorResult reports err as the APIError body HTTP would send, so both
// transports give a caller the same code for the same failure.
func mcpErrorResult(ctx context.Context, err error) *mcp.CallToolResult {
	apiErr, _ := classifyError(err)
	outcomeOf(ctx).failure = apiErr
	data, _ := json.Marshal(apiErr)
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		IsError: true,
	}
}
