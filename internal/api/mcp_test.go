package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/datafocal/datafocal/internal/query"
)

// connectMCP serves reg over an in-memory transport pair and returns a client
// session against it. Both sessions close with the test, client first.
func connectMCP(t *testing.T, reg *Registry) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()

	st, ct := mcp.NewInMemoryTransports()
	ss, err := NewMCPServer(reg, 0).Connect(ctx, st, nil)
	if err != nil {
		t.Fatalf("mcp server connect: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test-client"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatalf("mcp client connect: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// callTool calls tool and returns the text of its single content block. Every
// tool in the registry answers with exactly one JSON text block, success or
// error, so anything else is a bridging bug and fails here rather than in the
// caller's unmarshal.
func callTool(t *testing.T, cs *mcp.ClientSession, tool string, args map[string]any) (string, bool) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("CallTool %s: %v", tool, err)
	}
	if len(res.Content) != 1 {
		t.Fatalf("got %d content blocks, want 1", len(res.Content))
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok {
		t.Fatalf("content type = %T, want *mcp.TextContent", res.Content[0])
	}
	return tc.Text, res.IsError
}

func TestMCPServer_ToolsList(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "echo", Caching: NoCache,
		Doc:    "echoes input",
		Schema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) {
			return map[string]string{"echo": "ok"}, nil
		},
	})

	res, err := connectMCP(t, reg).ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Tools) != 1 {
		t.Fatalf("got %d tools, want 1", len(res.Tools))
	}
	tool := res.Tools[0]
	if tool.Name != "echo" {
		t.Errorf("tool name = %q, want %q", tool.Name, "echo")
	}
	if tool.Description != "echoes input" {
		t.Errorf("tool description = %q, want %q", tool.Description, "echoes input")
	}
}

func TestMCPServer_ToolsCall(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "echo", Caching: NoCache,
		Doc:    "echoes input",
		Schema: json.RawMessage(`{"type":"object","properties":{"msg":{"type":"string"}}}`),
		Handler: func(_ context.Context, raw json.RawMessage) (any, error) {
			var req struct {
				Msg string `json:"msg"`
			}
			if err := json.Unmarshal(raw, &req); err != nil {
				return nil, err
			}
			return map[string]string{"echo": req.Msg}, nil
		},
	})

	text, isErr := callTool(t, connectMCP(t, reg), "echo", map[string]any{"msg": "hello"})
	if isErr {
		t.Fatal("unexpected IsError=true")
	}

	var got map[string]string
	if err := json.Unmarshal([]byte(text), &got); err != nil {
		t.Fatalf("unmarshal text content: %v", err)
	}
	if got["echo"] != "hello" {
		t.Errorf("echo = %q, want %q", got["echo"], "hello")
	}
}

func TestMCPServer_ToolsCallError(t *testing.T) {
	reg := NewRegistry(nil)
	reg.Register(ToolDef{
		Name: "fail", Caching: NoCache,
		Doc:    "always fails",
		Schema: json.RawMessage(`{"type":"object"}`),
		Handler: func(_ context.Context, _ json.RawMessage) (any, error) {
			return nil, query.NewAPIError(query.ErrCodeUnboundedScan, "scan is unbounded").
				WithHint("add a time filter")
		},
	})

	text, isErr := callTool(t, connectMCP(t, reg), "fail", map[string]any{})
	if !isErr {
		t.Fatal("expected IsError=true")
	}

	var apiErr query.APIError
	if err := json.Unmarshal([]byte(text), &apiErr); err != nil {
		t.Fatalf("unmarshal APIError: %v", err)
	}
	if apiErr.Code != query.ErrCodeUnboundedScan {
		t.Errorf("error code = %q, want %q", apiErr.Code, query.ErrCodeUnboundedScan)
	}
	if apiErr.Hint != "add a time filter" {
		t.Errorf("hint = %q, want %q", apiErr.Hint, "add a time filter")
	}
}

// TestMCPServer_Instructions: initialize tells an agent where to start, in under 300 words.
func TestMCPServer_Instructions(t *testing.T) {
	got := connectMCP(t, NewRegistry(nil)).InitializeResult().Instructions
	if !strings.Contains(got, "Start with overview") {
		t.Errorf("instructions do not name overview first:\n%s", got)
	}
	if n := len(strings.Fields(got)); n >= 300 {
		t.Errorf("instructions are %d words, want under 300", n)
	}
}
