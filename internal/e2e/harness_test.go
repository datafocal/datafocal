package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/api"
	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// toolHarness owns one querier + httptest listener over the shared corpus.
// useSummary records whether this harness wired a populated summary store, so
// per-tool fixtures can skip the nil-summary pass when the tool requires one.
type toolHarness struct {
	server     *api.Server
	baseURL    string
	mcp        *mcp.ClientSession
	useSummary bool

	// billedGETs totals the object-store requests every call through this harness made,
	// so the matrix can compare what a fixture costs warm against what it costs cold
	// without each fixture having to thread its own envelopes out.
	billedGETs int

	// bodies holds each tool's first response body, the golden TestToolGoldens pins.
	bodies map[string][]byte
}

// newHarness opens a querier over corpusDir, building its summary first when useSummary
// is set, as cmd/querier does at startup.
func newHarness(t *testing.T, useSummary bool) *toolHarness {
	t.Helper()
	opts := querier.Options{Catalog: catalog.New(storage.NewFS(corpusDir), tenant), Store: storage.NewFS(corpusDir)}
	if useSummary {
		opts.Summary = &summary.Config{}
	}
	app := querier.Open(opts)
	require.NoError(t, app.Refresh(context.Background()), "summary build")
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)
	session, err := mcp.NewClient(&mcp.Implementation{Name: "e2e"}, nil).Connect(context.Background(),
		&mcp.StreamableClientTransport{Endpoint: ts.URL + "/mcp", DisableStandaloneSSE: true}, nil)
	require.NoError(t, err, "connect MCP")
	t.Cleanup(func() { _ = session.Close() })
	return &toolHarness{server: api.NewServer(app.Tools(), api.Limits{}), baseURL: ts.URL, mcp: session, useSummary: useSummary, bodies: map[string][]byte{}}
}

// postTool POSTs req to /v1/<tool> on the harness's HTTP endpoint and decodes
// query.Envelope[Resp]. A non-200 status fails the test — the tool surface
// returns 200 with an APIError body for re-plannable errors, so any 4xx/5xx
// here is a real failure.
func postTool[Resp any](t *testing.T, h *toolHarness, tool string, req any) query.Envelope[Resp] {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoErrorf(t, err, "marshal /v1/%s request", tool)
	resp, err := http.Post(h.baseURL+"/v1/"+tool, "application/json", bytes.NewReader(body))
	require.NoErrorf(t, err, "POST /v1/%s", tool)
	defer resp.Body.Close()
	require.Equalf(t, http.StatusOK, resp.StatusCode, "POST /v1/%s status", tool)
	raw, err := io.ReadAll(resp.Body)
	require.NoErrorf(t, err, "read /v1/%s body", tool)
	if _, ok := h.bodies[tool]; !ok {
		h.bodies[tool] = raw
	}
	var env query.Envelope[Resp]
	require.NoErrorf(t, json.Unmarshal(raw, &env), "decode /v1/%s response", tool)
	assertEnvelope(t, h, tool, env.Meta)

	// The same call over MCP at /mcp answers alike; Meta is not compared because the
	// HTTP call has already warmed the footer cache the MCP one bills against.
	res, err := h.mcp.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: json.RawMessage(body)})
	require.NoErrorf(t, err, "MCP %s", tool)
	text := res.Content[0].(*mcp.TextContent).Text
	var viaMCP struct {
		Data Resp
		Meta struct{ Truncated json.RawMessage }
	}
	require.NoErrorf(t, json.Unmarshal([]byte(text), &viaMCP), "decode MCP %s", tool)
	// A result past the MCP budget is cut there on purpose, so it answers alike only in fitting.
	if viaMCP.Meta.Truncated != nil {
		assert.LessOrEqualf(t, (len(text)+3)/4, api.DefaultMCPResultTokens, "MCP %s fits the budget", tool)
		return env
	}
	assert.Equalf(t, env.Data, viaMCP.Data, "/v1/%s and MCP %s answer alike", tool, tool)
	return env
}

// assertEnvelope checks the invariants every answer's Meta carries whichever tool
// produced it and whichever path served it. They live here because the failure they
// catch is one no Data assertion sees: a tool that short-circuits returns a zero
// Meta — snapshot "", coverage 0 — beside a nil payload, which a Data-only assertion
// reads as "nothing matched". A refusal decoded as an envelope looks the same.
//
// Cost is not asserted here either. A cold answer used to be guaranteed to bill a
// footer GET, but a segment sealed inside one bin is counted straight off its
// descriptor (scanner.countFromDescriptor), so zero is now a legitimate cold cost and
// the assertion belonged to a fixture that knows whether its query needs rows.
func assertEnvelope(t *testing.T, h *toolHarness, tool string, meta query.Meta) {
	t.Helper()
	assert.NotEmptyf(t, meta.Snapshot, "/v1/%s: Meta.Snapshot names the catalog snapshot the answer was read at", tool)
	assert.Equalf(t, 1.0, meta.Coverage, "/v1/%s: the corpus is fully sealed, so every window is fully covered", tool)
	assert.Positivef(t, meta.Tokens, "/v1/%s: Meta.Tokens budgets the answer for the caller", tool)
	h.billedGETs += meta.Cost.GETs
}

// postToolRaw POSTs req and returns the status and undecoded response body,
// asserting neither. It is the entry point for the passes that expect an error:
// decoding into an Envelope would turn a refusal into a zero-valued answer, and
// which status a refusal carries is itself part of what those passes check.
func postToolRaw(t *testing.T, h *toolHarness, tool string, req any) (int, []byte) {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoErrorf(t, err, "marshal /v1/%s request", tool)
	resp, err := http.Post(h.baseURL+"/v1/"+tool, "application/json", bytes.NewReader(body))
	require.NoErrorf(t, err, "POST /v1/%s", tool)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoErrorf(t, err, "read /v1/%s body", tool)
	return resp.StatusCode, raw
}

// incidentTimeSpec is the seed's incident window in RFC3339, as a TimeSpec the
// tool requests accept directly.
func incidentTimeSpec() selector.TimeSpec {
	start, end := seedcorpus.IncidentWindow()
	return selector.TimeSpec{
		From: start.UTC().Format(time.RFC3339),
		To:   end.UTC().Format(time.RFC3339),
	}
}

// healthyTimeSpec is the seed's healthy window in RFC3339.
func healthyTimeSpec() selector.TimeSpec {
	start, end := seedcorpus.HealthyWindow()
	return selector.TimeSpec{
		From: start.UTC().Format(time.RFC3339),
		To:   end.UTC().Format(time.RFC3339),
	}
}

// incidentSelector is the shared Lens the incident-window fixtures build on.
// Where predicates are added by the caller.
func incidentSelector() selector.Selector {
	return selector.Selector{Time: incidentTimeSpec()}
}

// healthySelector is the shared Lens the healthy-window fixtures build on.
func healthySelector() selector.Selector {
	return selector.Selector{Time: healthyTimeSpec()}
}

// withPredicates returns sel with predicates appended.
func withPredicates(sel selector.Selector, preds ...selector.Predicate) selector.Selector {
	sel.Where = append(sel.Where, preds...)
	return sel
}

// patternIDForPrefix exists because pattern ids are content hashes tests
// cannot hard-code, while each seed pattern has a stable, distinct rendered prefix.
func patternIDForPrefix(prefix string) string {
	for tmpl, id := range patternIDByStr {
		if strings.HasPrefix(tmpl, prefix) {
			return id
		}
	}
	return ""
}
