package tools

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/selector"
)

const defaultLanguage = "logql"

// QueryRequest is the wire request for the query tool, whose Limit and Snapshot pass to the tool the query calls.
type QueryRequest struct {
	Q        string            `json:"q" jsonschema:"the query text"`
	Lang     string            `json:"lang,omitempty" jsonschema:"query language; defaults to logql"`
	Time     selector.TimeSpec `json:"time,omitempty"`
	Limit    int               `json:"limit,omitempty"`
	Snapshot string            `json:"snapshot,omitempty"`
	Vars     map[string]string `json:"vars,omitempty" jsonschema:"the value of each $name a predicate's whole value names, where * (All) drops the predicate"`
}

// QueryResponse is the query in its language's canonical form, the tool call it made and
// that tool's response data.
type QueryResponse struct {
	Canonical string          `json:"canonical"`
	Call      ToolCall        `json:"call"`
	Result    json.RawMessage `json:"result"`
}

// ToolCall is one tool's name and the request it was called with.
type ToolCall struct {
	Tool string          `json:"tool"`
	Args json.RawMessage `json:"args"`
}

// Query parses q in its language and calls the one tool its stage names, answering with
// that tool's meta.
func (t toolSet) Query(ctx context.Context, req QueryRequest) (query.Envelope[QueryResponse], error) {
	name := cmp.Or(req.Lang, defaultLanguage)
	language, ok := t.Languages[name]
	if !ok {
		return query.Envelope[QueryResponse]{}, query.Invalidf("query: unknown language %q", name)
	}
	q, err := language.Parse(req.Q)
	if err != nil {
		return query.Envelope[QueryResponse]{}, query.Invalidf("query: %s: %v", name, err)
	}
	if q.Where, err = selector.Substitute(q.Where, req.Vars); err != nil {
		return query.Envelope[QueryResponse]{}, query.Invalidf("query: %v", err)
	}
	canonical, err := language.Format(q)
	if err != nil {
		return query.Envelope[QueryResponse]{}, fmt.Errorf("query: format %s: %w", name, err)
	}
	return t.runQuery(ctx, req, q, canonical)
}

func (t toolSet) runQuery(ctx context.Context, req QueryRequest, q selector.Query, canonical string) (query.Envelope[QueryResponse], error) {
	sel := selector.Selector{Time: req.Time, Where: q.Where}
	stage := selector.Stage{}
	if q.Stage != nil {
		stage = *q.Stage
	}
	switch stage.Kind {
	case "":
		r := SearchRequest{Selector: sel, Limit: req.Limit, Snapshot: req.Snapshot}
		env, err := t.SearchLogs(ctx, r)
		return called(canonical, "search_logs", r, env, err)
	case selector.StagePatterns:
		r := PatternsRequest{Selector: sel, Limit: req.Limit, Snapshot: req.Snapshot}
		env, err := t.Patterns(ctx, r)
		return called(canonical, "list_patterns", r, env, err)
	case selector.StageAggregate, selector.StageTop:
		// topk is aggregate's default sort, by each group's total, limited to k groups.
		r := AggregateRequest{Selector: sel, Measure: stage.Measure, GroupBy: stage.GroupBy, Bins: stage.Bins,
			Limit: cmp.Or(stage.K, req.Limit), Snapshot: req.Snapshot, Measures: stage.Measures, Having: stage.Having, Sort: stage.Sort, Rollup: stage.Rollup, OrdinalOf: stage.OrdinalOf}
		env, err := t.Aggregate(ctx, r)
		return called(canonical, "aggregate", r, env, err)
	case selector.StageExplain:
		r := ExplainSpikeRequest{Selector: selector.Selector{Time: req.Time}, Slice: q.Where, Snapshot: req.Snapshot}
		env, err := t.ExplainSpike(ctx, r)
		return called(canonical, "explain_spike", r, env, err)
	case selector.StageTrace:
		r, err := traceRequest(req, q.Where)
		if err != nil {
			return query.Envelope[QueryResponse]{}, err
		}
		env, err := t.GetTrace(ctx, r)
		return called(canonical, "get_trace", r, env, err)
	default:
		return query.Envelope[QueryResponse]{}, query.Invalidf("query: unknown stage %q", stage.Kind)
	}
}

// traceRequest reads the one correlation id a trace stage's predicates select.
func traceRequest(req QueryRequest, where []selector.Predicate) (TraceRequest, error) {
	if len(where) != 1 || where[0].Op != selector.OpEq {
		return TraceRequest{}, query.Invalidf("query: a trace stage takes one equality on a correlation id")
	}
	return TraceRequest{Field: where[0].Field, Value: where[0].Value, Time: req.Time, Snapshot: req.Snapshot, Limit: req.Limit}, nil
}

// called shapes the named tool's answer to req as the query's.
func called[Req, Resp any](canonical, tool string, req Req, env query.Envelope[Resp], err error) (query.Envelope[QueryResponse], error) {
	if err != nil {
		return query.Envelope[QueryResponse]{}, err
	}
	args, err := json.Marshal(req)
	if err != nil {
		return query.Envelope[QueryResponse]{}, fmt.Errorf("query: marshal %s request: %w", tool, err)
	}
	result, err := json.Marshal(env.Data)
	if err != nil {
		return query.Envelope[QueryResponse]{}, fmt.Errorf("query: marshal %s response: %w", tool, err)
	}
	resp := QueryResponse{Canonical: canonical, Call: ToolCall{Tool: tool, Args: args}, Result: result}
	meta := env.Meta
	meta.Tokens = query.EstimateTokens(resp)
	return query.Envelope[QueryResponse]{Data: resp, Meta: meta}, nil
}
