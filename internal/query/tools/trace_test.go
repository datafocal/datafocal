package tools_test

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/value"
)

var traceBase = time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)

// traceWindow brackets traceBase by an hour on each side, wider than any seeded corpus here.
var traceWindow = selector.TimeSpec{
	From: traceBase.Add(-time.Hour).Format(time.RFC3339),
	To:   traceBase.Add(time.Hour).Format(time.RFC3339),
}

// at is traceBase offset by ms milliseconds, and atMs the same as the epoch milliseconds a Span reports.
func at(ms int) time.Time { return traceBase.Add(time.Duration(ms) * time.Millisecond) }
func atMs(ms int) int64   { return at(ms).UnixMilli() }

// spanEvent builds one log event carrying the W3C traceparent attributes get_trace reads span structure from.
func spanEvent(ts time.Time, level, traceID, spanID, parentID, name string, durMs int64) segment.Record {
	return segment.LogRecord(ts, level, name, map[string]string{
		"trace_id":       traceID,
		"span_id":        spanID,
		"parent_span_id": parentID,
		"span.name":      name,
		"duration_ms":    strconv.FormatInt(durMs, 10),
	})
}

// opName is the ith of 26 operation names, each a pattern of its own since none holds a number.
func opName(i int) string { return "op " + string(rune('a'+i)) }

// logEvent builds one plain log event correlated only by field=value, the shape cmd/datafocal-seed emits.
func logEvent(ts time.Time, level, field, value, msg string) segment.Record {
	return segment.LogRecord(ts, level, msg, map[string]string{field: value})
}

// traceSeg is one segment a get_trace case seeds: a service and the events it writes.
type traceSeg struct {
	service string
	events  []segment.Record
}

// seedTrace writes every segment into a fresh corpus and returns its directory.
func seedTrace(t *testing.T, segs []traceSeg) string {
	t.Helper()
	dir, store, cat := newCorpus(t)
	for _, s := range segs {
		writeSegment(t, store, cat, s.service, s.events)
	}
	return dir
}

// traceCorrAttrs is one event's correlations, differing in how the system treats them (named ids, a producer id, not an id).
func traceCorrAttrs() map[string]string {
	return map[string]string{
		"trace_id":   "t-f",
		"request_id": "r-f",
		"user_id":    "u-f",
		"order_id":   "o-f",
		"session":    "s-f",
	}
}

// traceCorrCase groups the same single event by field, so an identical waterfall shows the column made no difference.
func traceCorrCase(field, value string) traceCase {
	return traceCase{
		name: "correlation field " + field,
		segs: []traceSeg{{"api", []segment.Record{
			segment.LogRecord(at(500), "info", "served 40 ms", traceCorrAttrs()),
		}}},
		field: field, value: value,
		want: tools.TraceResponse{Field: field, Value: value, Spans: []tools.Span{
			{Name: "served <*> ms", Service: "api", StartMs: atMs(460), DurMs: 40, Parent: -1,
				ID: "svc:api", DurationMethod: "reported", Lines: 1, Where: spanWhere("service", "api", field, value),
				SelfMs: 40, Critical: true},
		}},
		approx: true, coverage: 1,
	}
}

// traceDecoyCase seeds decoys xor-rejected segments ahead of the needle, since pruning them must not lower coverage.
func traceDecoyCase(decoys int) traceCase {
	segs := make([]traceSeg, 0, decoys+1)
	for i := range decoys {
		segs = append(segs, traceSeg{"api", []segment.Record{
			spanEvent(at(i), "info", fmt.Sprintf("decoy-%d", i), "d-root", "", "noise", 10),
		}})
	}
	segs = append(segs, traceSeg{"api", []segment.Record{spanEvent(at(1000), "info", "t-9", "s-root", "", "GET /", 20)}})
	return traceCase{
		name:  fmt.Sprintf("pruning %d decoys keeps coverage whole", decoys),
		segs:  segs,
		value: "t-9",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-9", Spans: []tools.Span{
			{Name: "GET /", Service: "api", StartMs: atMs(980), DurMs: 20, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-9"),
				SelfMs: 20, Critical: true},
		}},
		coverage: 1,
	}
}

// traceDurCase reads duration_ms as untrusted text, where "" omits the attribute and an unusable value counts as absent.
func traceDurCase(name, duration string, wantDur int64, approx bool) traceCase {
	attrs := map[string]string{"trace_id": "t-dur", "span_id": "s-1", "span.name": "op"}
	if duration != "" {
		attrs["duration_ms"] = duration
	}
	durMethod := "reported"
	if approx {
		durMethod = "observed"
	}
	return traceCase{
		name:  "duration " + name,
		segs:  []traceSeg{{"api", []segment.Record{segment.LogRecord(at(1000), "info", "op", attrs)}}},
		value: "t-dur",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-dur", Spans: []tools.Span{
			{Name: "op", Service: "api", StartMs: atMs(1000) - wantDur, DurMs: wantDur, Parent: -1,
				ID: "span:s-1", DurationMethod: durMethod, Lines: 1, Where: spanWhere("span_id", "s-1", "trace_id", "t-dur"),
				SelfMs: wantDur, Critical: true},
		}},
		approx: approx, coverage: 1,
	}
}

// transcriptEvent builds one message of a Claude Code session transcript, whose type and role repeat each other.
func transcriptEvent(ts time.Time, uuid, parent, role, msg string) segment.Record {
	attrs := map[string]string{"session_id": "sess-1", "uuid": uuid, "type": role, "message.role": role}
	if parent != "" {
		attrs["parentUuid"] = parent
	}
	return segment.LogRecord(ts, "info", msg, attrs)
}

// transcriptStep is the i-th step of session sess-1's claude lines, its one line named name at ms.
func transcriptStep(i int, name string, ms int) tools.Span {
	return tools.Span{Name: name, Service: "claude", StartMs: atMs(ms), Parent: 0, ID: "step:claude:" + strconv.Itoa(i),
		ParentMethod: "contained", DurationMethod: "observed", Lines: 1, Where: stepWhere(pattern.ID(name), "claude", "session_id", "sess-1"),
		Critical: true, CriticalInferred: true}
}

// withAttr is r with the string attribute name=v added.
func withAttr(r segment.Record, name, v string) segment.Record {
	r.Columns = append(r.Columns, segment.Column{Name: name, Type: value.TypeString, Value: value.Value{String: v}})
	return r
}

// withNumber is r with the float attribute name=v added.
func withNumber(r segment.Record, name string, v float64) segment.Record {
	r.Columns = append(r.Columns, segment.Column{Name: name, Type: value.TypeFloat, Value: value.Value{Float: v}})
	return r
}

// spanWhere is the predicates a span reports selecting its events: field=value within the trace's corrField=corrValue.
func spanWhere(field, value, corrField, corrValue string) []selector.Predicate {
	return []selector.Predicate{{Field: field, Op: selector.OpEq, Value: value}, {Field: corrField, Op: selector.OpEq, Value: corrValue}}
}

// stepWhere is the predicates a step reports selecting its events: its pattern's lines in service, within the trace's corrField=corrValue.
func stepWhere(pattern, service, corrField, corrValue string) []selector.Predicate {
	return append([]selector.Predicate{{Field: "pattern_id", Op: selector.OpEq, Value: pattern}}, spanWhere("service", service, corrField, corrValue)...)
}

// jobEvent builds one job runner event naming a parent job, which may have run outside this run.
func jobEvent(ts time.Time, id, parent string) segment.Record {
	return segment.LogRecord(ts, "info", "job "+id, map[string]string{"run": "run-1", "id": id, "parent_id": parent})
}

type traceCase struct {
	name        string
	segs        []traceSeg
	field       string
	value       string
	idField     string
	parentField string
	want        tools.TraceResponse
	approx      bool
	coverage    float64
}

// traceCases is every waterfall get_trace reconstructs, where an event's span starts its duration before its write time.
var traceCases = []traceCase{
	{
		name: "instrumented trace across three services and segments",
		segs: []traceSeg{
			{"api", []segment.Record{
				spanEvent(at(0), "info", "t-1", "s-root", "", "GET /checkout", 100),
				spanEvent(at(1000), "info", "t-1", "s-c1", "s-root", "auth", 20),
			}},
			{"db", []segment.Record{spanEvent(at(2000), "info", "t-1", "s-c2", "s-root", "db.query", 50)}},
			{"cache", []segment.Record{spanEvent(at(3000), "error", "t-1", "s-g", "s-c1", "cache.get", 5)}},
		},
		value: "t-1",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-1", Spans: []tools.Span{
			{Name: "GET /checkout", Service: "api", StartMs: atMs(-100), DurMs: 100, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-1"),
				SelfMs: 100, Critical: true},
			{Name: "auth", Service: "api", StartMs: atMs(980), DurMs: 20, Parent: 0,
				ID: "span:s-c1", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-c1", "trace_id", "t-1"),
				SelfMs: 20},
			{Name: "db.query", Service: "db", StartMs: atMs(1950), DurMs: 50, Parent: 0,
				ID: "span:s-c2", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-c2", "trace_id", "t-1"),
				SelfMs: 50},
			{Name: "cache.get", Service: "cache", StartMs: atMs(2995), DurMs: 5, Error: true, Parent: 1,
				ID: "span:s-g", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-g", "trace_id", "t-1"),
				SelfMs: 5},
		}},
		coverage: 1,
	},
	{
		name: "OTLP spells the span pair spanId/parentSpanId",
		segs: []traceSeg{
			{"api", []segment.Record{segment.LogRecord(at(100), "info", "GET /cart", map[string]string{
				"traceId": "t-o", "spanId": "a1", "name": "GET /cart", "duration_ms": "100"})}},
			{"cart", []segment.Record{segment.LogRecord(at(90), "info", "load", map[string]string{
				"traceId": "t-o", "spanId": "b2", "parentSpanId": "a1", "name": "load", "duration_ms": "40"})}},
		},
		field: "traceId",
		value: "t-o",
		want: tools.TraceResponse{Field: "traceId", Value: "t-o", IDField: "spanId", ParentField: "parentSpanId", Spans: []tools.Span{
			{Name: "GET /cart", Service: "api", StartMs: atMs(0), DurMs: 100, Parent: -1,
				ID: "span:a1", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "a1", "traceId", "t-o"), SelfMs: 60, Critical: true},
			{Name: "load", Service: "cart", StartMs: atMs(50), DurMs: 40, Parent: 0,
				ID: "span:b2", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "b2", "traceId", "t-o"), SelfMs: 40, Critical: true},
		}},
		coverage: 1,
	},
	{
		name: "a span whose named parent wrote no span, as a proxy's, nests under the span enclosing it",
		segs: []traceSeg{
			{"api", []segment.Record{
				segment.LogRecord(at(100), "info", "GET /cart", map[string]string{"traceId": "t-p", "spanId": "a1", "name": "GET /cart", "duration_ms": "100"}),
				segment.LogRecord(at(95), "info", "call cart", map[string]string{"traceId": "t-p", "spanId": "c1", "parentSpanId": "a1", "name": "call cart", "duration_ms": "80"})}},
			{"cart", []segment.Record{segment.LogRecord(at(90), "info", "load", map[string]string{
				"traceId": "t-p", "spanId": "b2", "parentSpanId": "w9", "name": "load", "duration_ms": "40"})}},
		},
		field: "traceId",
		value: "t-p",
		want: tools.TraceResponse{Field: "traceId", Value: "t-p", IDField: "spanId", ParentField: "parentSpanId", Spans: []tools.Span{
			{Name: "GET /cart", Service: "api", StartMs: atMs(0), DurMs: 100, Parent: -1,
				ID: "span:a1", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "a1", "traceId", "t-p"), SelfMs: 20, Critical: true},
			{Name: "call cart", Service: "api", StartMs: atMs(15), DurMs: 80, Parent: 0,
				ID: "span:c1", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "c1", "traceId", "t-p"), SelfMs: 40, Critical: true},
			{Name: "load", Service: "cart", StartMs: atMs(50), DurMs: 40, Parent: 1,
				ID: "span:b2", ParentMethod: "contained", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "b2", "traceId", "t-p"), SelfMs: 40, Critical: true,
				CriticalInferred: true},
		}},
		approx: true, coverage: 1,
	},
	{
		name: "a span missing its named parent stays a root rather than nest under its own child",
		segs: []traceSeg{
			{"cart", []segment.Record{segment.LogRecord(at(90), "info", "load", map[string]string{
				"traceId": "t-q", "spanId": "b2", "parentSpanId": "w9", "name": "load", "duration_ms": "40"})}},
			{"db", []segment.Record{segment.LogRecord(at(95), "info", "query", map[string]string{ // skewed past its parent
				"traceId": "t-q", "spanId": "d3", "parentSpanId": "b2", "name": "query", "duration_ms": "60"})}},
		},
		field: "traceId",
		value: "t-q",
		want: tools.TraceResponse{Field: "traceId", Value: "t-q", IDField: "spanId", ParentField: "parentSpanId", Spans: []tools.Span{
			{Name: "load", Service: "cart", StartMs: atMs(50), DurMs: 40, Parent: -1,
				ID: "span:b2", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "b2", "traceId", "t-q"), Critical: true},
			{Name: "query", Service: "db", StartMs: atMs(35), DurMs: 60, Parent: 0,
				ID: "span:d3", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("spanId", "d3", "traceId", "t-q"), SelfMs: 60},
		}},
		coverage: 1,
	},
	{
		// One span per service spanning its events, nested under the innermost enclosing
		// interval, and a step for each of its patterns.
		name: "derived from plain lines sharing a request_id",
		segs: []traceSeg{
			{"gateway", []segment.Record{logEvent(at(1000), "error", "request_id", "r-1", "POST /checkout 500 900 ms")}},
			{"checkout", []segment.Record{
				logEvent(at(400), "info", "request_id", "r-1", "auth ok 50 ms"),
				logEvent(at(950), "error", "request_id", "r-1", "charge failed 500 ms"),
			}},
			{"db", []segment.Record{logEvent(at(700), "info", "request_id", "r-1", "query ok 100 ms")}},
		},
		field: "request_id", value: "r-1",
		want: tools.TraceResponse{Field: "request_id", Value: "r-1", Spans: []tools.Span{
			{Name: "POST /checkout <*> <*> ms", Service: "gateway", StartMs: atMs(100), DurMs: 900, Error: true, Parent: -1,
				ID: "svc:gateway", DurationMethod: "reported", Lines: 1, Where: spanWhere("service", "gateway", "request_id", "r-1"),
				SelfMs: 300, Critical: true},
			{Name: "charge failed <*> ms", Service: "checkout", StartMs: atMs(350), DurMs: 600, Error: true, Parent: 0,
				ID: "svc:checkout", ParentMethod: "contained", DurationMethod: "reported", Lines: 2, Where: spanWhere("service", "checkout", "request_id", "r-1"),
				SelfMs: 50, Critical: true, CriticalInferred: true},
			{Name: "auth ok <*> ms", Service: "checkout", StartMs: atMs(350), DurMs: 50, Parent: 1,
				ID: "step:checkout:0", ParentMethod: "contained", DurationMethod: "reported", Lines: 1,
				Where: stepWhere(pattern.ID("auth ok <*> ms"), "checkout", "request_id", "r-1"), SelfMs: 50, Critical: true, CriticalInferred: true},
			{Name: "charge failed <*> ms", Service: "checkout", StartMs: atMs(450), DurMs: 500, Error: true, Parent: 1,
				ID: "step:checkout:1", ParentMethod: "contained", DurationMethod: "reported", Lines: 1,
				Where: stepWhere(pattern.ID("charge failed <*> ms"), "checkout", "request_id", "r-1"), SelfMs: 500, Critical: true, CriticalInferred: true},
			{Name: "query ok <*> ms", Service: "db", StartMs: atMs(600), DurMs: 100, Parent: 1,
				ID: "svc:db", ParentMethod: "contained", DurationMethod: "reported", Lines: 1, Where: spanWhere("service", "db", "request_id", "r-1"),
				SelfMs: 100},
		}},
		approx: true, coverage: 1,
	},
	{
		name: "a request logging a line per step is named by its covering line",
		segs: []traceSeg{{"api", []segment.Record{
			logEvent(at(2), "info", "request_id", "r-2", "scope fields 2 ms"),
			withNumber(withNumber(logEvent(at(400), "info", "request_id", "r-2", "segment read"), "gets", 4), "bytes_read", 100),
			logEvent(at(450), "info", "request_id", "r-2", "explain 300 ms"),
			logEvent(at(500), "info", "request_id", "r-2", "request 500 ms"),
		}}},
		field: "request_id", value: "r-2",
		want: tools.TraceResponse{Field: "request_id", Value: "r-2", Spans: []tools.Span{
			{Name: "request <*> ms", Service: "api", StartMs: atMs(0), DurMs: 500, Parent: -1,
				ID: "svc:api", DurationMethod: "reported", Lines: 4, Where: spanWhere("service", "api", "request_id", "r-2"),
				SelfMs: 198, Critical: true, Sums: map[string]float64{"gets": 4, "bytes_read": 100}},
			{Name: "scope fields <*> ms", Service: "api", StartMs: atMs(0), DurMs: 2, Parent: 0,
				ID: "step:api:0", ParentMethod: "contained", DurationMethod: "reported", Lines: 1,
				Where: stepWhere(pattern.ID("scope fields <*> ms"), "api", "request_id", "r-2"), SelfMs: 2, Critical: true, CriticalInferred: true},
			{Name: "explain <*> ms", Service: "api", StartMs: atMs(150), DurMs: 300, Parent: 0,
				ID: "step:api:2", ParentMethod: "contained", DurationMethod: "reported", Lines: 1,
				Where: stepWhere(pattern.ID("explain <*> ms"), "api", "request_id", "r-2"), SelfMs: 300, Critical: true, CriticalInferred: true},
			{Name: "segment read", Service: "api", StartMs: atMs(400), Parent: 2,
				ID: "step:api:1", ParentMethod: "contained", DurationMethod: "observed", Lines: 1,
				Where: stepWhere(pattern.ID("segment read"), "api", "request_id", "r-2"), Critical: true, CriticalInferred: true,
				Sums: map[string]float64{"gets": 4, "bytes_read": 100}},
		}},
		approx: true, coverage: 1,
	},
	{
		name: "mixed instrumentation nests the derived span under the read one",
		segs: []traceSeg{
			{"api", []segment.Record{spanEvent(at(1000), "info", "t-mix", "s-root", "", "GET /x", 300)}},
			{"worker", []segment.Record{logEvent(at(900), "info", "trace_id", "t-mix", "job done 100 ms")}},
		},
		value: "t-mix",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-mix", Spans: []tools.Span{
			{Name: "GET /x", Service: "api", StartMs: atMs(700), DurMs: 300, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-mix"),
				SelfMs: 200, Critical: true},
			{Name: "job done <*> ms", Service: "worker", StartMs: atMs(800), DurMs: 100, Parent: 0,
				ID: "svc:worker", ParentMethod: "contained", DurationMethod: "reported", Lines: 1, Where: spanWhere("service", "worker", "trace_id", "t-mix"),
				SelfMs: 100, Critical: true, CriticalInferred: true},
		}},
		approx: true, coverage: 1,
	},
	traceCorrCase("order_id", "o-f"),
	traceCorrCase("request_id", "r-f"),
	traceCorrCase("session", "s-f"),
	traceCorrCase("trace_id", "t-f"),
	traceCorrCase("user_id", "u-f"),
	{
		name: "a decoy trace never leaks in",
		segs: []traceSeg{
			{"api", []segment.Record{spanEvent(at(0), "info", "decoy", "d-root", "", "noise", 10)}},
			{"api", []segment.Record{spanEvent(at(1000), "info", "t-2", "s-root", "", "GET /pay", 30)}},
		},
		value: "t-2",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-2", Spans: []tools.Span{
			{Name: "GET /pay", Service: "api", StartMs: atMs(970), DurMs: 30, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-2"),
				SelfMs: 30, Critical: true},
		}},
		coverage: 1,
	},
	traceDecoyCase(1),
	traceDecoyCase(6),
	{
		// The plain line reports no duration, so it is a point at its write time.
		name: "correlated lines without span_id become spans",
		segs: []traceSeg{
			{"edge", []segment.Record{logEvent(at(400), "info", "trace_id", "t-3", "connecting")}},
			{"api", []segment.Record{spanEvent(at(1000), "info", "t-3", "s-root", "", "GET /", 15)}},
		},
		value: "t-3",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-3", Spans: []tools.Span{
			{Name: "connecting", Service: "edge", StartMs: atMs(400), Parent: -1,
				ID: "svc:edge", DurationMethod: "observed", Lines: 1, Where: spanWhere("service", "edge", "trace_id", "t-3")},
			{Name: "GET /", Service: "api", StartMs: atMs(985), DurMs: 15, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-3"),
				SelfMs: 15, Critical: true},
		}},
		approx: true, coverage: 1,
	},
	{
		name:     "an absent id is an empty answer",
		segs:     []traceSeg{{"api", []segment.Record{spanEvent(at(0), "info", "t-present", "s-root", "", "root", 10)}}},
		value:    "t-absent",
		want:     tools.TraceResponse{Field: "trace_id", Value: "t-absent", Spans: []tools.Span{}},
		coverage: 1,
	},
	{
		// Identical intervals tie-break on span id and must not make each the other's parent.
		name: "equal intervals order by span id",
		segs: []traceSeg{{"api", []segment.Record{
			spanEvent(at(0), "info", "t-4", "s-b", "", "b", 5),
			spanEvent(at(0), "info", "t-4", "s-a", "", "a", 5),
		}}},
		value: "t-4",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-4", Spans: []tools.Span{
			{Name: "a", Service: "api", StartMs: atMs(-5), DurMs: 5, Parent: -1,
				ID: "span:s-a", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-a", "trace_id", "t-4"),
				SelfMs: 5, Critical: true},
			{Name: "b", Service: "api", StartMs: atMs(-5), DurMs: 5, Parent: -1,
				ID: "span:s-b", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-b", "trace_id", "t-4"),
				SelfMs: 5},
		}},
		coverage: 1,
	},
	{
		// A reported duration can extend a span but never shrink it below the observed second.
		name: "split start and end events merge",
		segs: []traceSeg{{"api", []segment.Record{
			spanEvent(at(0), "info", "t-6", "s-root", "", "start", 0),
			spanEvent(at(1000), "error", "t-6", "s-root", "", "end", 42),
		}}},
		value: "t-6",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-6", Spans: []tools.Span{
			{Name: "start", Service: "api", StartMs: atMs(0), DurMs: 1000, Error: true, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 2, Where: spanWhere("span_id", "s-root", "trace_id", "t-6"),
				SelfMs: 1000, Critical: true},
		}},
		coverage: 1,
	},
	{
		name:  "a self-referencing parent resolves to a root",
		segs:  []traceSeg{{"api", []segment.Record{spanEvent(at(0), "info", "t-8", "s-root", "s-root", "self", 5)}}},
		value: "t-8",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-8", Spans: []tools.Span{
			{Name: "self", Service: "api", StartMs: atMs(-5), DurMs: 5, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-8"),
				SelfMs: 5, Critical: true},
		}},
		coverage: 1,
	},
	{
		// The child's span id sorts first, so a plain (start, key) sort would forward-reference the parent.
		name: "a parent starting with its child precedes it",
		segs: []traceSeg{{"api", []segment.Record{
			spanEvent(at(0), "info", "t-9", "s-child", "s-root", "child", 10),
			spanEvent(at(0), "info", "t-9", "s-root", "", "root", 10),
		}}},
		value: "t-9",
		want: tools.TraceResponse{Field: "trace_id", Value: "t-9", Spans: []tools.Span{
			{Name: "root", Service: "api", StartMs: atMs(-10), DurMs: 10, Parent: -1,
				ID: "span:s-root", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-root", "trace_id", "t-9"),
				Critical: true},
			{Name: "child", Service: "api", StartMs: atMs(-10), DurMs: 10, Parent: 0,
				ID: "span:s-child", ParentMethod: "read", DurationMethod: "reported", Lines: 1, Where: spanWhere("span_id", "s-child", "trace_id", "t-9"),
				SelfMs: 10, Critical: true},
		}},
		coverage: 1,
	},
	{
		// attachment.ref/replyTo links its one row as surely as uuid/parentUuid links four, and
		// sorts first; the pair linking more rows wins.
		name: "a transcript's parentUuid links are discovered across its main and subagent sessions",
		segs: []traceSeg{
			{"claude", []segment.Record{
				transcriptEvent(at(0), "u1", "", "user", "fix the bug"),
				withAttr(transcriptEvent(at(100), "u2", "u1", "assistant", "delegating"), "attachment.ref", "r1"),
				withAttr(transcriptEvent(at(900), "u3", "u2", "user", "subagent result"), "replyTo", "r1"),
			}},
			{"claude", []segment.Record{
				transcriptEvent(at(200), "a1", "u2", "user", "find the bug"),
				transcriptEvent(at(500), "a2", "a1", "assistant", "found it"),
			}},
		},
		field: "session_id", value: "sess-1",
		want: tools.TraceResponse{Field: "session_id", Value: "sess-1", IDField: "uuid", ParentField: "parentUuid", Spans: []tools.Span{
			{Name: "fix the bug", Service: "claude", StartMs: atMs(0), Parent: -1,
				ID: "span:u1", DurationMethod: "observed", Lines: 1, Where: spanWhere("uuid", "u1", "session_id", "sess-1"),
				Critical: true},
			{Name: "delegating", Service: "claude", StartMs: atMs(100), Parent: 0,
				ID: "span:u2", ParentMethod: "read", DurationMethod: "observed", Lines: 1, Where: spanWhere("uuid", "u2", "session_id", "sess-1")},
			{Name: "find the bug", Service: "claude", StartMs: atMs(200), Parent: 1,
				ID: "span:a1", ParentMethod: "read", DurationMethod: "observed", Lines: 1, Where: spanWhere("uuid", "a1", "session_id", "sess-1")},
			{Name: "found it", Service: "claude", StartMs: atMs(500), Parent: 2,
				ID: "span:a2", ParentMethod: "read", DurationMethod: "observed", Lines: 1, Where: spanWhere("uuid", "a2", "session_id", "sess-1")},
			{Name: "subagent result", Service: "claude", StartMs: atMs(900), Parent: 1,
				ID: "span:u3", ParentMethod: "read", DurationMethod: "observed", Lines: 1, Where: spanWhere("uuid", "u3", "session_id", "sess-1")},
		}},
		approx: true, coverage: 1,
	},
	{
		// Each message names the one before as its parent, which says no more than time order.
		name: "a transcript chaining each message to the one before is its service's steps",
		segs: []traceSeg{{"claude", []segment.Record{
			transcriptEvent(at(0), "u1", "u0", "user", "fix the bug"),
			transcriptEvent(at(100), "u2", "u1", "assistant", "looking"),
			transcriptEvent(at(200), "u3", "u2", "assistant", "fixed"),
		}}},
		field: "session_id", value: "sess-1",
		want: tools.TraceResponse{Field: "session_id", Value: "sess-1", Spans: []tools.Span{
			{Name: "fix the bug", Service: "claude", StartMs: atMs(0), DurMs: 200, Parent: -1,
				ID: "svc:claude", DurationMethod: "observed", Lines: 3, Where: spanWhere("service", "claude", "session_id", "sess-1"),
				SelfMs: 200, Critical: true},
			transcriptStep(0, "fix the bug", 0), transcriptStep(1, "looking", 100), transcriptStep(2, "fixed", 200),
		}},
		approx: true, coverage: 1,
	},
	{
		// Every line's cwd is /workspace, as is the first message's working directory, so
		// workingDirectory/cwd "links" five lines to it, queue lines with no uuid among them,
		// more than uuid/parentUuid links; a column on one row can't be the id of five.
		name: "a value every row repeats is no parent link, though one row's other column holds it",
		segs: []traceSeg{{"claude", []segment.Record{
			withAttr(withAttr(transcriptEvent(at(0), "u1", "", "user", "fix the bug"), "cwd", "/workspace"), "workingDirectory", "/workspace"),
			withAttr(transcriptEvent(at(100), "u2", "u1", "assistant", "looking"), "cwd", "/workspace"),
			withAttr(transcriptEvent(at(200), "u3", "u2", "assistant", "still looking"), "cwd", "/workspace"),
			withAttr(transcriptEvent(at(300), "u4", "u3", "assistant", "fixed"), "cwd", "/workspace"),
			segment.LogRecord(at(310), "info", "enqueue", map[string]string{"session_id": "sess-1", "cwd": "/workspace"}),
			segment.LogRecord(at(320), "info", "dequeue", map[string]string{"session_id": "sess-1", "cwd": "/workspace"}),
		}}},
		field: "session_id", value: "sess-1",
		want: tools.TraceResponse{Field: "session_id", Value: "sess-1", Spans: []tools.Span{
			{Name: "fix the bug", Service: "claude", StartMs: atMs(0), DurMs: 320, Parent: -1,
				ID: "svc:claude", DurationMethod: "observed", Lines: 6, Where: spanWhere("service", "claude", "session_id", "sess-1"),
				SelfMs: 320, Critical: true},
			transcriptStep(0, "fix the bug", 0), transcriptStep(1, "looking", 100), transcriptStep(2, "still looking", 200),
			transcriptStep(3, "fixed", 300), transcriptStep(4, "enqueue", 310), transcriptStep(5, "dequeue", 320),
		}},
		approx: true, coverage: 1,
	},
	{
		// Only one of three parent_id values names a job in this run, too few for discovery.
		name: "a named pair links what discovery would not",
		segs: []traceSeg{{"runner", []segment.Record{
			jobEvent(at(0), "j1", "elsewhere-1"),
			jobEvent(at(100), "j2", "elsewhere-2"),
			jobEvent(at(200), "j3", "j1"),
		}}},
		field: "run", value: "run-1", idField: "id", parentField: "parent_id",
		want: tools.TraceResponse{Field: "run", Value: "run-1", IDField: "id", ParentField: "parent_id", Spans: []tools.Span{
			{Name: "job <*>", Service: "runner", StartMs: atMs(0), Parent: -1,
				ID: "span:j1", DurationMethod: "observed", Lines: 1, Where: spanWhere("id", "j1", "run", "run-1")},
			{Name: "job <*>", Service: "runner", StartMs: atMs(100), Parent: -1,
				ID: "span:j2", DurationMethod: "observed", Lines: 1, Where: spanWhere("id", "j2", "run", "run-1"),
				Critical: true},
			{Name: "job <*>", Service: "runner", StartMs: atMs(200), Parent: 0,
				ID: "span:j3", ParentMethod: "read", DurationMethod: "observed", Lines: 1, Where: spanWhere("id", "j3", "run", "run-1")},
		}},
		approx: true, coverage: 1,
	},
	{
		name: "the same jobs without a named pair are one span per service",
		segs: []traceSeg{{"runner", []segment.Record{
			jobEvent(at(0), "j1", "elsewhere-1"),
			jobEvent(at(100), "j2", "elsewhere-2"),
			jobEvent(at(200), "j3", "j1"),
		}}},
		field: "run", value: "run-1",
		want: tools.TraceResponse{Field: "run", Value: "run-1", Spans: []tools.Span{
			{Name: "job <*>", Service: "runner", StartMs: atMs(0), DurMs: 200, Parent: -1,
				ID: "svc:runner", DurationMethod: "observed", Lines: 3, Where: spanWhere("service", "runner", "run", "run-1"),
				SelfMs: 200, Critical: true},
		}},
		approx: true, coverage: 1,
	},
	traceDurCase("integer", "40", 40, false),
	traceDurCase("fractional rounds up", "12.5", 13, false),
	traceDurCase("fractional rounds down", "12.4", 12, false),
	traceDurCase("explicit zero is reported", "0", 0, false),
	traceDurCase("omitted", "", 0, true),
	traceDurCase("malformed", "fast", 0, true),
	traceDurCase("negative", "-5", 0, true),
	traceDurCase("not a number", "NaN", 0, true),
}

// TestGetTrace reconstructs each case's waterfall twice and requires the same whole response and byte-identical JSON.
func TestGetTrace(t *testing.T) {
	for _, tc := range traceCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.want.Total = len(tc.want.Spans) // no case repeats a sibling, so none is grouped
			dir := seedTrace(t, tc.segs)
			var encoded [2][]byte
			for i := range encoded {
				cat, rd := readers(dir)
				env, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tools.TraceRequest{
					Field: tc.field, Value: tc.value, Time: traceWindow, IDField: tc.idField, ParentField: tc.parentField,
				})
				require.NoError(t, err)
				assert.Equal(t, tc.want, env.Data)
				assert.Equal(t, tc.approx, env.Meta.Approx, "approx")
				assert.Equal(t, tc.coverage, env.Meta.Coverage, "coverage")
				encoded[i], err = json.Marshal(env.Data)
				require.NoError(t, err)
			}
			assert.Equal(t, string(encoded[0]), string(encoded[1]), "output not deterministic")
		})
	}
}

// TestGetTraceRejects covers the requests get_trace refuses, and the windowless one it accepts because its id prunes.
func TestGetTraceRejects(t *testing.T) {
	dir := seedTrace(t, []traceSeg{{"api", []segment.Record{
		segment.LogRecord(at(500), "info", "served 40 ms", traceCorrAttrs()),
	}}})
	cat, rd := readers(dir)

	tests := []struct {
		name        string
		req         tools.TraceRequest
		wantAPI     *query.APIError // only Code and SuggestedFields are compared
		wantInvalid bool
	}{
		{name: "id needs no window", req: tools.TraceRequest{Field: "trace_id", Value: "t-f"}},
		{name: "any id needs no window", req: tools.TraceRequest{Field: "order_id", Value: "o-f"}},
		{
			name:    "non-id field without a window",
			req:     tools.TraceRequest{Field: "endpoint", Value: "/users"},
			wantAPI: &query.APIError{Code: query.ErrCodeUnboundedScan},
		},
		{
			name:    "column absent from corpus",
			req:     tools.TraceRequest{Field: "endpoint", Value: "/users", Time: traceWindow},
			wantAPI: &query.APIError{Code: query.ErrCodeUnknownField},
		},
		{name: "missing value", req: tools.TraceRequest{}, wantInvalid: true},
		{name: "id field without parent field", req: tools.TraceRequest{Value: "t-f", IDField: "uuid"}, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tc.req)
			switch {
			case tc.wantInvalid:
				var invErr *query.InvalidRequestError
				assert.ErrorAs(t, err, &invErr)
			case tc.wantAPI != nil:
				var apiErr *query.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tc.wantAPI, &query.APIError{Code: apiErr.Code, SuggestedFields: apiErr.SuggestedFields})
			default:
				assert.NoError(t, err)
			}
		})
	}
}

// TestGetTraceLimit bounds the returned spans by Limit, 100 when none is sent, whether the trace spans many
// segments or sits in one, reading every segment so Total counts the whole trace's spans after grouping.
func TestGetTraceLimit(t *testing.T) {
	perSegment := make([]traceSeg, 5)
	for i := range perSegment {
		perSegment[i] = traceSeg{"api", []segment.Record{
			spanEvent(at(i*1000), "info", "t-10", "s-"+strconv.Itoa(i), "", opName(i), 1),
		}}
	}
	chain := make([]segment.Record, 5)
	for i := range chain {
		parent := ""
		if i > 0 {
			parent = "s-" + strconv.Itoa(i-1)
		}
		chain[i] = spanEvent(at(i*10), "info", "t-11", "s-"+strconv.Itoa(i), parent, "op", 1)
	}
	// 238 spans in a chain are 238 spans; 238 roots named by 15 operations are 15 groups of
	// a group span and its 3 slowest members.
	distinct, grouped := make([]segment.Record, 238), make([]segment.Record, 238)
	for i := range distinct {
		distinct[i] = spanEvent(at(-i), "info", "t-12", "s-"+strconv.Itoa(i), "s-"+strconv.Itoa(i-1), "op", 1)
		grouped[i] = spanEvent(at(i), "info", "t-13", "s-"+strconv.Itoa(i), "", opName(i%15), 1)
	}

	tests := []struct {
		name         string
		segs         []traceSeg
		value        string
		limit        int
		spans, total int
	}{
		{name: "spans many segments", segs: perSegment, value: "t-10", limit: 2, spans: 2, total: 5},
		{name: "truncates one segment holding every span", segs: []traceSeg{{"api", chain}}, value: "t-11", limit: 2, spans: 2, total: 5},
		{name: "defaults to 100 spans", segs: []traceSeg{{"api", distinct}}, value: "t-12", spans: 100, total: 238},
		{name: "counts spans after grouping", segs: []traceSeg{{"api", grouped}}, value: "t-13", spans: 60, total: 60},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, rd := readers(seedTrace(t, tc.segs))
			env, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tools.TraceRequest{
				Value: tc.value, Limit: tc.limit,
			})
			require.NoError(t, err)
			spans := env.Data.Spans
			require.Len(t, spans, tc.spans)
			for i, s := range spans {
				assert.True(t, s.Parent >= -1 && s.Parent < len(spans), "span %d has Parent %d outside the returned slice", i, s.Parent)
			}
			assert.Equal(t, tc.total, env.Data.Total)
			assert.Equal(t, 1.0, env.Meta.Coverage)
		})
	}
}

// TestGetTraceSpanWhere requires each span's Where, searched over the trace window, to return exactly its Lines.
func TestGetTraceSpanWhere(t *testing.T) {
	cat, rd := readers(seedTrace(t, []traceSeg{
		{"api", []segment.Record{
			spanEvent(at(0), "info", "t-w", "s-root", "", "GET /", 0),
			spanEvent(at(1000), "info", "t-w", "s-root", "", "GET /", 1000),
			spanEvent(at(500), "info", "t-w", "s-child", "s-root", "auth", 10),
			spanEvent(at(600), "info", "other", "s-root", "", "GET /", 10),
		}},
		{"worker", []segment.Record{
			logEvent(at(700), "info", "trace_id", "t-w", "job started"),
			logEvent(at(900), "info", "trace_id", "t-w", "job done"),
			logEvent(at(800), "info", "trace_id", "other", "job done"),
		}},
	}))
	ts := toolsOver(cat, rd, nil)
	env, err := ts.GetTrace(context.Background(), tools.TraceRequest{Value: "t-w", Time: traceWindow})
	require.NoError(t, err)
	require.Len(t, env.Data.Spans, 5) // the worker's two lines are two steps
	for _, s := range env.Data.Spans {
		got, err := ts.SearchLogs(context.Background(), tools.SearchRequest{
			Selector: selector.Selector{Time: traceWindow, Where: s.Where},
		})
		require.NoError(t, err)
		assert.Len(t, got.Data.Lines, s.Lines, "span %s", s.ID)
	}
}

// TestGetTraceCriticalPath marks, in hand-built traces, the spans that set the latency and each span's self time.
func TestGetTraceCriticalPath(t *testing.T) {
	type mark struct {
		selfMs             int64
		critical, inferred bool
	}
	tests := []struct {
		name   string
		events []segment.Record
		want   map[string]mark
	}{
		{
			name: "sequential children are both critical and cover their parent",
			events: []segment.Record{
				spanEvent(at(100), "info", "t-cp", "s-root", "", "root", 100),
				spanEvent(at(50), "info", "t-cp", "s-a", "s-root", "a", 50),
				spanEvent(at(100), "info", "t-cp", "s-b", "s-root", "b", 50),
			},
			want: map[string]mark{"span:s-root": {0, true, false}, "span:s-a": {50, true, false}, "span:s-b": {50, true, false}},
		},
		{
			name: "of parallel children only the one ending later is critical",
			events: []segment.Record{
				spanEvent(at(100), "info", "t-cp", "s-root", "", "root", 100),
				spanEvent(at(60), "info", "t-cp", "s-a", "s-root", "a", 60),
				spanEvent(at(90), "info", "t-cp", "s-b", "s-root", "b", 80),
			},
			want: map[string]mark{"span:s-root": {10, true, false}, "span:s-a": {60, false, false}, "span:s-b": {80, true, false}},
		},
		{
			name: "a contained link marks every span below it inferred",
			events: []segment.Record{
				spanEvent(at(100), "info", "t-cp", "s-root", "", "root", 100),
				spanEvent(at(90), "info", "t-cp", "s-mid", "", "mid", 80),
				spanEvent(at(80), "info", "t-cp", "s-leaf", "s-mid", "leaf", 60),
			},
			want: map[string]mark{"span:s-root": {20, true, false}, "span:s-mid": {20, true, true}, "span:s-leaf": {60, true, true}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, rd := readers(seedTrace(t, []traceSeg{{"api", tc.events}}))
			env, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tools.TraceRequest{Value: "t-cp"})
			require.NoError(t, err)
			got := map[string]mark{}
			for _, s := range env.Data.Spans {
				got[s.ID] = mark{s.SelfMs, s.Critical, s.CriticalInferred}
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestGetTraceGroupsSiblings collapses 180 cache reads under one request into a group
// keeping the 3 slowest and the erroring reads, and caps the grouped output at Limit.
func TestGetTraceGroupsSiblings(t *testing.T) {
	events := []segment.Record{spanEvent(at(1000), "info", "t-g", "s-root", "", "GET /", 1000)}
	slow := map[int]int64{7: 90, 8: 80, 9: 70}
	for i := range 180 {
		level := "info"
		if i == 20 || i == 21 {
			level = "error"
		}
		events = append(events, spanEvent(at(200+i), level, "t-g", "c-"+strconv.Itoa(i), "s-root", "cache read", cmp.Or(slow[i], 10)))
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"api", events}}))

	type node struct {
		id     string
		parent int
	}
	tests := []struct {
		name  string
		limit int
		want  []node
	}{
		{name: "whole", want: []node{
			{"span:s-root", -1}, {"group:span:c-7", 0},
			{"span:c-7", 1}, {"span:c-8", 1}, {"span:c-9", 1}, {"span:c-20", 1}, {"span:c-21", 1},
		}},
		{name: "limited", limit: 3, want: []node{{"span:s-root", -1}, {"group:span:c-7", 0}, {"span:c-7", 1}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			env, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tools.TraceRequest{Value: "t-g", Limit: tc.limit})
			require.NoError(t, err)
			var got []node
			for _, s := range env.Data.Spans {
				got = append(got, node{s.ID, s.Parent})
			}
			assert.Equal(t, tc.want, got)
			assert.Equal(t, 7, env.Data.Total)
			group := env.Data.Spans[1]
			assert.Equal(t, &tools.SpanGroup{Count: 180, P50Ms: 10, P95Ms: 10, MaxMs: 90, Errors: 2}, group.Group)
			assert.Equal(t, 180, group.Lines)
		})
	}
}

// TestGetTraceAllocBudget gates reconstructing a trace at a fixed allocation budget per span:
// 10,000 spans of one request fanning out to ten operations, and a 600-span chain, as an
// agent's transcript is, and 600 spans naming a parent that wrote no span, each linked through
// the hop to its caller without walking every other span's ancestors.
func TestGetTraceAllocBudget(t *testing.T) {
	fanOut := []segment.Record{spanEvent(at(10000), "info", "t-big", "s-root", "", "GET /", 10000)}
	for i := 1; i < 10000; i++ {
		fanOut = append(fanOut, spanEvent(at(i), "info", "t-big", "c-"+strconv.Itoa(i), "s-root", opName(i%10), 1))
	}
	chain := []segment.Record{spanEvent(at(0), "info", "t-chain", "c-0", "", "turn", 1200)}
	for i := 1; i < 600; i++ {
		chain = append(chain, spanEvent(at(i), "info", "t-chain", "c-"+strconv.Itoa(i), "c-"+strconv.Itoa(i-1), opName(i%10), int64(1200-i)),
			spanEvent(at(600+i), "info", "t-chain", "l-"+strconv.Itoa(i), "hop-"+strconv.Itoa(i), opName(i%10), 1))
	}
	tests := []struct {
		name, trace string
		events      []segment.Record
		total       int
		perSpan     float64
	}{
		{name: "fan-out", trace: "t-big", events: fanOut, total: 41, perSpan: 16}, // the root, and each operation's group and 3 slowest
		{name: "chain through missing hops", trace: "t-chain", events: chain, total: 640, perSpan: 40},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cat, rd := readers(seedTrace(t, []traceSeg{{"api", tc.events}}))
			ts := toolsOver(cat, rd, nil)
			allocs := testing.AllocsPerRun(3, func() {
				env, err := ts.GetTrace(context.Background(), tools.TraceRequest{Value: tc.trace, Limit: len(tc.events)})
				require.NoError(t, err)
				require.Equal(t, tc.total, env.Data.Total)
			})
			assert.Less(t, allocs/float64(len(tc.events)), tc.perSpan, "%.0f allocs over %d spans", allocs, len(tc.events))
		})
	}
}

// TestGetTraceDiscovery reads a 2,000-message transcript and 1,300 lines between its
// messages naming none, all carrying 1,500 sparse id-like columns and a model copied onto
// every line, as ingest's carry leaves an agent's. Its uuid/parentUuid pair is found among
// 1,500² candidates, and the model, linking more lines but all to one value, does not pass
// for it.
func TestGetTraceDiscovery(t *testing.T) {
	const messages, between, sparse = 2000, 1300, 1500
	var events []segment.Record
	for i := range messages + between {
		var r segment.Record
		if i < messages {
			uuid, parent := "u"+strconv.Itoa(i), "u"+strconv.Itoa(max(0, i-1-i%2))
			if i == 0 {
				parent = ""
			}
			r = transcriptEvent(at(i), uuid, parent, "assistant", "step "+strconv.Itoa(i))
			if i%5 < 3 {
				r = withAttr(r, "toolUseResult.resolvedModel", "claude-opus-5-5")
			}
		} else {
			r = segment.LogRecord(at(i), "info", "progress", map[string]string{"session_id": "sess-1"})
		}
		r = withAttr(withAttr(r, "params.model", "claude-opus-5-5"), "x"+strconv.Itoa(i%sparse)+"_id", "v"+strconv.Itoa(i))
		events = append(events, r)
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"claude", events}}))
	env, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tools.TraceRequest{Field: "session_id", Value: "sess-1", Time: traceWindow, Limit: messages + between})
	require.NoError(t, err)
	type pair struct{ id, parent string }
	assert.Equal(t, pair{"uuid", "parentUuid"}, pair{env.Data.IDField, env.Data.ParentField})
}

// TestSpanNameFallsBackToService names a span whose row carries neither a span.name nor
// a pattern its footer holds text for by the span's service.
func TestSpanNameFallsBackToService(t *testing.T) {
	row := scan.NewRow(&segment.Descriptor{}, at(0), map[string][]byte{"pattern_id": []byte("p-1")})
	assert.Equal(t, "worker", tools.ExportSpanName(row, segment.Footer{}, "worker"))
}

// TestGetTraceJoinsASessionIntoSteps reads a 300-line session whose agent names it session_id
// and whose gateway, logging its cost, names it session: get_trace and aggregate join the two,
// and the agent's turns, each a prompt, a burst of tool calls and an answer, read as steps.
func TestGetTraceJoinsASessionIntoSteps(t *testing.T) {
	var agent []segment.Record
	for turn := range 50 {
		ms := turn * 5000
		agent = append(agent, logEvent(at(ms), "info", "session_id", "s-1", "user prompt"))
		for call := range 4 {
			agent = append(agent, logEvent(at(ms+100+10*call), "info", "session_id", "s-1", "tool call Bash"))
		}
		answer := logEvent(at(ms+900), "info", "session_id", "s-1", "model answered")
		agent = append(agent, withNumber(answer, "tokens", float64(100+turn)))
	}
	gateway := []segment.Record{
		withNumber(logEvent(at(50), "info", "session", "s-1", "usage recorded"), "cost", 0.5),
		withNumber(logEvent(at(100000), "info", "session", "s-1", "usage recorded"), "cost", 1.25),
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"agent", agent}, {"gateway", gateway}}))
	ts := toolsOver(cat, rd, nil)

	trace, err := ts.GetTrace(context.Background(), tools.TraceRequest{Field: "session_id", Value: "s-1", Time: traceWindow})
	require.NoError(t, err)
	got, err := json.MarshalIndent(trace.Data, "", "  ")
	require.NoError(t, err)
	path := filepath.Join("testdata", "get_trace_session.golden.json")
	if *updateGolden {
		require.NoError(t, os.WriteFile(path, append(got, '\n'), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "run with -update to create")
	assert.JSONEq(t, string(want), string(got))

	cost, err := ts.Aggregate(context.Background(), tools.AggregateRequest{
		Selector: selector.Selector{Time: traceWindow}, Measure: "sum:cost", GroupBy: "session_id", Bins: "all",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"session_id", "session"}, cost.Data.Fields)
	require.Len(t, cost.Data.Series, 1)
	assert.Equal(t, "s-1", cost.Data.Series[0].Key)
	assert.InDelta(t, 1.75, *cost.Data.Series[0].Values[0], 1e-9)
}

// TestSpanNameDiscovered names each span of a checkout by the column that names its
// operation, where one does: a column holding one value across a service's lines of a
// pattern, the most values of those, and span.name only when it is that column. Spans
// logged with no message, as OpenTelemetry's are, have no pattern to agree with, so a
// service doing two operations is named by them rather than by its pod.
func TestSpanNameDiscovered(t *testing.T) {
	// Each line is its service, span, parent span, message, operation and status.
	lines := [][6]string{
		{"api", "s-root", "", "GET /checkout ok in 50 ms", "Checkout", "ok"},
		{"payment", "p-1", "s-root", "Charge ok in 11 ms", "Charge", "ok"},
		{"payment", "p-2", "s-root", "Charge ok in 12 ms", "Charge", "ok"},
		{"payment", "p-3", "s-root", "Charge error in 30 ms", "Charge", "error"},
		{"db", "d-1", "p-1", "SELECT ok in 5 ms", "Query", "ok"},
		{"db", "d-2", "p-1", "SELECT ok in 5 ms", "Query", "ok"},
	}
	byOp := []string{"Checkout", "Charge", "Charge", "Charge", "Query", "Query"}
	tests := []struct {
		name  string
		attrs func(l [6]string, i int) map[string]string
		want  []string
		quiet bool
	}{
		{"op, level and status are named by op", func(l [6]string, _ int) map[string]string {
			return map[string]string{"op": l[4], "status": l[5]}
		}, byOp, false},
		{"no qualifying column is named by the pattern", func(_ [6]string, i int) map[string]string {
			return map[string]string{"user": []string{"ann", "bob"}[i%2]}
		}, []string{"GET /checkout ok in <*> ms", "Charge ok in <*> ms", "Charge ok in <*> ms", "Charge error in <*> ms", "SELECT ok in <*> ms", "SELECT ok in <*> ms"}, false},
		{"span.name is named by as it qualifies", func(l [6]string, _ int) map[string]string {
			return map[string]string{"span.name": l[4]}
		}, byOp, false},
		{"lines with no message are named by name, not their pod", func(l [6]string, i int) map[string]string {
			return map[string]string{"name": l[4] + map[bool]string{true: "→db.query"}[i == 2], "service.instance.id": l[0] + "-7f9c"}
		}, []string{"Checkout", "Charge", "Charge→db.query", "Charge", "Query", "Query"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			segs := map[string][]segment.Record{}
			for i, l := range lines {
				attrs := tc.attrs(l, i)
				attrs["trace_id"], attrs["span_id"], attrs["parent_span_id"] = "t-1", l[1], l[2]
				message := l[3]
				if tc.quiet {
					message = ""
				}
				segs[l[0]] = append(segs[l[0]], segment.LogRecord(at(100*i), map[string]string{"ok": "info", "error": "error"}[l[5]], message, attrs))
			}
			cat, rd := readers(seedTrace(t, []traceSeg{{"api", segs["api"]}, {"payment", segs["payment"]}, {"db", segs["db"]}}))
			env, err := toolsOver(cat, rd, nil).GetTrace(context.Background(), tools.TraceRequest{Value: "t-1"})
			require.NoError(t, err)
			var got []string
			for _, s := range env.Data.Spans {
				got = append(got, s.Name)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestTraceShapeMergesOkAndError merges four checkouts, one whose charge failed: named by
// span_name, the ok and failed Charge lines are one node, whose error share says which failed.
func TestTraceShapeMergesOkAndError(t *testing.T) {
	var api, payment []segment.Record
	for i := range 4 {
		id, level := fmt.Sprintf("t-%d", i), map[bool]string{true: "error", false: "info"}[i == 1]
		api = append(api, segment.LogRecord(at(1000*i), "info", "GET /checkout ok in 50 ms", map[string]string{"trace_id": id, "span_name": "Checkout"}))
		payment = append(payment, segment.LogRecord(at(1000*i-10), level, "Charge "+level+" in 20 ms", map[string]string{"trace_id": id, "span_name": "Charge"}))
	}
	cat, rd := readers(seedTrace(t, []traceSeg{{"api", api}, {"payment", payment}}))
	env, err := toolsOver(cat, rd, nil).TraceShape(context.Background(), tools.TraceShapeRequest{
		Selector: selector.Selector{Time: traceWindow}, Root: []selector.Predicate{{Field: "span_name", Op: selector.OpEq, Value: "Checkout"}},
	})
	require.NoError(t, err)
	assert.Equal(t, tools.TraceShapeResponse{Traces: 4, Nodes: []tools.ShapeNode{
		{Service: "api", Name: "Checkout", Parent: -1, Presence: 1, P50Ms: 50, P95Ms: 50, Count: 4},
		{Service: "payment", Name: "Charge", Parent: 0, Presence: 1, P50Ms: 20, P95Ms: 20, ErrorShare: 0.25, Count: 4},
	}}, env.Data)
}
