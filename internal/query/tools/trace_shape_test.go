package tools

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/value"
)

// TestTraceShape merges four GET /cart traces, one whose five queries come back as a group
// span and one of 101 spans, and diffs against them one trace skipping the query for a cache
// read and one slow. Root lines without a trace_id are no trace to sample.
func TestTraceShape(t *testing.T) {
	base := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	span := func(trace, service, id, parent, name, level string, endMs, durMs int) scan.Row {
		return scan.NewRow(&segment.Descriptor{}, base.Add(time.Duration(endMs)*time.Millisecond), map[string][]byte{
			"trace_id": []byte(trace), "service": []byte(service), "span_id": []byte(id), "parent_span_id": []byte(parent),
			"span.name": []byte(name), "pattern_id": []byte(name), "level": []byte(level), "duration_ms": []byte(strconv.Itoa(durMs)),
		})
	}
	traces := map[string][]scan.Row{
		"t-extra": {span("t-extra", "api", "root", "", "GET /cart", "info", 10000, 100), span("t-extra", "api", "c", "root", "GET", "info", 9990, 3)},
		"t-slow":  {span("t-slow", "api", "root", "", "GET /cart", "info", 11000, 600), span("t-slow", "db", "q", "root", "SELECT", "info", 10990, 500)},
	}
	sampled := []string{"t-0", "t-1", "t-2", "t-3"}
	for i, id := range sampled {
		traces[id] = []scan.Row{span(id, "api", "root", "", "GET /cart", "info", i*1000, 100+10*i),
			span(id, "db", "q", "root", "SELECT", map[bool]string{false: "info", true: "error"}[i == 1], i*1000-10, 20)}
	}
	for i := range 99 {
		traces["t-0"] = append(traces["t-0"], span("t-0", fmt.Sprintf("replica-%02d", i), "r"+strconv.Itoa(i), "root", "retry", "info", -10, 5))
	}
	for _, id := range []string{"q1", "q2", "q3", "q4"} {
		traces["t-3"] = append(traces["t-3"], span("t-3", "db", id, "root", "SELECT", "info", 2990, 20))
	}

	ts := New(Deps{Catalog: catalog.Fake{},
		Series: series.Fake{ReadFn: func(req series.Request) (series.Result, error) {
			first := map[string]map[int64]float64{}
			for i, id := range sampled {
				first[id] = map[int64]float64{req.Bins.Boundaries()[0]: float64(i)}
			}
			first[""] = map[int64]float64{req.Bins.Boundaries()[0]: -1}
			return series.Result{Coverage: 1, Cost: query.Cost{GETs: 1}, Measures: []series.Result{{Values: first}}}, nil
		}},
		Planner: plan.Fake{PruneFn: func(catalog.Version, selector.Resolved) (plan.Plan, error) {
			footer := segment.Footer{ZoneMaps: segment.ZoneMaps{"trace_id": {}, "span_id": {}, "span.name": {Type: value.TypeString}}}
			return plan.Plan{Items: []plan.WorkItem{{Footer: footer}}, Cost: query.Cost{GETs: 1}}, nil
		}},
		Scanner: scan.Fake{ScanFn: func(_ plan.Plan, q scan.Query) scan.Canned {
			var rows []scan.Row // the traces a read of one or of any of several names
			for _, p := range append([]selector.ParsedPredicate{q.Where[0]}, q.Where[0].Any...) {
				rows = append(rows, traces[p.Value]...)
			}
			return scan.Canned{Rows: rows, Cost: query.Cost{GETs: 1}}
		}},
	})
	shape := []ShapeNode{
		{Service: "api", Name: "GET /cart", Parent: -1, Presence: 1, P50Ms: 110, P95Ms: 130, Count: 4},
		{Service: "db", Name: "SELECT", Parent: 0, Presence: 1, P50Ms: 20, P95Ms: 20, ErrorShare: 0.125, Count: 8},
	}
	for i := range 99 {
		shape = append(shape, ShapeNode{Service: fmt.Sprintf("replica-%02d", i), Name: "retry", Parent: 0, Presence: 0.25, P50Ms: 5, P95Ms: 5, Count: 1})
	}
	extra := append([]ShapeNode{shape[0], {Service: "api", Name: "GET", Parent: 0, Diff: "extra", InTrace: true}}, shape[1:]...)
	extra[0].InTrace, extra[2].Diff = true, "missing"
	slow := append([]ShapeNode(nil), shape...)
	slow[0].Diff, slow[1].Diff = "slower", "slower"
	slow[0].InTrace, slow[1].InTrace = true, true
	tests := []struct {
		compare string
		want    []ShapeNode
		gets    int
	}{
		{want: shape, gets: 6},
		{compare: "t-extra", want: extra, gets: 8},
		{compare: "t-slow", want: slow, gets: 8},
	}
	for _, tt := range tests {
		t.Run("compare "+tt.compare, func(t *testing.T) {
			req := TraceShapeRequest{Selector: selector.Selector{Time: selector.TimeSpec{From: base.Add(-time.Hour).Format(time.RFC3339),
				To: base.Add(time.Hour).Format(time.RFC3339)}}, Root: []selector.Predicate{{Field: "span.name", Op: selector.OpEq, Value: "GET /cart"}}, Compare: tt.compare}
			first, err := ts.TraceShape(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, TraceShapeResponse{Traces: 4, Nodes: tt.want}, first.Data)
			assert.Equal(t, tt.gets, first.Meta.Cost.GETs, "one aggregate, a prune a sampled trace, one scan reading them all, and the compared one's prune and scan")

			second, _ := ts.TraceShape(context.Background(), req)
			assert.Equal(t, first, second, "the same request and snapshot must give the same output")
		})
	}
}
