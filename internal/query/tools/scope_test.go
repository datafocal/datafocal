package tools_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// TestAScopeAnswersAloneInASharedSegment: a call scoped to one service answers the same
// whether that service's rows are in segments of their own or in a compacted segment shared
// with another service, whose fields (here a path with more values than an enum holds) are
// no part of the scope.
func TestAScopeAnswersAloneInASharedSegment(t *testing.T) {
	var api, web []segment.Record
	for i := range 400 {
		at := costBase.Add(time.Duration(i) * time.Second)
		attrs := map[string]string{segment.ColService: "api", "path": []string{"/a", "/b", "/c", "/d"}[i%4],
			"duration_ms": fmt.Sprint(i % 90)}
		switch i % 20 {
		case 0:
			attrs["path"], attrs["duration_ms"] = "/slow", fmt.Sprint(3000+i)
		case 10:
			attrs["path"] = "/slow" // and as often fast
		}
		api = append(api, segment.LogRecord(at, "info", "request", attrs))
		web = append(web, segment.LogRecord(at, "info", "page", map[string]string{segment.ColService: "web",
			"path": fmt.Sprintf("/page/%d", i), "theme": "dark"}))
	}
	apart, shared := corpusOf(t, map[string][]segment.Record{"api": api, "web": web}), corpusOf(t, map[string][]segment.Record{"": append(api, web...)})

	ctx := context.Background()
	scope := selector.Selector{Time: costWindow, Where: []selector.Predicate{{Field: segment.ColService, Op: selector.OpEq, Value: "api"}}}
	slow := selector.Predicate{Field: "duration_ms", Op: selector.OpGt, Value: "2000"}
	for _, tc := range []struct {
		name string
		call func(tools.Tools) (any, error)
	}{
		{"rank_fields", func(ts tools.Tools) (any, error) {
			env, err := ts.RankFields(ctx, tools.FieldsRequest{Selector: scope})
			return env.Data, err
		}},
		{"explain_spike", func(ts tools.Tools) (any, error) {
			env, err := ts.ExplainSpike(ctx, tools.ExplainSpikeRequest{Selector: scope, Slice: append(scope.Where, slow)})
			return env.Data, err
		}},
		{"aggregate", func(ts tools.Tools) (any, error) {
			env, err := ts.Aggregate(ctx, tools.AggregateRequest{Selector: scope, Measure: "count", GroupBy: "path", Bins: "all"})
			return env.Data, err
		}},
		{"patterns", func(ts tools.Tools) (any, error) {
			env, err := ts.Patterns(ctx, tools.PatternsRequest{Selector: scope, By: "path"})
			return env.Data, err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := tc.call(apart)
			require.NoError(t, err)
			got, err := tc.call(shared)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

// corpusOf writes each service's records as one segment, "" a compacted segment of every
// service its rows name, and returns the tools over them.
func corpusOf(t *testing.T, segments map[string][]segment.Record) tools.Tools {
	t.Helper()
	dir, store, cat := newCorpus(t)
	for service, records := range segments {
		writeRecords(t, store, cat, service, records)
	}
	cr, rd := readers(dir)
	return toolsOver(cr, rd, nil)
}
