package tools_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

var fieldsBase = time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)

// hostEvents builds events whose "host" attribute rotates through the given
// values, one event per host. extra attributes are added to every event.
func hostEvents(start time.Time, hosts []string, extra map[string]string) []segment.Record {
	out := make([]segment.Record, len(hosts))
	for i, h := range hosts {
		attrs := map[string]string{"host": h}
		for k, v := range extra {
			attrs[k] = v
		}
		out[i] = segment.LogRecord(start.Add(time.Duration(i)*time.Second), "info", "hello world", attrs)
	}
	return out
}

// typedEvents builds 80 events whose id, numeric param, enum and free-text columns each infer a different type.
func typedEvents() []segment.Record {
	events := make([]segment.Record, 80)
	for i := range events {
		events[i] = segment.LogRecord(
			fieldsBase.Add(time.Duration(i)*time.Second),
			"info",
			// The trailing "ms" of pattern "duration <*> ms" names the param duration_ms, not param_1.
			fmt.Sprintf("duration %d ms", 10+i),
			map[string]string{
				"trace_id": fmt.Sprintf("t-%d", i%5), // id by its name: distinct-count 5
				// An id nobody put on a list, by its values whatever its name: rank_fields
				// is how an agent discovers that req can group a trace, so it has to say "id".
				"req":        fmt.Sprintf("q-%d", i/3),
				"order_id":   fmt.Sprintf("o-%d", i),
				"user_agent": fmt.Sprintf("agent %d", i), // str: 80 distinct, holding spaces
				"region":     fmt.Sprintf("r-%d", i%3),   // enum: 3 distinct
			},
		)
	}
	return events
}

// TestRankFields pins what rank_fields reports for each corpus, always sorted by signal descending.
func TestRankFields(t *testing.T) {
	type segmentSeed struct {
		service string
		events  []segment.Record
	}
	hosts := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("h%d", i+1)
		}
		return out
	}

	tests := []struct {
		name     string
		segments []segmentSeed
		// check asserts the row's behaviour over the fields keyed by name.
		check func(t *testing.T, fields map[string]tools.FieldStat)
	}{
		{
			// HLLs merge across segments, so the cardinality is the union count, not any single segment's.
			name: "cardinality is merged across segments",
			segments: []segmentSeed{
				{"svc-a", hostEvents(fieldsBase, hosts(5), nil)},
				{"svc-b", hostEvents(fieldsBase.Add(time.Minute), hosts(3), nil)},
			},
			check: func(t *testing.T, fields map[string]tools.FieldStat) {
				require.Contains(t, fields, "host")
				assert.GreaterOrEqual(t, fields["host"].Cardinality, int64(5))
			},
		},
		{
			name: "a column in half the segments is half present",
			segments: []segmentSeed{
				{"svc-a", hostEvents(fieldsBase, []string{"h1", "h2"}, map[string]string{"deploy_version": "v1"})},
				{"svc-b", hostEvents(fieldsBase.Add(time.Minute), []string{"h3", "h4"}, nil)},
			},
			check: func(t *testing.T, fields map[string]tools.FieldStat) {
				require.Contains(t, fields, "deploy_version")
				assert.Equal(t, 1.0, fields["host"].Present)
				assert.Equal(t, 0.5, fields["deploy_version"].Present)
			},
		},
		{
			name:     "a high-cardinality field outranks a constant one",
			segments: []segmentSeed{{"svc-a", hostEvents(fieldsBase, hosts(8), nil)}},
			check: func(t *testing.T, fields map[string]tools.FieldStat) {
				assert.Greater(t, fields["host"].Signal, fields["level"].Signal)
			},
		},
		{
			// A constant field sits at the signal floor and is not high-signal, while a multi-valued one clears the threshold.
			name:     "high signal",
			segments: []segmentSeed{{"svc-a", hostEvents(fieldsBase, hosts(5), nil)}},
			check: func(t *testing.T, fields map[string]tools.FieldStat) {
				assert.True(t, fields["host"].HighSignal, "host signal=%v", fields["host"].Signal)
				assert.False(t, fields["level"].HighSignal, "constant level signal=%v", fields["level"].Signal)
			},
		},
		{
			name:     "type inference",
			segments: []segmentSeed{{"svc-a", typedEvents()}},
			check: func(t *testing.T, fields map[string]tools.FieldStat) {
				want := map[string]string{
					"trace_id":    "id",
					"order_id":    "id",
					"req":         "id",
					"duration_ms": "num",
					"region":      "enum",
					"user_agent":  "str",
				}
				got := map[string]string{}
				for name := range want {
					require.Contains(t, fields, name)
					got[name] = fields[name].Type
				}
				assert.Equal(t, want, got)
			},
		},
		{
			// No matching descriptors is an empty field set, not an error.
			name: "empty corpus",
			check: func(t *testing.T, fields map[string]tools.FieldStat) {
				assert.Empty(t, fields)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			store, cat := storage.NewFS(dir), catalog.New(storage.NewFS(dir), testTenant)
			for _, s := range tt.segments {
				writeSegment(t, store, cat, s.service, s.events)
			}
			cr, rd := readers(dir)
			env, err := toolsOver(cr, rd, nil).RankFields(context.Background(),
				tools.FieldsRequest{Selector: selector.Selector{}})
			require.NoError(t, err)

			byName := map[string]tools.FieldStat{}
			for i, f := range env.Data.Fields {
				byName[f.Name] = f
				if prev := env.Data.Fields[max(i-1, 0)]; i > 0 && (prev.Type == "id") == (f.Type == "id") {
					assert.LessOrEqual(t, f.Signal, prev.Signal, "not sorted by signal desc at %d", i)
				} else if i > 0 {
					assert.Equal(t, "id", f.Type, "a field after an id at %d", i)
				}
			}
			tt.check(t, byName)
		})
	}
}

// TestRankFieldsNeedsNoTenant asserts an empty selector is a valid request: the
// tenant is the catalog's, not the caller's.
func TestRankFieldsNeedsNoTenant(t *testing.T) {
	t.Parallel()
	cat, rd := readers(t.TempDir())
	_, err := toolsOver(cat, rd, nil).RankFields(context.Background(),
		tools.FieldsRequest{Selector: selector.Selector{}},
	)
	require.NoError(t, err)
}

// TestRankFieldsReportsDerivedColumns lists a derived column with the presence of the most
// present column it reads, typed num when arithmetic makes its every value a number, and
// leaves out one whose columns the window does not hold.
func TestRankFieldsReportsDerivedColumns(t *testing.T) {
	t.Parallel()
	dir, store, cat := newCorpus(t)
	writeSegment(t, store, cat, "agent", hostEvents(fieldsBase, []string{"h1"}, map[string]string{"tokens": "10", "model": "opus"}))
	writeSegment(t, store, cat, "agent", hostEvents(fieldsBase.Add(time.Minute), []string{"h2"}, nil))
	derived, err := plan.NewDerived([]plan.DerivedColumn{
		{Name: "cost_usd", Expr: "tokens/1e6*lookup(model, price_in)"},
		{Name: "tier", Expr: "lookup(model, tiers)"},
		{Name: "absent", Expr: "nothing*2"},
	}, map[string]map[string]string{"price_in": {"opus": "15"}, "tiers": {"opus": "large"}})
	require.NoError(t, err)
	cr, rd := readers(dir)
	planner := plan.New(plan.Deps{Catalog: cr, Reader: rd, Derived: derived})
	scanner := scan.New(rd, derived)
	env, err := tools.New(tools.Deps{Catalog: cr, Series: series.New(planner, scanner), Planner: planner, Scanner: scanner}).
		RankFields(context.Background(), tools.FieldsRequest{Selector: selector.Selector{}})
	require.NoError(t, err)

	got := map[string]tools.FieldStat{}
	for _, f := range env.Data.Fields {
		if derived.Has(f.Name) {
			got[f.Name] = f
		}
	}
	assert.Equal(t, map[string]tools.FieldStat{
		"cost_usd": {Name: "cost_usd", Type: "num", Present: 0.5, Lines: 0.5},
		"tier":     {Name: "tier", Type: "str", Present: 0.5, Lines: 0.5},
	}, got)
}

// TestRankFieldsLinesAndRepeats: a column's lines are the fraction of rows holding it, and
// it is an enum only when its values repeat, read from the footer or, under a predicate the
// descriptor does not settle, from the matched rows.
func TestRankFieldsLinesAndRepeats(t *testing.T) {
	t.Parallel()
	dir, store, cat := newCorpus(t)
	events := make([]segment.Record, 10)
	for i := range events {
		attrs := map[string]string{"a": fmt.Sprintf("a-%d", i%2), "b": fmt.Sprintf("b %d", i)}
		if i < 3 {
			attrs["c"] = "held"
		}
		events[i] = segment.LogRecord(fieldsBase.Add(time.Duration(i)*time.Second), "info", "hello", attrs)
	}
	writeSegment(t, store, cat, "api", events)
	cr, rd := readers(dir)
	for _, where := range [][]selector.Predicate{nil, {{Field: "b", Op: selector.OpExists}}} {
		env, err := toolsOver(cr, rd, nil).RankFields(context.Background(), tools.FieldsRequest{Selector: selector.Selector{Where: where}})
		require.NoError(t, err)
		got := map[string]string{}
		for _, f := range env.Data.Fields {
			got[f.Name] = fmt.Sprintf("%s %.1f", f.Type, f.Lines)
		}
		assert.Equal(t, map[string]string{"a": "enum 1.0", "b": "str 1.0", "c": "enum 0.3"},
			map[string]string{"a": got["a"], "b": got["b"], "c": got["c"]}, "where %v", where)
	}
}

// TestRankFieldsOverMatchedRows: rank_fields describes only the rows the selector matches,
// leaving out the columns the system writes for its own reads.
func TestRankFieldsOverMatchedRows(t *testing.T) {
	t.Parallel()
	cr, rd := readers(mixedCorpus(t))
	env, err := toolsOver(cr, rd, nil).RankFields(context.Background(), tools.FieldsRequest{Selector: selector.Selector{
		Where: []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "a"}}}})
	require.NoError(t, err)
	got := map[string]int64{}
	for _, f := range env.Data.Fields {
		got[f.Name] = f.Cardinality
	}
	assert.Equal(t, map[string]int64{"service": 1, "level": 2, "message": 3, "route": 1}, got)
}

// TestRankFieldsBound: past the scan bound rank_fields refuses before reading a row.
func TestRankFieldsBound(t *testing.T) {
	t.Parallel()
	dir := mixedCorpus(t)
	cr, rd := readers(dir)
	ctx := context.Background()
	version, err := cr.Version(ctx)
	require.NoError(t, err)
	descs, err := cr.Prune(ctx, version, selector.Resolved{})
	require.NoError(t, err)
	// The segment mixing both services, whose rows a ranking of service a's errors must scan:
	// its view of a answers for all of a's rows, not for its errors alone.
	i := slices.IndexFunc(descs, func(d segment.Descriptor) bool { _, settled := plan.SegmentValue(d, "service"); return !settled })
	require.GreaterOrEqual(t, i, 0)
	huge := descs[i]
	huge.ID, huge.EstimatedSize = huge.ID+"-huge", 5<<30
	require.NoError(t, cr.Append(ctx, huge))

	planner := plan.New(plan.Deps{Catalog: cr, Reader: rd})
	scanner := scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned {
		t.Error("rank_fields read a row past its bound")
		return scan.Canned{}
	}}
	_, err = tools.New(tools.Deps{Catalog: cr, Series: series.New(planner, scanner), Planner: planner, Scanner: scanner}).
		RankFields(ctx, tools.FieldsRequest{Selector: selector.Selector{Where: []selector.Predicate{
			{Field: "service", Op: selector.OpEq, Value: "a"}, {Field: "level", Op: selector.OpEq, Value: "error"}}}})
	var apiErr *query.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, query.ErrCodeScanTooLarge, apiErr.Code)
}

// TestDescribeSchemaProducers lists one producer per profile version, sorted by profile then
// version, each with the highest degradation any of its segments reached.
func TestDescribeSchemaProducers(t *testing.T) {
	t.Parallel()
	footer := func(id, name string, version, degradation int) plan.WorkItem {
		return plan.WorkItem{
			Segment: segment.Descriptor{ID: id},
			Footer:  segment.Footer{PatternProfile: segment.PatternProfile{Name: name, Version: version, Degradation: degradation}},
		}
	}
	tests := []struct {
		name  string
		items []plan.WorkItem
		want  []tools.Producer
	}{
		{
			name:  "two profiles",
			items: []plan.WorkItem{footer("a", "logs.text", 1, 0), footer("b", "auto", 1, 2), footer("c", "auto", 1, 1)},
			want: []tools.Producer{
				{Profile: "auto", Version: 1, Degradation: "catch_all", Segments: 2},
				{Profile: "logs.text", Version: 1, Degradation: "none", Segments: 1},
			},
		},
		{
			name:  "two versions of one profile",
			items: []plan.WorkItem{footer("a", "auto", 2, 0), footer("b", "auto", 1, 1)},
			want: []tools.Producer{
				{Profile: "auto", Version: 1, Degradation: "column_names", Segments: 1},
				{Profile: "auto", Version: 2, Degradation: "none", Segments: 1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			planner := plan.Fake{PruneFn: func(catalog.Version, selector.Resolved) (plan.Plan, error) {
				return plan.Plan{Items: tt.items}, nil
			}}
			env, err := tools.New(tools.Deps{Catalog: catalog.Fake{}, Planner: planner}).
				DescribeSchema(context.Background(), tools.FieldsRequest{})
			require.NoError(t, err)
			assert.Equal(t, tools.DescribeSchemaResponse{Columns: []tools.SchemaColumn{}, Producers: tt.want}, env.Data)
		})
	}
}

// countedScanner counts the scans made through it.
type countedScanner struct {
	scan.Scanner
	scans *int
}

func (c countedScanner) Scan(ctx context.Context, p plan.Plan, q scan.Query) *scan.Rows {
	if len(p.Items) > 0 {
		*c.scans++
	}
	return c.Scanner.Scan(ctx, p, q)
}

// TestRankFieldsReadsWhatItMust: over a segment mixing two services, a ranking of one
// service is read from its view and scans nothing; one the view cannot answer scans the
// segment once, and the same ranking again answers the same from what that scan kept.
func TestRankFieldsReadsWhatItMust(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		where []selector.Predicate
		scans []int
	}{
		{"a service", []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "a"}}, []int{0, 0}},
		{"a service's errors", []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "a"},
			{Field: "level", Op: selector.OpEq, Value: "error"}}, []int{1, 1}},
		{"every service's errors", []selector.Predicate{{Field: "level", Op: selector.OpEq, Value: "error"}}, []int{1, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cr, rd := readers(mixedCorpus(t))
			planner, scans := plan.New(plan.Deps{Catalog: cr, Reader: rd}), 0
			scanner := countedScanner{scan.New(rd, plan.Derived{}), &scans}
			ts := tools.New(tools.Deps{Catalog: cr, Series: series.New(planner, scanner), Planner: planner, Scanner: scanner})
			var answers []tools.FieldsResponse
			var got []int
			for range 2 {
				env, err := ts.RankFields(context.Background(), tools.FieldsRequest{Selector: selector.Selector{Where: tc.where}})
				require.NoError(t, err)
				answers, got = append(answers, env.Data), append(got, scans)
			}
			assert.Equal(t, tc.scans, got, "scans made by the first call, then by both")
			assert.Equal(t, answers[0], answers[1])
		})
	}
}
