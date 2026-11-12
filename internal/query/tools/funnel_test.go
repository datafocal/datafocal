package tools

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// TestFunnel pins the hand count of transcript sessions going from a user prompt to an
// Edit call to a failing test to a passing one, and the session within_seconds drops
// for taking a minute and a half.
func TestFunnel(t *testing.T) {
	base := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	line := func(session string, sec int, field, value string) scan.Row {
		return scan.NewRow(&segment.Descriptor{}, base.Add(time.Duration(sec)*time.Second),
			map[string][]byte{"session_id": []byte(session), field: []byte(value)})
	}
	prompt := func(s string, sec int) scan.Row { return line(s, sec, "type", "user") }
	edit := func(s string, sec int) scan.Row { return line(s, sec, "tool_name", "Edit") }
	test := func(s string, sec int, result string) scan.Row {
		return line(s, sec, "message", "--- "+result+": TestX")
	}
	rows := []scan.Row{
		prompt("s-1", 0), edit("s-1", 10), test("s-1", 20, "FAIL"), test("s-1", 40, "PASS"),
		prompt("s-2", 0), edit("s-2", 5), test("s-2", 30, "FAIL"),
		prompt("s-3", 0), edit("s-3", 20),
		prompt("s-4", 0),
		edit("s-5", 0), prompt("s-5", 1),
		line("s-6", 0, "type", "assistant"),
		prompt("s-7", 0), edit("s-7", 10), test("s-7", 30, "PASS"), test("s-7", 60, "FAIL"), test("s-7", 90, "PASS"),
	}
	slices.Reverse(rows)

	tests := []struct {
		name   string
		within int
		last   []FunnelStep
	}{
		{name: "no limit", last: []FunnelStep{
			{Reached: 3, MedianFromMs: 25000, StoppedHere: []string{"s-2"}},
			{Reached: 2, MedianFromMs: 20000, StoppedHere: []string{}},
		}},
		{name: "within a minute", within: 60, last: []FunnelStep{
			{Reached: 3, MedianFromMs: 25000, StoppedHere: []string{"s-2", "s-7"}},
			{Reached: 1, MedianFromMs: 20000, StoppedHere: []string{}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := New(Deps{Catalog: catalog.Fake{}, Planner: plan.Fake{},
				Scanner: scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned { return scan.Canned{Rows: rows} }}})
			req := FunnelRequest{
				Selector: selector.Selector{Time: selector.TimeSpec{From: base.Format(time.RFC3339), To: base.Add(time.Hour).Format(time.RFC3339)}},
				Entity:   "session_id",
				Steps: [][]selector.Predicate{
					{{Field: "type", Op: selector.OpEq, Value: "user"}},
					{{Field: "tool_name", Op: selector.OpEq, Value: "Edit"}},
					{{Field: "message", Op: selector.OpMatch, Value: "FAIL"}},
					{{Field: "message", Op: selector.OpMatch, Value: "PASS"}},
				},
				WithinSeconds: tt.within,
			}
			first, err := ts.Funnel(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, FunnelResponse{Entities: 7, Steps: append([]FunnelStep{
				{Reached: 6, StoppedHere: []string{"s-4", "s-5"}},
				{Reached: 4, MedianFromMs: 10000, StoppedHere: []string{"s-3"}},
			}, tt.last...)}, first.Data)

			second, err := ts.Funnel(context.Background(), req)
			require.NoError(t, err)
			a, _ := json.Marshal(first)
			b, _ := json.Marshal(second)
			assert.Equal(t, string(a), string(b), "the same request and snapshot must give byte-identical output")
		})
	}
}
