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

// explainOver writes records as one segment and asks explain_spike about slice within it.
func explainOver(t *testing.T, records []segment.Record, slice ...selector.Predicate) tools.ExplainSpikeResponse {
	t.Helper()
	dir, store, cat := newCorpus(t)
	writeRecords(t, store, cat, "mixed", records)
	cr, rd := readers(dir)
	env, err := toolsOver(cr, rd, nil).ExplainSpike(context.Background(), tools.ExplainSpikeRequest{
		Selector: selector.Selector{Time: costWindow}, Slice: slice,
	})
	require.NoError(t, err)
	return env.Data
}

func fields(shifts []tools.Shift) []string {
	fs := make([]string, len(shifts))
	for i, s := range shifts {
		fs[i] = s.Field + "=" + s.Value
	}
	return fs
}

// TestExplainSpikeLeavesOutWhatThePatternFixes: focused on a workspace-only pattern,
// service=workspace is on every one of its lines and 5% of the rest's, which clears the
// thresholds, but the pattern fixes it, so only what varies within the pattern is kept.
func TestExplainSpikeLeavesOutWhatThePatternFixes(t *testing.T) {
	var records []segment.Record
	slice, rest := map[string]int64{}, map[string]int64{} // each tool's lines in p1 and outside it
	for i := range 200 {
		pattern, service, tool := "p2", "api", "none"
		switch {
		case i < 10:
			pattern, service, tool = "p1", "workspace", []string{"bash", "read"}[min(i/8, 1)]
		case i < 20:
			pattern, service, tool = "p3", "workspace", "bash"
		}
		if pattern == "p1" {
			slice[tool]++
		} else {
			rest[tool]++
		}
		records = append(records, segment.LogRecord(costBase.Add(time.Duration(i)*time.Second), "info", "ran",
			map[string]string{segment.ColService: service, segment.ColPatternID: pattern, "tool": tool}))
	}

	got := explainOver(t, records, selector.Predicate{Field: segment.ColPatternID, Op: selector.OpEq, Value: "p1"})
	assert.Equal(t, []string{"tool=read", "tool=bash"}, fields(got.Shifts), "read never shows in the rest; bash is 80% here, 5% there")
	assert.Equal(t, int64(10), got.Lines)
	assert.Equal(t, int64(190), got.Rest)
	for _, s := range got.Shifts {
		assert.Equal(t, []int64{slice[s.Value], rest[s.Value]}, []int64{s.SliceCount, s.BaselineCount}, "tool=%s", s.Value)
	}

	// Narrowed within the pattern, its service is still the pattern's, read off its own lines.
	bash := explainOver(t, records, selector.Predicate{Field: segment.ColPatternID, Op: selector.OpEq, Value: "p1"},
		selector.Predicate{Field: "tool", Op: selector.OpEq, Value: "bash"})
	assert.Empty(t, bash.Shifts)
	assert.Equal(t, int64(8), bash.Lines)
}

// TestExplainSpikeKeepsTheLargestLifts: of seven values the slice's lines all hold, the
// five least common in the rest are kept, the largest lift first.
func TestExplainSpikeKeepsTheLargestLifts(t *testing.T) {
	var records []segment.Record
	for i := range 120 {
		attrs := map[string]string{"kind": "fast"}
		if i < 20 {
			attrs["kind"] = "slow"
		}
		for f := 1; f <= 7; f++ {
			// Field f holds x on the slice's lines and on f of the rest's.
			attrs[fmt.Sprintf("f%d", f)] = map[bool]string{true: "x", false: "y"}[i < 20+f]
		}
		records = append(records, segment.LogRecord(costBase.Add(time.Duration(i)*time.Second), "info", "ran", attrs))
	}

	got := explainOver(t, records, selector.Predicate{Field: "kind", Op: selector.OpEq, Value: "slow"})
	assert.Equal(t, []string{"f1=x", "f2=x", "f3=x", "f4=x", "f5=x"}, fields(got.Shifts))

	// A field the slice only bounds still splits its lines, and is kept.
	bounded := explainOver(t, records, selector.Predicate{Field: "kind", Op: selector.OpEq, Value: "slow"},
		selector.Predicate{Field: "f1", Op: selector.OpNe, Value: "z"})
	assert.Equal(t, fields(got.Shifts), fields(bounded.Shifts))
}
