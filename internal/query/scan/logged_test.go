package scan_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/segment"
)

// TestLoggedLogsEachDrainedScan: a scan drained through Logged logs one step of what it
// read, its failure too, and one of nothing or that its caller stops early logs none.
func TestLoggedLogsEachDrainedScan(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	seg := segment.Descriptor{ID: "s1"}
	p := plan.Plan{Items: []plan.WorkItem{{Segment: seg, RowGroup: 0}, {Segment: seg, RowGroup: 1}}}
	for _, tc := range []struct {
		name  string
		plan  plan.Plan
		drain bool
		err   error
		want  map[string]any
	}{
		{"drained", p, true, nil, map[string]any{"msg": "scan", "segments": 1.0, "row_groups": 2.0, "gets": 3.0, "bytes_read": 40.0}},
		{"failed", p, true, errors.New("gone"), map[string]any{"msg": "scan", "segments": 1.0, "row_groups": 2.0, "gets": 3.0, "bytes_read": 40.0, "error": "gone"}},
		{"stopped early", p, false, nil, nil},
		{"of nothing", plan.Plan{}, true, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			s := scan.Logged(scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned {
				return scan.Canned{Rows: []scan.Row{{}, {}}, Cost: query.Cost{GETs: 3, BytesRead: 40}, Err: tc.err}
			}})
			rows := s.Scan(context.Background(), tc.plan, scan.Query{})
			require.True(t, rows.Next())
			for tc.drain && rows.Next() {
			}
			if tc.want == nil {
				assert.Empty(t, logs.String())
				return
			}
			var got map[string]any
			require.NoError(t, json.Unmarshal(logs.Bytes(), &got))
			assert.IsType(t, 0.0, got["duration_ms"])
			for _, k := range []string{"time", "level", "duration_ms", "predicates", "columns"} {
				delete(got, k)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}
