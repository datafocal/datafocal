package tools

import (
	"context"
	"encoding/json"
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

// TestCoOccurrence pins the pair two sessions editing the same file make: one when the
// edits come within the gap, none when they are hours apart, and only the shared path.
func TestCoOccurrence(t *testing.T) {
	base := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	ms := func(d time.Duration) int64 { return base.Add(d).UnixMilli() }
	tests := []struct {
		name  string
		apart time.Duration
		gap   int
		want  []EntityPair
	}{
		{
			name: "three minutes apart within a five-minute gap", apart: 3 * time.Minute, gap: 300,
			want: []EntityPair{{A: "s-1", B: "s-2", Value: "/src/main.go", FromMs: ms(3 * time.Minute), ToMs: ms(time.Second), RowsA: 2, RowsB: 1}},
		},
		{name: "hours apart at the default gap", apart: 3 * time.Hour, want: []EntityPair{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seg := &segment.Descriptor{}
			edit := func(at time.Duration, session, path string) scan.Row {
				return scan.NewRow(seg, base.Add(at), map[string][]byte{"session_id": []byte(session), "file_path": []byte(path)})
			}
			rows := []scan.Row{edit(0, "s-1", "/src/main.go"), edit(time.Second, "s-1", "/src/main.go"),
				edit(time.Second, "s-1", "/README.md"), edit(tt.apart, "s-2", "/src/main.go"), edit(tt.apart, "s-2", "/go.mod")}
			ts := New(Deps{Catalog: catalog.Fake{}, Planner: plan.Fake{},
				Scanner: scan.Fake{ScanFn: func(plan.Plan, scan.Query) scan.Canned { return scan.Canned{Rows: rows} }}})
			req := CoOccurrenceRequest{
				Selector: selector.Selector{Time: selector.TimeSpec{From: base.Format(time.RFC3339), To: base.Add(4 * time.Hour).Format(time.RFC3339)}},
				Entity:   "session_id", Shared: "file_path", MaxGapSeconds: tt.gap,
			}
			first, err := ts.CoOccurrence(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, CoOccurrenceResponse{Pairs: tt.want}, first.Data)

			second, err := ts.CoOccurrence(context.Background(), req)
			require.NoError(t, err)
			a, _ := json.Marshal(first)
			b, _ := json.Marshal(second)
			assert.Equal(t, string(a), string(b), "the same request and snapshot must give byte-identical output")
		})
	}
}
