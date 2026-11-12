package scan

import (
	"context"
	"log/slog"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
)

// Logged is s logging each scan as a step of the request it serves once its last row is
// passed: how long it took, what it was asked for and what it read. A scan of nothing, or
// one its caller stops early, logs nothing.
func Logged(s Scanner) Scanner { return logged{s} }

type logged struct{ Scanner }

func (l logged) Scan(ctx context.Context, p plan.Plan, q Query) *Rows {
	start := time.Now()
	rows := l.Scanner.Scan(ctx, p, q)
	if len(p.Items) == 0 {
		return rows
	}
	segments := map[string]bool{}
	for _, wi := range p.Items {
		segments[wi.Segment.ID] = true
	}
	rows.done = func(cost query.Cost, err error) {
		attrs := []any{"duration_ms", float64(time.Since(start).Microseconds()) / 1000,
			"segments", len(segments), "row_groups", len(p.Items), "predicates", len(q.Where), "columns", len(q.Columns),
			"gets", cost.GETs, "bytes_read", cost.BytesRead}
		if err != nil {
			attrs = append(attrs, "error", err.Error())
		}
		slog.InfoContext(ctx, "scan", attrs...)
	}
	return rows
}
