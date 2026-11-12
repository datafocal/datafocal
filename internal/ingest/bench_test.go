package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/tail"
)

func BenchmarkIngest(b *testing.B) {
	for _, format := range []string{"auto", "json"} {
		b.Run(format, func(b *testing.B) {
			p, err := logformat.New(logformat.Config{Format: format})
			require.NoError(b, err)
			in := &ingest{
				collector: collector.Fake{},
				resolver:  identity.New("node-1"),
				parsers:   map[string]logformat.Parser{"a.log": p},
				offsets:   map[string]tail.Position{},
				pending:   map[string][]tail.Position{},
				dated:     map[string]time.Time{},
			}
			line := sourceLine{path: "a.log", line: tail.Line{Text: `{"ts":"2026-10-20T12:00:00Z","level":"info","msg":"request completed","status":200}`}}
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				require.NoError(b, in.collect(ctx, line))
			}
		})
	}
}
