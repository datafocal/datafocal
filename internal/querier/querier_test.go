package querier

import (
	"io/fs"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueryPastRetentionReadsNoSegment pins that a search over a window wholly past
// retention answers empty without a GET, though a lifecycle rule deleted its segments.
func TestQueryPastRetentionReadsNoSegment(t *testing.T) {
	ctx := t.Context()
	const retention = 7 * 24 * time.Hour
	hour := time.Now().Add(-30 * 24 * time.Hour).Truncate(time.Hour)
	cat := catalog.NewWithRetention(storage.NewFS(t.TempDir()), "acme", retention)
	require.NoError(t, cat.Append(ctx, segment.Descriptor{
		ID:        "expired",
		TimeRange: segment.TimeRange{Min: hour, Max: hour.Add(time.Hour)},
		Object:    segment.ObjectLocation{Provider: "fs", Key: "acme/hour/expired.seg"},
	}))
	gets := 0
	deleted := func() error { gets++; return fs.ErrNotExist }
	store := storage.Fake{
		GetRangeFn:  func(string, int64, int64) ([]byte, error) { return nil, deleted() },
		GetSuffixFn: func(string, int64) ([]byte, error) { return nil, deleted() },
	}

	env, err := Open(Options{Catalog: cat, Store: store}).Tools().SearchLogs(ctx, tools.SearchRequest{
		Selector: selector.Selector{Time: selector.TimeSpec{From: hour.Format(time.RFC3339), To: hour.Add(time.Hour).Format(time.RFC3339)}},
	})
	require.NoError(t, err)
	assert.Empty(t, env.Data.Lines)
	assert.Zero(t, gets, "GETs of segments past retention")
}
