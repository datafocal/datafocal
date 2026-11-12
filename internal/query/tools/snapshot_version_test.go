package tools_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	tools "github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The gate tests tell the warm path from the scan by Meta.Cost.GETs, so the
// request grid, the store's grid, and the fixture all have to agree: the scan
// answers a segment that sits inside one bin from its descriptor row count alone,
// at zero GETs, which would make the two paths indistinguishable. gateBinSpec is
// the request grid, gateBinWidth the same width for the store, and
// straddlingEvents writes segments wide enough to cross a bin boundary.
const (
	gateBinSpec  = "15s"
	gateBinWidth = 15 * time.Second
)

// straddlingEvents is n lines one second apart, so n > 15 spans more than one
// gateBinWidth bin.
func straddlingEvents(start time.Time, n int) []segment.Record {
	msgs := make([]string, n)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("handled request %d", i)
	}
	return messageEvents(start, msgs...)
}

var gateBase = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// TestVersionGate: a store serves warm (Cost.GETs == 0) only when the request's
// pin, if any, names the summary version it reached;
// otherwise the request scans and answers exactly what the nil-store path does.
func TestVersionGate(t *testing.T) {
	dir, store, cat := newCorpus(t)
	writeSegment(t, store, cat, "svc", straddlingEvents(gateBase, 20))
	catalogVersion, err := catalog.New(storage.NewFS(dir), testTenant).Version(context.Background())
	require.NoError(t, err)

	builtStore := func(t *testing.T, cr catalog.Catalog, rd segment.Reader) summary.Store {
		rs := wireSummary(t, cr, rd, summary.Config{BinWidth: gateBinWidth})
		require.NotZero(t, rs.CatalogVersion())
		return rs
	}
	tests := []struct {
		name     string
		store    func(*testing.T, catalog.Catalog, segment.Reader) summary.Store
		pinned   bool // pin the current catalog version and the store's version plus pinAhead
		pinAhead bool // pin a summary version past the store's, which it never reached
		wantWarm bool
	}{
		{name: "pinned equals current", store: builtStore, pinned: true, wantWarm: true},
		// The pin still identifies a catalog snapshot, so the scan answers it.
		{name: "unreachable pin scans", store: builtStore, pinned: true, pinAhead: true},
		{
			// No summary disables version gating entirely, leaving the plain scan.
			name:  "nil store",
			store: func(*testing.T, catalog.Catalog, segment.Reader) summary.Store { return nil },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cr, rd := readers(dir)
			rs := tc.store(t, cr, rd)
			req := tools.AggregateRequest{
				Selector: selector.Selector{Time: window(gateBase.Add(-time.Minute), gateBase.Add(5*time.Minute))},
				Measure:  "count",
				GroupBy:  "service",
				Bins:     gateBinSpec,
			}
			if tc.pinned {
				req.Snapshot = fmt.Sprintf("e%s.s%s", catalogVersion, rs.CatalogVersion())
				if tc.pinAhead {
					req.Snapshot += "00"
				}
			}
			got, err := toolsOver(cr, rd, rs).Aggregate(context.Background(), req)
			require.NoError(t, err)
			if tc.wantWarm {
				assert.Zero(t, got.Meta.Cost.GETs, "should serve from the store with 0 GETs")
				return
			}
			assert.NotZero(t, got.Meta.Cost.GETs, "must fall back to the scan")

			nilEnv, err := toolsOver(cr, rd, nil).Aggregate(context.Background(), req)
			require.NoError(t, err)
			assert.NotEmpty(t, nilEnv.Data.Series, "nil store should still return data")
			assert.Equal(t, nilEnv.Data, got.Data, "scan fallback must match the nil-store answer")
		})
	}
}

// TestVersionGate_StoreBehindCatalog: a store that has not yet absorbed the
// newest catalog epoch must not answer — the request scans instead, so a reader
// never sees the segments the store is missing. Refreshing the store restores
// the warm path.
func TestVersionGate_StoreBehindCatalog(t *testing.T) {
	dir, fsStore, cat := newCorpus(t)
	writeSegment(t, fsStore, cat, "svc", straddlingEvents(gateBase, 20))

	cr, rd := readers(dir)
	bldr := summary.NewBuilder(cr, rd, summary.Config{BinWidth: gateBinWidth})
	require.NoError(t, bldr.Build(context.Background()))
	rs := bldr.Store()
	absorbed := rs.CatalogVersion()
	require.NotZero(t, absorbed)

	// A second segment lands in the catalog; the store is left un-refreshed, so
	// its absorbed epoch is now behind the snapshot's.
	base2 := gateBase.Add(time.Minute)
	writeSegment(t, fsStore, cat, "svc", straddlingEvents(base2, 20))
	cr2, rd2 := readers(dir)

	catalogVersion, err := cr2.Version(context.Background())
	require.NoError(t, err)
	require.True(t, absorbed.Before(catalogVersion), "catalog should have moved past the store")

	req := tools.AggregateRequest{
		Selector: selector.Selector{Time: window(gateBase.Add(-time.Minute), base2.Add(5*time.Minute))},
		Measure:  "count",
		GroupBy:  "service",
		Bins:     gateBinSpec,
	}
	stale, err := toolsOver(cr2, rd2, rs).Aggregate(context.Background(), req)
	require.NoError(t, err)
	assert.NotZero(t, stale.Meta.Cost.GETs, "a store behind the catalog must not serve warm")

	nilEnv, err := toolsOver(cr2, rd2, nil).Aggregate(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, nilEnv.Data, stale.Data, "stale-store data must equal nil-store data")

	// Once the store absorbs the new epoch it serves warm again.
	require.NoError(t, bldr.Build(context.Background()))
	require.Equal(t, catalogVersion, rs.CatalogVersion(), "store should have absorbed the new epoch")

	warm, err := toolsOver(cr2, rd2, rs).Aggregate(context.Background(), req)
	require.NoError(t, err)
	assert.Zero(t, warm.Meta.Cost.GETs, "refreshed store should serve warm")
	// The fixture's segments straddle a bin boundary, which the summary counts from rows.
	assert.False(t, warm.Meta.Approx)
	assert.Equal(t, nilEnv.Data, warm.Data)
}

// TestPinnedSnapshotIgnoresLaterSegments: a request pinned to a snapshot answers
// from that snapshot's segments, even once a segment matching it is appended.
func TestPinnedSnapshotIgnoresLaterSegments(t *testing.T) {
	traceSegment := func(ms int) func(*testing.T, storage.Store, catalog.Catalog) {
		return func(t *testing.T, store storage.Store, cat catalog.Catalog) {
			writeSegment(t, store, cat, "api", []segment.Record{
				spanEvent(at(ms), "info", "t1", fmt.Sprintf("s%d", ms), "", "handle", 5),
			})
		}
	}
	type read func(context.Context, catalog.Catalog, segment.Reader, string) (any, query.Meta, error)
	cases := []struct {
		name  string
		seed  func(*testing.T) string
		later func(*testing.T, storage.Store, catalog.Catalog)
		read  read
	}{
		{
			name:  "get_trace",
			later: traceSegment(10),
			read: func(ctx context.Context, cat catalog.Catalog, rd segment.Reader, pin string) (any, query.Meta, error) {
				env, err := toolsOver(cat, rd, nil).GetTrace(ctx, tools.TraceRequest{
					Value: "t1", Time: traceWindow, Snapshot: pin,
				})
				return env.Data, env.Meta, err
			},
		},
		{
			name:  "search_logs needle",
			later: traceSegment(10),
			read: func(ctx context.Context, cat catalog.Catalog, rd segment.Reader, pin string) (any, query.Meta, error) {
				env, err := toolsOver(cat, rd, nil).SearchLogs(ctx, tools.SearchRequest{
					Selector: selector.Selector{
						Time:  traceWindow,
						Where: []selector.Predicate{{Field: segment.ColTraceID, Op: selector.OpEq, Value: "t1"}},
					},
					Snapshot: pin,
				})
				return env.Data, env.Meta, err
			},
		},
		{
			name: "attribute_writes",
			seed: seedAttributionCorpus,
			later: func(t *testing.T, store storage.Store, cat catalog.Catalog) {
				writeSegment(t, store, cat, "scheduler", []segment.Record{
					placementEvent(attributeBase.Add(2*time.Minute), "gateway-0", "host-7"),
				})
			},
			read: func(ctx context.Context, cat catalog.Catalog, rd segment.Reader, pin string) (any, query.Meta, error) {
				req := attributeRequest("host-7")
				req.Snapshot = pin
				env, err := toolsOver(cat, rd, nil).AttributeWrites(ctx, req)
				return env.Data, env.Meta, err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			dir, store, cat := newCorpus(t)
			if tc.seed != nil {
				dir = tc.seed(t)
				store, cat = storage.NewFS(dir), catalog.New(storage.NewFS(dir), testTenant)
			} else {
				traceSegment(0)(t, store, cat)
			}

			cr, rd := uncachedReaders(dir)
			catalogVersion, err := cr.Version(ctx)
			require.NoError(t, err)
			pin := fmt.Sprintf("e%s.s0", catalogVersion)
			wantData, wantMeta, err := tc.read(ctx, cr, rd, pin)
			require.NoError(t, err)

			tc.later(t, store, cat)
			cr, rd = uncachedReaders(dir)
			gotData, gotMeta, err := tc.read(ctx, cr, rd, pin)
			require.NoError(t, err)

			assert.Equal(t, pin, gotMeta.Snapshot)
			assert.Equal(t, wantMeta, gotMeta)
			assert.Equal(t, wantData, gotData)
		})
	}
}
