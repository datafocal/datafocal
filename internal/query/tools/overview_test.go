package tools

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// TestOverview writes three namespaces' pods, one service in two of them, and checks each
// (namespace, service) row against the records written and that a summary covering the
// window answers it without a GET; then adds a pod's termination line and checks it counts.
func TestOverview(t *testing.T) {
	base := time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)
	ctx, store := context.Background(), storage.NewFS(t.TempDir())
	cat := catalog.New(store, "acme")
	// Each call is one segment, as a collector seals a pod's lines.
	writeSegment := func(service string, records []segment.Record) {
		cfg := collector.DefaultConfig()
		cfg.Tenant, cfg.Stream.Service, cfg.FlushInterval = "acme", service, time.Hour
		c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: cat})
		require.NoError(t, err)
		for _, r := range records {
			_, err = c.Collect(ctx, r)
			require.NoError(t, err)
		}
		_, err = c.Flush(ctx)
		require.NoError(t, err)
	}
	want := map[[2]string]*OverviewRow{}
	pod := func(at time.Duration, namespace, service, name string, lines, errs int) {
		key := [2]string{namespace, service}
		if want[key] == nil {
			want[key] = &OverviewRow{Namespace: namespace, Service: service}
		}
		want[key].Lines += int64(lines)
		want[key].ErrorLines += int64(errs)
		want[key].Pods++
		records := make([]segment.Record, lines)
		for i := range records {
			level := "info"
			if i < errs {
				level = "error"
			}
			records[i] = segment.LogRecord(base.Add(at+time.Duration(i)*time.Second), level, fmt.Sprintf("served request %d", i),
				map[string]string{"namespace": namespace, "pod": name})
		}
		writeSegment(service, records)
	}
	pod(0, "shop", "api", "api-1", 6, 2)
	pod(10*time.Minute, "shop", "api", "api-2", 4, 0)
	pod(20*time.Minute, "shop", "db", "db-0", 5, 0)
	pod(5*time.Minute, "ops", "api", "api-x", 3, 3)
	pod(30*time.Minute, "kube-system", "coredns", "coredns-1", 8, 1)

	overview := func() query.Envelope[OverviewResponse] {
		t.Helper()
		// The summary's build reads every footer through rd, as a querier's does.
		rd := segment.NewFooterCache(store)
		builder := summary.NewBuilder(cat, rd, summary.Config{BinWidth: time.Minute})
		require.NoError(t, builder.Build(ctx))
		planner := plan.New(plan.Deps{Catalog: cat, Reader: rd, Summary: builder.Store()})
		ts := New(Deps{Catalog: cat, Series: series.New(planner, scan.New(rd, plan.Derived{})), Planner: planner, Summary: builder.Store()})
		env, err := ts.Overview(ctx, OverviewRequest{Selector: selector.Selector{Time: selector.TimeSpec{From: base.Format(time.RFC3339), To: base.Add(time.Hour).Format(time.RFC3339)}}})
		require.NoError(t, err)
		require.Len(t, env.Data.Services, len(want))
		for _, got := range env.Data.Services {
			w := want[[2]string{got.Namespace, got.Service}]
			require.NotNil(t, w, "unexpected row %s/%s", got.Namespace, got.Service)
			got.FirstSeen, got.LastSeen = time.Time{}, time.Time{}
			assert.Equal(t, *w, got)
		}
		return env
	}
	env := overview()
	assert.Zero(t, env.Meta.Cost.GETs, "cost.gets: the summary covers the window")
	got := env.Data
	assert.Equal(t, []string{"ops/api", "shop/api", "kube-system/coredns", "shop/db"}, rowNames(got), "most error lines, then most lines, first")
	assert.Equal(t, base, got.Services[1].FirstSeen, "shop/api first seen")
	assert.Equal(t, base.Add(10*time.Minute), got.Services[1].LastSeen, "shop/api last seen")

	// A lifecycle line is filed under its pod, so it adds a line and an error line too.
	writeSegment("api", []segment.Record{segment.LogRecord(base.Add(6*time.Minute), "error",
		"container api terminated: OOMKilled (exit 137), restarts 2",
		map[string]string{"namespace": "ops", "pod": "api-x", "reason": "OOMKilled", "exit_code": "137", "restart_count": "2"})})
	*want[[2]string{"ops", "api"}] = OverviewRow{Namespace: "ops", Service: "api", Lines: 4, ErrorLines: 4, Pods: 1, Terminations: 1, Restarts: 2}
	overview()
}

func rowNames(resp OverviewResponse) []string {
	names := make([]string, len(resp.Services))
	for i, r := range resp.Services {
		names[i] = r.Namespace + "/" + r.Service
	}
	return names
}
