package tools

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strconv"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// OverviewRequest is find_anomalies': the lines to sum up, the last hour's by default, and the pin.
type OverviewRequest = FindAnomaliesRequest

// OverviewRow is one (namespace, service) over the window, its lifecycle counts read from
// the cluster's lines about its pods.
type OverviewRow struct {
	Namespace    string    `json:"namespace"`
	Service      string    `json:"service"`
	Lines        int64     `json:"lines"`
	ErrorLines   int64     `json:"error_lines"`
	FirstSeen    time.Time `json:"first_seen"` // the start of the first bin holding its lines
	LastSeen     time.Time `json:"last_seen"`  // the start of the last
	Pods         int       `json:"pods"`
	Terminations int64     `json:"terminations,omitempty"` // lines carrying an exit_code
	Restarts     int64     `json:"restarts,omitempty"`     // the highest restart_count a pod reported
}

// OverviewResponse is every (namespace, service) seen in the window, most error lines
// first, then most lines.
type OverviewResponse struct {
	Services []OverviewRow `json:"services"`
}

// overviewGroups is the groups each aggregate call pages, beyond the services, namespaces
// or pods one window holds.
const overviewGroups = 10000 //nolint:dupdefault // a page of groups, unrelated to series.MaxBins

// The lifecycle columns a pod's terminations and restarts are read from.
const (
	colExitCode     = "exit_code"
	colRestartCount = "restart_count"
)

// Overview reads each count with its own aggregate call grouped by one column, the shape
// the summary and the segments' footers answer without reading rows: the services, each
// service's namespaces over bins, then each pair's levels, pods and lifecycle values.
func (t toolSet) Overview(ctx context.Context, req OverviewRequest) (query.Envelope[OverviewResponse], error) {
	if req.Selector.Time == (selector.TimeSpec{}) {
		req.Selector.Time = selector.TimeSpec{From: "now-1h", To: "now"}
	}
	// Resolved once, so every call reads the same window.
	sel, err := validateSelector(ctx, "overview", req.Selector)
	if err != nil {
		return query.Envelope[OverviewResponse]{}, err
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "overview", req.Snapshot)
	if err != nil {
		return query.Envelope[OverviewResponse]{}, err
	}
	o := overviewer{t: t, where: sel.Where, meta: query.Meta{Snapshot: snap.String(), Coverage: 1},
		time: selector.TimeSpec{From: sel.Window.From.Format(time.RFC3339Nano), To: sel.Window.To.Format(time.RFC3339Nano)}}
	rows, err := o.rows(ctx)
	if err != nil {
		return query.Envelope[OverviewResponse]{}, err
	}
	slices.SortFunc(rows, func(a, b OverviewRow) int {
		return cmp.Or(cmp.Compare(b.ErrorLines, a.ErrorLines), cmp.Compare(b.Lines, a.Lines),
			cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Service, b.Service))
	})
	resp := OverviewResponse{Services: rows}
	o.meta.Tokens = query.EstimateTokens(resp)
	return query.Envelope[OverviewResponse]{Data: resp, Meta: o.meta}, nil
}

// overviewer makes an overview's aggregate calls at meta's pin and over time, folding each
// one's cost into meta.
type overviewer struct {
	t     toolSet
	time  selector.TimeSpec
	where []selector.Predicate // the request's, narrowing every call
	meta  query.Meta
}

// rows is one row per (namespace, service) the window's lines name, a service's lines
// without a namespace under "".
func (o *overviewer) rows(ctx context.Context) ([]OverviewRow, error) {
	services, err := o.count(ctx, segment.ColService, "auto")
	if err != nil {
		return nil, err
	}
	rows := []OverviewRow{}
	for _, svc := range services {
		if svc.Key == "" {
			continue
		}
		namespaces, err := o.count(ctx, segment.ColNamespace, "auto", eq(segment.ColService, svc.Key))
		if err != nil {
			return nil, err
		}
		rest := unnamed(svc, namespaces)
		for _, ns := range append(slices.DeleteFunc(namespaces, func(ns Series) bool { return ns.Key == "" }), rest) {
			if !slices.ContainsFunc(ns.Values, func(v *float64) bool { return v != nil && *v > 0 }) {
				continue
			}
			row, err := o.row(ctx, svc.Key, ns)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// unnamed is svc's lines in no one of namespaces, keyed "", since a count grouped by a
// column leaves out the lines without it when it reads rows but keys them "" off footers.
func unnamed(svc Series, namespaces []Series) Series {
	rest := Series{Start: svc.Start, Step: svc.Step, Values: make([]*float64, len(svc.Values))}
	for i, v := range svc.Values {
		if v != nil {
			left := *v
			for _, ns := range namespaces {
				if ns.Key != "" && ns.Values[i] != nil {
					left -= *ns.Values[i]
				}
			}
			rest.Values[i] = &left
		}
	}
	return rest
}

// row is service's row in the namespace ns is the series of.
func (o *overviewer) row(ctx context.Context, service string, ns Series) (OverviewRow, error) {
	row := OverviewRow{Namespace: ns.Key, Service: service}
	for i, v := range ns.Values {
		if v == nil || *v == 0 {
			continue
		}
		at := time.UnixMilli(ns.Start + int64(i)*ns.Step).UTC()
		if row.Lines == 0 {
			row.FirstSeen = at
		}
		row.Lines += int64(*v)
		row.LastSeen = at
	}
	where := []selector.Predicate{eq(segment.ColService, service), eq(segment.ColNamespace, ns.Key)}
	folds := map[string]func(key string, n int64){
		segment.ColLevel: func(key string, n int64) {
			if key == levelError || key == "fatal" {
				row.ErrorLines += n
			}
		},
		segment.ColPod: func(string, int64) { row.Pods++ },
		colExitCode:    func(_ string, n int64) { row.Terminations += n },
		colRestartCount: func(key string, _ int64) {
			if n, err := strconv.ParseInt(key, 10, 64); err == nil {
				row.Restarts = max(row.Restarts, n)
			}
		},
	}
	for _, column := range slices.Sorted(maps.Keys(folds)) {
		if err := o.each(ctx, column, where, folds[column]); err != nil {
			return OverviewRow{}, err
		}
	}
	return row, nil
}

// each calls fold with each value of column the lines where matches hold, and how many
// hold it.
func (o *overviewer) each(ctx context.Context, column string, where []selector.Predicate, fold func(key string, n int64)) error {
	groups, err := o.count(ctx, column, "all", where...)
	for _, g := range groups {
		if g.Key != "" {
			fold(g.Key, total(g))
		}
	}
	return err
}

// count is the window's lines grouped by column over bins.
func (o *overviewer) count(ctx context.Context, column, bins string, where ...selector.Predicate) ([]Series, error) {
	env, err := o.t.Aggregate(ctx, AggregateRequest{
		Selector: selector.Selector{Time: o.time, Where: slices.Concat(o.where, where)},
		Measure:  "count", GroupBy: column, Bins: bins, Limit: overviewGroups, Snapshot: o.meta.Snapshot,
	})
	if err != nil {
		return nil, err
	}
	o.meta.Cost = addCost(o.meta.Cost, env.Meta.Cost)
	o.meta.Coverage = min(o.meta.Coverage, env.Meta.Coverage)
	o.meta.Approx = o.meta.Approx || env.Meta.Approx
	return env.Data.Series, nil
}

func eq(field, value string) selector.Predicate {
	return selector.Predicate{Field: field, Op: selector.OpEq, Value: value}
}

// total is a series' value in its one bin, which bins=all gives it.
func total(s Series) int64 {
	if len(s.Values) == 0 || s.Values[0] == nil {
		return 0
	}
	return int64(*s.Values[0])
}
