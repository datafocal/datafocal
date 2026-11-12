package tools

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/selector"
)

// FunnelRequest is the wire request for the funnel tool.
type FunnelRequest struct {
	Selector      selector.Selector      `json:"selector" jsonschema:"time range required"`
	Entity        string                 `json:"entity" jsonschema:"the column naming each entity"`
	Steps         [][]selector.Predicate `json:"steps" jsonschema:"2 to 10 steps in order, each the predicates one row must all satisfy"`
	WithinSeconds int                    `json:"within_seconds,omitempty" jsonschema:"how long the whole sequence may take; 0 means no limit"`
	Snapshot      string                 `json:"snapshot,omitempty"`
}

// FunnelResponse is the wire response for funnel: the entities in scope, and how many of
// them reached each step.
type FunnelResponse struct {
	Entities int64        `json:"entities"`
	Steps    []FunnelStep `json:"steps"`
}

// FunnelStep is the entities that reached a step, the median time from the previous
// step, and up to three, by name, that reached it and no further.
type FunnelStep struct {
	Reached      int64    `json:"reached"`
	MedianFromMs int64    `json:"median_from_prev_ms"`
	StoppedHere  []string `json:"stopped_here"`
}

const (
	maxFunnelSteps = 10
	funnelExamples = 3
)

// event is one of an entity's rows: its time in unix ms and the steps it satisfies, bit
// i for step i.
type event struct {
	ms    int64
	steps uint16
}

// Funnel scans the window, orders each entity's rows by time and counts the entities
// reaching each step, matching the steps greedily in order.
func (t toolSet) Funnel(ctx context.Context, req FunnelRequest) (query.Envelope[FunnelResponse], error) {
	var zero query.Envelope[FunnelResponse]
	sel, err := validateSelector(ctx, "funnel", req.Selector)
	if err != nil {
		return zero, err
	}
	if sel.Window.From.IsZero() || sel.Window.To.IsZero() {
		return zero, query.Invalidf("funnel: a bounded time window is required")
	}
	if req.Entity == "" {
		return zero, query.Invalidf("funnel: entity is required")
	}
	if len(req.Steps) < 2 || len(req.Steps) > maxFunnelSteps {
		return zero, query.Invalidf("funnel: 2 to %d steps are required, got %d", maxFunnelSteps, len(req.Steps))
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "funnel", req.Snapshot)
	if err != nil {
		return zero, err
	}
	p, err := t.Planner.Prune(ctx, snap.catalog, sel)
	if err != nil {
		return zero, err
	}

	byEntity, cost, err := t.funnelEvents(ctx, p, sel, req)
	if err != nil {
		return zero, err
	}

	resp := funnelSteps(byEntity, len(req.Steps), int64(req.WithinSeconds)*1000)
	return query.Envelope[FunnelResponse]{
		Data: resp,
		Meta: query.Meta{Snapshot: snap.String(), Coverage: 1.0, Cost: cost, Tokens: query.EstimateTokens(resp)},
	}, nil
}

// funnelEvents scans p for each entity's rows satisfying a step; an entity without one
// is in scope with no events.
func (t toolSet) funnelEvents(ctx context.Context, p plan.Plan, sel selector.Resolved,
	req FunnelRequest) (map[string]*[]event, query.Cost, error) {
	steps := make([][]selector.ParsedPredicate, len(req.Steps))
	columns := []string{req.Entity}
	for i, step := range req.Steps {
		steps[i] = selector.ParsePredicates(step)
		columns = append(columns, scan.Fields(steps[i])...)
	}
	byEntity := map[string]*[]event{}
	rows := t.Scanner.Scan(ctx, p, scan.Query{Window: sel.Window, Where: selector.ParsePredicates(sel.Where), Columns: columns, Require: []string{req.Entity}})
	for rows.Next() {
		r := rows.Row()
		e := event{ms: r.Time.UnixMilli()}
		for i, step := range steps {
			if r.Matches(step) {
				e.steps |= 1 << i
			}
		}
		for _, entity := range r.Keys(req.Entity) {
			if entity != "" {
				record(byEntity, entity, e)
			}
		}
	}
	cost := addCost(p.Cost, rows.Cost())
	if err := rows.Err(); err != nil {
		return nil, cost, fmt.Errorf("funnel: scan: %w", err)
	}
	return byEntity, cost, nil
}

// record adds e to entity's events when it satisfies a step, cloning the entity it keeps
// since a row's text shares the column's bytes.
func record(byEntity map[string]*[]event, entity string, e event) {
	events, ok := byEntity[entity]
	if !ok {
		events = &[]event{}
		byEntity[strings.Clone(entity)] = events
	}
	if e.steps != 0 {
		*events = append(*events, e)
	}
}

// funnelSteps counts the entities reaching each of n steps, the median gap from the
// previous step, and the first entities by name that stopped at each.
func funnelSteps(byEntity map[string]*[]event, n int, withinMs int64) FunnelResponse {
	resp := FunnelResponse{Entities: int64(len(byEntity)), Steps: make([]FunnelStep, n)}
	gaps := make([][]int64, n)
	for i := range resp.Steps {
		resp.Steps[i].StoppedHere = []string{}
	}
	for _, entity := range slices.Sorted(maps.Keys(byEntity)) {
		events := *byEntity[entity]
		slices.SortFunc(events, func(a, b event) int { return cmp.Or(cmp.Compare(a.ms, b.ms), cmp.Compare(a.steps, b.steps)) })
		run := funnelRun(events, n, withinMs)
		for i := range run {
			resp.Steps[i].Reached++
			if i > 0 {
				gaps[i] = append(gaps[i], run[i]-run[i-1])
			}
		}
		if last := len(run) - 1; last >= 0 && last < n-1 && len(resp.Steps[last].StoppedHere) < funnelExamples {
			resp.Steps[last].StoppedHere = append(resp.Steps[last].StoppedHere, entity)
		}
	}
	for i := range resp.Steps {
		resp.Steps[i].MedianFromMs = lowerMedian(gaps[i])
	}
	return resp
}

// funnelRun is the furthest greedy run from any first-step event, which without a time
// limit is always the run from the first.
func funnelRun(events []event, n int, withinMs int64) []int64 {
	var best []int64
	for i, e := range events {
		if e.steps&1 == 0 {
			continue
		}
		if run := greedyRun(events[i:], n, withinMs); len(run) > len(best) {
			best = run
		}
		if withinMs <= 0 || len(best) == n {
			break
		}
	}
	return best
}

// greedyRun matches each step after the first event to the first later event
// satisfying it, within withinMs of the first when it is set.
func greedyRun(events []event, n int, withinMs int64) []int64 {
	run := []int64{events[0].ms}
	for _, e := range events[1:] {
		if len(run) == n || withinMs > 0 && e.ms-run[0] > withinMs {
			break
		}
		if e.steps&(1<<len(run)) != 0 {
			run = append(run, e.ms)
		}
	}
	return run
}

// lowerMedian is the lower of the middle values, and 0 for none.
func lowerMedian(vals []int64) int64 {
	if len(vals) == 0 {
		return 0
	}
	slices.Sort(vals)
	return vals[(len(vals)-1)/2]
}
