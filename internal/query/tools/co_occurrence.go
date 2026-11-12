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

// CoOccurrenceRequest is the wire request for the co_occurrence tool.
type CoOccurrenceRequest struct {
	Selector      selector.Selector `json:"selector" jsonschema:"time range required"`
	Entity        string            `json:"entity" jsonschema:"the column naming each entity"`
	Shared        string            `json:"shared" jsonschema:"the column whose values they may share"`
	MaxGapSeconds int               `json:"max_gap_seconds,omitempty" jsonschema:"how far apart two entities' intervals may be and still pair; 0 means they must overlap"`
	Limit         int               `json:"limit,omitempty" jsonschema:"pairs returned; defaults to 100"`
	Snapshot      string            `json:"snapshot,omitempty"`
}

// CoOccurrenceResponse is the wire response for co_occurrence.
type CoOccurrenceResponse struct {
	Pairs []EntityPair `json:"pairs"`
}

// EntityPair is two entities, A < B, that carried Value over FromMs to ToMs, which runs
// backwards by the gap when their intervals only come within the request's gap.
type EntityPair struct {
	A      string `json:"a"`
	B      string `json:"b"`
	Value  string `json:"value"`
	FromMs int64  `json:"from_ms"`
	ToMs   int64  `json:"to_ms"`
	RowsA  int64  `json:"rows_a"`
	RowsB  int64  `json:"rows_b"`
}

const defaultCoOccurrenceLimit = 100

// interval is the first to the last time, in unix ms, an entity carried a value, over
// that many rows.
type interval struct {
	from, to, rows int64
}

// CoOccurrence scans the window's entity and shared columns and pairs the entities whose
// intervals carrying the same value overlap, longest overlap first.
func (t toolSet) CoOccurrence(ctx context.Context, req CoOccurrenceRequest) (query.Envelope[CoOccurrenceResponse], error) {
	var zero query.Envelope[CoOccurrenceResponse]
	sel, err := validateSelector(ctx, "co_occurrence", req.Selector)
	if err != nil {
		return zero, err
	}
	if sel.Window.From.IsZero() || sel.Window.To.IsZero() {
		return zero, query.Invalidf("co_occurrence: a bounded time window is required")
	}
	if req.Entity == "" || req.Shared == "" {
		return zero, query.Invalidf("co_occurrence: entity and shared are required")
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "co_occurrence", req.Snapshot)
	if err != nil {
		return zero, err
	}
	p, err := t.Planner.Prune(ctx, snap.catalog, sel)
	if err != nil {
		return zero, err
	}

	byValue, cost, err := t.sharedIntervals(ctx, p, sel, req)
	if err != nil {
		return zero, err
	}

	pairs, coverage := limitPairs(entityPairs(byValue, int64(req.MaxGapSeconds)*1000), req.Limit)
	resp := CoOccurrenceResponse{Pairs: pairs}
	return query.Envelope[CoOccurrenceResponse]{
		Data: resp,
		Meta: query.Meta{Snapshot: snap.String(), Coverage: coverage, Cost: cost, Tokens: query.EstimateTokens(resp)},
	}, nil
}

// limitPairs keeps the first limit pairs, defaulted, and reports the fraction kept as
// coverage.
func limitPairs(pairs []EntityPair, limit int) ([]EntityPair, float64) {
	if limit <= 0 {
		limit = defaultCoOccurrenceLimit
	}
	if len(pairs) <= limit {
		return pairs, 1.0
	}
	return pairs[:limit], float64(limit) / float64(len(pairs))
}

// sharedIntervals scans p for each shared value's intervals, by entity; a list column
// contributes each element.
func (t toolSet) sharedIntervals(ctx context.Context, p plan.Plan, sel selector.Resolved,
	req CoOccurrenceRequest) (map[string]map[string]*interval, query.Cost, error) {
	byValue := map[string]map[string]*interval{}
	columns := []string{req.Entity, req.Shared}
	rows := t.Scanner.Scan(ctx, p, scan.Query{Window: sel.Window, Where: selector.ParsePredicates(sel.Where), Columns: columns, Require: columns})
	for rows.Next() {
		r := rows.Row()
		for _, value := range r.Keys(req.Shared) {
			for _, entity := range r.Keys(req.Entity) {
				if value != "" && entity != "" {
					carry(byValue, value, entity, r.Time.UnixMilli())
				}
			}
		}
	}
	cost := addCost(p.Cost, rows.Cost())
	if err := rows.Err(); err != nil {
		return nil, cost, fmt.Errorf("co_occurrence: scan: %w", err)
	}
	return byValue, cost, nil
}

// carry extends entity's interval carrying value to ms, cloning the strings it keeps
// since a row's text shares the column's bytes.
func carry(byValue map[string]map[string]*interval, value, entity string, ms int64) {
	byEntity, ok := byValue[value]
	if !ok {
		byEntity = map[string]*interval{}
		byValue[strings.Clone(value)] = byEntity
	}
	in, ok := byEntity[entity]
	if !ok {
		in = &interval{from: ms, to: ms}
		byEntity[strings.Clone(entity)] = in
	}
	in.from, in.to, in.rows = min(in.from, ms), max(in.to, ms), in.rows+1
}

// entityPairs pairs the entities of each value whose intervals come within gapMs of each
// other, ordered by overlap length, then by names.
func entityPairs(byValue map[string]map[string]*interval, gapMs int64) []EntityPair {
	pairs := []EntityPair{}
	for value, byEntity := range byValue {
		entities := slices.Sorted(maps.Keys(byEntity))
		for i, a := range entities {
			for _, b := range entities[i+1:] {
				ia, ib := byEntity[a], byEntity[b]
				from, to := max(ia.from, ib.from), min(ia.to, ib.to)
				if from-to <= gapMs {
					pairs = append(pairs, EntityPair{A: a, B: b, Value: value, FromMs: from, ToMs: to, RowsA: ia.rows, RowsB: ib.rows})
				}
			}
		}
	}
	slices.SortFunc(pairs, func(x, y EntityPair) int {
		return cmp.Or(cmp.Compare(y.ToMs-y.FromMs, x.ToMs-x.FromMs),
			cmp.Compare(x.A, y.A), cmp.Compare(x.B, y.B), cmp.Compare(x.Value, y.Value))
	})
	return pairs
}
