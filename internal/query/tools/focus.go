package tools

import (
	"context"
	"slices"
	"sync"

	"github.com/datafocal/datafocal/internal/query"
)

// FocusRequest is explain_spike's: a scope, the slice to focus on within it, and the pin.
type FocusRequest = ExplainSpikeRequest

// commonShare and commonLift are what makes a value something the focused lines have in
// common: at least half of them hold it, at least three times as often as the rest do.
// explain_spike's own bar is lower, since a smaller rise can still point at a cause.
const commonShare, commonLift = 0.5, 3

// FocusResponse is the slice's trend, the values its lines have in common against the rest
// of the scope, patterns and example lines, each left out when its tool failed and that
// failure named in Errors.
type FocusResponse struct {
	Trend    *AggregateResponse    `json:"trend,omitempty"`
	Lift     *ExplainSpikeResponse `json:"lift,omitempty"`
	Related  *PatternsResponse     `json:"related,omitempty"`
	Examples *SearchResponse       `json:"examples,omitempty"`
	Errors   map[string]string     `json:"errors,omitempty"` // trend|lift|related|examples → error
}

// Focus pins the snapshot once so every section reads the same one.
func (t toolSet) Focus(ctx context.Context, req FocusRequest) (query.Envelope[FocusResponse], error) {
	// A section's failure is reported beside the others, so a request no section could
	// answer is refused up front.
	if _, err := validateSelector(ctx, "focus", req.Selector); err != nil {
		return query.Envelope[FocusResponse]{}, err
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "focus", req.Snapshot)
	if err != nil {
		return query.Envelope[FocusResponse]{}, err
	}
	return focus(ctx, t, req, snap.String())
}

// focus calls aggregate, explain_spike, list_patterns and search_logs over the slice at pin,
// at once since none needs another's answer, then folds them in a fixed order so the
// response is the same whichever finishes first.
func focus(ctx context.Context, t Tools, req FocusRequest, pin string) (query.Envelope[FocusResponse], error) { //nolint:chatter // focus is the one call composing four tools.
	sel := req.Selector
	sel.Where = slices.Concat(req.Selector.Where, req.Slice)
	var (
		wg                               sync.WaitGroup
		trend                            query.Envelope[AggregateResponse]
		lift                             query.Envelope[ExplainSpikeResponse]
		related                          query.Envelope[PatternsResponse]
		examples                         query.Envelope[SearchResponse]
		trendErr, liftErr, relErr, exErr error
	)
	wg.Go(func() {
		trend, trendErr = t.Aggregate(ctx, AggregateRequest{Selector: sel, Measure: "count", Bins: "auto", Snapshot: pin})
	})
	wg.Go(func() {
		lift, liftErr = t.ExplainSpike(ctx, ExplainSpikeRequest{Selector: req.Selector, Slice: req.Slice, Snapshot: pin})
	})
	wg.Go(func() { related, relErr = t.Patterns(ctx, PatternsRequest{Selector: sel, Limit: 10, Snapshot: pin}) })
	wg.Go(func() { examples, exErr = t.SearchLogs(ctx, SearchRequest{Selector: sel, Limit: 5, Snapshot: pin}) })
	wg.Wait()

	meta := query.Meta{Snapshot: pin, Coverage: 1}
	resp := FocusResponse{Errors: map[string]string{}}
	resp.Trend = section("trend", trend, trendErr, &meta, resp.Errors)
	resp.Lift = section("lift", lift, liftErr, &meta, resp.Errors)
	if resp.Lift != nil {
		resp.Lift.Shifts = slices.DeleteFunc(resp.Lift.Shifts, func(s Shift) bool {
			return s.SliceShare < commonShare || s.SliceShare < commonLift*s.BaselineShare
		})
	}
	resp.Related = section("related", related, relErr, &meta, resp.Errors)
	resp.Examples = section("examples", examples, exErr, &meta, resp.Errors)
	meta.Tokens = query.EstimateTokens(resp)
	return query.Envelope[FocusResponse]{Data: resp, Meta: meta}, nil
}

// section folds one section's answer into meta and returns it, or records its error and
// returns nil.
func section[T any](name string, env query.Envelope[T], err error, meta *query.Meta, errs map[string]string) *T {
	if err != nil {
		errs[name] = err.Error()
		return nil
	}
	meta.Cost = addCost(meta.Cost, env.Meta.Cost)
	meta.Coverage = min(meta.Coverage, env.Meta.Coverage)
	meta.Approx = meta.Approx || env.Meta.Approx
	return &env.Data
}
