package tools

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
)

// SeriesRef carries Measure and Where because the seed symptom is the 5xx slice of a
// flat pattern and its disk signal a column measure, neither of which a count sees.
type SeriesRef struct {
	PatternID string `json:"pattern_id" jsonschema:"pattern_id whose series to correlate"`
	// Measure is the aggregate measure this series samples, in aggregate's own
	// vocabulary (count|rate|sum:<field>|p95|distinct:<field>|ratio). Empty means count.
	Measure string `json:"measure,omitempty" jsonschema:"count|rate|sum:<field>|p95|distinct:<field>|ratio; empty means count"`
	// Where narrows this series alone, on top of the request selector's predicates,
	// because the interesting comparison is usually asymmetric: the 5xx slice of one
	// pattern against another pattern's whole output. For a ratio it is instead the
	// numerator, the denominator being this pattern's own unfiltered count.
	Where []selector.Predicate `json:"where,omitempty" jsonschema:"predicates narrowing this series only; the numerator for a ratio measure"`
}

// scopeKey is the (measure, predicates) pair that decides which series can be read
// together. Series sharing a scope come out of one read, grouped by pattern, so the
// read count tracks distinct scopes rather than series.
func (r SeriesRef) scopeKey() string {
	var b strings.Builder
	b.WriteString(r.Measure)
	for _, p := range r.Where {
		b.WriteString("\x00")
		b.WriteString(p.Field)
		b.WriteString("\x00")
		b.WriteString(string(p.Op))
		b.WriteString("\x00")
		b.WriteString(p.Value)
	}
	return b.String()
}

// CorrelateRequest is the wire request for the correlate tool. It cross-correlates
// two count series over the selector window and reports the lag of peak
// correlation.
type CorrelateRequest struct {
	Selector selector.Selector `json:"selector"`
	SeriesA  SeriesRef         `json:"series_a"`
	SeriesB  SeriesRef         `json:"series_b"`
	// MaxLagSeconds bounds the ± lag search. It is clamped to the window so an
	// oversized value cannot make the O(N·maxLag) sweep run past the series.
	MaxLagSeconds int `json:"max_lag_seconds,omitempty" jsonschema:"Max ± lag to search, in seconds; clamped to the window"`
	// Bins sets the resolution of both series and with it the granularity of the
	// reported lag — a 1m grid cannot resolve a 6s lead. Same vocabulary as
	// aggregate. Empty takes the summary's own width when a store is present, so
	// the request lands on the grid the summary can serve without a scan.
	Bins     string `json:"bins,omitempty" jsonschema:"15s|1m|5m|1h|auto"`
	Snapshot string `json:"snapshot,omitempty"`
}

// CorrelateResponse carries the peak of the cross-correlation: the signed
// coefficient, the lag (in seconds; positive means series B lags series A), and
// a human-readable direction ("a_leads_b", "b_leads_a", or "none").
type CorrelateResponse struct {
	Rho        float64 `json:"rho"`
	LagSeconds float64 `json:"lag_seconds"`
	Direction  string  `json:"direction"`
}

// Correlate is the correlate tool entry point. It samples both series onto one
// bin grid over the selector window, runs the cross-correlation, and returns the
// peak lag.
func (t toolSet) Correlate(ctx context.Context, req CorrelateRequest) (query.Envelope[CorrelateResponse], error) {
	var zero query.Envelope[CorrelateResponse]

	if req.SeriesA.PatternID == "" || req.SeriesB.PatternID == "" {
		return zero, query.Invalidf("correlate: series_a and series_b each require a pattern_id")
	}
	if req.MaxLagSeconds < 0 {
		return zero, query.Invalidf("correlate: max_lag_seconds must be non-negative")
	}

	sel, err := validateSelector(ctx, "correlate", req.Selector)
	if err != nil {
		return zero, err
	}
	if !sel.Window.Bounded() {
		return zero, query.Invalidf("correlate: a bounded time window is required to align both series")
	}

	snap, err := t.resolveSnapshot(ctx, t.Summary, "correlate", req.Snapshot)
	if err != nil {
		return zero, err
	}
	// The grid comes off the configured store, not the gated one — bin width is
	// process configuration, and both paths must bin identically.
	bins, err := series.GridFor(req.Bins, t.summaryWidth(), sel.Window)
	if err != nil {
		return zero, query.Invalidf("correlate: %v", err)
	}

	series, cost, err := t.resolveSeries(ctx, readScope{snap: snap, sel: sel, bins: bins}, []SeriesRef{req.SeriesA, req.SeriesB})
	if err != nil {
		return zero, err
	}
	a, b := series[0], series[1]

	result := stats.CrossCorrelate(a, b, bins.Width, clampLagBins(req.MaxLagSeconds, bins.Width, len(a)))
	resp := CorrelateResponse{
		Rho:        result.Rho,
		LagSeconds: result.LagDuration.Seconds(),
		Direction:  result.Direction,
	}
	return query.Envelope[CorrelateResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: 1.0,
			Cost:     cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}, nil
}

// readScope is what every series read in one correlate or rca_walk call shares: the
// pinned snapshot, the resolved selector, the bin grid, and how much a scan may read.
type readScope struct {
	snap  snapshot
	sel   selector.Resolved
	bins  series.Bins
	bound series.Bound
}

// resolveSeries samples each ref onto the bin grid, returning one dense series per ref
// in the order given.
//
// Series sharing a scope — the same measure and the same per-series predicates — come
// out of a single read grouped by pattern, because every measure this reads groups by
// pattern and so yields every pattern at once. Asking per series would read the same
// segments once per series for answers already in hand, which is the fan-out §6 bounds.
// Distinct scopes need distinct reads: a filtered series and an unfiltered one are
// different row sets, and a sum and a count are different columns.
func (t toolSet) resolveSeries(ctx context.Context, scope readScope, refs []SeriesRef) ([][]float64, query.Cost, error) {

	var cost query.Cost
	out := make([][]float64, len(refs))

	// Group the refs by scope, keeping first-seen order so the reads happen in a
	// deterministic sequence and the cost is the same on every call.
	var order []string
	byScope := make(map[string][]int, len(refs))
	for i, r := range refs {
		key := r.scopeKey()
		if _, seen := byScope[key]; !seen {
			order = append(order, key)
		}
		byScope[key] = append(byScope[key], i)
	}

	for _, key := range order {
		idx := byScope[key]
		ref := refs[idx[0]]
		wanted := make(map[string]struct{}, len(idx))
		for _, i := range idx {
			wanted[refs[i].PatternID] = struct{}{}
		}
		res, err := t.readScopedSeries(ctx, scope, ref, wanted)
		cost = addCost(cost, res.Cost)
		if err != nil {
			return nil, cost, err
		}
		// Every ref in a scope shares its measure (scopeKey folds it in), so the gap
		// treatment is decided once per scope: NaN for a ratio's 0/0, which
		// CrossCorrelate skips, and 0 for every other measure's genuinely empty bin.
		missing := 0.0
		if isRatio(ref.Measure) {
			missing = math.NaN()
		}
		for _, i := range idx {
			out[i] = densify(res.Values[refs[i].PatternID], scope.bins, missing)
		}
	}
	return out, cost, nil
}

// readScopedSeries reads one scope's series by pattern from the segments listing a
// wanted one, treating a ratio's Where as its numerator and letting only a count or a
// ratio come off the summary.
func (t toolSet) readScopedSeries(ctx context.Context, scope readScope, ref SeriesRef, wanted map[string]struct{}) (series.Result, error) {
	measure, err := series.ParseMeasure(cmp.Or(ref.Measure, series.KindCount.String()))
	if err != nil {
		return series.Result{}, err
	}
	req := series.Request{
		Version:  scope.snap.catalog,
		Selector: scope.sel,
		Window:   scope.bins.Window(),
		Measure:  measure,
		GroupBy:  segment.ColPatternID,
		Bins:     scope.bins,
		Patterns: wanted,
		Bound:    scope.bound,
	}
	switch measure.Kind {
	case series.KindRatio:
		if err := validateNumerator("correlate", series.KindRatio, ref.Where); err != nil {
			return series.Result{}, err
		}
		req.Numerator, req.Summary = ref.Where, scope.snap.summaryServes()
	case series.KindCount:
		req.Summary = scope.snap.summaryServes()
		fallthrough
	default:
		// Cloned so one scope's predicates never leak into the next.
		req.Selector.Where = append(slices.Clone(scope.sel.Where), ref.Where...)
	}
	res, err := t.Series.Read(ctx, req)
	if err != nil {
		return res, fmt.Errorf("correlate: %q series: %w", ref.Measure, err)
	}
	return res, nil
}

// clampLagBins converts a max-lag in seconds to bin units and caps it at the
// series length. Searching past the series only adds empty overlaps, and the cap
// bounds the O(N·maxLag) sweep when a caller passes an oversized lag.
func clampLagBins(maxLagSeconds int, binWidth time.Duration, n int) int {
	if binWidth <= 0 || n < 2 {
		return 0
	}
	bins := int(time.Duration(maxLagSeconds) * time.Second / binWidth)
	if bins > n-1 {
		bins = n - 1
	}
	return bins
}
