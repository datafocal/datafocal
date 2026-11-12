package tools

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/summary"
)

// PatternsRequest is the wire request for the list_patterns tool.
type PatternsRequest struct {
	Selector selector.Selector `json:"selector"`
	Sort     string            `json:"sort,omitempty" jsonschema:"Sort order; defaults to count"`
	Limit    int               `json:"limit,omitempty" jsonschema:"Max patterns per page; defaults to 50, max 1000"`
	Cursor   string            `json:"cursor,omitempty" jsonschema:"Opaque pagination cursor from a previous response"`
	Snapshot string            `json:"snapshot,omitempty" jsonschema:"Pin to a catalog snapshot for stable pagination"`
	By       string            `json:"by,omitempty" jsonschema:"Column whose values split each pattern into a row per value"`
}

// Pattern is one deduped pattern row in the response.
type Pattern struct {
	PatternID string  `json:"pattern_id"`
	Pattern   string  `json:"pattern"`
	Service   string  `json:"service"`
	Count     int64   `json:"count"`
	Rate      float64 `json:"rate"`
	// Signal is this pattern's signal score, set only when sort=signal.
	Signal float64 `json:"signal,omitempty"`
	// Status is "new" for a pattern the manifest has no record of before the window, else
	// "rising" at twice or more its lines per bin in the window before, when it had any, else
	// "routine" if those were stats.Routine.
	Status string `json:"status,omitempty"`
	// Value is the row's value of the request's by column, its lines Count.
	Value string `json:"value,omitempty"`
}

// PatternsResponse is the wire response: the paginated slice of Patterns plus
// the total size of the full result set.
type PatternsResponse struct {
	Patterns []Pattern `json:"patterns"`
	Total    int       `json:"total"`
}

// PatternSortModes are the accepted sort orders for list_patterns, in the order
// the schema advertises them. It is the single source of truth for both request
// validation (validatePatternsRequest) and the published schema enum, so the
// two can never drift.
var PatternSortModes = []string{sortModeCount, sortModeRate, sortModeSignal}

// The sort orders: by count or rate, spelled as those measures are, or by the
// annotated signal score.
const (
	sortModeCount  = "count"
	sortModeRate   = "rate"
	sortModeSignal = "signal"
)

// noveltyLookback is a day because anything shorter calls a nightly job's patterns new
// every morning, and the bound keeps novelty from walking the whole retention period.
const noveltyLookback = 24 * time.Hour

// Patterns serves list_patterns as a paged envelope, counting the rows the selector
// matches per pattern_id and naming each from the footers.
func (t toolSet) Patterns(ctx context.Context, req PatternsRequest) (query.Envelope[PatternsResponse], error) {
	var zero query.Envelope[PatternsResponse]

	// Ordered so the selector check still reports before sort/limit, as it did when
	// both lived in validatePatternsRequest.
	sel, err := validateSelector(ctx, "list_patterns", req.Selector)
	if err != nil {
		return zero, err
	}
	if err := validatePatternsRequest(&req, sel); err != nil {
		return zero, err
	}

	// A cursor pins the query that produced it. Offset-based paging is only
	// stable if every later page runs the identical sort and selector against
	// the same snapshot, so reject a cursor replayed against a changed query
	// rather than silently returning duplicated or skipped rows. The fingerprint
	// covers the *unresolved* selector: a relative bound like "now-1h" resolves
	// differently on each page, so fingerprinting the resolved form would reject
	// every follow-up page.
	selFP := selectorFingerprint(req.Selector) + req.By
	cur, err := matchingPatternsCursor(req, selFP)
	if err != nil {
		return zero, err
	}

	snapPin, err := t.resolvePatternSnapshot(ctx, cur, req.Snapshot)
	if err != nil {
		return zero, err
	}

	descs, err := t.Planner.Segments(ctx, snapPin.catalog, sel)
	if err != nil {
		return zero, fmt.Errorf("list_patterns: prune: %w", err)
	}
	footers, err := t.footers(ctx, descs)
	if err != nil {
		return zero, fmt.Errorf("list_patterns: %w", err)
	}
	bins, err := series.ParseBins("all", sel.Window.From, sel.Window.To)
	if err != nil {
		return zero, err
	}
	values, countCost, err := t.patternCounts(ctx, snapPin, sel, bins)
	cost := addCost(footers.Cost, countCost)
	if err != nil {
		return zero, err
	}

	allPatterns := buildPatterns(aggregateFooters(footers), keptTotals(values, slices.Collect(maps.Keys(values))), sel)
	annotateCost, err := t.annotatePatterns(ctx, snapPin, sel, allPatterns, req.Sort == sortModeSignal)
	cost = addCost(cost, annotateCost)
	if err != nil {
		return zero, err
	}
	allPatterns, splitCost, err := t.splitByValue(ctx, snapPin, footers, req.By, sel.Window, allPatterns)
	cost = addCost(cost, splitCost)
	if err != nil {
		return zero, err
	}
	sortPatterns(req.Sort, allPatterns)
	page, nextCursor := paginate(allPatterns, cur.Offset, req.Limit, snapPin.String(), req.Sort, selFP)

	resp := PatternsResponse{Patterns: page, Total: len(allPatterns)}
	return query.Envelope[PatternsResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snapPin.String(), Cursor: nextCursor, Coverage: 1.0,
			Cost: cost, Tokens: query.EstimateTokens(resp),
		},
	}, nil
}

// validatePatternsRequest checks and defaults the list_patterns-specific fields.
// The selector itself is validated by validateSelector.
func validatePatternsRequest(req *PatternsRequest, sel selector.Resolved) error {
	if sel.Window.Duration() <= 0 {
		return query.Invalidf("list_patterns: a time range with from before to is required to count the patterns")
	}
	if req.By != "" && !patternScoped(sel) {
		return query.Invalidf("list_patterns: by counts every line of a pattern, so it takes no predicate but service")
	}
	if req.Sort == "" {
		req.Sort = sortModeCount
	}
	if !slices.Contains(PatternSortModes, req.Sort) {
		return query.Invalidf("list_patterns: unsupported sort %q", req.Sort)
	}
	if req.Limit <= 0 {
		req.Limit = 50
	}
	if req.Limit > 1000 {
		req.Limit = 1000
	}
	return nil
}

type patternAgg struct {
	text      string
	count     int64
	byService map[string]int64
}

func (t toolSet) aggregatePatterns(ctx context.Context, descs []segment.Descriptor) (map[string]*patternAgg, query.Cost, error) {
	p, err := t.footers(ctx, descs)
	if err != nil {
		return nil, p.Cost, fmt.Errorf("list_patterns: %w", err)
	}
	return aggregateFooters(p), p.Cost, nil
}

// aggregateFooters names and counts each pattern p's footers hold.
func aggregateFooters(p plan.Plan) map[string]*patternAgg {
	byPattern := make(map[string]*patternAgg)
	for _, footer := range p.Footers() {
		for key, n := range footer.ServicePatternCounts {
			agg := byPattern[key.Pattern]
			if agg == nil {
				text, _ := footer.PatternText(key.Pattern)
				agg = &patternAgg{text: text, byService: make(map[string]int64)}
				byPattern[key.Pattern] = agg
			}
			agg.count += int64(n)
			agg.byService[key.Service] += int64(n)
		}
	}
	return byPattern
}

// dominantService returns the service with the most occurrences, ties to the smallest name.
func dominantService(byService map[string]int64) string {
	var best string
	var bestCount int64 = -1
	for svc, c := range byService {
		if c > bestCount || (c == bestCount && svc < best) {
			best, bestCount = svc, c
		}
	}
	return best
}

// patternCounts counts the rows sel matches in its window per pattern_id and bin.
func (t toolSet) patternCounts(ctx context.Context, snap snapshot, sel selector.Resolved, bins series.Bins) (series.Values, query.Cost, error) {
	res, err := t.Series.Read(ctx, series.Request{
		Version:  snap.catalog,
		Summary:  snap.summaryServes(),
		Selector: sel,
		Window:   sel.Window,
		Measure:  series.Measure{Kind: series.KindCount},
		GroupBy:  segment.ColPatternID,
		Bins:     bins,
		Bound:    series.Bound{Segments: defaultMaxScanSegments, Bytes: defaultMaxScanBytes},
	})
	if err != nil {
		return nil, res.Cost, fmt.Errorf("list_patterns: count: %w", err)
	}
	return res.Values, res.Cost, nil
}

// buildPatterns names each pattern sel's rows hold by the service sel equates, else the
// one emitting it most.
func buildPatterns(byPattern map[string]*patternAgg, counts map[string]int64, sel selector.Resolved) []Pattern {
	var service string
	for _, p := range sel.Where {
		if p.Field == segment.ColService && p.Op == selector.OpEq {
			service = p.Value
		}
	}
	windowSecs := sel.Window.Duration().Seconds()
	patterns := make([]Pattern, 0, len(counts))
	for id, agg := range byPattern {
		if counts[id] == 0 {
			continue
		}
		patterns = append(patterns, Pattern{
			PatternID: id,
			Pattern:   agg.text,
			Service:   cmp.Or(service, dominantService(agg.byService)),
			Count:     counts[id],
			Rate:      float64(counts[id]) / windowSecs,
		})
	}
	return patterns
}

func paginate(patterns []Pattern, offset, limit int, snap, sortMode, selFP string) ([]Pattern, string) {
	total := len(patterns)
	offset = min(offset, total)
	end := min(offset+limit, total)
	page := patterns[offset:end]
	var nextCursor string
	if end < total {
		nextCursor = encodePatternsCursor(patternsCursor{Snapshot: snap, Sort: sortMode, Selector: selFP, Offset: end})
	}
	return page, nextCursor
}

func (t toolSet) resolvePatternSnapshot(ctx context.Context, cur patternsCursor, reqSnap string) (snapshot, error) {
	if cur.Snapshot != "" && reqSnap != "" && reqSnap != cur.Snapshot {
		return snapshot{}, query.Invalidf("list_patterns: cursor snapshot %q does not match request snapshot %q", cur.Snapshot, reqSnap)
	}
	return t.resolveSnapshot(ctx, t.Summary, "list_patterns", cmp.Or(cur.Snapshot, reqSnap))
}

func sortPatterns(mode string, ps []Pattern) {
	sort.SliceStable(ps, func(i, j int) bool {
		var vi, vj float64
		switch mode {
		case sortModeCount:
			vi, vj = float64(ps[i].Count), float64(ps[j].Count)
		case sortModeRate:
			vi, vj = ps[i].Rate, ps[j].Rate
		case sortModeSignal:
			vi, vj = ps[i].Signal, ps[j].Signal
		}
		if vi != vj {
			return vi > vj
		}
		if ps[i].PatternID != ps[j].PatternID {
			return ps[i].PatternID < ps[j].PatternID
		}
		return ps[i].Value < ps[j].Value
	})
}

// annotatePatterns sets each pattern's status and, when signal is set, its Signal score
// from durable artifacts only, consulting the summary just where it cannot change the
// answer, so two queriers at one snapshot agree whatever their caches hold.
func (t toolSet) annotatePatterns(ctx context.Context, snap snapshot, sel selector.Resolved, ps []Pattern, signal bool) (query.Cost, error) {
	ids := make([]string, len(ps))
	var windowLines int64
	for i := range ps {
		ids[i] = ps[i].PatternID
		windowLines += ps[i].Count
	}
	novel, err := t.novelPatterns(ctx, snap, sel, ids)
	if err != nil {
		return query.Cost{}, err
	}
	baseline, bins, cost, err := t.baselineCounts(ctx, snap, sel)
	if err != nil {
		return cost, err
	}
	windowBins, baselineBins := sel.Window.Duration().Seconds()/bins.Width.Seconds(), observedBins(baseline, bins)

	for i := range ps {
		_, isNew := novel[ps[i].PatternID]
		was := keptTotals(baseline, []string{ps[i].PatternID})[ps[i].PatternID]
		switch {
		case isNew:
			ps[i].Status = "new"
		case was > 0 && float64(ps[i].Count)/windowBins >= 2*float64(was)/baselineBins:
			ps[i].Status = "rising"
		case stats.Routine(densify(baseline[ps[i].PatternID], bins, 0)):
			ps[i].Status = "routine"
		}
		if !signal {
			continue
		}
		_, measured := baseline[ps[i].PatternID]
		ps[i].Signal = stats.Score(stats.Evidence{
			IsNew:    isNew,
			Rate:     ps[i].Rate,
			PrevRate: float64(was) / sel.Window.Duration().Seconds(),
			// Reading an unmeasured baseline as zero would fire rate-of-change for every
			// new pattern and lose the ranking among several new ones.
			FirstObservation: !measured,
			Lines:            ps[i].Count,
			TotalLines:       windowLines,
		})
	}
	return cost, nil
}

// baselineCounts keeps the caller's predicates, unlike novelty's lookback, because
// rate-of-change compares this same slice against the preceding equal-width window.
func (t toolSet) baselineCounts(ctx context.Context, snap snapshot, sel selector.Resolved) (series.Values, series.Bins, query.Cost, error) {
	prior := sel
	prior.Window = selector.Window{From: sel.Window.From.Add(-sel.Window.Duration()), To: sel.Window.From}
	bins, err := series.ParseBins("auto", prior.Window.From, prior.Window.To)
	if err != nil {
		return nil, bins, query.Cost{}, err
	}
	counts, cost, err := t.patternCounts(ctx, snap, prior, bins)
	return counts, bins, cost, err
}

// observedBins is how many of bins there are from the first holding any of baseline's lines,
// since those before it, in a corpus younger than the baseline, say nothing of a rate.
func observedBins(baseline series.Values, bins series.Bins) float64 {
	first := bins.To.UnixMilli()
	for _, byBin := range baseline {
		for start, n := range byBin {
			if n > 0 {
				first = min(first, start)
			}
		}
	}
	return float64(bins.To.UnixMilli()-first) / float64(bins.Width.Milliseconds())
}

// novelPatterns answers from the catalog manifest, since an empty summary looks like a
// corpus with no history and would make answers depend on the querier's uptime.
func (t toolSet) novelPatterns(ctx context.Context, snap snapshot, sel selector.Resolved, ids []string) (map[string]struct{}, error) {
	store := t.store(snap)
	from := sel.Window.From
	candidates := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if store != nil && store.ExistedBefore(id, from) {
			continue
		}
		candidates[id] = struct{}{}
	}
	if len(candidates) == 0 || from.IsZero() {
		return candidates, nil
	}

	// Only the lookback carries over, dropping the caller's predicates on purpose: a
	// pattern already emitted by another service is not new, so scoping the history to
	// this query's services would call it new on its first appearance in each one.
	history := selector.Resolved{Window: selector.Window{From: from.Add(-noveltyLookback), To: from}}
	descs, err := t.Planner.Segments(ctx, snap.catalog, history)
	if err != nil {
		return nil, fmt.Errorf("list_patterns: prune novelty lookback: %w", err)
	}
	for _, d := range descs {
		// The catalog keeps a segment starting at the lookback's end, though its rows lie in the window.
		if !d.TimeRange.Min.Before(from) {
			continue
		}
		for _, id := range d.PatternIDs {
			delete(candidates, id)
		}
	}
	return candidates, nil
}

// patternScoped reports whether sel names only services, so the value series count every row
// of a pattern it keeps.
func patternScoped(sel selector.Resolved) bool {
	return !slices.ContainsFunc(sel.Where, func(p selector.Predicate) bool {
		return p.Field != segment.ColService || p.Op != selector.OpEq
	})
}

// splitByValue is a row per value of by of each pattern's lines in w, keeping the rest of its row, or
// ps when by is empty.
func (t toolSet) splitByValue(ctx context.Context, snap snapshot, p plan.Plan, by string, w selector.Window, ps []Pattern) ([]Pattern, query.Cost, error) {
	if by == "" {
		return ps, query.Cost{}, nil
	}
	values, cost, err := t.valueCounts(ctx, snap, p, by, w)
	if err != nil {
		return nil, cost, fmt.Errorf("list_patterns: %w", err)
	}
	byPattern := map[string]map[string]int64{}
	for k, n := range values {
		if byPattern[k.PatternID] == nil {
			byPattern[k.PatternID] = map[string]int64{}
		}
		byPattern[k.PatternID][k.Value] += int64(n)
	}
	rows := make([]Pattern, 0, len(ps))
	for _, p := range ps {
		for v, lines := range byPattern[p.PatternID] {
			p.Value, p.Count, p.Rate = v, lines, float64(lines)/w.Duration().Seconds()
			rows = append(rows, p)
		}
	}
	return rows, cost, nil
}

// valueCounts is each pattern's rows by value of column, or of every counted column when it is
// "", in w's bins: from the summary for each column it holds, and folded from p as the summary
// folds its segments for the rest, a column too wide for it among them. Of every counted column,
// one too wide for the summary holding text is left out, since an id is neither a code nor a measure.
func (t toolSet) valueCounts(ctx context.Context, snap snapshot, p plan.Plan, column string, w selector.Window) (summary.ValueCounts, query.Cost, error) {
	width := t.summaryWidth().Milliseconds()
	from, to := segment.FloorBin(w.From.UnixMilli(), width), w.To.UnixMilli()
	columns, counts := countedColumns(p, column), summary.ValueCounts{}
	store := t.store(snap)
	if store != nil && column == "" {
		maps.DeleteFunc(columns, func(c string, _ struct{}) bool { return store.Widened(c) && holdsText(p, c) })
	}
	if store != nil && store.CompleteFrom(from) {
		held := maps.Clone(columns)
		maps.DeleteFunc(held, func(c string, _ struct{}) bool { return !store.HoldsValues(c, from) })
		maps.DeleteFunc(columns, func(c string, _ struct{}) bool { _, ok := held[c]; return ok })
		counts = storeValues(store, held, from, to)
	}
	if len(columns) == 0 {
		return counts, query.Cost{}, nil
	}
	var cost query.Cost
	counted := valueCounter{wanted: columns, width: width, from: from, to: to, counts: map[summary.ValueKey]*uint64{}, owned: map[string]string{}}
	for d, footer := range p.Footers() {
		rowCost, err := t.segmentValues(ctx, p, d, footer, counted)
		if cost = addCost(cost, rowCost); err != nil {
			return nil, cost, err
		}
	}
	merged := make(summary.ValueCounts, len(counts)+len(counted.counts))
	maps.Copy(merged, counts)
	for k, n := range counted.counts {
		merged[k] = *n
	}
	return merged, cost, nil
}

// valueCounter counts the value series of wanted in bins [from, to) through a pointer, since
// assigning would store a scanned row's strings, which go with the scan, as the key; a new
// key's strings are cloned once each across keys.
type valueCounter struct {
	wanted          map[string]struct{}
	width, from, to int64
	counts          map[summary.ValueKey]*uint64
	owned           map[string]string
}

func (c valueCounter) add(k summary.ValueKey, n uint64) {
	if counted := c.counts[k]; counted != nil {
		*counted += n
		return
	}
	k.PatternID, k.Value = c.own(k.PatternID), c.own(k.Value)
	counted := new(uint64)
	*counted, c.counts[k] = n, counted
}

// addFooter adds a one-bin segment's value series at bin from its footer.
func (c valueCounter) addFooter(footer segment.Footer, bin int64) {
	for k, n := range summary.FooterValues(footer, bin) {
		if _, ok := c.wanted[k.Column]; ok && k.Bin >= c.from && k.Bin < c.to {
			c.add(k, n)
		}
	}
}

func (c valueCounter) own(s string) string {
	if owned, ok := c.owned[s]; ok {
		return owned
	}
	owned := strings.Clone(s)
	c.owned[owned] = owned
	return owned
}

// storeValues is store's value series of columns in bins [from, to).
func storeValues(store summary.Store, columns map[string]struct{}, from, to int64) summary.ValueCounts {
	counts := summary.ValueCounts{}
	for c := range columns {
		for _, pt := range store.ValueSlice(c, from, to) {
			counts[summary.ValueKey{PatternID: pt.PatternID, Column: c, Value: pt.Value, Bin: pt.Bin}] = pt.Count
		}
	}
	return counts
}

// countedColumns is column, or every column when it is "", that a footer of p counts the values of.
func countedColumns(p plan.Plan, column string) map[string]struct{} {
	columns := map[string]struct{}{}
	for _, footer := range p.Footers() {
		for _, c := range summary.CountedColumns(footer) {
			if column == "" || c == column {
				columns[c] = struct{}{}
			}
		}
	}
	return columns
}

// holdsText reports whether a footer of p counts a value of column that is not a number.
func holdsText(p plan.Plan, column string) bool {
	for _, footer := range p.Footers() {
		byValue, _ := footer.ValueCounts(column)
		for v := range byValue {
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				return true
			}
		}
	}
	return false
}

// segmentValues counts d's value series into counted, a multi-bin one's counted from its rows
// in p as the summary's builder counts them.
func (t toolSet) segmentValues(ctx context.Context, p plan.Plan, d segment.Descriptor, footer segment.Footer, counted valueCounter) (query.Cost, error) {
	wanted, width, from, to := counted.wanted, counted.width, counted.from, counted.to
	if bin := segment.FloorBin(d.TimeRange.Min.UnixMilli(), width); bin == segment.FloorBin(d.TimeRange.Max.UnixMilli(), width) {
		counted.addFooter(footer, bin)
		return query.Cost{}, nil
	}
	items := slices.DeleteFunc(slices.Clone(p.Items), func(wi plan.WorkItem) bool { return wi.Segment.ID != d.ID })
	columns := slices.DeleteFunc(summary.CountedColumns(footer), func(c string) bool { _, ok := wanted[c]; return !ok })
	if len(columns) == 0 {
		return query.Cost{}, nil
	}
	rows := t.Scanner.Scan(ctx, plan.Plan{Items: items}, scan.Query{Columns: append([]string{segment.ColTimestamp, segment.ColService, segment.ColPatternID}, columns...)})
	for rows.Next() {
		r := rows.Row()
		service, _ := r.Text(segment.ColService)
		pattern, _ := r.Text(segment.ColPatternID)
		bin := segment.FloorBin(r.Time.UnixMilli(), width)
		if service == "" || pattern == "" || bin < from || bin >= to {
			continue
		}
		for _, c := range columns {
			v, _ := r.Text(c)
			counted.add(summary.ValueKey{PatternID: pattern, Column: c, Value: v, Bin: bin}, 1)
		}
	}
	return rows.Cost(), rows.Err()
}
