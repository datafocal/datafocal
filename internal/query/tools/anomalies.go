package tools

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/summary"
)

// The anomaly kinds find_anomalies reports.
const (
	anomalyKindSpike      = "spike"
	anomalyKindNewPattern = "new_pattern"
	anomalyKindDrift      = "drift"
	anomalyKindNewValue   = "new_value"
	anomalyKindMixShift   = "mix_shift"
)

const (
	// maxCodes is the most values a numeric column holds and still reads as a code, as an
	// HTTP status does, rather than a measure, as a latency does.
	maxCodes = 32
	// driftThreshold is how many baseline spreads a measure's p99 must move to drift.
	driftThreshold = 3.0
	// mixShiftThreshold is the divergence, in nats, of a code's mix from before that shifts it.
	mixShiftThreshold = 0.05
)

// FindAnomaliesRequest is the wire request for the find_anomalies tool.
type FindAnomaliesRequest struct {
	Selector selector.Selector `json:"selector"`
	Snapshot string            `json:"snapshot,omitempty"`
}

// FindAnomaliesResponse is the wire response: the anomalies in scope, grouped, by score.
type FindAnomaliesResponse struct {
	Anomalies []Anomaly `json:"anomalies"`
}

// Anomaly is one pattern's count spike, its first bin in the window when nothing places it
// before the window, or a shift in the values of one of its columns.
type Anomaly struct {
	Kind      string    `json:"kind"` // "spike" | "new_pattern" | "drift" | "new_value" | "mix_shift"
	PatternID string    `json:"pattern_id"`
	Pattern   string    `json:"pattern"`
	Service   string    `json:"service"`
	At        time.Time `json:"at"`
	Observed  float64   `json:"observed"`
	Expected  float64   `json:"expected"`
	// Column is a value anomaly's column, and Value its new value or the one whose share rose most.
	Column string `json:"column,omitempty"`
	Value  string `json:"value,omitempty"`
	// Level is "error" for a pattern with error or fatal lines, else "warn" for one with warn lines.
	Level string `json:"level,omitempty"`
	// Score weighs how far the anomaly stands out and the log of its lines by its level.
	Score float64 `json:"score"`
	// Routine marks a pattern logging near-constantly in every bin, a heartbeat or health check.
	Routine bool `json:"routine,omitempty"`
	// Members are the rest of its group, ranked.
	Members []Anomaly `json:"members,omitempty"`

	// deviation is a spike's standard deviations, excess its lines over baseline or a new pattern's lines.
	deviation, excess float64
}

// The levels that weigh an anomaly up.
const (
	levelError = "error"
	levelWarn  = "warn"
)

// levelWeights weigh an anomaly's score by its level, any other level by 1.
var levelWeights = map[string]float64{levelError: 4, levelWarn: 2}

// FindAnomalies reads one count series per pattern_id over the window, reports each
// pattern's strongest spike and every pattern new to the window, and ranks them by score.
func (t toolSet) FindAnomalies(ctx context.Context, req FindAnomaliesRequest) (query.Envelope[FindAnomaliesResponse], error) {
	var zero query.Envelope[FindAnomaliesResponse]

	sel, err := validateSelector(ctx, "find_anomalies", req.Selector)
	if err != nil {
		return zero, err
	}
	if !sel.Window.Bounded() {
		return zero, query.Invalidf("find_anomalies: a bounded time window is required to bin the series")
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "find_anomalies", req.Snapshot)
	if err != nil {
		return zero, err
	}
	bins, err := series.GridFor("", t.summaryWidth(), sel.Window)
	if err != nil {
		return zero, query.Invalidf("find_anomalies: %v", err)
	}

	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return zero, fmt.Errorf("find_anomalies: prune: %w", err)
	}
	footers, err := t.footers(ctx, descs)
	if err != nil {
		return zero, fmt.Errorf("find_anomalies: %w", err)
	}
	byPattern, cost := aggregateFooters(footers), footers.Cost
	res, err := t.Series.Read(ctx, series.Request{
		Version:  snap.catalog,
		Summary:  snap.summaryServes(),
		Selector: sel,
		Window:   bins.Window(),
		Measure:  series.Measure{Kind: series.KindCount},
		GroupBy:  segment.ColPatternID,
		Bins:     bins,
	})
	cost = addCost(cost, res.Cost)
	if err != nil {
		return zero, fmt.Errorf("find_anomalies: count series: %w", err)
	}
	ids := slices.Sorted(maps.Keys(res.Values))
	novel, err := t.novelPatterns(ctx, snap, sel, ids)
	if err != nil {
		return zero, err
	}

	found := detectAnomalies(ids, res.Values, bins, byPattern, novel)
	if patternScoped(sel) {
		values, valueCost, err := t.valueCounts(ctx, snap, footers, "", bins.Window())
		cost = addCost(cost, valueCost)
		if err != nil {
			return zero, fmt.Errorf("find_anomalies: %w", err)
		}
		found = append(found, valueAnomalies(values, res.Values, bins, byPattern)...)
	}
	levels, levelCost, err := t.patternLevels(ctx, snap, sel, found)
	cost = addCost(cost, levelCost)
	if err != nil {
		return zero, err
	}

	resp := FindAnomaliesResponse{Anomalies: rankAnomalies(found, levels)}
	return query.Envelope[FindAnomaliesResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: res.Coverage,
			Approx:   res.Approx,
			Cost:     cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}, nil
}

// detectAnomalies drops the spike of a pattern that is its service's every line, since that is the service's volume.
func detectAnomalies(ids []string, values series.Values, bins series.Bins, byPattern map[string]*patternAgg,
	novel map[string]struct{}) []Anomaly {
	serviceLines := make(map[string]int64)
	for _, agg := range byPattern {
		for svc, lines := range agg.byService {
			serviceLines[svc] += lines
		}
	}
	anomalies := make([]Anomaly, 0, len(ids))
	for _, id := range ids {
		var service string
		sole := false
		if agg := byPattern[id]; agg != nil {
			service = dominantService(agg.byService)
			sole = agg.count == serviceLines[service]
		}
		_, isNew := novel[id]
		found := patternAnomalies(id, service, densify(values[id], bins, 0), bins, isNew)
		if sole {
			found = slices.DeleteFunc(found, func(a Anomaly) bool { return a.Kind == anomalyKindSpike })
		}
		anomalies = append(anomalies, found...)
	}
	for i := range anomalies {
		if agg := byPattern[anomalies[i].PatternID]; agg != nil {
			anomalies[i].Pattern = agg.text
		}
	}
	return anomalies
}

// levelWhere selects the lines that weigh an anomaly up, error and fatal ones before warn.
var levelWhere = [][]selector.Predicate{
	{{Field: segment.ColLevel, Op: selector.OpRegex, Value: "^(error|fatal)$"}},
	{{Field: segment.ColLevel, Op: selector.OpEq, Value: levelWarn}},
}

// patternLevels names the level of each anomalous pattern but routine ones, which rank last anyway.
func (t toolSet) patternLevels(ctx context.Context, snap snapshot, sel selector.Resolved, found []Anomaly) (map[string]string, query.Cost, error) {
	ids := make(map[string]struct{}, len(found))
	for _, a := range found {
		if !a.Routine {
			ids[a.PatternID] = struct{}{}
		}
	}
	if len(ids) == 0 {
		return nil, query.Cost{}, nil
	}
	bins, err := series.ParseBins("all", sel.Window.From, sel.Window.To)
	if err != nil {
		return nil, query.Cost{}, err
	}
	measures := make([]series.FilteredMeasure, len(levelWhere))
	for i, where := range levelWhere {
		measures[i] = series.FilteredMeasure{Measure: series.Measure{Kind: series.KindCount}, Where: where}
	}
	res, err := t.Series.Read(ctx, series.Request{
		Version: snap.catalog, Selector: sel, Window: sel.Window, GroupBy: segment.ColPatternID, Bins: bins,
		Measures: measures, Patterns: ids,
	})
	if err != nil {
		return nil, res.Cost, fmt.Errorf("find_anomalies: levels: %w", err)
	}
	levels := make(map[string]string)
	for i, level := range []string{levelError, levelWarn} {
		for id := range res.Measures[i].Values {
			if levels[id] == "" {
				levels[id] = level
			}
		}
	}
	return levels, res.Cost, nil
}

// rankAnomalies groups found under its first anomaly of a pattern, or of a service's new
// patterns or spikes in one bin, routine anomalies last, then by score, then pattern_id.
func rankAnomalies(found []Anomaly, levels map[string]string) []Anomaly {
	for i := range found {
		a := &found[i]
		a.Level = levels[a.PatternID]
		a.Score = max(levelWeights[a.Level], 1) * (math.Log1p(a.excess) + math.Log1p(a.deviation))
	}
	slices.SortStableFunc(found, func(a, b Anomaly) int {
		if a.Routine != b.Routine {
			if a.Routine {
				return 1
			}
			return -1
		}
		return cmp.Or(cmp.Compare(b.Score, a.Score), cmp.Compare(a.PatternID, b.PatternID))
	})
	ranked := make([]Anomaly, 0, len(found))
	heads := make(map[string]int)
	for _, a := range found {
		key := a.Service + "\x00" + a.Kind
		if a.Kind == anomalyKindSpike {
			key += a.At.String()
		}
		i, ok := heads[a.PatternID]
		if !ok {
			i, ok = heads[key]
		}
		if ok {
			ranked[i].Members = append(ranked[i].Members, a)
			continue
		}
		heads[key], heads[a.PatternID] = len(ranked), len(ranked)
		ranked = append(ranked, a)
	}
	return ranked
}

// patternAnomalies reports one pattern's strongest spike, and its first bin with lines
// when it is new. A routine pattern's few more lines are many deviations, so it spikes only past its variation.
func patternAnomalies(id, service string, counts []float64, bins series.Bins, isNew bool) []Anomaly {
	starts := bins.Boundaries()
	routine := stats.Routine(counts)
	spikes := stats.Spikes(counts)
	if routine {
		spikes = slices.DeleteFunc(spikes, func(s stats.Spike) bool { return counts[s.Index]-s.Expected < stats.RoutineVariation*s.Expected })
	}
	var out []Anomaly
	if len(spikes) > 0 {
		top := slices.MaxFunc(spikes, func(a, b stats.Spike) int { return cmp.Compare(a.Score, b.Score) })
		out = append(out, Anomaly{
			Kind: anomalyKindSpike, PatternID: id, Service: service, Routine: routine,
			At:       time.UnixMilli(starts[top.Index]).UTC(),
			Observed: counts[top.Index], Expected: top.Expected,
			deviation: top.Score, excess: counts[top.Index] - top.Expected,
		})
	}
	if first := slices.IndexFunc(counts, func(c float64) bool { return c > 0 }); isNew && first >= 0 {
		var lines float64
		for _, c := range counts {
			lines += c
		}
		out = append(out, Anomaly{
			Kind: anomalyKindNewPattern, PatternID: id, Service: service, Routine: routine,
			At: time.UnixMilli(starts[first]).UTC(), Observed: counts[first], excess: lines,
		})
	}
	return out
}

// valueAnomalies reports each counted column of each pattern in byPattern: a measure's drift, or a
// code's mix shift and new values, over the bins whose values count every one of the pattern's lines.
func valueAnomalies(counts summary.ValueCounts, lines series.Values, bins series.Bins, byPattern map[string]*patternAgg) []Anomaly {
	starts := bins.Boundaries()
	type patternColumn struct{ pattern, column string }
	// Kept sparse and made dense a column at a time into one buffer, since a dense series for
	// every value of every column outgrew the querier's memory.
	byColumn := map[patternColumn][]binValue{}
	for k, n := range counts {
		i := int((k.Bin - bins.From.UnixMilli()) / bins.Width.Milliseconds())
		if i < 0 || i >= len(starts) || byPattern[k.PatternID] == nil {
			continue
		}
		pc := patternColumn{k.PatternID, k.Column}
		byColumn[pc] = append(byColumn[pc], binValue{value: k.Value, bin: i, lines: float64(n)})
	}
	var out []Anomaly
	var dense []float64
	patternLines := map[string][]float64{} // each pattern's lines by bin, which its every column reads
	for _, pc := range slices.SortedFunc(maps.Keys(byColumn), func(a, b patternColumn) int {
		return cmp.Or(cmp.Compare(a.pattern, b.pattern), cmp.Compare(a.column, b.column))
	}) {
		if patternLines[pc.pattern] == nil {
			patternLines[pc.pattern] = densifyAt(lines[pc.pattern], starts, 0)
		}
		var byValue map[string][]float64
		byValue, dense = densifyValues(byColumn[pc], len(starts), dense)
		dropUncounted(byValue, patternLines[pc.pattern])
		agg := byPattern[pc.pattern]
		base := Anomaly{PatternID: pc.pattern, Pattern: agg.text, Service: dominantService(agg.byService), Column: pc.column}
		if measure, ok := measureValues(byValue); ok {
			out = append(out, driftAnomaly(base, measure, starts)...)
			continue
		}
		out = append(out, codeAnomalies(base, byValue, starts)...)
	}
	return out
}

// binValue is one value's lines in one bin.
type binValue struct {
	value string
	bin   int
	lines float64
}

// densifyValues is points' lines by value over n bins, held in buf grown to fit and returned
// for the next column to reuse.
func densifyValues(points []binValue, n int, buf []float64) (map[string][]float64, []float64) {
	at := map[string]int{}
	for _, p := range points {
		if _, ok := at[p.value]; !ok {
			at[p.value] = len(at) * n
		}
	}
	buf = slices.Grow(buf[:0], len(at)*n)[:len(at)*n]
	clear(buf)
	byValue := make(map[string][]float64, len(at))
	for v, i := range at {
		byValue[v] = buf[i : i+n : i+n]
	}
	for _, p := range points {
		byValue[p.value][p.bin] += p.lines
	}
	return byValue, buf
}

// dropUncounted empties each bin of byValue holding fewer than all of the pattern's lines in it,
// as a segment holding several patterns' rows of one service leaves it.
func dropUncounted(byValue map[string][]float64, patternLines []float64) {
	series := slices.Collect(maps.Values(byValue)) // walked once a bin, which a map walk is slow at
	for i, want := range patternLines {
		var counted float64
		for _, lines := range series {
			counted += lines[i]
		}
		if counted != want {
			for _, lines := range series {
				lines[i] = 0
			}
		}
	}
}

// measureValue is one value of a measure and its lines in each bin.
type measureValue struct {
	value float64
	lines []float64
}

// measureValues is byValue ascending by value when it is a measure: more than maxCodes values, all numbers.
func measureValues(byValue map[string][]float64) ([]measureValue, bool) {
	if len(byValue) <= maxCodes {
		return nil, false
	}
	values := make([]measureValue, 0, len(byValue))
	for text, lines := range byValue {
		v, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, false
		}
		values = append(values, measureValue{value: v, lines: lines})
	}
	slices.SortFunc(values, func(a, b measureValue) int { return cmp.Compare(a.value, b.value) })
	return values, true
}

// p99 is the 99th percentile of values' lines in bins [from, to), false with none.
func p99(values []measureValue, from, to int) (float64, bool) {
	var total float64
	for _, v := range values {
		total += sum(v.lines[from:to])
	}
	var seen float64
	for _, v := range values {
		if seen += sum(v.lines[from:to]); total > 0 && seen >= 0.99*total {
			return v.value, true
		}
	}
	return 0, false
}

// driftAnomaly reports where a measure's p99 by bin moved most from the bins before, when by
// driftThreshold of their spread.
func driftAnomaly(base Anomaly, values []measureValue, starts []int64) []Anomaly {
	var at []int
	var byBin []float64
	for i := range starts {
		if q, ok := p99(values, i, i+1); ok {
			at, byBin = append(at, i), append(byBin, q)
		}
	}
	best, drift := bestSplit(byBin, func(split int) float64 { return stats.Drift(byBin[:split], byBin[split:]) })
	if math.Abs(drift) < driftThreshold {
		return nil
	}
	base.Kind, base.At, base.deviation = anomalyKindDrift, time.UnixMilli(starts[at[best]]).UTC(), math.Abs(drift)
	base.Expected, _ = p99(values, 0, at[best])
	base.Observed, _ = p99(values, at[best], len(starts))
	return []Anomaly{base}
}

// codeAnomalies reports each value of a code first seen after bins with lines and seen on after,
// and the split of the bins where its mix diverged most from the bins before, past mixShiftThreshold.
func codeAnomalies(base Anomaly, byValue map[string][]float64, starts []int64) []Anomaly {
	values := slices.Sorted(maps.Keys(byValue))
	totals := make([]float64, len(starts))
	for _, lines := range byValue {
		for i, n := range lines {
			totals[i] += n
		}
	}
	var out []Anomaly
	for _, v := range values {
		first := slices.IndexFunc(byValue[v], func(n float64) bool { return n > 0 })
		if first > 0 && stats.Supported(totals[:first]) && stats.Supported(byValue[v][first:]) {
			a := base
			a.Kind, a.Value, a.At = anomalyKindNewValue, v, time.UnixMilli(starts[first]).UTC()
			a.Observed, a.excess = byValue[v][first], sum(byValue[v])
			out = append(out, a)
		}
	}
	best, divergence := bestSplit(totals, mixDivergence(byValue, values))
	if divergence < mixShiftThreshold {
		return out
	}
	return append(out, mixShift(base, byValue, values, best, divergence, starts))
}

// mixDivergence scores a split by how far values' mix after it diverged from theirs before it,
// for bestSplit's ascending splits: each value's lines before the split are kept as the splits
// pass each bin, and in all, so a split's mixes are differences rather than sums.
func mixDivergence(byValue map[string][]float64, values []string) func(split int) float64 {
	before, all := make([]float64, len(values)), make([]float64, len(values))
	for j, v := range values {
		for _, n := range byValue[v] {
			all[j] += n
		}
	}
	passed := 0
	after, prior := make([]int64, len(values)), make([]int64, len(values))
	return func(split int) float64 {
		for ; passed < split; passed++ {
			for j, v := range values {
				before[j] += byValue[v][passed]
			}
		}
		for j := range values {
			after[j], prior[j] = int64(all[j]-before[j]), int64(before[j])
		}
		return stats.AlignedDivergence(after, prior)
	}
}

// mixShift is the mix shift at split, naming the value whose lines rose most over its share before.
func mixShift(base Anomaly, byValue map[string][]float64, values []string, split int, divergence float64, starts []int64) Anomaly {
	before, after := mix(byValue, 0, split), mix(byValue, split, len(starts))
	beforeLines, afterLines := float64(totalOf(before)), float64(totalOf(after))
	base.Kind, base.At, base.deviation = anomalyKindMixShift, time.UnixMilli(starts[split]).UTC(), divergence
	for _, v := range values {
		expected := float64(before[v]) / beforeLines * afterLines
		if excess := float64(after[v]) - expected; excess > base.excess {
			base.Value, base.Observed, base.Expected, base.excess = v, float64(after[v]), expected, excess
		}
	}
	return base
}

// bestSplit is the split of series, both sides with lines in enough bins, whose score is furthest from zero,
// scored in ascending order.
func bestSplit(series []float64, score func(split int) float64) (best int, top float64) {
	first, last := stats.SupportedSplits(series)
	for split := max(first, 1); split <= min(last, len(series)-1); split++ {
		if s := score(split); math.Abs(s) > math.Abs(top) {
			best, top = split, s
		}
	}
	return best, top
}

// mix is each value's lines in bins [from, to).
func mix(byValue map[string][]float64, from, to int) map[string]int64 {
	out := make(map[string]int64, len(byValue))
	for v, lines := range byValue {
		out[v] = int64(sum(lines[from:to]))
	}
	return out
}

func sum(values []float64) float64 {
	var total float64
	for _, v := range values {
		total += v
	}
	return total
}
