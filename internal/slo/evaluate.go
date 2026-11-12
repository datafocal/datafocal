package slo

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/definitions"
)

// Cell is an hour's good and valid counts for each group, keyed "" for an ungrouped SLO.
type Cell struct {
	// Hour is the hour's start in unix milliseconds.
	Hour   int64
	Groups map[string]Counts
}

// Counts are a group's good and valid counts per 1-minute bin, or in one bin for a whole
// hour past the fine horizon.
type Counts struct{ Good, Valid []int64 }

const (
	statePending = "pending"
	minute       = int64(time.Minute / time.Millisecond)
	hourMs       = int64(time.Hour / time.Millisecond)
)

// Evaluate is def's status at at over cells, each SLO's state decided here and nowhere else.
func Evaluate(def definitions.SLO, cells []Cell, at time.Time) Status {
	return newLines(cells).evaluate(def, at)
}

// lines are cells in time order laid out as timelines: each group's, and every group's summed.
type lines struct {
	cells  []Cell
	total  *timeline
	groups map[string]*timeline
}

func newLines(cells []Cell) lines {
	cells = slices.SortedFunc(slices.Values(cells), func(a, b Cell) int { return cmp.Compare(a.Hour, b.Hour) })
	total, groups := timelines(cells)
	return lines{cells: cells, total: total, groups: groups}
}

func (l lines) evaluate(def definitions.SLO, at time.Time) Status {
	st := Status{Name: def.Name, Service: def.Service, Objective: def.Target}
	if def.Unbound {
		st.State, st.Query = StateUnbound, def.Source
		return st
	}
	from, to := def.Window.From(at).UnixMilli(), at.UnixMilli()
	st.Coverage, st.WindowStart = coverage(l.cells, from, to), time.UnixMilli(l.total.start(from)).UTC()
	l.total.status(def, &st, from, to)
	for _, g := range slices.Sorted(maps.Keys(l.groups)) {
		if g != "" {
			gs := Status{Name: def.Name, Service: def.Service, Group: g, Objective: def.Target, Coverage: st.Coverage, WindowStart: st.WindowStart}
			l.groups[g].status(def, &gs, from, to)
			st.Groups = append(st.Groups, gs)
		}
	}
	return st
}

// untrackedGroup is the group the groups past the caps fold into.
const untrackedGroup = "untracked"

// fold keeps the keep groups with the most valid events, ties by name, and merges the rest
// into "untracked", returning how many groups there were and how many it folded.
func (l lines) fold(keep int) (groups, untracked int) {
	n := len(l.groups)
	if n <= keep {
		return n, 0
	}
	valid := func(g string) int64 { return l.groups[g].valid[len(l.groups[g].valid)-1] }
	ranked := slices.SortedFunc(maps.Keys(l.groups), func(a, b string) int { return cmp.Or(cmp.Compare(valid(b), valid(a)), strings.Compare(a, b)) })
	merged := &timeline{grid: l.total.grid, good: make([]int64, len(l.total.good)), valid: make([]int64, len(l.total.valid))}
	for _, g := range ranked[keep:] {
		for k := range merged.good {
			merged.good[k] += l.groups[g].good[k]
			merged.valid[k] += l.groups[g].valid[k]
		}
		delete(l.groups, g)
	}
	l.groups[untrackedGroup] = merged
	return n, n - keep
}

// coverage is the fraction of the hours in [from, to) with a cell.
func coverage(cells []Cell, from, to int64) float64 {
	first, n := from-from%hourMs, 0
	for _, c := range cells {
		if c.Hour >= first && c.Hour < to {
			n++
		}
	}
	return min(1, float64(n)/float64((to-first+hourMs-1)/hourMs))
}

// timeline is a group's counts on the cells' bins, with prefix sums so any span sums in
// O(log n).
type timeline struct {
	*grid
	good, valid []int64
}

// grid is the cells' bins in time order, and fine the start of the first 1-minute one,
// before which since is not looked for.
type grid struct {
	starts, ends []int64
	fine         int64
}

// newGrid lays out the cells' bins, each cell's first at its offset.
func newGrid(cells []Cell) (*grid, []int) {
	g := &grid{fine: math.MaxInt64}
	offsets := make([]int, len(cells))
	for i, c := range cells {
		n := int64(1)
		for _, counts := range c.Groups {
			n = int64(len(counts.Valid))
			break
		}
		if n > 1 && g.fine == math.MaxInt64 {
			g.fine = c.Hour
		}
		offsets[i] = len(g.starts)
		for b := range n {
			g.starts, g.ends = append(g.starts, c.Hour+b*hourMs/n), append(g.ends, c.Hour+(b+1)*hourMs/n)
		}
	}
	return g, offsets
}

// timelines lays out each group's counts over cells, in one pass, and every group's summed.
func timelines(cells []Cell) (*timeline, map[string]*timeline) {
	g, offsets := newGrid(cells)
	line := func() *timeline {
		return &timeline{grid: g, good: make([]int64, len(g.starts)+1), valid: make([]int64, len(g.starts)+1)}
	}
	total, groups := line(), map[string]*timeline{}
	for i, c := range cells {
		for name, counts := range c.Groups {
			t := groups[name]
			if t == nil {
				t = line()
				groups[name] = t
			}
			for b := range counts.Valid {
				at := offsets[i] + b + 1
				t.good[at], t.valid[at] = counts.Good[b], counts.Valid[b]
				total.good[at], total.valid[at] = total.good[at]+counts.Good[b], total.valid[at]+counts.Valid[b]
			}
		}
	}
	for _, t := range append(slices.Collect(maps.Values(groups)), total) {
		for k := 1; k < len(t.good); k++ {
			t.good[k], t.valid[k] = t.good[k]+t.good[k-1], t.valid[k]+t.valid[k-1]
		}
	}
	return total, groups
}

// sum is the good and valid counts of the bins overlapping [from, to).
func (t timeline) sum(from, to int64) (good, valid int64) {
	i := sort.Search(len(t.ends), func(k int) bool { return t.ends[k] > from })
	j := sort.Search(len(t.starts), func(k int) bool { return t.starts[k] >= to })
	if j <= i {
		return 0, 0
	}
	return t.good[j] - t.good[i], t.valid[j] - t.valid[i]
}

// start is where the first bin overlapping from begins.
func (t timeline) start(from int64) int64 {
	if i := sort.Search(len(t.ends), func(k int) bool { return t.ends[k] > from }); i < len(t.starts) {
		return min(from, t.starts[i])
	}
	return from
}

// burn is the bad fraction over [to-lookback, to) divided by the error budget, nil without data.
func (t timeline) burn(target float64, lookback time.Duration, to int64) *float64 {
	good, valid := t.sum(to-lookback.Milliseconds(), to)
	if valid == 0 {
		return nil
	}
	return ptr(float64(valid-good) / float64(valid) / (1 - target))
}

func (t timeline) holds(def definitions.SLO, c definitions.Condition, at int64) bool {
	long, short := t.burn(def.Target, c.Lookback, at), t.burn(def.Target, c.Short, at)
	return long != nil && short != nil && meets(c.Op, *long, c.Threshold) && meets(c.Op, *short, c.Threshold)
}

// condition is c's state at at, stepping back a bin at a time through the fine tier to
// find when it began holding.
func (t timeline) condition(def definitions.SLO, p definitions.Policy, c definitions.Condition, at int64) ConditionStatus {
	cs := ConditionStatus{Policy: p.Name, Name: c.Name, Severity: c.Severity, State: StateOK, Threshold: c.Threshold,
		Lookback: spanText(c.Lookback), ShortWindow: spanText(c.Short),
		BurnRate: t.burn(def.Target, c.Lookback, at), ShortBurnRate: t.burn(def.Target, c.Short, at)}
	if !t.holds(def, c, at) {
		return cs
	}
	began := at
	for began-minute >= t.fine && t.holds(def, c, began-minute) {
		began -= minute
	}
	cs.State, cs.Since = statePending, ptr(time.UnixMilli(began).UTC())
	if fired := began + c.AlertAfter.Milliseconds(); fired <= at {
		cs.State, cs.Since = StateFiring, ptr(time.UnixMilli(fired).UTC())
	}
	return cs
}

// forecast carries the burn over twice def's horizon before to a horizon on: a burn of 1
// spends the budget over the window, so a horizon spends burn × horizon / window of it.
func (t timeline) forecast(def definitions.SLO, budget *float64, to int64) *Forecast {
	h := cmp.Or(def.Forecast, time.Hour)
	f := &Forecast{Horizon: spanText(h), Lookback: spanText(2 * h), BurnRate: t.burn(def.Target, 2*h, to)}
	if f.BurnRate == nil || budget == nil {
		return f
	}
	at := time.UnixMilli(to)
	w := float64(def.Window.Length(at))
	f.BudgetRemaining = ptr(*budget - *f.BurnRate*float64(h)/w)
	if *f.BurnRate > 0 && *budget > 0 {
		f.ExhaustedAt = ptr(at.Add(time.Duration(*budget / *f.BurnRate * w)).UTC())
	}
	return f
}

// spanText is a duration as a definition writes one, as 1h, 90m or 5m; empty for none.
func spanText(d time.Duration) string {
	switch {
	case d <= 0:
		return ""
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	return d.String()
}

// point is the SLI and budget remaining over def's window to at, each condition's burn rate,
// and the step's own counts, of the step ending at at.
func (t timeline) point(def definitions.SLO, at time.Time, step time.Duration) Point {
	p := Point{At: at}
	p.Good, p.Valid = t.sum(at.Add(-step).UnixMilli(), at.UnixMilli())
	p.SLI, p.BudgetRemaining = t.budget(def.Target, def.Window.From(at).UnixMilli(), at.UnixMilli())
	for _, policy := range def.Policies {
		for _, c := range policy.Conditions {
			p.BurnRates = append(p.BurnRates, t.burn(def.Target, c.Lookback, at.UnixMilli()))
		}
	}
	return p
}

// trendPoints is how many budget values a listed SLO's trend holds across its window.
const trendPoints = 48

// trend is the budget remaining at trendPoints even steps across def's window ending at at,
// each over the window ending there, nil where it held no valid events.
func (t timeline) trend(def definitions.SLO, at time.Time) []*float64 {
	if def.Unbound {
		return nil
	}
	from := def.Window.From(at)
	step := at.Sub(from) / trendPoints
	out := make([]*float64, trendPoints)
	for k := range out {
		end := from.Add(step * time.Duration(k+1))
		_, out[k] = t.budget(def.Target, def.Window.From(end).UnixMilli(), end.UnixMilli())
	}
	return out
}

// budget is the SLI and budget remaining over [from, to), nil without valid events.
func (t timeline) budget(target float64, from, to int64) (sli, remaining *float64) {
	good, valid := t.sum(from, to)
	if valid == 0 {
		return nil, nil
	}
	return ptr(float64(good) / float64(valid)), ptr(1 - float64(valid-good)/((1-target)*float64(valid)))
}

// status fills st from t over the window [from, to).
func (t timeline) status(def definitions.SLO, st *Status, from, to int64) {
	st.SLI, st.BudgetRemaining = t.budget(def.Target, from, to)
	st.BurnRate1h, st.BurnRate6h = t.burn(def.Target, time.Hour, to), t.burn(def.Target, 6*time.Hour, to)
	st.Forecast = t.forecast(def, st.BudgetRemaining, to)
	st.State = StateOK
	longest := to - from
	if len(def.Policies) > 0 {
		longest = 0
	}
	for _, p := range def.Policies {
		for _, c := range p.Conditions {
			longest = max(longest, c.Lookback.Milliseconds())
			st.fold(t.condition(def, p, c, to))
		}
	}
	if _, v := t.sum(to-longest, to); v == 0 {
		st.State, st.Pending, st.Since = StateNoData, false, nil
		if slices.ContainsFunc(def.Policies, func(p definitions.Policy) bool { return p.AlertWhenNoData }) {
			st.State = StateFiring
		}
	}
	if st.Coverage < 1 && (st.State == StateOK || st.State == StateNoData) {
		st.State = StatePartial
	}
}

// fold adds a condition's state to st's: firing over pending over ok, since the earliest.
func (st *Status) fold(cs ConditionStatus) {
	st.Conditions = append(st.Conditions, cs)
	switch {
	case cs.State == StateFiring && (st.State != StateFiring || cs.Since.Before(*st.Since)):
		st.State, st.Since, st.Pending = StateFiring, cs.Since, false
	case cs.State == statePending && st.State != StateFiring && (!st.Pending || cs.Since.Before(*st.Since)):
		st.Pending, st.Since = true, cs.Since
	}
}

// meets reports whether v op threshold holds, for an OpenSLO op.
func meets(op string, v, threshold float64) bool {
	switch op {
	case "lt":
		return v < threshold
	case "lte":
		return v <= threshold
	case "gt":
		return v > threshold
	}
	return v >= threshold
}

// add is a and b's counts summed, both in one bin when either is.
func add(a, b Counts) Counts {
	if len(a.Valid) == 0 {
		return Counts{Good: slices.Clone(b.Good), Valid: slices.Clone(b.Valid)}
	}
	if len(a.Valid) != len(b.Valid) {
		a, b = collapse(a), collapse(b)
	}
	for i := range b.Valid {
		a.Good[i] += b.Good[i]
		a.Valid[i] += b.Valid[i]
	}
	return a
}

// collapse is c's counts in one bin, as an hour past the fine horizon keeps them.
func collapse(c Counts) Counts {
	return Counts{Good: []int64{sum(c.Good)}, Valid: []int64{sum(c.Valid)}}
}

func sum(bins []int64) int64 {
	var n int64
	for _, b := range bins {
		n += b
	}
	return n
}

func ptr[T any](v T) *T { return &v }
