package slo

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/definitions"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// snapshot is a catalog version's segments by each hour they hold rows of, and the time
// status is read at: the end of the minute of its newest row, so replicas agree.
type snapshot struct {
	v     catalog.Version
	at    time.Time
	hours map[int64]hour
}

// hour is an hour's segments and their sorted IDs, the set a cell of it counts.
type hour struct {
	segs []segment.Descriptor
	ids  []string
}

func (s *store) Refresh(ctx context.Context) error {
	v, err := s.cat.Version(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.refresh(ctx, v, true)
	return err
}

// refresh brings every cell already counted up to date with v's segments, newest hour first,
// and when backfill is set counts the hours not yet counted, within BackfillBytes.
func (s *store) refresh(ctx context.Context, v catalog.Version, backfill bool) (snapshot, error) {
	if f := s.fresh; !backfill && f != nil && f.v == v && f.gen == s.gen {
		return f.snap, nil
	}
	snap, err := s.snapshot(ctx, v)
	if err != nil {
		return snap, err
	}
	var budget int64
	if backfill {
		budget = s.cfg.BackfillBytes
	}
	for _, h := range slices.Backward(slices.Sorted(maps.Keys(snap.hours))) {
		spent, err := s.count(ctx, snap, h, budget)
		if err != nil {
			return snap, err
		}
		budget -= spent
	}
	s.prune(snap.at)
	// A catalog holding no segments puts the snapshot at the clock's minute, which moves.
	if len(snap.hours) > 0 {
		s.fresh = &fresh{v: v, gen: s.gen, snap: snap}
	}
	return snap, nil
}

func (s *store) snapshot(ctx context.Context, v catalog.Version) (snapshot, error) {
	descs, err := s.cat.Prune(ctx, v, selector.Resolved{})
	if err != nil {
		return snapshot{}, fmt.Errorf("slo: prune: %w", err)
	}
	snap := snapshot{v: v, at: time.Now().UTC().Truncate(time.Minute), hours: map[int64]hour{}}
	if len(descs) > 0 {
		newest := slices.MaxFunc(descs, func(a, b segment.Descriptor) int { return a.TimeRange.Max.Compare(b.TimeRange.Max) })
		snap.at = newest.TimeRange.Max.UTC().Truncate(time.Minute).Add(time.Minute)
	}
	for _, d := range descs {
		for h := hourOf(d.TimeRange.Min); h <= d.TimeRange.Max.UnixMilli(); h += hourMs {
			hr := snap.hours[h]
			hr.segs, hr.ids = append(hr.segs, d), append(hr.ids, d.ID)
			snap.hours[h] = hr
		}
	}
	for _, hr := range snap.hours {
		slices.Sort(hr.ids)
	}
	return snap, nil
}

// count brings each SLO's cell of hour h up to date: an event ratio's by adding the segments
// the hour gained, any other by reading them all; a cell not yet counted only while budget
// lasts, returning the bytes those reads cost: the hour once for the event ratios, which share
// a read, and once for each time-slice SLO, which reads alone.
func (s *store) count(ctx context.Context, snap snapshot, h int64, budget int64) (int64, error) {
	hr := snap.hours[h]
	size := bytesOf(hr.segs)
	var whole []*tracked
	// Cells that counted the same segments gain the same ones.
	gaining := map[string][]*tracked{}
	var spent int64
	ratioRead := false
	for _, t := range s.slos {
		c, counted := t.cells[h]
		switch {
		case !t.behind(snap.at, h, hr):
		case t.gains(h, hr) != nil:
			prior := strings.Join(c.ids, "\n")
			gaining[prior] = append(gaining[prior], t)
		case counted:
			whole = append(whole, t)
		case spent < budget:
			whole = append(whole, t)
			if t.def.Method != definitions.Occurrences || !ratioRead {
				spent += size
			}
			ratioRead = ratioRead || t.def.Method == definitions.Occurrences
		}
	}
	for _, ts := range gaining {
		if err := s.read(ctx, snap.v, h, hour{segs: ts[0].gains(h, hr), ids: hr.ids}, ts, true); err != nil {
			return 0, err
		}
	}
	return spent, s.read(ctx, snap.v, h, hr, whole, false)
}

// behind reports whether t's window ending at at holds hour h, whose segments are hr, and
// its cell of h has not counted them.
func (t *tracked) behind(at time.Time, h int64, hr hour) bool {
	c, counted := t.cells[h]
	return !t.def.Unbound && h+hourMs > t.def.Window.From(at).UnixMilli() && (!counted || !slices.Equal(c.ids, hr.ids))
}

// gains is hr's segments that t's cell of hour h can add to its counts: an event ratio's,
// which lacks them and counted every other; nil when only a whole read counts the hour.
func (t *tracked) gains(h int64, hr hour) []segment.Descriptor {
	ids := t.cells[h].ids
	if t.def.Method != definitions.Occurrences || ids == nil ||
		slices.ContainsFunc(ids, func(id string) bool { _, ok := slices.BinarySearch(hr.ids, id); return !ok }) {
		return nil
	}
	return slices.DeleteFunc(slices.Clone(hr.segs), func(d segment.Descriptor) bool { _, ok := slices.BinarySearch(ids, d.ID); return ok })
}

// read counts hr's segments of hour h for ts, the event ratios in one read and each
// time-slice SLO in its own, adding to each cell when increment is set and replacing it
// otherwise; each cell then holds hr's IDs.
func (s *store) read(ctx context.Context, v catalog.Version, h int64, hr hour, ts []*tracked, increment bool) error {
	req := request(v, h, hr.segs, time.Minute)
	var ratios []*tracked
	for _, t := range ts {
		if t.def.Method != definitions.Occurrences {
			groups, err := s.slices(ctx, request(v, h, hr.segs, t.def.SliceWindow), t.def)
			if err != nil {
				return err
			}
			t.cells[h] = cell{Cell: Cell{Hour: h, Groups: groups}, ids: hr.ids}
			s.gen++
			continue
		}
		ratios = append(ratios, t)
		req.Measures = append(req.Measures, measures(t.def, t.def.GroupBy)...)
	}
	if len(ratios) == 0 {
		return nil
	}
	res, err := s.rd.Read(ctx, req)
	if err != nil {
		return fmt.Errorf("slo: count %s: %w", time.UnixMilli(h).UTC().Format(time.RFC3339), err)
	}
	for _, t := range ratios {
		n := len(measures(t.def, t.def.GroupBy))
		groups := events(t.def, h, res.Measures[:n])
		res.Measures = res.Measures[n:]
		for g, c := range t.cells[h].Groups {
			if increment {
				groups[g] = add(groups[g], c)
			}
		}
		t.cells[h] = cell{Cell: Cell{Hour: h, Groups: groups}, ids: hr.ids}
	}
	s.gen++
	return nil
}

func request(v catalog.Version, h int64, segs []segment.Descriptor, width time.Duration) series.Request {
	from := time.UnixMilli(h).UTC()
	w := selector.Window{From: from, To: from.Add(time.Hour)}
	return series.Request{Version: v, Selector: selector.Resolved{Window: w}, Window: w,
		Bins: series.Bins{Width: width, From: w.From, To: w.To}, Segments: segs}
}

// ungrouped keys every row of an SLO without groups under one series.
const ungrouped = series.Total

// measures are a ratio's valid and good, or bad, counts keyed by group, or for an
// ungrouped SLO whose good or bad query adds one equality, its total keyed by that column,
// which a footer counting the column answers.
func measures(def definitions.SLO, group string) []series.FilteredMeasure {
	count := series.Measure{Kind: series.KindCount}
	if eq, ok := keyed(def, group); ok {
		return []series.FilteredMeasure{{Measure: count, Where: def.Total.Where, GroupBy: eq.Field, DistinctBy: def.DistinctBy}}
	}
	return []series.FilteredMeasure{{Measure: count, Where: def.Total.Where, GroupBy: group, DistinctBy: def.DistinctBy},
		{Measure: count, Where: cmp.Or(def.Good, def.Bad).Where, GroupBy: group, DistinctBy: def.DistinctBy}}
}

// keyed is the one equality an ungrouped ratio's good or bad query adds to its total.
func keyed(def definitions.SLO, group string) (selector.Predicate, bool) {
	more := added(def)
	if group != "" || len(more) != 1 || more[0].Op != selector.OpEq || more[0].Field == "" || more[0].Quantifier != "" {
		return selector.Predicate{}, false
	}
	return more[0], true
}

// events are an event ratio's counts of hour h per group from what its measures read: by
// pattern, or by its narrower's column, for an ungrouped SLO, which sums them.
func events(def definitions.SLO, h int64, read []series.Result) map[string]Counts {
	groups := map[string]Counts{}
	under := func(key string) Counts {
		if def.GroupBy == "" {
			key = ""
		}
		return group(groups, key)
	}
	eq, isKeyed := keyed(def, def.GroupBy)
	for key, bins := range read[0].Values {
		addBins(under(key).Valid, h, bins)
		if isKeyed && key == eq.Value {
			addBins(under(key).Good, h, bins)
		}
	}
	if !isKeyed {
		for key, bins := range read[1].Values {
			addBins(under(key).Good, h, bins)
		}
	}
	for _, c := range groups {
		for i := range c.Good {
			if def.Bad != nil {
				c.Good[i] = c.Valid[i] - c.Good[i]
			}
		}
	}
	return groups
}

// group is key's counts of an hour, made on first use.
func group(groups map[string]Counts, key string) Counts {
	c, ok := groups[key]
	if !ok {
		c = Counts{Good: make([]int64, 60), Valid: make([]int64, 60)}
		groups[key] = c
	}
	return c
}

// addBins adds hour h's per-bin values into its counts.
func addBins(into []int64, h int64, bins map[int64]float64) {
	for b, n := range bins {
		into[(b-h)/minute] += int64(n)
	}
}

// slices reads a time-slice SLO's slices for req.
func (s *store) slices(ctx context.Context, req series.Request, def definitions.SLO) (map[string]Counts, error) {
	group := cmp.Or(def.GroupBy, ungrouped)
	if m := def.Metric; m != nil {
		measure, err := series.ParseMeasure(m.Stage.Measure)
		if err != nil {
			return nil, err
		}
		req.Measures = []series.FilteredMeasure{{Measure: measure, Where: m.Where, GroupBy: group, DistinctBy: def.DistinctBy}}
	} else {
		req.Measures = measures(def, group)
	}
	// A count's slices also count every row, of any service, to tell a silent service from
	// a slice nothing was collected in.
	if counts(def) {
		req.Measures = append(req.Measures, series.FilteredMeasure{Measure: series.Measure{Kind: series.KindCount}, GroupBy: ungrouped})
	}
	res, err := s.rd.Read(ctx, req)
	// An hour none of whose segments carry the field a quantile or sum reads holds no values
	// of it, which leaves its slices without data rather than failing the refresh.
	var unknown *query.APIError
	if errors.As(err, &unknown) && unknown.Code == query.ErrCodeUnknownField {
		res, err = series.Result{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("slo: count %s: %w", def.Name, err)
	}
	var collected map[int64]float64
	if counts(def) && len(res.Measures) > 1 {
		collected = res.Measures[len(res.Measures)-1].Values[""]
	}
	return sliceCounts(def, req.Bins.From.UnixMilli(), collected, res.Measures), nil
}

// counts reports whether def's slices count events, so that one holds a value, zero,
// whether or not they were logged.
func counts(def definitions.SLO) bool {
	return def.Metric != nil && slices.Contains([]string{"count", "rate", "sum"}, strings.Split(def.Metric.Stage.Measure, ":")[0])
}

// sliceCounts counts a time-slice SLO's slices of hour h, each good when its value in
// results meets the objective; a count's slice holds one whether or not the SLI's events
// were logged, so long as collected, every row's count by slice, shows anything was logged
// then, since a slice nothing was collected in says nothing of the service.
func sliceCounts(def definitions.SLO, h int64, collected map[int64]float64, results []series.Result) map[string]Counts {
	results = append(results, series.Result{}, series.Result{})
	counted := counts(def)
	keys := slices.Collect(maps.Keys(results[0].Values))
	if counted && def.GroupBy == "" && len(keys) == 0 {
		keys = []string{""}
	}
	groups := map[string]Counts{}
	for _, key := range keys {
		c := group(groups, key)
		for start := h; start < h+hourMs; start += def.SliceWindow.Milliseconds() {
			if value, ok := slice(def, results, key, start); ok || counted && collected[start] > 0 {
				c.Valid[(start-h)/minute]++
				if meets(def.Op, value, def.Value) {
					c.Good[(start-h)/minute]++
				}
			}
		}
	}
	return groups
}

// slice is a slice's value: its metric query's, or its ratio of good events, if it has one.
func slice(def definitions.SLO, results []series.Result, key string, start int64) (float64, bool) {
	value, ok := results[0].Values[key][start]
	if def.Metric != nil {
		return value, ok
	}
	ratio := results[1].Values[key][start] / value
	if def.Bad != nil {
		ratio = 1 - ratio
	}
	return ratio, value > 0
}

// prune drops the cells no window ending at at reaches, and collapses those past the fine
// horizon to one bin a group.
func (s *store) prune(at time.Time) {
	fine := hourOf(at.Add(-s.cfg.FineHorizon))
	for _, t := range s.slos {
		from := hourOf(t.def.Window.From(at))
		for h, c := range t.cells {
			switch {
			case h < from:
				delete(t.cells, h)
				s.gen++
			case h < fine && !c.coarse:
				for g, counts := range c.Groups {
					c.Groups[g] = collapse(counts)
				}
				c.coarse = true
				t.cells[h] = c
				s.gen++
			}
		}
	}
}

// cells are t's cells over the hours from first to snap's time: an hour holding no segments
// counted as empty, since nothing was collected in it, and one whose cell is not up to date
// left out.
func (s *store) cells(t *tracked, snap snapshot, first int64) []Cell {
	var out []Cell
	for h := first - first%hourMs; h < snap.at.UnixMilli(); h += hourMs {
		hr, held := snap.hours[h]
		c, counted := t.cells[h]
		switch {
		case held && counted && slices.Equal(c.ids, hr.ids):
			out = append(out, c.Cell)
		case !held:
			out = append(out, Cell{Hour: h})
		}
	}
	return out
}

func hourOf(t time.Time) int64 { return t.UnixMilli() - t.UnixMilli()%hourMs }

func bytesOf(descs []segment.Descriptor) int64 {
	var n int64
	for _, d := range descs {
		n += d.EstimatedSize
	}
	return n
}
