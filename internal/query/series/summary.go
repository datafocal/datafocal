package series

import (
	"time"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/summary"
)

// fromSummary answers req with no footer or column GET, or reports a miss whenever the
// summary holds no evidence for what was asked, so it only ever makes an answer
// cheaper.
func (r reader) fromSummary(req Request) (Result, bool) {
	if !req.Summary || req.OrdinalOf != "" || !summaryKeys(req.Selector.Where) {
		return Result{}, false
	}
	store := r.planner.Summary(req.Version)
	if store == nil || !store.CanServe(req.Bins.Width, req.Bins.From) {
		return Result{}, false
	}
	service := summaryService(req.Selector)
	switch req.Measure.Kind {
	case KindCount, KindRate:
		return summaryRows(store, service, req)
	case KindQuantile, KindDistinct:
		// The sketch measures are approximate whatever path answers them, so a
		// sketch point merged across the bins it touches needs no separate flag.
		values, ok := summarySketches(store, service, req)
		return Result{Values: values, Coverage: 1, Approx: true}, ok
	}
	return Result{}, false
}

// summaryRows answers a count or a rate, each exact since the summary counts every row
// in its bin.
func summaryRows(store summary.Store, service string, req Request) (Result, bool) {
	values, ok := summaryCounts(store, service, req.Bins, req.GroupBy)
	if !ok {
		return Result{}, false
	}
	if req.Measure.Kind == KindRate {
		divideByWidth(values, req.Bins.Width)
	}
	return Result{Values: values, Coverage: 1}, true
}

// summaryKeys reports whether the summary can apply every predicate in where, which it
// can only for the one service it slices by (Store.CountSlice).
func summaryKeys(where []selector.Predicate) bool {
	for i, p := range where {
		if i > 0 || p.Field != segment.ColService || p.Op != selector.OpEq {
			return false
		}
	}
	return true
}

// summaryService is the service a selector the summary answers slices it by; "" reads
// all services.
func summaryService(sel selector.Resolved) string {
	if len(sel.Where) == 0 {
		return ""
	}
	return sel.Where[0].Value
}

// summaryCounts re-bins each count point by its own start, exact because Store.CanServe
// established each summary bin falls inside one requested bin, and what keeps the bin
// starts identical to the scan's.
func summaryCounts(store summary.Store, service string, bins Bins, groupBy string) (Values, bool) {
	if groupBy != segment.ColService && groupBy != segment.ColPatternID {
		return nil, false
	}
	points := store.CountSlice(service, bins.From.UnixMilli(), bins.To.UnixMilli())
	if len(points) == 0 {
		return nil, false
	}
	out := Values{}
	for _, c := range points {
		key := c.Key.Service
		if groupBy == segment.ColPatternID {
			key = c.Key.PatternID
		}
		addTo(out, key, bins.BinStart(time.UnixMilli(c.Key.Bin)), float64(c.Lines))
	}
	return out, true
}

// summarySketches merges the summary's sketches for a named column grouped by service,
// the only key it merges them on, leaving a column it does not hold to come back as a
// miss.
func summarySketches(store summary.Store, service string, req Request) (Values, bool) {
	field := req.Measure.Field
	if req.GroupBy != segment.ColService || field == "" || req.Measure.Length != "" {
		return nil, false
	}
	// A copy failure is a miss, not an error: the scan path answers the same
	// question from the same data, so degrading to it costs GETs and nothing else.
	points, err := store.SketchSlice(service, req.Bins.From.UnixMilli(), req.Bins.To.UnixMilli())
	if err != nil || len(points) == 0 {
		return nil, false
	}
	if req.Measure.Kind == KindDistinct {
		return mergeSummarySketches(points, req.Bins, sketch.NewHLL,
			func(c summary.SketchPoint) *sketch.HLL { return c.Distinct[field] },
			(*sketch.HLL).Estimate)
	}
	return mergeSummarySketches(points, req.Bins,
		func() *sketch.DDSketch { return sketch.NewDDSketch(sketch.QuantileAccuracy) },
		func(c summary.SketchPoint) *sketch.DDSketch { return c.Quantiles[field] },
		func(s *sketch.DDSketch) float64 { return s.Quantile(req.Measure.Quantile) })
}

// mergeSummarySketches re-bins the points into the requested bins, merging each
// service's sketches per bin, then reduces each merged sketch to a value. pick returns
// nil when a point holds no sketch for the column; holding it for no point at all is
// the miss that sends the caller to the segments.
func mergeSummarySketches[T mergeable[T]](points []summary.SketchPoint, bins Bins, newEmpty func() T,
	pick func(summary.SketchPoint) T, reduce func(T) float64) (Values, bool) {
	var absent T
	merged := map[string]map[int64]T{}
	for _, c := range points {
		src := pick(c)
		if src == absent {
			continue
		}
		into := binSketch(merged, c.Key.Service, bins.BinStart(time.UnixMilli(c.Key.Bin)), newEmpty)
		if err := into.Merge(src); err != nil {
			return nil, false
		}
	}
	values := reduceBins(merged, reduce)
	return values, len(values) > 0
}

// divideByWidth converts count-per-bin to rate-per-second in place.
func divideByWidth(values Values, width time.Duration) {
	secs := width.Seconds()
	if secs <= 0 {
		return
	}
	for _, byBin := range values {
		for b := range byBin {
			byBin[b] /= secs
		}
	}
}
