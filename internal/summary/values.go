package summary

import (
	"cmp"
	"maps"
	"slices"

	"github.com/datafocal/datafocal/internal/segment"
)

// ValuePoint is one bin's rows of a pattern holding one value of a counted column.
type ValuePoint struct {
	PatternID string
	Value     string
	Bin       int64
	Count     uint64
}

// ValueKey names one bin of a value series: a pattern's rows holding Value in Column.
type ValueKey struct {
	PatternID, Column, Value string
	Bin                      int64
}

// ValueCounts is the rows of each value series bin.
type ValueCounts map[ValueKey]uint64

// valueSeries is a ValueKey without its bin.
type valueSeries struct{ PatternID, Column, Value string } //nolint:shadowstruct // a series key, not an anomaly

// uncountedColumns name a row's service, pattern and text rather than a value of it.
var uncountedColumns = []string{segment.ColService, segment.ColPatternID, segment.ColMessage}

// CountedColumns is every column footer counts the values of, sorted.
func CountedColumns(footer segment.Footer) []string {
	var columns []string
	for _, column := range slices.Sorted(maps.Keys(footer.ZoneMaps)) {
		if _, ok := footer.ValueCounts(column); ok && !slices.Contains(uncountedColumns, column) {
			columns = append(columns, column)
		}
	}
	return columns
}

// FooterValues is a one-bin segment's value series at bin, a service's value counts kept only
// when all its rows are one pattern's, since a footer counts values per service; a row without
// the column holds "".
func FooterValues(footer segment.Footer, bin int64) ValueCounts {
	counts := ValueCounts{}
	columns := CountedColumns(footer)
	for sp, rows := range footer.ServicePatternCounts {
		view := footer.ForService(sp.Service)
		if rows != view.Rows {
			continue
		}
		for _, column := range columns {
			byValue, _ := view.ValueCounts(column)
			missing := uint64(rows)
			for v, n := range byValue {
				counts[ValueKey{PatternID: sp.Pattern, Column: column, Value: v, Bin: bin}] += n
				missing -= min(n, missing)
			}
			if missing > 0 {
				counts[ValueKey{PatternID: sp.Pattern, Column: column, Bin: bin}] += missing
			}
		}
	}
	return counts
}

// valueTable holds the value series, at most limit of them, of the columns with at most
// maxValues distinct values.
type valueTable struct {
	limit, maxValues int
	// series holds each series' bins under one key, since a key per bin repeated the series'
	// three strings for each of its points.
	series map[valueSeries]*valueBins
	// distinct is each held column's values, and wide the columns that passed maxValues.
	distinct map[string]map[string]struct{}
	wide     map[string]bool
	// evicted is how many series the limit has evicted, evictedThrough each column's newest
	// bin one of them reached, and evictedTo the retention start the last eviction ran at.
	evicted        int
	evictedThrough map[string]int64
	evictedTo      int64
}

// valueBins is one series' rows by bin, and its newest bin.
type valueBins struct {
	last int64
	rows map[int64]uint64
}

func newValueTable(limit, maxValues int) *valueTable {
	return &valueTable{limit: limit, maxValues: maxValues, series: map[valueSeries]*valueBins{},
		distinct: map[string]map[string]struct{}{}, wide: map[string]bool{}, evictedThrough: map[string]int64{}}
}

// add merges counts at or after retentionStart, then evicts past it and past the limit.
func (t *valueTable) add(counts ValueCounts, retentionStart int64) {
	for k, n := range counts {
		if k.Bin < retentionStart || t.wide[k.Column] || t.widen(k) {
			continue
		}
		s := valueSeries{PatternID: k.PatternID, Column: k.Column, Value: k.Value}
		bins := t.series[s]
		if bins == nil {
			bins = &valueBins{last: k.Bin, rows: map[int64]uint64{}}
			t.series[s] = bins
		}
		bins.rows[k.Bin] += n
		bins.last = max(bins.last, k.Bin)
	}
	if retentionStart > t.evictedTo {
		maps.DeleteFunc(t.series, func(_ valueSeries, bins *valueBins) bool {
			maps.DeleteFunc(bins.rows, func(bin int64, _ uint64) bool { return bin < retentionStart })
			return bins.last < retentionStart
		})
		t.evictedTo = retentionStart
	}
	t.evictOldest()
}

// widen counts k's value among its column's, and past maxValues drops the column's series
// for good, reporting whether it did.
func (t *valueTable) widen(k ValueKey) bool {
	values := t.distinct[k.Column]
	if values == nil {
		values = map[string]struct{}{}
		t.distinct[k.Column] = values
	}
	if values[k.Value] = struct{}{}; len(values) <= t.maxValues {
		return false
	}
	t.wide[k.Column] = true
	delete(t.distinct, k.Column)
	maps.DeleteFunc(t.series, func(s valueSeries, _ *valueBins) bool { return s.Column == k.Column })
	return true
}

// evictOldest keeps the limit series whose newest bins are newest, in a new map so one add's
// burst of series does not leave the table sized for it.
func (t *valueTable) evictOldest() {
	over := len(t.series) - t.limit
	if over <= 0 {
		return
	}
	newest := slices.SortedFunc(maps.Keys(t.series), func(a, b valueSeries) int {
		return cmp.Or(cmp.Compare(t.series[b].last, t.series[a].last), cmp.Compare(b.PatternID, a.PatternID),
			cmp.Compare(b.Column, a.Column), cmp.Compare(b.Value, a.Value))
	})
	kept := make(map[valueSeries]*valueBins, t.limit)
	for _, s := range newest[:t.limit] {
		kept[s] = t.series[s]
	}
	for _, s := range newest[t.limit:] {
		if through, ok := t.evictedThrough[s.Column]; !ok || t.series[s].last > through {
			t.evictedThrough[s.Column] = t.series[s].last
		}
	}
	t.series = kept
	t.evicted += over
}

// slice is column's points in bins [from, to), sorted by pattern, value and bin.
func (t *valueTable) slice(column string, from, to int64) []ValuePoint {
	out := make([]ValuePoint, 0)
	for s, bins := range t.series {
		if s.Column != column {
			continue
		}
		for bin, n := range bins.rows {
			if bin >= from && bin < to {
				out = append(out, ValuePoint{PatternID: s.PatternID, Value: s.Value, Bin: bin, Count: n})
			}
		}
	}
	slices.SortFunc(out, func(a, b ValuePoint) int {
		return cmp.Or(cmp.Compare(a.PatternID, b.PatternID), cmp.Compare(a.Value, b.Value), cmp.Compare(a.Bin, b.Bin))
	})
	return out
}
