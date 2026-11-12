// Package summary implements the materialized summary store: a
// sparse per-bin table built by merging segment footers rather than scanning data,
// plus a seen index. This file defines the bin table; the seen index and
// store facade live alongside.
package summary

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/topology"
)

// Config is the series' shape. Every field has a default, so the zero Config is
// usable.
type Config struct {
	// BinWidth is the width of one point. It is fixed for the life of the series
	// rather than supplied per merge: points placed at different widths cannot be
	// read back as one series, because nothing records which width produced which
	// point.
	BinWidth time.Duration

	// Retention is how far back from the newest bin observed the series keeps
	// points. Older points are dropped on the next merge.
	Retention time.Duration

	// SketchColumns names the columns the series keeps merged sketches for. Empty
	// means none, and a series with none answers count and rate but declines
	// distinct and quantile to the scan path.
	//
	// There is no "all columns" default because the cost is not marginal: an HLL
	// grows to 16KB with its cardinality (a dense p=14 register array), so one
	// column of many values costs ~92MB per service per day at the default width
	// and retention. Naming the columns is high-signal gating (sketching only the
	// columns worth it) done by hand — which is the only way it exists until
	// the automatic gating does.
	SketchColumns []string

	// LoadWindow is how far back from the newest descriptor a builder's first pass adds,
	// bounding startup cost the way Retention bounds steady-state size; it defaults to
	// Retention, and Validate rejects a larger value.
	LoadWindow time.Duration

	// RefreshInterval is how often Builder.Refresh rebuilds. It is a summary setting
	// rather than the caller's loop parameter because what it trades off is this
	// package's: a longer interval leaves new segments answered from the scan path
	// for longer, and a shorter one re-reads the catalog manifest for nothing.
	RefreshInterval time.Duration

	// MaxValueSeries is how many value series the store keeps, evicting the series whose
	// newest bin is oldest past it.
	MaxValueSeries int

	// MaxColumnValues is how many distinct values of a column the store keeps series for; a
	// column past it, as a per-node column is in a fleet, is read from segments instead.
	MaxColumnValues int

	// Topology holds the hourly topology graph sub-config.
	Topology topology.Config
}

const (
	// DefaultBinWidth is the design's recent-data granularity.
	DefaultBinWidth = 15 * time.Second

	// DefaultRetention bounds a default-width series at 5760 bins.
	DefaultRetention = 24 * time.Hour

	// DefaultMaxValueSeries is how many value series a store keeps by default, each holding
	// at most a point per retained bin.
	DefaultMaxValueSeries = 10000 //nolint:dupdefault // a value series cap, unrelated to series' bin cap

	// DefaultMaxColumnValues keeps a fleet's per-node columns from filling DefaultMaxValueSeries.
	DefaultMaxColumnValues = 256

	// DefaultRefreshInterval is how often Builder.Refresh rebuilds. A refresh with no
	// new segments costs one catalog read and no footers, so this is short enough
	// that a freshly sealed segment is answered from the store within a tick.
	DefaultRefreshInterval = 30 * time.Second
)

func (c Config) withDefaults() Config {
	if c.BinWidth <= 0 {
		c.BinWidth = DefaultBinWidth
	}
	if c.Retention <= 0 {
		c.Retention = DefaultRetention
	}
	if c.LoadWindow <= 0 {
		c.LoadWindow = c.Retention
	}
	if c.RefreshInterval <= 0 {
		c.RefreshInterval = DefaultRefreshInterval
	}
	if c.MaxValueSeries <= 0 {
		c.MaxValueSeries = DefaultMaxValueSeries
	}
	if c.MaxColumnValues <= 0 {
		c.MaxColumnValues = DefaultMaxColumnValues
	}
	// Sorted and deduped so every merge walks the columns in one order, and
	// copied so a later mutation of the caller's slice cannot change which
	// columns already-merged points hold.
	c.SketchColumns = slices.Compact(slices.Sorted(slices.Values(c.SketchColumns)))
	return c
}

// Validate reports whether the values a caller actually set can work together.
//
// Zero means unset — withDefaults fills those in, and a zero Config is usable by
// design — so this checks only what someone stated. It exists so an operator
// surface can refuse a bad value at startup rather than have withDefaults quietly
// substitute one, and it lives here because the rules are facts about the store:
// any binary that builds a summary owes the same ones.
func (c Config) Validate() error {
	var errs []error
	for _, span := range []struct {
		name  string
		value time.Duration
	}{
		{"bin width", c.BinWidth},
		{"retention", c.Retention},
		{"prewarm", c.LoadWindow},
		{"refresh interval", c.RefreshInterval},
	} {
		if span.value < 0 {
			errs = append(errs, fmt.Errorf("summary: %s %s must be positive", span.name, span.value))
		}
	}
	// A store cannot start warmer than it stays: everything the load window reached past
	// retention is evicted on the first merge, so the extra footers are read and
	// thrown away.
	if c.LoadWindow > 0 && c.Retention > 0 && c.LoadWindow > c.Retention {
		errs = append(errs, fmt.Errorf("summary: prewarm %s exceeds retention %s", c.LoadWindow, c.Retention))
	}
	return errors.Join(errs...)
}

// CountKey indexes the counts: (pattern_id, service, bin_start_unix_ms).
type CountKey struct {
	PatternID string
	Service   string
	Bin       int64
}

// BinCounts is a multi-bin segment's rows per (pattern, service, bin), counted from
// its rows since its footer counts only the whole segment's.
type BinCounts map[CountKey]int64

// CountPoint is one point of the count series.
type CountPoint struct {
	Key   CountKey
	Lines int64
}

// SketchKey indexes the sketches by (service, bin_start_unix_ms) with no pattern, because a
// footer's sketches cover the whole segment and a per-pattern key would count it once per pattern.
type SketchKey struct {
	Service string
	Bin     int64
}

// SketchPoint is one point of the sketch series: the merged summaries of every
// segment overlapping this (service, bin), keyed by column.
//
// Both maps are sparse over Config.SketchColumns — a column appears only if some
// contributing footer actually carried that family for it, so a read has to
// treat a missing column as "the series cannot answer this" rather than as zero.
//
// A point's sketches cover the segments that *touch* this bin, not the part of
// them that falls inside it: a sketch cannot be split the way a count can, so
// a straddling segment contributes whole to every bin it spans. Reads are
// over-approximations. Only Lines is exact.
type SketchPoint struct {
	Key SketchKey
	// Distinct holds one HLL per column, for distinct:<column>.
	Distinct map[string]*sketch.HLL
	// Quantiles holds one DDSketch per numeric column, for p50/p95/p99:<column>.
	Quantiles map[string]*sketch.DDSketch
}

// PatternLines is one pattern's segment-wide line count as the series stored it, so the seen
// index scores the same numbers rather than recomputing them from a second source.
type PatternLines struct {
	PatternID string
	Lines     int64
}

// BinTable is the sparse per-bin table every series is read from, written by
// MergeFooter under the store's lock.
type BinTable struct {
	cfg       Config
	counts    map[CountKey]int64
	sketches  map[SketchKey]*SketchPoint
	newestBin int64
	seen      bool // whether newestBin means anything yet
	// evictedTo is the retention start the last eviction ran at, so a merge that cannot
	// have expired anything skips the walk.
	evictedTo int64
}

// NewBinTable returns an empty bin table; a zero Config takes the package defaults.
func NewBinTable(cfg Config) *BinTable {
	return &BinTable{
		cfg:      cfg.withDefaults(),
		counts:   make(map[CountKey]int64),
		sketches: make(map[SketchKey]*SketchPoint),
	}
}

// BinWidth is the width of one point.
func (c *BinTable) BinWidth() time.Duration { return c.cfg.BinWidth }

// MergeFooter merges one sealed segment's footer into every point its time range
// overlaps, and returns the per-pattern line counts it derived; a segment spanning
// several bins takes its counts from counts, and one inside a bin from its footer.
func (c *BinTable) MergeFooter(desc segment.Descriptor, footer segment.Footer, counts BinCounts) ([]PatternLines, error) {
	return c.merge(desc, footer, Rollup{Counts: counts})
}

// merge is MergeFooter taking a multi-bin segment's counts, and each bin's own sketches of the
// columns it carries, from r.
func (c *BinTable) merge(desc segment.Descriptor, footer segment.Footer, r Rollup) ([]PatternLines, error) {
	services := footer.Services()
	if len(services) == 0 {
		return nil, nil
	}
	bins, err := desc.TimeRange.Bins(c.cfg.BinWidth)
	if err != nil {
		return nil, fmt.Errorf("summary: segment %s: %w", desc.ID, err)
	}

	// The sketches are staged before anything is written, because this is the
	// only step that can fail. A merge that half-lands cannot be undone, and
	// Store.Add leaves a descriptor unadded when this returns an error, so whatever
	// had landed would be added a second time by the retry.
	staged := map[SketchKey]*SketchPoint{}
	for _, service := range services {
		points, err := c.stageSketches(service, bins, footer.ForService(service), r)
		if err != nil {
			return nil, fmt.Errorf("summary: merge segment %s: %w", desc.ID, err)
		}
		maps.Copy(staged, points)
	}

	counts := r.Counts
	if !spansBins(desc, c.cfg.BinWidth) {
		counts = BinCounts{}
		for sp, n := range footer.ServicePatternCounts {
			counts[CountKey{PatternID: sp.Pattern, Service: sp.Service, Bin: bins[0].Start}] = int64(n)
		}
	}
	lines := map[string]int64{}
	for key, n := range counts {
		lines[key.PatternID] += n
		c.counts[key] += n
	}
	maps.Copy(c.sketches, staged)
	// Sorted so the returned counts are in a stable order.
	var perPattern []PatternLines
	for _, tmpl := range slices.Sorted(maps.Keys(lines)) {
		perPattern = append(perPattern, PatternLines{PatternID: tmpl, Lines: lines[tmpl]})
	}

	c.note(bins[0].Start, bins[len(bins)-1].Start)
	return perPattern, nil
}

// note records that a write landed content in bins [oldest, newest] and ages the
// series if that moved the retention start.
//
// Eviction walks both maps in full, so it runs only on a write that could have
// left a point behind the retention start: one that moved the retention start, or one that reached
// back past it. A write that does neither cannot have changed which points are
// expired — the retention start only ever advances, and everything the write inserted
// starts at or after it.
func (c *BinTable) note(oldest, newest int64) {
	if !c.seen || newest > c.newestBin {
		c.newestBin, c.seen = newest, true
	}
	if retentionStart := c.retentionStart(); retentionStart > c.evictedTo || oldest < retentionStart {
		c.evict(retentionStart)
		c.evictedTo = retentionStart
	}
}

// stageSketches returns the post-merge point for every bin the segment overlaps,
// folding in the footer's segment-wide summaries once per bin rather than once
// per pattern, for each configured column. It reads the series but does not
// write it: the caller installs the result only once every bin has merged.
//
// Each point is built fresh and merged into — starting from a copy of the stored
// one where there is one — so the series never holds a pointer into a caller's
// footer, including a cached footer the caller may still be using, and a failure
// part-way through leaves the stored points exactly as they were.
func (c *BinTable) stageSketches(service string, bins []segment.Bin, footer segment.Footer, r Rollup) (map[SketchKey]*SketchPoint, error) {
	staged := make(map[SketchKey]*SketchPoint, len(bins))
	for _, bin := range bins {
		key := SketchKey{Service: service, Bin: bin.Start}
		point := newSketchPoint(key)
		if stored := c.sketches[key]; stored != nil {
			if err := foldPoint(point, stored); err != nil {
				return nil, err
			}
		}
		if err := c.foldFooter(point, footer, r); err != nil {
			return nil, err
		}
		// A footer carrying none of the configured columns must not leave a point
		// behind: an empty point reads as a bin the series can answer for, and it
		// would answer zero.
		if len(point.Distinct) > 0 || len(point.Quantiles) > 0 {
			staged[key] = point
		}
	}
	return staged, nil
}

// foldFooter merges into point, for each configured column, the bin's own sketches when r
// carries the column and the footer's segment-wide ones when it does not.
func (c *BinTable) foldFooter(point *SketchPoint, footer segment.Footer, r Rollup) error {
	binned := cmp.Or(r.Sketches[point.Key], &SketchPoint{})
	for _, col := range c.cfg.SketchColumns {
		src := SketchPoint{Distinct: footer.DistinctCounts, Quantiles: footer.Distributions}
		if slices.Contains(r.Columns, col) {
			src = *binned
		}
		if err := mergeColumn(point.Distinct, col, src.Distinct[col], sketch.NewHLL); err != nil {
			return fmt.Errorf("distinct %s: %w", col, err)
		}
		if err := mergeColumn(point.Quantiles, col, src.Quantiles[col], newLatencySketch); err != nil {
			return fmt.Errorf("distribution %s: %w", col, err)
		}
	}
	return nil
}

// foldPoint merges every column of src into dst, allocating dst's sketches as it
// goes. Both sides come from this package's constructors, so it folds like into
// like.
func foldPoint(dst, src *SketchPoint) error {
	for col, s := range src.Distinct {
		if err := mergeColumn(dst.Distinct, col, s, sketch.NewHLL); err != nil {
			return fmt.Errorf("distinct %s: %w", col, err)
		}
	}
	for col, s := range src.Quantiles {
		if err := mergeColumn(dst.Quantiles, col, s, newLatencySketch); err != nil {
			return fmt.Errorf("distribution %s: %w", col, err)
		}
	}
	return nil
}

func newSketchPoint(key SketchKey) *SketchPoint {
	return &SketchPoint{
		Key:       key,
		Distinct:  make(map[string]*sketch.HLL),
		Quantiles: make(map[string]*sketch.DDSketch),
	}
}

func newLatencySketch() *sketch.DDSketch { return sketch.NewDDSketch(sketch.QuantileAccuracy) }

// mergeColumn folds src into dst[col], allocating the destination on first
// touch. A nil src is a footer that simply does not carry that column, which is
// the common case for a sparse attribute — not an error.
func mergeColumn[T interface {
	comparable
	Merge(T) error
}](dst map[string]T, col string, src T, newEmpty func() T) error {
	var absent T
	if src == absent {
		return nil
	}
	if dst[col] == absent {
		dst[col] = newEmpty()
	}
	return dst[col].Merge(src)
}

// retentionStart is measured back from the newest bin the table has seen rather than
// the wall clock, so eviction is reproducible for the same merges.
func (c *BinTable) retentionStart() int64 {
	return c.newestBin - c.cfg.Retention.Milliseconds()
}

// RetentionStart is the oldest bin the bin table still holds points for.
func (c *BinTable) RetentionStart() int64 {
	return c.retentionStart()
}

// evict drops every point older than the retention start.
func (c *BinTable) evict(retentionStart int64) {
	maps.DeleteFunc(c.counts, func(k CountKey, _ int64) bool { return k.Bin < retentionStart })
	maps.DeleteFunc(c.sketches, func(k SketchKey, _ *SketchPoint) bool { return k.Bin < retentionStart })
}

// CountSlice returns every count point whose Service matches (empty string = all
// services) and whose Bin falls in [from, to), sorted by (Service, PatternID,
// Bin) so the read path produces byte-identical output for the same series
// contents.
func (c *BinTable) CountSlice(service string, from, to int64) []CountPoint {
	out := make([]CountPoint, 0)
	for key, lines := range c.counts {
		if inSlice(key.Service, key.Bin, service, from, to) {
			out = append(out, CountPoint{Key: key, Lines: lines})
		}
	}
	slices.SortFunc(out, func(a, b CountPoint) int {
		return compareKeys(a.Key.Service, b.Key.Service, a.Key.PatternID, b.Key.PatternID, a.Key.Bin, b.Key.Bin)
	})
	return out
}

// SketchSlice returns the sketch points in [from, to), sorted by (Service, Bin),
// with every sketch copied.
//
// The copy is deliberate. Handing back the series' own pointers would let a
// caller read registers that a concurrent MergeFooter is merging into — a data
// race no discipline at the call site can fix, because the caller doing nothing
// but reading is exactly the losing case. Copying costs one merge into an empty
// sketch per point, against a scan of the underlying segments avoided.
func (c *BinTable) SketchSlice(service string, from, to int64) ([]SketchPoint, error) {
	out := make([]SketchPoint, 0)
	for key, point := range c.sketches {
		if !inSlice(key.Service, key.Bin, service, from, to) {
			continue
		}
		copied := newSketchPoint(key)
		if err := foldPoint(copied, point); err != nil {
			return nil, fmt.Errorf("summary: copy point at %+v: %w", key, err)
		}
		out = append(out, *copied)
	}
	slices.SortFunc(out, func(a, b SketchPoint) int {
		return compareKeys(a.Key.Service, b.Key.Service, "", "", a.Key.Bin, b.Key.Bin)
	})
	return out, nil
}

// inSlice applies both Slice filters: the service (empty = all) and the bin
// range.
//
// The range is half-open to match the [From, To) query windows the read path
// works in. A bin at exactly to holds the data starting at to, which is outside
// the window; including it puts lines the caller did not ask for into the last
// bin of the answer.
func inSlice(pointService string, bin int64, service string, from, to int64) bool {
	if service != "" && pointService != service {
		return false
	}
	return bin >= from && bin < to
}

// compareKeys orders by service, pattern, then bin, so sketch points with no pattern sort by bin.
func compareKeys(svcA, svcB, tmplA, tmplB string, binA, binB int64) int {
	if svcA != svcB {
		return strings.Compare(svcA, svcB)
	}
	if tmplA != tmplB {
		return strings.Compare(tmplA, tmplB)
	}
	return cmp.Compare(binA, binB)
}

// spansBins reports whether desc's rows may fall in more than one bin of width, which
// Bins does not say of a range ending on a bin's start.
func spansBins(desc segment.Descriptor, width time.Duration) bool {
	w := width.Milliseconds()
	return segment.FloorBin(desc.TimeRange.Min.UnixMilli(), w) != segment.FloorBin(desc.TimeRange.Max.UnixMilli(), w)
}
