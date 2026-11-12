package summary

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/topology"
)

// Store is the summary's read side, as of the catalog version it reports.
type Store interface {
	CatalogVersion() catalog.Version
	BinWidth() time.Duration
	CompleteFrom(binMillis int64) bool
	CanServe(width time.Duration, from time.Time) bool
	CountSlice(service string, from, to int64) []CountPoint
	SketchSlice(service string, from, to int64) ([]SketchPoint, error)
	// ValueSlice is each pattern's rows by value of column in bins [from, to).
	ValueSlice(column string, from, to int64) []ValuePoint
	// HoldsValues reports whether ValueSlice holds all of column's rows in bins from on, which
	// it does not past MaxColumnValues, nor at a bin a series MaxValueSeries evicted reached.
	HoldsValues(column string, from int64) bool
	// Widened reports whether column passed MaxColumnValues, so ValueSlice holds none of it.
	Widened(column string) bool
	ExistedBefore(patternID string, t time.Time) bool
	PatternText(patternID string) (string, bool)
	HasAdded(descriptorID string) bool
	TopologyView(filter topology.Filter) topology.View
	TopologyConfig() topology.Config
}

// Segment is one sealed segment's descriptor and footer, and a multi-bin one's counts.
type Segment struct {
	Descriptor segment.Descriptor
	Footer     segment.Footer
	Counts     BinCounts
	Values     ValueCounts
}

// NewStore returns a store holding segs as a builder leaves one that read them at
// catalog version v, and that holds every segment from the bin containing completeFrom.
func NewStore(cfg Config, v catalog.Version, completeFrom time.Time, segs ...Segment) (Store, error) {
	s := newStore(cfg)
	for _, seg := range segs {
		if err := s.Add(seg.Descriptor, seg.Footer, Rollup{Counts: seg.Counts, Values: seg.Values}); err != nil {
			return nil, fmt.Errorf("add %s: %w", seg.Descriptor.ID, err)
		}
	}
	s.SetCompleteFrom(completeFrom)
	s.MarkSynced(v)
	return s, nil
}

// store composes the BinTable, SeenIndex, and topology graph behind one lock, with
// MarkSynced recording which catalog epoch the added content reflects.
type store struct {
	mu       sync.RWMutex
	cfg      Config
	bins     *BinTable
	seen     *SeenIndex
	values   *valueTable
	topology topology.Graph
	added    map[string]int64 // descriptor ID -> newest bin it touched
	// epoch is the catalog epoch this store's content fully reflects — see
	// MarkSynced and CatalogVersion.
	epoch catalog.Version
	// completeFrom is the oldest bin the store's content is complete from, and
	// completeSet whether anything has claimed one (see SetCompleteFrom).
	completeFrom int64
	completeSet  bool
}

func newStore(cfg Config) *store {
	cfg = cfg.withDefaults()
	return &store{
		cfg:      cfg,
		bins:     NewBinTable(cfg),
		seen:     NewSeenIndex(),
		values:   newValueTable(cfg.MaxValueSeries, cfg.MaxColumnValues),
		topology: topology.New(cfg.Topology),
		added:    make(map[string]int64),
	}
}

// Add merges a segment's footer and a multi-bin one's counts, values and sketches in r into the
// bin table and value series and scores each of its patterns, once per descriptor ID.
func (s *store) Add(desc segment.Descriptor, footer segment.Footer, r Rollup) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, done := s.added[desc.ID]; done || s.addedInputs(desc) {
		return nil
	}

	// The series returns the pattern set it actually stored counts under, so the
	// seen index indexes the same set rather than a second spelling of it from the
	// descriptor — the two do not have to agree.
	perPattern, err := s.bins.merge(desc, footer, r)
	if err != nil {
		return err
	}

	bin := segment.FloorBin(desc.TimeRange.Max.UnixMilli(), s.bins.BinWidth().Milliseconds())
	for _, tl := range perPattern {
		text, _ := footer.PatternText(tl.PatternID)
		s.seen.Observe(Observation{PatternID: tl.PatternID, Bin: bin, Text: text})
	}
	values := r.Values
	if !spansBins(desc, s.bins.BinWidth()) {
		values = FooterValues(footer, bin)
	}
	s.values.add(values, s.bins.RetentionStart())

	if err := s.topology.Add(desc, footer); err != nil {
		return fmt.Errorf("topology add %s: %w", desc.ID, err)
	}

	s.added[desc.ID] = bin
	s.evictArtifacts()

	return nil
}

// addedInputs marks a merged desc added, unmerged, when the store holds any input's rows;
// callers hold s.mu.
func (s *store) addedInputs(desc segment.Descriptor) bool {
	if !slices.ContainsFunc(desc.Supersedes, func(id string) bool { _, ok := s.added[id]; return ok }) {
		return false
	}
	s.added[desc.ID] = segment.FloorBin(desc.TimeRange.Max.UnixMilli(), s.bins.BinWidth().Milliseconds())
	return true
}

// evictArtifacts ages every structure the series does not age itself, at the one
// retention start the series just set. Callers hold s.mu.
//
// One retention start drives them all, so the maps cannot disagree about what is still
// live.
func (s *store) evictArtifacts() {
	retentionStart := s.bins.RetentionStart()
	s.seen.Evict(retentionStart)
	s.topology.Evict(retentionStart)
	maps.DeleteFunc(s.added, func(_ string, b int64) bool { return b < retentionStart })
}

// CompleteFrom reports whether the store holds every segment for the bin containing
// binMillis, a floor that must be declared because an absent bin and a bin with no lines
// look the same from the bin table.
func (s *store) CompleteFrom(binMillis int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.completeSet && binMillis >= max(s.completeFrom, s.bins.RetentionStart())
}

// SetCompleteFrom declares the store complete from the bin containing from, and only a
// caller that added every descriptor reaching into that span may claim it; the last value
// wins because the floor only rises as content below it is evicted.
func (s *store) SetCompleteFrom(from time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completeFrom, s.completeSet = segment.FloorBin(from.UnixMilli(), s.bins.BinWidth().Milliseconds()), true
}

// CanServe reports whether the store can answer a window of the given width
// starting at from: the store must hold content that far back, and the series'
// points must tile the requested bins exactly — the width a whole number of series
// bins, the start on a series-bin boundary.
//
// An unaligned window leaves a series bin straddling two requested bins, and the
// series has no sub-bin resolution to split it with. Attributing the whole point to
// whichever side its start falls in would be a silent error at every boundary. A
// window reaching below the complete-from floor is worse: the series holds no points
// there and returns none, which a caller summing them reads as bins with no lines,
// so the answer is not late but wrong. Either way the caller scans instead.
func (s *store) CanServe(width time.Duration, from time.Time) bool {
	binWidth := s.bins.BinWidth().Milliseconds()
	w := width.Milliseconds()
	if w < binWidth || w%binWidth != 0 || from.UnixMilli()%binWidth != 0 {
		return false
	}
	return s.CompleteFrom(from.UnixMilli())
}

// CountSlice returns the series' count points for the given service filter and
// half-open bin range [from, to).
func (s *store) CountSlice(service string, from, to int64) []CountPoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bins.CountSlice(service, from, to)
}

// SketchSlice returns the series' sketch points for the given service filter and
// half-open bin range [from, to), with every sketch copied.
func (s *store) SketchSlice(service string, from, to int64) ([]SketchPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bins.SketchSlice(service, from, to)
}

// ValueSlice returns column's value points in the half-open bin range [from, to).
func (s *store) ValueSlice(column string, from, to int64) []ValuePoint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.values.slice(column, from, to)
}

func (s *store) HoldsValues(column string, from int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	through, evicted := s.values.evictedThrough[column]
	return !s.values.wide[column] && (!evicted || from > through)
}

func (s *store) Widened(column string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.values.wide[column]
}

// valueSeriesEvicted is how many value series the series limit has evicted.
func (s *store) valueSeriesEvicted() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.values.evicted
}

// ExistedBefore reports whether this store holds positive evidence that the
// pattern was already being emitted before t. See SeenIndex.ExistedBefore: true
// is trustworthy, false only means the store cannot say.
func (s *store) ExistedBefore(patternID string, t time.Time) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seen.ExistedBefore(patternID, segment.FloorBin(t.UnixMilli(), s.bins.BinWidth().Milliseconds()))
}

// PatternText returns a pattern's text until its seen entry ages out.
func (s *store) PatternText(patternID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.seen.PatternText(patternID)
}

// MarkSynced records that the store's content now reflects every descriptor in
// catalog epoch epoch. Only a builder that merged all of them may call it: a
// partial sync leaves the store at its previous epoch, so it stops matching
// reads and every query falls to the scan path until the sync completes.
func (s *store) MarkSynced(epoch catalog.Version) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch.Before(epoch) {
		s.epoch = epoch
	}
}

// CatalogVersion is the catalog version a snapshot pins: the append-only catalog
// epoch this store's content fully reflects, "" until anything has been added.
func (s *store) CatalogVersion() catalog.Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epoch
}

// HasAdded reports whether the descriptor with the given ID has been added
// and is still at or after the retention start.
func (s *store) HasAdded(descriptorID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.added[descriptorID]
	return ok
}

// TopologyView returns a deterministically-sorted snapshot of the topology
// graph filtered by filter. It is the read seam the service_graph tool uses.
func (s *store) TopologyView(filter topology.Filter) topology.View {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.topology.Edges(filter)
}

// BinWidth returns the series' bin width, re-exporting the facade.
func (s *store) BinWidth() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bins.BinWidth()
}

// TopologyConfig returns the shape this store's graph indexed segments under, so a
// graph a tool folds itself maps workloads and bins the same way.
func (s *store) TopologyConfig() topology.Config {
	return s.cfg.Topology
}
