package segment

import (
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/value"
)

// Footer is the self-describing, self-indexed trailer of a segment object.
// It is cached hot (~1-3% of object size) and read on essentially every
// query: a query can decide whether to read a segment, and which byte ranges
// to read, from the footer alone.
//
// This contract is the keystone of the design — freeze it before building
// anything on top.
type Footer struct {
	grams Grams
	// size is the bytes the decoded footer holds, its distinct-count sketches as held and
	// the rest as encoded, which the cache budgets.
	size int64

	// RowGroups is the number of row groups in the segment body, at least one.
	RowGroups int

	// Rows is the number of rows the footer summarizes.
	Rows int

	// ZoneMaps holds per-column (min,max) for coarse zone-map pruning.
	ZoneMaps ZoneMaps

	// RowGroupZoneMaps holds each row group's zone maps, indexed by row group, so a
	// planner can skip row groups as ZoneMaps lets it skip the segment.
	RowGroupZoneMaps []ZoneMaps

	// Patterns is the set of pattern_ids present in this segment.
	Patterns     PresentSet
	patternTexts map[string]string

	// PatternProfile is how the collector derived those pattern_ids.
	PatternProfile PatternProfile

	// The mergeable sketches per column, decoded into typed handles by the codec.
	DistinctCounts map[string]*sketch.HLL
	Distributions  map[string]*sketch.DDSketch

	// Offsets is the column-chunk offset table used for ranged reads.
	Offsets OffsetTable

	// ServicePatternCounts is each service's rows of each pattern_id, held exactly
	// because a frequency sketch per service would outweigh the rows.
	ServicePatternCounts map[ServicePattern]int

	// services is each service's view of its own rows.
	services map[string]Footer

	// filters locates each filtered column's membership filter section, which only an
	// equality lookup on that column reads (§5.2).
	filters map[string]ByteRange

	// valueCounts is the rows holding each value of a counted string column: one with at
	// least two rows per value in the segment, and at most maxCountedValues values in
	// the view's rows.
	valueCounts map[string]map[string]uint64

	// fields is a service view's stats of each column its rows hold, nil on the whole footer.
	fields map[string]FieldStats

	// valuePairs is a service view's lines holding each pair of values of two counted
	// columns that may name an endpoint, as an access log names both ends of a call.
	valuePairs map[ValuePair]uint64
}

// LargestGroupCells is the most rows times columns any one row group holds, which bounds
// what decoding a group costs.
func (f Footer) LargestGroupCells() int {
	cols := make([]int, f.RowGroups)
	for key := range f.Offsets {
		cols[key.RowGroup]++
	}
	most := 0
	for g, zm := range f.RowGroupZoneMaps {
		most = max(most, zm[ColTimestamp].PresentRows*cols[g])
	}
	return most
}

// PatternText is the text of a pattern this footer holds, cut to 512 bytes.
func (f Footer) PatternText(id string) (string, bool) {
	text, ok := f.patternTexts[id]
	return text, ok
}

// maxCountedValues is the most distinct values a string column may hold in one
// service's rows and still be counted exactly.
const maxCountedValues = 1024

// ValueCounts is the rows holding each value of a counted string column, on the whole
// footer only when every service's view counts it too.
func (f Footer) ValueCounts(column string) (map[string]uint64, bool) {
	counts, ok := f.valueCounts[column]
	return counts, ok
}

// ValuePair is the values one line holds in two columns, Columns sorted by name.
type ValuePair struct {
	Columns, Values [2]string
}

// ValuePairs is a service view's lines holding each value pair, for the column pairs
// with at most maxCountedValues of them.
func (f Footer) ValuePairs() map[ValuePair]uint64 {
	return f.valuePairs
}

// HasFilter reports whether the segment carries a membership filter for column.
func (f Footer) HasFilter(column string) bool {
	_, ok := f.filters[column]
	return ok
}

// UnmarshalBinary decodes what MarshalBinary encodes.
func (f *Footer) UnmarshalBinary(b []byte) (err error) {
	*f, err = decodeFooter(b)
	return err
}

// Filters decodes the membership filter of each filtered column of body, the segment f is
// the footer of.
func (f Footer) Filters(body []byte) (map[string]*sketch.XorFilter, error) {
	filters := make(map[string]*sketch.XorFilter, len(f.filters))
	for column, br := range f.filters {
		if column == GramsSection {
			continue
		}
		filters[column] = &sketch.XorFilter{}
		if err := filters[column].UnmarshalBinary(body[br.Offset : br.Offset+br.Length]); err != nil {
			return nil, fmt.Errorf("segment: decode filter %s: %w", column, err)
		}
	}
	return filters, nil
}

// Services is every service a row names, sorted.
func (f Footer) Services() []string {
	return slices.Sorted(maps.Keys(f.services))
}

// FieldStats is one column over a service's rows: its type, the values its rows hold (a
// list column's elements), a sketch of their distinct values, and whether one held a space,
// as free text does.
type FieldStats struct {
	Type     value.Type
	Values   int
	Distinct *sketch.HLL
	Text     bool
}

// ID reports whether the column is an id by its values over the view's rows, as the writer
// judges a column it filters.
func (s FieldStats) ID() bool {
	return !s.Text && IDByValues(int(math.Round(s.Distinct.Estimate())), s.Values)
}

// Fields is a service view's stats of each column its rows hold, and false for a view
// written before views kept them, or for a whole footer.
func (f Footer) Fields() (map[string]FieldStats, bool) {
	return f.fields, f.fields != nil
}

// ServicePattern names one service's rows of one pattern_id.
type ServicePattern struct {
	Service, Pattern string
}

// ForService is the view of service's rows: Rows, a distribution per number column, a
// distinct sketch per correlation column and its value counts.
func (f Footer) ForService(service string) Footer {
	return f.services[service]
}

// PatternProfile records the profile behind a segment's pattern_ids and the
// degradation level its stream's cardinality guard had reached at seal.
type PatternProfile struct {
	Name        string
	Version     int
	Components  []string
	Degradation int
}

// The columns the system names in more than one package: the three the querier
// privileges for sketches (trace, pattern, latency) and the two remaining
// correlation ids. One spelling each, here, because a second private copy is a
// chance for the two to drift.
const (
	ColHost       = "host"
	ColPod        = "pod"
	ColNamespace  = "namespace"
	ColContainer  = "container"
	ColTraceID    = "trace_id"
	ColPatternID  = "pattern_id"
	ColDurationMs = "duration_ms"
	ColTimestamp  = "ts"
	ColLevel      = "level"
	ColMessage    = "message"
	ColService    = "service"
	ColRequestID  = "request_id"
	ColUserID     = "user_id"
	// ParamPrefix names a masked digit token's column, param_1, param_2, …
	ParamPrefix = "param_"
	// PatternSuffix names a string column's masked copy.
	PatternSuffix = "_pattern"
)
