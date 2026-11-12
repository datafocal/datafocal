package segment

import (
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/value"
)

type Record struct {
	Timestamp time.Time
	Columns   []Column
}

// Text is the record's value of the named column as its canonical text, "" when it has
// none; it binary-searches columns sorted by name, as a sealed or read record's are.
func (r Record) Text(name string) string {
	i, ok := slices.BinarySearchFunc(r.Columns, name, func(c Column, name string) int { return strings.Compare(c.Name, name) })
	if !ok {
		return ""
	}
	return value.Format(r.Columns[i].Type, r.Columns[i].Value)
}

type Column struct {
	Name  string
	Type  value.Type
	Value value.Value
}

type Stream struct {
	Service string            `json:"service"`
	Labels  map[string]string `json:"labels,omitempty"`
}

type Stats struct {
	Rows           int
	EstimatedBytes int64
	CreatedAt      time.Time
	TimeRange      TimeRange
}

type Sealed struct {
	Records        []Record
	Descriptor     Descriptor
	PatternProfile PatternProfile
	// table is Builder.Seal's rows in place of Records, whose descriptor's zone maps are
	// the footer's, so Encode does not fold them again.
	table    *table
	patterns map[string]string
}

// Descriptor's JSON form is the catalog commit record.
type Descriptor struct {
	ID         string    `json:"id"`
	Stream     Stream    `json:"stream"`
	TimeRange  TimeRange `json:"time_range"`
	Rows       int       `json:"rows"`
	PatternIDs []string  `json:"pattern_ids,omitempty"`
	Services   []string  `json:"services,omitempty"` // sorted, every service a row names
	// ZoneMaps carries the per-column (min,max) for manifest-level pruning. The
	// mergeable sketches deliberately live only in the segment footer (their
	// handles hold unexported state that does not JSON round-trip), so they are
	// not duplicated here.
	ZoneMaps      ZoneMaps       `json:"zone_maps,omitempty"`
	Object        ObjectLocation `json:"object"`
	CreatedAt     time.Time      `json:"created_at"`
	EstimatedSize int64          `json:"estimated_size"`
	// Supersedes holds the IDs of the segments a merged one replaces, sorted.
	Supersedes []string `json:"supersedes,omitempty"`
	// Rollup locates a merged segment's rollup, which the compactor writes beside it.
	Rollup ObjectLocation `json:"rollup,omitzero"`
	// RollupFilters locates a merged segment's membership filters, which the compactor writes
	// beside its rollup.
	RollupFilters ObjectLocation `json:"rollup_filters,omitzero"`
}

type ObjectLocation struct {
	Provider string
	Bucket   string
	Key      string
	URI      string
}
