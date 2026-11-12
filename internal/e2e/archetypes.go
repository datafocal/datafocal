package e2e

import (
	"time"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// Archetype is one query of the query-under-load benchmark, as the
// tool request the in-process and the stack benchmarks both send.
type Archetype struct {
	Name    string
	Tool    string
	Request any
}

// Corpus is what the archetypes query: copies hours of the seeded logs from Start, the
// first copy's incident starting there, a trace_id they carry and the symptom's pattern.
type Corpus struct {
	Start          time.Time
	Copies         int
	TraceID        string
	SymptomPattern string
}

// Archetypes is a point lookup, a summary aggregate, a sketch aggregate, a scan and an
// RCA walk over c.
func Archetypes(c Corpus) []Archetype {
	all := selector.TimeSpec{From: rfc3339(c.Start), To: rfc3339(c.Start.Add(time.Duration(c.Copies) * time.Hour))}
	from, to := seedcorpus.IncidentWindow()
	incident := selector.TimeSpec{From: rfc3339(c.Start), To: rfc3339(c.Start.Add(to.Sub(from)))}
	return []Archetype{
		{"point lookup: trace_id over all data", "search_logs", tools.SearchRequest{
			Selector: selector.Selector{Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: c.TraceID}}},
		}},
		{"aggregate: count by service, 1m bins", selector.StageAggregate, tools.AggregateRequest{
			Selector: selector.Selector{Time: all}, Measure: "count", GroupBy: segment.ColService, Bins: "1m",
		}},
		{"aggregate: db p95 by instance, 15s bins", selector.StageAggregate, tools.AggregateRequest{
			Selector: selector.Selector{Time: all, Where: []selector.Predicate{{Field: segment.ColService, Op: selector.OpEq, Value: "db"}}},
			Measure:  "p95", GroupBy: "instance", Bins: "15s",
		}},
		{"scan: count where message matches", selector.StageAggregate, tools.AggregateRequest{
			Selector: selector.Selector{Time: all, Where: []selector.Predicate{{Field: "message", Op: selector.OpMatch, Value: "slow query 1"}}},
			Measure:  "count", GroupBy: segment.ColService, Bins: "all", MaxScanSegments: -1,
		}},
		{"rca_walk: first incident", "rca_walk", tools.RCAWalkRequest{
			Selector: selector.Selector{Time: incident},
			Symptom:  tools.SeriesRef{PatternID: c.SymptomPattern},
			Entity:   tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
			MinRho:   0.5,
		}},
	}
}

func rfc3339(t time.Time) string { return t.UTC().Format(time.RFC3339) }
