package seedcorpus

import (
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// syntheticIncidentStreams produces the stream generators for one RCA scenario.
// Every scenario emits five streams: a symptom, a cause, a scheduler (for
// topology/attribution mechanisms), and two decoys.
func syntheticIncidentStreams(sc RCAScenario) []func() stream {
	if sc.Vocabulary == VocabNoPlacement {
		return []func() stream{
			func() stream { return genScenarioSymptom(sc) },
			func() stream { return genScenarioCause(sc) },
			genScenarioDecoyCoMoves,
			genScenarioDecoyMechanism,
		}
	}
	return []func() stream{
		func() stream { return genScenarioScheduler(sc) },
		func() stream { return genScenarioSymptom(sc) },
		func() stream { return genScenarioCause(sc) },
		genScenarioDecoyCoMoves,
		genScenarioDecoyMechanism,
	}
}

// Scenario corpus uses a fixed 180-second window (12 bins at 15s) starting at
// a fixed anchor, so each scenario builds quickly and the corpus is tiny.
const (
	scenarioDurSec = 180
)

// statusAttr is the HTTP status attribute the generated rows carry and symptom predicates filter on.
const statusAttr = "status"

func scenarioStart() time.Time { return time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC) }

// placement is a pod and the host the scheduler put it on.
type placement struct{ pod, host string }

// scenarioPlacements is where the scheduler puts each pod under mech.
func scenarioPlacements(mech Mechanism) []placement {
	placements := []placement{
		{"cause-0", "bench-host-1"},
		{"symptom-0", "bench-host-1"},
	}

	switch mech {
	case MechanismTopology:
		// Topology needs shared trace_id between cause and symptom; scheduler
		// placement still required for the attribution fallback to exist.
		placements = append(placements, placement{"decoy-comoves-0", "bench-host-2"})
		placements = append(placements, placement{"decoy-mechanism-0", "bench-host-1"})
	case MechanismAttribution:
		// Attribution needs cause and symptom on the same host via scheduler.
		placements = append(placements, placement{"decoy-comoves-0", "bench-host-2"})
		placements = append(placements, placement{"decoy-mechanism-0", "bench-host-1"})
	case MechanismEntity:
		// Entity mechanism: symptom carries the entity field directly.
		placements = append(placements, placement{"decoy-comoves-0", "bench-host-2"})
		placements = append(placements, placement{"decoy-mechanism-0", "bench-host-1"})
	case MechanismNone:
		// No mechanism: cause is on a different host, no shared trace, no entity.
		placements = []placement{
			{"cause-0", "bench-host-3"},
			{"symptom-0", "bench-host-1"},
			{"decoy-comoves-0", "bench-host-2"},
			{"decoy-mechanism-0", "bench-host-4"},
		}
	}
	return placements
}

// genScenarioScheduler emits the pod→host placement records that establish the
// topology and attribution linkages.
func genScenarioScheduler(sc RCAScenario) stream {
	labels := map[string]string{"instance": "bench-scheduler-0", "region": "us-east-1"}
	s := &stream{service: "scheduler", labels: labels}
	start := scenarioStart()
	for i, p := range scenarioPlacements(sc.GroundTruth.Mechanism) {
		ts := start.Add(time.Duration(i) * 400 * time.Millisecond)
		s.emit(ts, "info", fmt.Sprintf("scheduled pod %s host %s", p.pod, p.host),
			map[string]string{"pod": p.pod, "host": p.host})
	}
	return *s
}

// genScenarioSymptom emits the symptom stream. The shape axis controls what
// kind of series the stream produces.
func genScenarioSymptom(sc RCAScenario) stream {
	labels := map[string]string{"instance": "symptom-0", "region": "us-east-1"}
	s := &stream{service: "bench-symptom", labels: labels}
	r := rngFor(s.service, labels)
	start := scenarioStart()

	// The symptom ramps across the middle third of the window.
	rampStart := scenarioDurSec / 3   // 60s
	rampEnd := 2 * scenarioDurSec / 3 // 120s

	for sec := range scenarioDurSec {
		k := countFor(20.0, r)
		for i := range k {
			attrs := map[string]string{
				"instance": "symptom-0",
				"pod":      "symptom-0",
				"region":   "us-east-1",
				"host":     "bench-host-1",
			}
			if sc.Mechanism == MechanismSharedField {
				attrs["task"] = benchTask(r)
			}
			s.emitSymptomRow(symptomRow{
				shape:     sc.GroundTruth.SymptomShape,
				vocab:     sc.Vocabulary,
				ts:        tsWithin(start, sec, i, k),
				attrs:     attrs,
				t:         float64(sec),
				rampStart: float64(rampStart),
				rampEnd:   float64(rampEnd),
				r:         r,
			})
		}
	}
	return *s
}

// benchTask draws a task from the pool cause and symptom rows share under MechanismSharedField.
func benchTask(r *rand.Rand) string { return fmt.Sprintf("task-%02d", r.Intn(40)) }

// symptomRow is everything emitSymptomRow needs. It is a struct for the reason
// rca_walk's pathInput is one: the alternative is a seven-parameter call whose
// argument order is its own hazard.
type symptomRow struct {
	shape     SymptomShape
	vocab     Vocabulary
	ts        time.Time
	attrs     map[string]string
	t         float64
	rampStart float64
	rampEnd   float64
	r         *rand.Rand
}

// errStatus rolls one request outcome at the given error rate, returning the
// status, its level, and a latency drawn from the matching distribution.
func (row symptomRow) errStatus(errRate, okMS, slowLo, slowHi float64) (int, string, int) {
	ms := int(okMS) + row.r.Intn(20)
	if row.r.Float64() >= errRate {
		return 200, "info", ms
	}
	return 500, "error", int(slowLo) + row.r.Intn(int(slowHi-slowLo))
}

// ramp interpolates v0..v1 across the symptom's ramp window.
func (row symptomRow) ramp(v0, v1 float64) float64 {
	return lerp(row.t, row.rampStart, row.rampEnd, v0, v1)
}

// emitSymptomRow emits the one row this shape calls for.
func (s *stream) emitSymptomRow(row symptomRow) {
	switch row.shape {
	case ShapeCount:
		// Count ramp: the line count itself rises. One baseline row plus the ramp's
		// extras, drawn in that order so the corpus stays byte-identical.
		rows := countFor(row.ramp(0, 15), row.r) + 1
		for range rows {
			s.emit(row.ts, "info",
				fmt.Sprintf("symptom request ok %d ms", 5+row.r.Intn(20)), row.attrs)
		}

	case ShapeFilteredCount:
		// Filtered count: the 5xx fraction rises, counted over the 5xx slice.
		status, level, ms := row.errStatus(row.ramp(0.01, 0.30), 5, 200, 500)
		row.attrs[statusAttr] = fmt.Sprintf("%d", status)
		s.emit(row.ts, level, fmt.Sprintf("symptom request %s %d ms",
			statusWord(status), ms), row.attrs)

	case ShapeRatio:
		// The message must not name the status, because splitting on err/ok would
		// make the ratio a constant with no variance to correlate.
		status, level, ms := row.errStatus(row.ramp(0.01, 0.30), 5, 200, 500)
		row.attrs[statusAttr] = fmt.Sprintf("%d", status)
		msg := fmt.Sprintf("symptom request completed %d ms", ms)
		if row.vocab == VocabSplitOutcome {
			msg = fmt.Sprintf("symptom request %s %d ms", statusWord(status), ms)
		}
		s.emit(row.ts, level, msg, row.attrs)

	case ShapeSumRatio:
		// Sum ratio: error time / total time rises, over one pattern for the same
		// reason ShapeRatio uses one.
		status, level, ms := row.errStatus(row.ramp(0.01, 0.15), 5, 500, 1300)
		row.attrs[statusAttr] = fmt.Sprintf("%d", status)
		row.attrs["duration_ms"] = fmt.Sprintf("%d", ms)
		s.emit(row.ts, level, fmt.Sprintf("symptom request completed %d ms", ms), row.attrs)

	case ShapeQuantile:
		// Quantile: p99 of duration_ms rises. Under VocabInlineDuration the unit is glued
		// to the number and the attribute is dropped, so no latency column exists at all.
		ms := int(sampleLatency(row.ramp(10, 10), row.ramp(30, 500), row.r) + 0.5)
		if row.vocab == VocabInlineDuration {
			s.emit(row.ts, "info", fmt.Sprintf("symptom request ok %dms", ms), row.attrs)
			return
		}
		row.attrs["duration_ms"] = fmt.Sprintf("%d", ms)
		s.emit(row.ts, "info", fmt.Sprintf("symptom request ok %d ms", ms), row.attrs)

	case ShapeFilteredQuantile:
		// Filtered quantile: p99 of duration_ms among errors rises. This draws its
		// own outcome rather than calling errStatus, whose slow branch would consume
		// an extra draw and shift the whole corpus.
		ms := 5 + row.r.Intn(20)
		status, level := 200, "info"
		if row.r.Float64() < row.ramp(0.05, 0.25) {
			status, level = 500, "error"
			ms = int(sampleLatency(row.ramp(50, 200), row.ramp(100, 800), row.r) + 0.5)
		}
		row.attrs[statusAttr] = fmt.Sprintf("%d", status)
		row.attrs["duration_ms"] = fmt.Sprintf("%d", ms)
		s.emit(row.ts, level, fmt.Sprintf("symptom request %s %d ms",
			statusWord(status), ms), row.attrs)
	}
}

func statusWord(status int) string {
	if status >= 500 {
		return "err"
	}
	return "ok"
}

// genScenarioCause emits the cause stream. Volume controls the count; lead
// time controls when the ramp starts relative to the symptom; mechanism
// controls whether shared trace_ids or entity fields link it to the symptom.
func genScenarioCause(sc RCAScenario) stream {
	labels := map[string]string{"instance": "cause-0", "region": "us-east-1"}
	s := &stream{service: "bench-cause", labels: labels}
	r := rngFor(s.service, labels)
	start := scenarioStart()
	gt := sc.GroundTruth

	symptomRampStart := scenarioDurSec / 3
	var causeRampStart int
	switch gt.LeadTime {
	case LeadWellSeparated:
		causeRampStart = symptomRampStart - 30 // 30s before symptom
	case LeadOneBin:
		causeRampStart = symptomRampStart - 15 // one 15s bin before
	case LeadSimultaneous:
		causeRampStart = symptomRampStart // same bin
	case LeadFeaturelessCoramp:
		causeRampStart = 0 // slow monotonic from the start
	}
	causeRampEnd := causeRampStart + (scenarioDurSec / 3)
	if gt.LeadTime == LeadFeaturelessCoramp {
		causeRampEnd = scenarioDurSec // ramp spans entire window
	}

	for sec := range scenarioDurSec {
		t := float64(sec)

		var baseRate, rampRate float64
		switch sc.Volume {
		case VolumeLoud:
			baseRate = 15.0
			rampRate = lerp(t, float64(causeRampStart), float64(causeRampEnd), 15, 60)
		case VolumeQuiet:
			baseRate = 3.0
			rampRate = lerp(t, float64(causeRampStart), float64(causeRampEnd), 3, 12)
		case VolumeSilentThenPresent:
			if t < float64(causeRampStart) {
				baseRate = 0
				rampRate = 0
			} else {
				baseRate = 0
				rampRate = lerp(t, float64(causeRampStart), float64(causeRampEnd), 1, 30)
			}
		}

		rate := baseRate
		if t >= float64(causeRampStart) {
			rate = rampRate
		}
		k := countFor(rate, r)

		for i := range k {
			ts := tsWithin(start, sec, i, k)
			attrs := map[string]string{
				"instance": "cause-0",
				"pod":      "cause-0",
				"region":   "us-east-1",
			}

			switch gt.Mechanism {
			case MechanismTopology:
				// Shared trace_id between cause and symptom services.
				attrs["trace_id"] = hexID(r, "shared")
				rows := 100 + r.Intn(900)
				s.emit(ts, "info", fmt.Sprintf("cause write batch %d rows", rows), attrs)
			case MechanismAttribution:
				// Pod on same host; attribution via scheduler placement.
				attrs["host"] = "bench-host-1"
				s.emit(ts, "info", fmt.Sprintf("cause process item %s", pick(r, "ok", "done")), attrs)
			case MechanismEntity:
				// Symptom carries entity field directly; cause shares the entity value.
				attrs["host"] = "bench-host-1"
				s.emit(ts, "info", fmt.Sprintf("cause process item %s", pick(r, "ok", "done")), attrs)
			case MechanismSharedField:
				// Linked to the symptom only by the task values both draw.
				attrs["task"] = benchTask(r)
				s.emit(ts, "info", fmt.Sprintf("cause process item %s", pick(r, "ok", "done")), attrs)
			case MechanismNone:
				// No linkage at all.
				s.emit(ts, "info", fmt.Sprintf("cause process item %s", pick(r, "ok", "done")), attrs)
			}
		}
	}
	return *s
}

// genScenarioDecoyCoMoves emits a pattern that ramps in lockstep with the
// symptom but sits on a different host with no mechanism linking it.
func genScenarioDecoyCoMoves() stream {
	labels := map[string]string{"instance": "decoy-comoves-0", "region": "us-east-1"}
	s := &stream{service: "bench-decoy-comoves", labels: labels}
	r := rngFor(s.service, labels)
	start := scenarioStart()

	rampStart := scenarioDurSec / 3
	rampEnd := 2 * scenarioDurSec / 3

	for sec := range scenarioDurSec {
		t := float64(sec)
		rate := lerp(t, float64(rampStart), float64(rampEnd), 5, 40)
		k := countFor(rate, r)
		for i := range k {
			ts := tsWithin(start, sec, i, k)
			s.emit(ts, "info",
				fmt.Sprintf("decoy-comoves activity %d", 100+r.Intn(500)),
				map[string]string{
					"instance": "decoy-comoves-0",
					"pod":      "decoy-comoves-0",
					"region":   "us-east-1",
				})
		}
	}
	return *s
}

// genScenarioDecoyMechanism emits a pattern on a host that shares the entity
// mechanism with the symptom (same host in the scheduler) but stays flat — it
// does not co-move with the symptom, so rho falls below the floor.
func genScenarioDecoyMechanism() stream {
	labels := map[string]string{"instance": "decoy-mechanism-0", "region": "us-east-1"}
	s := &stream{service: "bench-decoy-mechanism", labels: labels}
	r := rngFor(s.service, labels)
	start := scenarioStart()

	for sec := range scenarioDurSec {
		k := countFor(8.0, r)
		for i := range k {
			ts := tsWithin(start, sec, i, k)
			s.emit(ts, "info",
				fmt.Sprintf("decoy-mechanism idle %d", 10+r.Intn(20)),
				map[string]string{
					"instance": "decoy-mechanism-0",
					"pod":      "decoy-mechanism-0",
					"region":   "us-east-1",
					"host":     "bench-host-1",
				})
		}
	}
	return *s
}

// pgLinePrefix is how a parsed postgres line's message starts: its masked pid, then its level.
const pgLinePrefix = "<*> LOG: "

// rawLines rewrites s as a collector on the node reads it: each row written as its software
// logs it, the cause as postgres's text and the rest as logfmt, parsed back, then given the
// pod and the host the scheduler put it on where the line names no pod of its own.
func rawLines(s stream, mech Mechanism) stream {
	hosts := map[string]string{}
	for _, p := range scenarioPlacements(mech) {
		hosts[p.pod] = p.host
	}
	rows := s.events
	s.events = make([]segment.Record, 0, len(rows))
	for _, row := range rows {
		fields := map[string]string{}
		for _, c := range row.Columns {
			fields[c.Name] = value.Format(c.Type, c.Value)
		}
		pod := fields["pod"]
		own := pod == s.labels["instance"]                      // a scheduler's rows name the pods it placed
		p, _ := logformat.New(logformat.Config{Format: "auto"}) // auto is always a format
		r, ok := p.Parse(rawLine(s.service, row.Timestamp, fields, own))
		if !ok {
			r, _ = p.Flush()
		}
		if own {
			r.Columns = append(r.Columns, segment.Column{Name: "pod", Type: value.TypeString, Value: value.Value{String: pod}})
			if host := hosts[pod]; host != "" {
				r.Columns = append(r.Columns, segment.Column{Name: "host", Type: value.TypeString, Value: value.Value{String: host}})
			}
			slices.SortFunc(r.Columns, func(a, b segment.Column) int { return strings.Compare(a.Name, b.Name) })
		}
		s.events = append(s.events, r)
	}
	return s
}

// rawLine is one row as its software writes it, leaving out the stream's identity, which
// the line does not carry: postgres's text for the cause, logfmt for everything else.
func rawLine(service string, ts time.Time, fields map[string]string, own bool) string {
	if service == "bench-cause" {
		return fmt.Sprintf("%s UTC [57] LOG:  %s", ts.UTC().Format("2006-01-02 15:04:05.000"), fields["message"])
	}
	line := fmt.Sprintf("time=%s level=%s msg=%q", ts.UTC().Format(time.RFC3339Nano), strings.ToUpper(fields["level"]), fields["message"])
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		switch name {
		case "message", "level", "instance", "region":
			continue
		case "pod", "host":
			if own {
				continue
			}
		}
		line += " " + name + "=" + fields[name]
	}
	return line
}
