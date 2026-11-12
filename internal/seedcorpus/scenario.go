package seedcorpus

import (
	"context"
	"slices"

	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/topology"
)

// Vocabulary is a corpus that does not use the field and stream names the tools
// privilege. These exist because the rest of the matrix satisfies every hardcoded name
// the walk depends on, so it measures the tool against logs shaped to suit it.
type Vocabulary string

const (
	// VocabHouse uses the names the tools default to: duration_ms with a space before
	// "ms", host as the entity, a "scheduler" service emitting "scheduled pod X host Y".
	VocabHouse Vocabulary = "house"
	// VocabForeign renames the same information: elapsed instead of duration_ms, node
	// instead of host, and a placement stream called "orchestrator". Nothing about the
	// incident changes, only what the fields are called.
	VocabForeign Vocabulary = "foreign"
	// VocabNoPlacement omits the placement stream entirely, which is the common case for
	// a deployment with no scheduler logs. Topology and attribution both lose their input.
	VocabNoPlacement Vocabulary = "no_placement"
	// VocabInlineDuration writes "120ms" with no space, so masking cannot name the
	// parameter duration_ms and no latency column exists.
	VocabInlineDuration Vocabulary = "inline_duration"
	// VocabSplitOutcome puts the outcome in the message text, so masking splits the
	// symptom into separate patterns and a ratio over either is constant.
	VocabSplitOutcome Vocabulary = "split_outcome"
)

// LogShape is whether a scenario's rows are handed over as fields or read from raw lines.
type LogShape string

const (
	LogsStructured LogShape = "structured"
	LogsRaw        LogShape = "raw"
)

// CauseVolume controls how many lines the cause pattern emits.
type CauseVolume string

const (
	VolumeLoud              CauseVolume = "loud"
	VolumeQuiet             CauseVolume = "quiet"
	VolumeSilentThenPresent CauseVolume = "silent_then_present"
)

// Mechanism selects how the cause is linked to the symptom.
type Mechanism string

const (
	MechanismTopology    Mechanism = Mechanism(topology.EdgePlacement)
	MechanismAttribution Mechanism = "attribution"
	MechanismEntity      Mechanism = "entity"
	MechanismNone        Mechanism = "none"
	// MechanismSharedField links cause and symptom only by the task values their rows share.
	MechanismSharedField Mechanism = "shared_field"
)

// LeadTime controls the temporal offset between cause and symptom onset.
type LeadTime string

const (
	LeadWellSeparated     LeadTime = "well_separated"
	LeadOneBin            LeadTime = "one_bin"
	LeadSimultaneous      LeadTime = "simultaneous"
	LeadFeaturelessCoramp LeadTime = "featureless_coramp"
)

// SymptomShape selects the aggregate measure driving the symptom series.
type SymptomShape string

const (
	ShapeCount            SymptomShape = "count"
	ShapeFilteredCount    SymptomShape = "filtered_count"
	ShapeRatio            SymptomShape = "ratio"
	ShapeSumRatio         SymptomShape = "sum_ratio"
	ShapeQuantile         SymptomShape = "quantile"
	ShapeFilteredQuantile SymptomShape = "filtered_quantile"
)

// Decoy is one injected non-cause and how a correct tool must treat it.
type Decoy struct {
	PatternPrefix    string
	Kind             string // "co_moves_no_mechanism" | "mechanism_no_co_move"
	ExpectRejectedBy string // gate reason rca_walk should reject it with
}

// GroundTruth is machine-readable injected truth for one scenario.
type GroundTruth struct {
	CausePatternPrefix   string
	CauseService         string
	Mechanism            Mechanism
	SymptomPatternPrefix string
	SymptomShape         SymptomShape
	SymptomMeasure       string
	SymptomWhere         []selector.Predicate
	EntityField          string
	EntityValue          string
	LeadTime             LeadTime
	Orderable            bool
	ExpectRefusal        bool
	Decoys               []Decoy
	// KnownGap names a shape the tools cannot serve today, with why. A scenario carrying
	// one is reported as a documented limitation rather than a failure, and the scorecard
	// fails if it starts working, so the marker cannot outlive the gap.
	KnownGap string
}

// RCAScenario is one point in the benchmark matrix.
type RCAScenario struct {
	Name         string
	Volume       CauseVolume
	Mechanism    Mechanism
	LeadTime     LeadTime
	SymptomShape SymptomShape
	// Vocabulary selects whether the corpus uses the field and stream names the tools
	// privilege. Anything but VocabHouse is an adversarial shape.
	Vocabulary  Vocabulary
	Logs        LogShape
	GroundTruth GroundTruth
	streams     func() []func() stream
}

// RCAScenarios returns the full benchmark matrix in a stable order, each scenario in both log shapes.
func RCAScenarios() []RCAScenario {
	structured := []RCAScenario{
		scenarioBaseline(),
		scenarioVolumeQuiet(),
		scenarioVolumeSilentThenPresent(),
		scenarioMechanismAttribution(),
		scenarioMechanismEntity(),
		scenarioMechanismNone(),
		withVocabulary(buildScenarioSpec("mechanism_shared_field",
			VolumeLoud, MechanismSharedField, LeadWellSeparated, ShapeCount), VocabNoPlacement, ""),
		scenarioLeadOneBin(),
		scenarioLeadSimultaneous(),
		scenarioLeadFeaturelessCoramp(),
		scenarioShapeFilteredCount(),
		scenarioShapeRatio(),
		scenarioShapeSumRatio(),
		scenarioShapeQuantile(),
		scenarioShapeFilteredQuantile(),
		scenarioVocabSplitOutcome(),
		scenarioVocabNoPlacement(),
		scenarioVocabInlineDuration(),
	}
	scenarios := slices.Clone(structured)
	for _, sc := range structured {
		scenarios = append(scenarios, withRawLogs(sc))
	}
	return scenarios
}

// withRawLogs re-binds a scenario to raw lines, rebuilding its streams as withVocabulary does.
func withRawLogs(s RCAScenario) RCAScenario {
	s.Name += "_raw"
	s.Logs = LogsRaw
	s.GroundTruth.CausePatternPrefix = pgLinePrefix + s.GroundTruth.CausePatternPrefix
	if s.Vocabulary == VocabNoPlacement {
		s.GroundTruth.KnownGap = "" // the collector's host stands in for the placements
	}
	s.streams = func() []func() stream {
		gens := syntheticIncidentStreams(s)
		for i, gen := range gens {
			gens[i] = func() stream { return rawLines(gen(), s.GroundTruth.Mechanism) }
		}
		return gens
	}
	return s
}

// --- adversarial vocabulary scenarios ---
//
// The matrix above satisfies every field and stream name the tools privilege. These three
// break one assumption each, so the scorecard measures the gap instead of avoiding it.

// withVocabulary re-binds a scenario to an adversarial vocabulary. It rebuilds the
// streams closure because buildScenarioSpec captures the scenario by value, so setting
// the field afterwards would leave every generator reading the default.
func withVocabulary(s RCAScenario, v Vocabulary, gap string) RCAScenario {
	s.Vocabulary = v
	s.GroundTruth.KnownGap = gap
	s.streams = func() []func() stream { return syntheticIncidentStreams(s) }
	return s
}

func scenarioVocabSplitOutcome() RCAScenario {
	return withVocabulary(buildScenarioSpec("vocab_split_outcome",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeRatio),
		VocabSplitOutcome,
		"the outcome is in the message text, so masking splits the symptom into err/ok "+
			"patterns and a ratio over either is then constant")
}

func scenarioVocabNoPlacement() RCAScenario {
	return withVocabulary(buildScenarioSpec("vocab_no_placement",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeCount),
		VocabNoPlacement,
		"no placement stream, which is the common case without scheduler logs, so the "+
			"topology and attribution gates lose their declared input")
}

func scenarioVocabInlineDuration() RCAScenario {
	return withVocabulary(buildScenarioSpec("vocab_inline_duration",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeQuantile),
		VocabInlineDuration,
		"latency is written 120ms with no space, so masking cannot name the parameter "+
			"duration_ms and no latency column exists for a quantile")
}

// BuildScenario builds one scenario's deterministic corpus into outDir.
func BuildScenario(ctx context.Context, outDir string, scenario RCAScenario) (BuildResult, error) {
	return buildStreams(ctx, outDir, scenario.streams())
}

// --- scenario builders ---

func scenarioBaseline() RCAScenario {
	return buildScenarioSpec("baseline",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeCount)
}

func scenarioVolumeQuiet() RCAScenario {
	return buildScenarioSpec("volume_quiet",
		VolumeQuiet, MechanismTopology, LeadWellSeparated, ShapeCount)
}

func scenarioVolumeSilentThenPresent() RCAScenario {
	return buildScenarioSpec("volume_silent_then_present",
		VolumeSilentThenPresent, MechanismTopology, LeadWellSeparated, ShapeCount)
}

func scenarioMechanismAttribution() RCAScenario {
	return buildScenarioSpec("mechanism_attribution",
		VolumeLoud, MechanismAttribution, LeadWellSeparated, ShapeCount)
}

func scenarioMechanismEntity() RCAScenario {
	return buildScenarioSpec("mechanism_entity",
		VolumeLoud, MechanismEntity, LeadWellSeparated, ShapeCount)
}

func scenarioMechanismNone() RCAScenario {
	return buildScenarioSpec("mechanism_none",
		VolumeLoud, MechanismNone, LeadWellSeparated, ShapeCount)
}

func scenarioLeadOneBin() RCAScenario {
	return buildScenarioSpec("lead_one_bin",
		VolumeLoud, MechanismTopology, LeadOneBin, ShapeCount)
}

func scenarioLeadSimultaneous() RCAScenario {
	return buildScenarioSpec("lead_simultaneous",
		VolumeLoud, MechanismTopology, LeadSimultaneous, ShapeCount)
}

func scenarioLeadFeaturelessCoramp() RCAScenario {
	return buildScenarioSpec("lead_featureless_coramp",
		VolumeLoud, MechanismTopology, LeadFeaturelessCoramp, ShapeCount)
}

func scenarioShapeFilteredCount() RCAScenario {
	return buildScenarioSpec("shape_filtered_count",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeFilteredCount)
}

func scenarioShapeRatio() RCAScenario {
	return buildScenarioSpec("shape_ratio",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeRatio)
}

func scenarioShapeSumRatio() RCAScenario {
	return buildScenarioSpec("shape_sum_ratio",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeSumRatio)
}

func scenarioShapeQuantile() RCAScenario {
	return buildScenarioSpec("shape_quantile",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeQuantile)
}

func scenarioShapeFilteredQuantile() RCAScenario {
	return buildScenarioSpec("shape_filtered_quantile",
		VolumeLoud, MechanismTopology, LeadWellSeparated, ShapeFilteredQuantile)
}

func buildScenarioSpec(name string, vol CauseVolume, mech Mechanism, lead LeadTime, shape SymptomShape) RCAScenario {
	gt := buildGroundTruth(mech, lead, shape)
	s := RCAScenario{
		Name:         name,
		Logs:         LogsStructured,
		Volume:       vol,
		Mechanism:    mech,
		LeadTime:     lead,
		SymptomShape: shape,
		GroundTruth:  gt,
	}
	s.streams = func() []func() stream {
		return syntheticIncidentStreams(s)
	}
	return s
}

func buildGroundTruth(mech Mechanism, lead LeadTime, shape SymptomShape) GroundTruth {
	gt := GroundTruth{
		CauseService:         "bench-cause",
		SymptomPatternPrefix: "symptom request ",
		SymptomShape:         shape,
		Mechanism:            mech,
		LeadTime:             lead,
		Orderable:            lead != LeadFeaturelessCoramp,
		ExpectRefusal:        mech == MechanismNone,
	}

	switch mech {
	case MechanismTopology:
		gt.CausePatternPrefix = "cause write batch "
		gt.EntityField = "host"
		gt.EntityValue = "bench-host-1"
	case MechanismAttribution, MechanismEntity, MechanismNone, MechanismSharedField:
		gt.CausePatternPrefix = "cause process item "
		gt.EntityField = "host"
		gt.EntityValue = "bench-host-1"
	}

	serverErrors := []selector.Predicate{{Field: statusAttr, Op: selector.OpGe, Value: "500"}}
	switch shape {
	case ShapeCount:
		// measure="" (default count)
	case ShapeFilteredCount:
		gt.SymptomWhere = serverErrors
	case ShapeRatio:
		gt.SymptomMeasure = "ratio"
		gt.SymptomWhere = serverErrors
	case ShapeSumRatio:
		gt.SymptomMeasure = "ratio:duration_ms"
		gt.SymptomWhere = serverErrors
	case ShapeQuantile:
		gt.SymptomMeasure = "p99:duration_ms"
	case ShapeFilteredQuantile:
		gt.SymptomMeasure = "p99:duration_ms"
		gt.SymptomWhere = serverErrors
	}

	gt.Decoys = []Decoy{
		{
			PatternPrefix:    "decoy-comoves activity ",
			Kind:             "co_moves_no_mechanism",
			ExpectRejectedBy: "no_attribution",
		},
		{
			PatternPrefix:    "decoy-mechanism idle ",
			Kind:             "mechanism_no_co_move",
			ExpectRejectedBy: "below_rho",
		},
	}

	return gt
}
