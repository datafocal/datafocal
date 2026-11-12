package seedcorpus

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRCAScenariosCoverAllAxes(t *testing.T) {
	scenarios := RCAScenarios()

	volumes := make(map[CauseVolume]bool)
	mechanisms := make(map[Mechanism]bool)
	leads := make(map[LeadTime]bool)
	shapes := make(map[SymptomShape]bool)
	decoyKinds := make(map[string]bool)

	for _, s := range scenarios {
		volumes[s.Volume] = true
		mechanisms[s.Mechanism] = true
		leads[s.LeadTime] = true
		shapes[s.SymptomShape] = true
		for _, d := range s.GroundTruth.Decoys {
			decoyKinds[d.Kind] = true
		}
	}

	for _, v := range []CauseVolume{VolumeLoud, VolumeQuiet, VolumeSilentThenPresent} {
		assert.True(t, volumes[v], "missing CauseVolume %q", v)
	}
	for _, m := range []Mechanism{MechanismTopology, MechanismAttribution, MechanismEntity, MechanismNone} {
		assert.True(t, mechanisms[m], "missing Mechanism %q", m)
	}
	for _, l := range []LeadTime{LeadWellSeparated, LeadOneBin, LeadSimultaneous, LeadFeaturelessCoramp} {
		assert.True(t, leads[l], "missing LeadTime %q", l)
	}
	for _, sh := range []SymptomShape{ShapeCount, ShapeFilteredCount, ShapeRatio, ShapeSumRatio, ShapeQuantile, ShapeFilteredQuantile} {
		assert.True(t, shapes[sh], "missing SymptomShape %q", sh)
	}
	assert.True(t, decoyKinds["co_moves_no_mechanism"], "missing decoy kind co_moves_no_mechanism")
	assert.True(t, decoyKinds["mechanism_no_co_move"], "missing decoy kind mechanism_no_co_move")
}

func TestRCAScenarioNamesUnique(t *testing.T) {
	scenarios := RCAScenarios()
	seen := make(map[string]bool, len(scenarios))
	for _, s := range scenarios {
		require.False(t, seen[s.Name], "duplicate scenario name %q", s.Name)
		seen[s.Name] = true
	}
}

func TestBuildScenarioDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("builds corpora")
	}
	ctx := context.Background()
	scenarios := RCAScenarios()

	// Test a representative subset to keep the test fast.
	subset := []RCAScenario{scenarios[0]} // baseline
	for _, s := range scenarios {
		if s.Name == "mechanism_none" || s.Name == "shape_ratio" {
			subset = append(subset, s)
		}
	}

	for _, sc := range subset {
		t.Run(sc.Name, func(t *testing.T) {
			dir1 := t.TempDir()
			dir2 := t.TempDir()

			r1, err := BuildScenario(ctx, dir1, sc)
			require.NoError(t, err)
			r2, err := BuildScenario(ctx, dir2, sc)
			require.NoError(t, err)

			require.Equal(t, len(r1.Descriptors), len(r2.Descriptors),
				"descriptor count mismatch")
			assert.Equal(t, r1.Patterns, r2.Patterns,
				"pattern maps differ between runs")

			// Compare descriptor IDs (which include content hashes).
			ids1 := descriptorIDs(r1)
			ids2 := descriptorIDs(r2)
			assert.Equal(t, ids1, ids2, "descriptor IDs differ between runs")
		})
	}
}

func TestGroundTruthWellFormed(t *testing.T) {
	if testing.Short() {
		t.Skip("builds corpora")
	}
	ctx := context.Background()
	scenarios := RCAScenarios()

	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			dir := t.TempDir()
			result, err := BuildScenario(ctx, dir, sc)
			require.NoError(t, err)

			gt := sc.GroundTruth
			patterns := result.Patterns

			causeFound := false
			for _, tmpl := range patterns {
				if strings.HasPrefix(tmpl, gt.CausePatternPrefix) {
					causeFound = true
					break
				}
			}
			assert.True(t, causeFound,
				"CausePatternPrefix %q not found in patterns", gt.CausePatternPrefix)

			symptomFound := false
			for _, tmpl := range patterns {
				if strings.HasPrefix(tmpl, gt.SymptomPatternPrefix) {
					symptomFound = true
					break
				}
			}
			assert.True(t, symptomFound,
				"SymptomPatternPrefix %q not found in patterns", gt.SymptomPatternPrefix)

			for _, d := range gt.Decoys {
				decoyFound := false
				for _, tmpl := range patterns {
					if strings.HasPrefix(tmpl, d.PatternPrefix) {
						decoyFound = true
						break
					}
				}
				assert.True(t, decoyFound,
					"Decoy.PatternPrefix %q not found in patterns", d.PatternPrefix)
			}

			// Mechanism-specific invariants.
			if gt.Mechanism != MechanismNone {
				assert.False(t, gt.ExpectRefusal,
					"non-none mechanism should not expect refusal")
				assert.NotEmpty(t, gt.EntityField, "mechanism %s needs EntityField", gt.Mechanism)
				assert.NotEmpty(t, gt.EntityValue, "mechanism %s needs EntityValue", gt.Mechanism)
			} else {
				assert.True(t, gt.ExpectRefusal,
					"mechanism=none must expect refusal")
			}

			// LeadTime invariants.
			if gt.LeadTime == LeadFeaturelessCoramp {
				assert.False(t, gt.Orderable,
					"featureless co-ramp must not be orderable")
			} else {
				assert.True(t, gt.Orderable,
					"non-featureless lead time should be orderable")
			}

			// Corpus has a sensible number of segments.
			assert.NotEmpty(t, result.Descriptors, "scenario must produce segments")
		})
	}
}

func TestBuildScenarioCreatesDir(t *testing.T) {
	ctx := context.Background()
	scenarios := RCAScenarios()
	dir := filepath.Join(t.TempDir(), "nested", "dir")

	result, err := BuildScenario(ctx, dir, scenarios[0])
	require.NoError(t, err)
	assert.NotEmpty(t, result.Descriptors)

	_, err = os.Stat(dir)
	assert.NoError(t, err)
}

func TestBuildStreamsRefactorPreservesExisting(t *testing.T) {
	if testing.Short() {
		t.Skip("builds corpus")
	}
	ctx := context.Background()
	dir := t.TempDir()

	result, err := Build(ctx, dir, ScenarioIncident)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Descriptors)
	assert.NotEmpty(t, result.Patterns)
}

func descriptorIDs(r BuildResult) []string {
	ids := make([]string, len(r.Descriptors))
	for i, d := range r.Descriptors {
		ids[i] = d.ID
	}
	return ids
}

// TestRawLinesCarryOnlyWhatTheLineSays: a raw scenario's cause rows hold the parsed
// postgres line and the collector's pod and host, never the trace id its structured twin shares.
func TestRawLinesCarryOnlyWhatTheLineSays(t *testing.T) {
	var raw RCAScenario
	for _, sc := range RCAScenarios() {
		if sc.Name == "baseline_raw" {
			raw = sc
		}
	}
	require.Equal(t, LogsRaw, raw.Logs)
	cause := raw.streams()[2]()
	require.Equal(t, "bench-cause", cause.service)
	for _, r := range cause.events[:20] {
		var names []string
		for _, c := range r.Columns {
			names = append(names, c.Name)
		}
		assert.Equal(t, []string{"host", "level", "message", "pod"}, names)
		assert.Equal(t, "bench-host-1", r.Text("host"))
		assert.True(t, strings.HasPrefix(r.Text("message"), "[57] LOG: cause write batch "), r.Text("message"))
		assert.False(t, r.Timestamp.IsZero())
	}
}
