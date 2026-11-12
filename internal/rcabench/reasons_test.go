package rcabench

import (
	"reflect"
	"strings"
	"testing"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRejectReasonsAreExercised compares the reasons the matrix actually emitted against
// tools.RejectReasons in both directions. The forward direction catches a site inventing
// an undeclared reason, and the reverse catches a gate gone dead, which is how
// no_topology_edge sat unreachable while the topology graph was empty.
func TestRejectReasonsAreExercised(t *testing.T) {
	observed := map[tools.RejectReason]int{}
	for _, run := range matrixRuns(t) {
		for _, r := range run.Envelope.Data.Rejected {
			observed[r.RejectedBy]++
		}
	}

	declared := make(map[tools.RejectReason]struct{}, len(tools.RejectReasons))
	for _, r := range tools.RejectReasons {
		declared[r] = struct{}{}
	}

	for r := range observed {
		assert.Containsf(t, declared, r,
			"the walk emitted %q, which is not in tools.RejectReasons — add it there so the "+
				"wire enum and this test both see it", r)
	}

	var unreachable []tools.RejectReason
	for _, r := range tools.RejectReasons {
		if observed[r] == 0 && notScenarioReachable[r] == "" {
			unreachable = append(unreachable, r)
		}
	}
	assert.Emptyf(t, unreachable,
		"no scenario reaches %v: either the gate is dead or the matrix is missing the case "+
			"that triggers it. Observed: %v", unreachable, observed)
}

// notScenarioReachable records the reasons no corpus can trigger, with why. An entry
// here is a claim that the gate needs a unit test rather than a scenario, so anything
// not listed must stay reachable from the matrix.
var notScenarioReachable = map[tools.RejectReason]string{
	tools.RejectEntityIndeterminate: "needs a stream whose segments straddle the entity " +
		"and others at once; no scenario builds that placement yet",
}

// TestRejectReasonEnumMatchesConstants pins the documented wire enum to the constants,
// so a new reason cannot land with the schema still describing the old set.
func TestRejectReasonEnumMatchesConstants(t *testing.T) {
	f, ok := reflect.TypeOf(tools.RCARejection{}).FieldByName("RejectedBy")
	require.True(t, ok)
	reasons := make([]string, len(tools.RejectReasons))
	for i, r := range tools.RejectReasons {
		reasons[i] = string(r)
	}
	assert.Equal(t, strings.Join(reasons, "|"), f.Tag.Get("jsonschema"),
		"the jsonschema tag on RCARejection.RejectedBy must list exactly tools.RejectReasons")
}

// TestKnownGapsStillGap fails when a documented limitation starts working, so the marker
// cannot outlive the gap. A closed gap should become an ordinary scored scenario.
func TestKnownGapsStillGap(t *testing.T) {
	byName := make(map[string]ScenarioScore)
	for _, run := range matrixRuns(t) {
		byName[run.Score.Name] = run.Score
	}
	for _, sc := range seedcorpus.RCAScenarios() {
		if sc.GroundTruth.KnownGap == "" {
			continue
		}
		score, ok := byName[sc.Name]
		require.Truef(t, ok, "scenario %s missing from the matrix", sc.Name)
		assert.Zerof(t, score.CauseRank,
			"%s is marked a known gap but found the cause at rank %d — remove the KnownGap "+
				"marker and let it be scored. Gap was: %s",
			sc.Name, score.CauseRank, sc.GroundTruth.KnownGap)
	}
}
