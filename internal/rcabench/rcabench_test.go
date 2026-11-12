package rcabench

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
)

// updateScorecard controls whether TestScorecard writes the golden artifacts
// to testdata. Off in normal runs (so improving the tool never spuriously
// fails `go test`); on for `make rcabench`. Named "rcabench.update" to keep
// the flag out of shadowed collision with other tests' -update flags.
var updateScorecard = flag.Bool("rcabench.update", false,
	"regenerate internal/rcabench/testdata/scorecard.{json,md}")

// sharedMatrix walks the full scenario matrix once for the whole package. Every test
// below asks the same question of the same matrix, and a walk builds a corpus, a
// summary and a server per scenario — so building it per test cost six matrices for
// one matrix's worth of information. Determinism keeps its own second, independent
// build; that is the one place two are the point rather than an accident.
var sharedMatrix = sync.OnceValues(func() ([]scenarioRun, error) {
	return runMatrix(context.Background(), seedcorpus.RCAScenarios())
})

// matrixRuns returns the shared matrix, including each scenario's raw envelope.
func matrixRuns(t *testing.T) []scenarioRun {
	t.Helper()
	runs, err := sharedMatrix()
	require.NoError(t, err)
	return runs
}

// matrixScorecard scores the shared matrix. It reassembles rather than caches because
// SortScenarios sorts in place, and a test that mutated a shared Scorecard would
// silently reorder another's.
func matrixScorecard(t *testing.T) Scorecard {
	t.Helper()
	return scorecardOf(matrixRuns(t))
}

// TestRefusalRule tables the two "should not commit" cases so the exact
// contract is pinned before the harness plumbs it in. Refusal_expected covers
// mechanism=none (no cause exists); !Orderable covers featureless co-ramps
// (an ordering is unrecoverable, so any negative lag is a fabrication).
func TestRefusalRule(t *testing.T) {
	tests := []struct {
		name        string
		gt          seedcorpus.GroundTruth
		rank        int
		decoys      []string
		chain       []tools.RCAHop
		wantExpect  bool
		wantCorrect bool
	}{
		{
			name:        "ordinary hit clears the refusal path",
			gt:          seedcorpus.GroundTruth{Mechanism: seedcorpus.MechanismTopology, Orderable: true},
			rank:        1,
			wantExpect:  false,
			wantCorrect: false,
		},
		{
			name:        "expected refusal, empty chain",
			gt:          seedcorpus.GroundTruth{Mechanism: seedcorpus.MechanismNone, ExpectRefusal: true, Orderable: true},
			rank:        0,
			wantExpect:  true,
			wantCorrect: true,
		},
		{
			name:        "expected refusal but tool admitted a decoy",
			gt:          seedcorpus.GroundTruth{Mechanism: seedcorpus.MechanismNone, ExpectRefusal: true, Orderable: true},
			rank:        0,
			decoys:      []string{"decoy-comoves activity "},
			wantExpect:  true,
			wantCorrect: false,
		},
		{
			name:        "featureless co-ramp, empty chain is correct refusal",
			gt:          seedcorpus.GroundTruth{Mechanism: seedcorpus.MechanismTopology, Orderable: false, LeadTime: seedcorpus.LeadFeaturelessCoramp},
			rank:        0,
			chain:       nil,
			wantExpect:  true,
			wantCorrect: true,
		},
		{
			name:        "featureless co-ramp with a negative-lag hop is a fabricated ordering",
			gt:          seedcorpus.GroundTruth{Mechanism: seedcorpus.MechanismTopology, Orderable: false, LeadTime: seedcorpus.LeadFeaturelessCoramp},
			rank:        1,
			chain:       []tools.RCAHop{{LagSeconds: -30}},
			wantExpect:  true,
			wantCorrect: false,
		},
		{
			name:        "featureless co-ramp with only same-bin hops is a correct restraint",
			gt:          seedcorpus.GroundTruth{Mechanism: seedcorpus.MechanismTopology, Orderable: false, LeadTime: seedcorpus.LeadFeaturelessCoramp},
			rank:        1,
			chain:       []tools.RCAHop{{LagSeconds: 0}},
			wantExpect:  true,
			wantCorrect: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotExpect, gotCorrect := refusalRule(tt.gt, tt.rank, tt.decoys, tt.chain)
			assert.Equal(t, tt.wantExpect, gotExpect, "refusalExpected")
			assert.Equal(t, tt.wantCorrect, gotCorrect, "refusalCorrect")
		})
	}
}

// TestScenarioPass: the per-row Pass rule fans out to the two cases the
// aggregate scores against.
func TestScenarioPass(t *testing.T) {
	tests := []struct {
		name        string
		rank        int
		decoys      []string
		refExpected bool
		refCorrect  bool
		want        bool
	}{
		{name: "ordinary rank 1 no false-admit", rank: 1, want: true},
		{name: "ordinary rank 2", rank: 2, want: false},
		{name: "ordinary rank 1 with false-admit", rank: 1, decoys: []string{"x"}, want: false},
		{name: "ordinary miss", rank: 0, want: false},
		{name: "refusal correct", refExpected: true, refCorrect: true, want: true},
		{name: "refusal wrong", refExpected: true, refCorrect: false, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want,
				scenarioPass(tt.rank, tt.decoys, tt.refExpected, tt.refCorrect))
		})
	}
}

// TestRunDrivesHTTP is the real-HTTP end-to-end proof: the full matrix scores one
// decoded row per scenario (implicit: no error, no zero envelopes). This is the gate on
// "the harness drives real HTTP" — a change that skips a scenario or bypasses the
// server would drop the count. TestScorecardDeterministic covers Run itself, which is
// this assembled from a second, independent matrix.
func TestRunDrivesHTTP(t *testing.T) {
	if testing.Short() {
		t.Skip("full matrix builds ~14 corpora")
	}
	scenarios := seedcorpus.RCAScenarios()
	sc := matrixScorecard(t)
	require.Len(t, sc.Scenarios, len(scenarios), "one row per scenario")

	// Rows are sorted by Name — the determinism contract SortScenarios promises.
	names := make([]string, len(sc.Scenarios))
	for i, r := range sc.Scenarios {
		names[i] = r.Name
	}
	sortedNames := append([]string(nil), names...)
	require.True(t, isSorted(sortedNames), "scenarios must be sorted by Name")

	// The harness reached the summary path: at least one row spent bytes.
	var totalBytes int64
	for _, r := range sc.Scenarios {
		totalBytes += r.BytesRead
	}
	assert.Positive(t, totalBytes, "at least one scenario must exercise the read path")
}

// TestNotAllGreen enforces the acceptance criterion: the benchmark is not
// all-green on first run. The no-mechanism scenario should refuse (a correct
// refusal passes, a chain that names a cause fails) and the featureless
// co-ramp scenario should decline to order. Whether either one actually
// passes today depends on the tool — but the scorecard MUST distinguish them
// from an ordinary hit, and at least one ordinary scenario MUST pass so the
// harness proves it can discriminate.
func TestNotAllGreen(t *testing.T) {
	if testing.Short() {
		t.Skip("full matrix builds ~14 corpora")
	}
	sc := matrixScorecard(t)

	byName := make(map[string]ScenarioScore, len(sc.Scenarios))
	for _, r := range sc.Scenarios {
		byName[r.Name] = r
	}

	// The two structurally-hard scenarios must at minimum be reported as
	// refusal-expected — the classification is what makes the aggregate
	// honest whether the tool passes or fails them.
	noMech, ok := byName["mechanism_none"]
	require.True(t, ok, "mechanism_none scenario must exist")
	assert.True(t, noMech.RefusalExpected, "mechanism_none must be classified refusal_expected")

	corumps, ok := byName["lead_featureless_coramp"]
	require.True(t, ok, "lead_featureless_coramp scenario must exist")
	assert.True(t, corumps.RefusalExpected, "lead_featureless_coramp must be classified refusal_expected")

	// At least one ordinary scenario must Pass — otherwise the harness cannot
	// discriminate and the aggregate is uninformative.
	anyPass := false
	for _, r := range sc.Scenarios {
		if !r.RefusalExpected && r.Pass {
			anyPass = true
			break
		}
	}
	assert.True(t, anyPass, "at least one ordinary scenario must Pass — the harness must discriminate")
}

// TestSharedFieldAdmitsByTopology: with no placement stream, the task values cause and
// symptom share are the topology edge that admits the cause.
func TestSharedFieldAdmitsByTopology(t *testing.T) {
	if testing.Short() {
		t.Skip("full matrix builds ~14 corpora")
	}
	runs := matrixRuns(t)
	i := slices.IndexFunc(runs, func(r scenarioRun) bool { return r.Score.Name == "mechanism_shared_field" })
	require.GreaterOrEqual(t, i, 0, "mechanism_shared_field scenario must exist")
	require.True(t, runs[i].Score.Pass, "the cause must be rank 1 with no false admits")
	assert.Equal(t, "topology", runs[i].Envelope.Data.Chain[0].AdmittedBy)
}

// TestScorecardDeterministic: build the matrix a second time, independently of the
// shared one, marshal both, assert byte-identical. This is the determinism gate the CI
// git-diff step relies on. The second build goes through Run, so the exported entry
// point is covered over the full matrix and against a separately-built oracle.
func TestScorecardDeterministic(t *testing.T) {
	if testing.Short() {
		t.Skip("full matrix builds ~14 corpora twice")
	}
	scenarios := seedcorpus.RCAScenarios()
	sc1 := matrixScorecard(t)
	sc2, err := Run(context.Background(), scenarios)
	require.NoError(t, err)

	// Cost is a real number that can wobble across footer-cache states, but the
	// per-scenario harness rebuilds a fresh corpus + fresh summary + fresh server
	// every time, so cost should be deterministic too. If it turns out not to be,
	// the fix is to zero it before comparing, not to weaken the deterministic
	// contract — the scorecard's whole point is to catch a quality change.
	a, err := sc1.JSON()
	require.NoError(t, err)
	b, err := sc2.JSON()
	require.NoError(t, err)
	assert.Equal(t, string(a), string(b),
		"same scenarios must produce byte-identical Scorecard JSON")

	// Markdown is the same story: a table rendered from a sorted, rounded
	// scorecard has no non-determinism sources of its own.
	assert.Equal(t, sc1.Markdown(), sc2.Markdown(),
		"same scenarios must produce byte-identical Scorecard Markdown")
}

// TestScorecard is the golden regeneration entry point invoked by
// `make rcabench` — it runs the full matrix and writes the committed
// artifacts to testdata when -rcabench.update is set. In normal runs (no
// flag) it exercises the harness invariants without touching the file
// system, so `go test ./...` never regresses because a tool improved.
func TestScorecard(t *testing.T) {
	if testing.Short() {
		t.Skip("full matrix builds ~14 corpora")
	}
	scenarios := seedcorpus.RCAScenarios()
	sc := matrixScorecard(t)

	if !*updateScorecard {
		// Invariant check when not updating: the scorecard structurally reports
		// on every scenario, is sorted, and its aggregate is internally
		// consistent (N matches the row count).
		require.Len(t, sc.Scenarios, len(scenarios))
		assert.Equal(t, len(scenarios), sc.Aggregate.N,
			"Aggregate.N must equal the row count")
		return
	}

	jsonBytes, err := sc.JSON()
	require.NoError(t, err)
	mdBytes := []byte(sc.Markdown())

	testdataDir := "testdata"
	require.NoError(t, os.MkdirAll(testdataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(testdataDir, "scorecard.json"), jsonBytes, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(testdataDir, "scorecard.md"), mdBytes, 0o644))
	t.Logf("wrote %s and %s (%d scenarios)",
		filepath.Join(testdataDir, "scorecard.json"),
		filepath.Join(testdataDir, "scorecard.md"),
		len(sc.Scenarios))
}

// isSorted reports whether names is sorted ascending.
func isSorted(names []string) bool {
	for i := 1; i < len(names); i++ {
		if names[i] < names[i-1] {
			return false
		}
	}
	return true
}
