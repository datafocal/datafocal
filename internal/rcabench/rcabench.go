package rcabench

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// Run drives every scenario through rca_walk over a fresh corpus and querier each, so no
// footer-cache state crosses scenarios, and returns the aggregated Scorecard.
func Run(ctx context.Context, scenarios []seedcorpus.RCAScenario) (Scorecard, error) {
	runs, err := runMatrix(ctx, scenarios)
	if err != nil {
		return Scorecard{}, err
	}
	return scorecardOf(runs), nil
}

// scenarioRun is one scenario's walk: the row it scored to, and the envelope that
// produced it. The envelope is carried because the reject-reason audit reads the
// rejections a score does not keep, and a walk is far too expensive to repeat for them.
type scenarioRun struct {
	Score    ScenarioScore
	Envelope query.Envelope[tools.RCAWalkResponse]
}

// runMatrix walks every scenario once, concurrently. Sharing nothing across scenarios
// is already this harness's determinism argument — own corpus, own summary, own querier
// — so the walks are independent, and results stay keyed by input index rather than by
// which goroutine finished first.
func runMatrix(ctx context.Context, scenarios []seedcorpus.RCAScenario) ([]scenarioRun, error) {
	runs := make([]scenarioRun, len(scenarios))
	errs := make([]error, len(scenarios))

	sem := make(chan struct{}, runtime.GOMAXPROCS(0))
	var wg sync.WaitGroup
	for i, sc := range scenarios {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			run, err := runOne(ctx, sc)
			if err != nil {
				errs[i] = fmt.Errorf("scenario %s: %w", sc.Name, err)
				return
			}
			runs[i] = run
		}()
	}
	wg.Wait()

	// Report by scenario order, not by whichever goroutine happened to fail first, so
	// the same broken matrix always names the same scenario.
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return runs, nil
}

// scorecardOf sorts and aggregates the matrix into the reported Scorecard.
func scorecardOf(runs []scenarioRun) Scorecard {
	scores := make([]ScenarioScore, len(runs))
	for i, r := range runs {
		scores[i] = r.Score
	}
	SortScenarios(scores)
	byLogs := map[string]AggregateMetrics{}
	for _, shape := range []seedcorpus.LogShape{seedcorpus.LogsStructured, seedcorpus.LogsRaw} {
		byLogs[string(shape)] = AggregateOver(slices.DeleteFunc(slices.Clone(scores), func(s ScenarioScore) bool {
			return s.Logs != string(shape)
		}))
	}
	return Scorecard{Scenarios: scores, Aggregate: AggregateOver(scores), ByLogs: byLogs}
}

// runOne scores one scenario's rca_walk, cleaning up every temp resource before return.
func runOne(ctx context.Context, sc seedcorpus.RCAScenario) (scenarioRun, error) {
	env, build, err := walkScenario(ctx, sc)
	if err != nil {
		// A refusal is the expected outcome for a known gap, so it is recorded rather
		// than aborting the run. Anything else is a harness failure.
		if sc.GroundTruth.KnownGap == "" {
			return scenarioRun{}, err
		}
		return scenarioRun{Score: gapScore(sc, err)}, nil
	}
	return scenarioRun{Score: scoreScenario(sc, build, env), Envelope: env}, nil
}

// gapScore records a known-gap scenario whose walk refused outright, keeping the refusal
// code as the reason so the report says which limit was hit.
func gapScore(sc seedcorpus.RCAScenario, err error) ScenarioScore {
	reason := "walk_error"
	var api *query.APIError
	if errors.As(err, &api) {
		reason = api.Code
	}
	return ScenarioScore{
		Name:            sc.Name,
		Volume:          string(sc.Volume),
		Mechanism:       string(sc.Mechanism),
		LeadTime:        string(sc.LeadTime),
		SymptomShape:    string(sc.SymptomShape),
		Logs:            string(sc.Logs),
		CauseRejectedBy: reason,
		KnownGap:        sc.GroundTruth.KnownGap,
	}
}

// walkScenario returns one scenario's unscored rca_walk response, so a test reading the
// rejections takes the same path the scorecard does.
func walkScenario(ctx context.Context, sc seedcorpus.RCAScenario) (
	query.Envelope[tools.RCAWalkResponse], seedcorpus.BuildResult, error) {

	var zeroEnv query.Envelope[tools.RCAWalkResponse]
	dir, err := os.MkdirTemp("", "rcabench-"+sc.Name+"-*")
	if err != nil {
		return zeroEnv, seedcorpus.BuildResult{}, fmt.Errorf("mkdirtemp: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	build, err := seedcorpus.BuildScenario(ctx, dir, sc)
	if err != nil {
		return zeroEnv, build, fmt.Errorf("build corpus: %w", err)
	}

	store := storage.NewFS(dir)
	app := querier.Open(querier.Options{
		Catalog: catalog.New(store, seedcorpus.Tenant),
		Store:   store,
		Summary: &summary.Config{},
	})
	if err := app.Refresh(ctx); err != nil {
		return zeroEnv, build, fmt.Errorf("build summary: %w", err)
	}

	req, err := requestFor(sc, build)
	if err != nil {
		return zeroEnv, build, fmt.Errorf("build request: %w", err)
	}

	env, err := app.Tools().RCAWalk(ctx, req)
	if err != nil {
		return zeroEnv, build, fmt.Errorf("rca_walk: %w", err)
	}
	return env, build, nil
}

// scenarioWindow is the fixed 180-second window every scenario builds into. It
// mirrors the anchor in seedcorpus/scenario_gen.go (scenarioStart +
// scenarioDurSec) — the corpus deliberately anchors to a specific wall time
// rather than time.Now() so the corpus and its RCA walk are byte-identical
// across runs and platforms.
func scenarioWindow() (start, end time.Time) {
	start = time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	end = start.Add(180 * time.Second)
	return start, end
}

// requestFor turns the scenario's GroundTruth into an RCAWalkRequest. The
// symptom pattern id is resolved from BuildResult.Patterns, so the request
// hits the real symptom the generator emitted rather than a hand-guessed id.
func requestFor(sc seedcorpus.RCAScenario, build seedcorpus.BuildResult) (tools.RCAWalkRequest, error) {
	symptomID := patternIDForPrefix(build.Patterns, sc.GroundTruth.SymptomPatternPrefix)
	if symptomID == "" {
		return tools.RCAWalkRequest{}, fmt.Errorf("symptom pattern %q not in corpus", sc.GroundTruth.SymptomPatternPrefix)
	}
	start, end := scenarioWindow()
	req := tools.RCAWalkRequest{
		Selector: selector.Selector{
			Time: selector.TimeSpec{
				From: start.UTC().Format(time.RFC3339),
				To:   end.UTC().Format(time.RFC3339),
			},
		},
		Symptom: tools.SeriesRef{
			PatternID: symptomID,
			Measure:   sc.GroundTruth.SymptomMeasure,
			Where:     sc.GroundTruth.SymptomWhere,
		},
		Entity: tools.EntityRef{
			Field: sc.GroundTruth.EntityField,
			Value: sc.GroundTruth.EntityValue,
		},
		MinRho: 0.5,
	}
	return req, nil
}

// scoreScenario translates the tool's response into a ScenarioScore. Rank is
// derived from where the injected cause's rendered pattern appears in the
// admitted chain; false-admits are decoy prefixes appearing in that same
// chain; refusal correctness follows the rule documented on refusalRule.
func scoreScenario(sc seedcorpus.RCAScenario, build seedcorpus.BuildResult, env query.Envelope[tools.RCAWalkResponse]) ScenarioScore {
	gt := sc.GroundTruth

	rank := 0
	rendered := make([]string, 0, len(env.Data.Chain))
	for i, hop := range env.Data.Chain {
		tmpl := build.Patterns[hop.PatternTo]
		rendered = append(rendered, tmpl)
		if rank == 0 && strings.HasPrefix(tmpl, gt.CausePatternPrefix) {
			rank = i + 1
		}
	}

	admittedDecoys := admittedDecoyPrefixes(gt.Decoys, rendered)
	refusalExpected, refusalCorrect := refusalRule(gt, rank, admittedDecoys, env.Data.Chain)
	pass := scenarioPass(rank, admittedDecoys, refusalExpected, refusalCorrect)

	rejectedBy, admittedBy := "", ""
	if rank == 0 {
		rejectedBy = causeRejection(gt.CausePatternPrefix, build, env.Data.Rejected)
	} else {
		admittedBy = env.Data.Chain[rank-1].AdmittedBy
	}

	return ScenarioScore{
		Name:            sc.Name,
		Volume:          string(sc.Volume),
		Mechanism:       string(sc.Mechanism),
		LeadTime:        string(sc.LeadTime),
		SymptomShape:    string(sc.SymptomShape),
		Logs:            string(sc.Logs),
		CauseRank:       rank,
		Found:           rank > 0,
		CauseAdmittedBy: admittedBy,
		CauseRejectedBy: rejectedBy,
		KnownGap:        gt.KnownGap,
		FalseAdmits:     admittedDecoys,
		RefusalExpected: refusalExpected,
		RefusalCorrect:  refusalCorrect,
		GETs:            env.Meta.Cost.GETs,
		BytesRead:       env.Meta.Cost.BytesRead,
		Pass:            pass,
	}
}

// causeRejection returns the gate reason that dropped the true cause, or
// "not_a_candidate" when it never reached a gate, which blames selection rather than the
// gate. The candidate is PatternTo — every RCARejection puts the symptom in
// PatternFrom, so reading From here reports not_a_candidate for every miss.
func causeRejection(causePrefix string, build seedcorpus.BuildResult, rejected []tools.RCARejection) string {
	seen := make(map[string]struct{})
	for _, r := range rejected {
		if strings.HasPrefix(build.Patterns[r.PatternTo], causePrefix) {
			seen[string(r.RejectedBy)] = struct{}{}
		}
	}
	if len(seen) == 0 {
		return "not_a_candidate"
	}
	return strings.Join(slices.Sorted(maps.Keys(seen)), "+")
}

// admittedDecoyPrefixes returns the sorted decoy pattern prefixes found in the admitted chain.
func admittedDecoyPrefixes(decoys []seedcorpus.Decoy, renderedChain []string) []string {
	var out []string
	for _, d := range decoys {
		for _, tmpl := range renderedChain {
			if strings.HasPrefix(tmpl, d.PatternPrefix) {
				out = append(out, d.PatternPrefix)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}

// refusalRule collapses the two "the tool should not commit" cases:
//
//   - ExpectRefusal (mechanism=none): a correct tool admits no cause and no
//     decoy — the chain is empty (or at least holds no cause and no decoy).
//   - !Orderable (featureless co-ramps): a correct tool declines to fabricate
//     a lead. Every chain hop must report a non-negative lag against the
//     symptom — a negative lag would be an invented causal ordering the
//     corpus does not license.
//
// The two flags are the ones the aggregate reports on, so they are returned
// as a pair rather than muxed into a single boolean.
func refusalRule(gt seedcorpus.GroundTruth, rank int, admittedDecoys []string, chain []tools.RCAHop) (expected, correct bool) {
	if gt.ExpectRefusal {
		return true, rank == 0 && len(admittedDecoys) == 0
	}
	if !gt.Orderable {
		for _, h := range chain {
			if h.LagSeconds < 0 {
				return true, false
			}
		}
		return true, true
	}
	return false, false
}

// scenarioPass is the per-row pass rule. An ordinary scenario passes when the
// injected cause is at rank 1 with no false-admits; a refusal-expected
// scenario passes when the refusal was scored correct.
func scenarioPass(rank int, admittedDecoys []string, refusalExpected, refusalCorrect bool) bool {
	if refusalExpected {
		return refusalCorrect
	}
	return rank == 1 && len(admittedDecoys) == 0
}

// patternIDForPrefix looks ids up by rendered-pattern prefix because pattern ids are
// content hashes no test can hard-code, while the rendered pattern is stable.
func patternIDForPrefix(patterns map[string]string, prefix string) string {
	// Iterate in id order so a scenario whose corpus happens to hold two
	// patterns matching the same prefix picks the same one twice.
	ids := slices.Sorted(maps.Keys(patterns))
	for _, id := range ids {
		if strings.HasPrefix(patterns[id], prefix) {
			return id
		}
	}
	return ""
}
