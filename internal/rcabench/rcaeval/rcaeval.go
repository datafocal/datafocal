// Package rcaeval adapts RCAEval RE2/RE3 labeled cases (service-level ground
// truth) to our ranking metrics, proving external metric comparability. Load
// is the seam for full RE2/RE3 log ingestion into our corpus as a follow-up.
package rcaeval

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"

	"github.com/datafocal/datafocal/internal/rcabench"
)

// Case is one labeled RCAEval case: a root-cause service and a ranked
// candidate list produced by a system under evaluation.
type Case struct {
	Suite            string   `json:"suite"`
	ID               string   `json:"id"`
	RootCauseService string   `json:"root_cause_service"`
	FaultType        string   `json:"fault_type"`
	Candidates       []string `json:"candidates"`
}

type fixtureFile struct {
	Suite string `json:"suite"`
	Cases []struct {
		ID               string   `json:"id"`
		RootCauseService string   `json:"root_cause_service"`
		FaultType        string   `json:"fault_type"`
		Candidates       []string `json:"candidates"`
	} `json:"cases"`
}

// Load reads RE2/RE3-format JSON fixtures from dir. Returns (nil, nil) when
// dir does not exist so CI without the dataset is a clean skip.
func Load(dir string) ([]Case, error) {
	_, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)

	var all []Case
	for _, path := range matches {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var ff fixtureFile
		if err := json.Unmarshal(data, &ff); err != nil {
			return nil, err
		}
		for _, c := range ff.Cases {
			all = append(all, Case{
				Suite:            ff.Suite,
				ID:               c.ID,
				RootCauseService: c.RootCauseService,
				FaultType:        c.FaultType,
				Candidates:       c.Candidates,
			})
		}
	}
	return all, nil
}

// Score ranks each case's Candidates against its RootCauseService using the
// same AccuracyAtK/MRR primitives as the main rcabench scorecard.
func Score(cases []Case) rcabench.Scorecard {
	scores := make([]rcabench.ScenarioScore, len(cases))
	ranks := make([]int, len(cases))

	for i, c := range cases {
		rank := 0
		for j, svc := range c.Candidates {
			if svc == c.RootCauseService {
				rank = j + 1
				break
			}
		}
		ranks[i] = rank
		scores[i] = rcabench.ScenarioScore{
			Name:      c.ID,
			CauseRank: rank,
			Found:     rank > 0,
			Pass:      rank == 1,
		}
	}

	return rcabench.Scorecard{
		Scenarios: scores,
		Aggregate: rcabench.AggregateMetrics{
			N:   len(cases),
			AC1: rcabench.AccuracyAtK(ranks, 1),
			AC3: rcabench.AccuracyAtK(ranks, 3),
			AC5: rcabench.AccuracyAtK(ranks, 5),
			MRR: rcabench.MRR(ranks),
		},
	}
}
