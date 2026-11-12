// Package rcabench is the RCA scoring harness. It drives every scenario in
// seedcorpus.RCAScenarios() through the real HTTP rca_walk surface and emits a
// deterministic Scorecard so a regression in insight quality shows up as a
// git-diff on the committed artifact.
package rcabench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
)

// ScenarioScore is one row of the committed scorecard.
type ScenarioScore struct {
	Name         string `json:"name"`
	Volume       string `json:"cause_volume"`
	Mechanism    string `json:"mechanism"`
	LeadTime     string `json:"lead_time"`
	SymptomShape string `json:"symptom_shape"`
	Logs         string `json:"logs"` // seedcorpus.LogShape: structured rows or raw lines
	// CauseRank is the 1-based rank of the true cause pattern in the chain; 0
	// means the cause was not admitted, and drives AC@k / MRR = 0 for that row.
	CauseRank int  `json:"cause_rank"`
	Found     bool `json:"found"`
	// CauseAdmittedBy is the gate that admitted the true cause, empty when none did; a cause
	// admitted by entity is named by its host rather than its service.
	CauseAdmittedBy string `json:"cause_admitted_by,omitempty"`
	// CauseRejectedBy is the gate reason that dropped the true cause, empty when it was
	// admitted. Without it a miss says only that the walk failed, and each reason points
	// at a different fix.
	CauseRejectedBy string `json:"cause_rejected_by,omitempty"`
	// KnownGap is set when the scenario is a documented limitation rather than a target.
	// Such a row is excluded from AC@k and MRR, so a gap cannot flatter the headline.
	KnownGap string `json:"known_gap,omitempty"`
	// FalseAdmits is the sorted set of decoy pattern prefixes that reached the
	// admitted chain, where empty is the correct outcome.
	FalseAdmits     []string `json:"false_admits"`
	RefusalExpected bool     `json:"refusal_expected"`
	RefusalCorrect  bool     `json:"refusal_correct"`
	GETs            int      `json:"gets"`
	BytesRead       int64    `json:"bytes_read"`
	// Pass carries the per-scenario expectation: an ordinary scenario passes
	// when the true cause is admitted at rank 1 with no false-admits; a refusal
	// scenario passes when the tool correctly refuses.
	Pass bool `json:"pass"`
}

// AggregateMetrics is the roll-up over every scenario in a Scorecard.
type AggregateMetrics struct {
	// N is the row count. Scored is how many of those rows feed AC@k and MRR: refusal
	// scenarios are scored by RefusalAccuracy instead, and known gaps by nothing.
	N               int     `json:"n"`
	Scored          int     `json:"scored"`
	AC1             float64 `json:"ac_at_1"`
	AC3             float64 `json:"ac_at_3"`
	AC5             float64 `json:"ac_at_5"`
	MRR             float64 `json:"mrr"`
	FalseAdmitRate  float64 `json:"false_admit_rate"`
	RefusalAccuracy float64 `json:"refusal_accuracy"`
	// KnownGaps counts scenarios excluded as documented limitations. N excludes them, so
	// the headline never improves by declaring a shape out of scope.
	KnownGaps      int   `json:"known_gaps"`
	TotalGETs      int   `json:"total_gets"`
	TotalBytesRead int64 `json:"total_bytes_read"`
}

// Scorecard is the committed report. Scenarios are sorted by Name and floats
// are rendered at fixed precision so the JSON and Markdown outputs are
// byte-identical run to run — the determinism contract the git-diff CI gate
// depends on.
type Scorecard struct {
	Scenarios []ScenarioScore  `json:"scenarios"`
	Aggregate AggregateMetrics `json:"aggregate"`
	// ByLogs is the aggregate over each log shape's rows, so the gap between structured rows
	// and raw lines reads off one table.
	ByLogs map[string]AggregateMetrics `json:"by_logs,omitempty"`
}

// AccuracyAtK returns the fraction of ranks in [1, k]. Rank 0 counts as a
// miss. Matches the AC@k form used by RCAEval
// so pkg_3's adapter scores its labeled cases with the same primitive.
func AccuracyAtK(ranks []int, k int) float64 {
	if len(ranks) == 0 || k <= 0 {
		return 0
	}
	hit := 0
	for _, r := range ranks {
		if r > 0 && r <= k {
			hit++
		}
	}
	return float64(hit) / float64(len(ranks))
}

// MRR is the mean reciprocal rank over ranks. Rank 0 contributes 0. Matches
// RCAEval's MRR definition.
func MRR(ranks []int) float64 {
	if len(ranks) == 0 {
		return 0
	}
	var sum float64
	for _, r := range ranks {
		if r > 0 {
			sum += 1.0 / float64(r)
		}
	}
	return sum / float64(len(ranks))
}

// AggregateOver rolls up per-scenario rows into AggregateMetrics. Refusal
// scenarios are excluded from the ranking metrics — a refusal is scored by
// RefusalCorrect, not by where a nonexistent cause landed — but their cost
// still counts.
func AggregateOver(scenarios []ScenarioScore) AggregateMetrics {
	var (
		ranks         []int
		decoysAll     int
		decoysAdmit   int
		refusalsN     int
		refusalsRight int
		gaps          int
		totalGETs     int
		totalBytes    int64
	)
	for _, s := range scenarios {
		if s.KnownGap != "" {
			gaps++
			totalGETs += s.GETs
			totalBytes += s.BytesRead
			continue
		}
		if s.RefusalExpected {
			refusalsN++
			if s.RefusalCorrect {
				refusalsRight++
			}
		} else {
			ranks = append(ranks, s.CauseRank)
		}
		decoysAll += 2 // every scenario declares two decoys (GroundTruth.Decoys)
		decoysAdmit += len(s.FalseAdmits)
		totalGETs += s.GETs
		totalBytes += s.BytesRead
	}
	agg := AggregateMetrics{
		N:              len(scenarios),
		Scored:         len(ranks),
		KnownGaps:      gaps,
		AC1:            AccuracyAtK(ranks, 1),
		AC3:            AccuracyAtK(ranks, 3),
		AC5:            AccuracyAtK(ranks, 5),
		MRR:            MRR(ranks),
		TotalGETs:      totalGETs,
		TotalBytesRead: totalBytes,
	}
	if decoysAll > 0 {
		agg.FalseAdmitRate = float64(decoysAdmit) / float64(decoysAll)
	}
	if refusalsN > 0 {
		agg.RefusalAccuracy = float64(refusalsRight) / float64(refusalsN)
	}
	return agg
}

// SortScenarios sorts rows by Name and normalises every FalseAdmits slice so
// two runs of the same scenarios produce byte-identical output.
func SortScenarios(scenarios []ScenarioScore) {
	for i := range scenarios {
		sort.Strings(scenarios[i].FalseAdmits)
	}
	sort.SliceStable(scenarios, func(i, j int) bool {
		return scenarios[i].Name < scenarios[j].Name
	})
}

// JSON renders the scorecard with rounded floats and sorted rows so the same
// scorecard marshals byte-identically twice.
func (s Scorecard) JSON() ([]byte, error) {
	rounded := s
	rounded.Scenarios = append([]ScenarioScore(nil), s.Scenarios...)
	SortScenarios(rounded.Scenarios)
	rounded.Aggregate = roundAggregate(rounded.Aggregate)
	rounded.ByLogs = maps.Clone(s.ByLogs)
	for shape, agg := range rounded.ByLogs {
		rounded.ByLogs[shape] = roundAggregate(agg)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rounded); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Markdown renders a human-readable scorecard table plus the aggregate row.
// Same rounding rules as JSON.
func (s Scorecard) Markdown() string {
	rows := append([]ScenarioScore(nil), s.Scenarios...)
	SortScenarios(rows)
	agg := roundAggregate(s.Aggregate)

	var b strings.Builder
	b.WriteString("# RCA Scorecard\n\n")
	fmt.Fprintf(&b, "Aggregate over %d scenarios (%d scored, %d refusals, %d known gaps):\n\n",
		agg.N, agg.Scored, agg.N-agg.Scored-agg.KnownGaps, agg.KnownGaps)
	b.WriteString("| metric | value |\n")
	b.WriteString("| --- | --- |\n")
	fmt.Fprintf(&b, "| AC@1 | %s |\n", fmtF(agg.AC1))
	fmt.Fprintf(&b, "| AC@3 | %s |\n", fmtF(agg.AC3))
	fmt.Fprintf(&b, "| AC@5 | %s |\n", fmtF(agg.AC5))
	fmt.Fprintf(&b, "| MRR | %s |\n", fmtF(agg.MRR))
	fmt.Fprintf(&b, "| false_admit_rate | %s |\n", fmtF(agg.FalseAdmitRate))
	fmt.Fprintf(&b, "| refusal_accuracy | %s |\n", fmtF(agg.RefusalAccuracy))
	fmt.Fprintf(&b, "| known_gaps | %d |\n", agg.KnownGaps)
	fmt.Fprintf(&b, "| total_gets | %d |\n", agg.TotalGETs)
	fmt.Fprintf(&b, "| total_bytes_read | %d |\n\n", agg.TotalBytesRead)

	b.WriteString(logsSection(s.ByLogs))

	b.WriteString(gapSection(rows))

	b.WriteString("## Per-scenario\n\n")
	b.WriteString("| name | volume | mechanism | lead | shape | rank | found | admitted_by | rejected_by | false_admits | refusal_expected | refusal_correct | gets | pass |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %d | %v | %s | %s | %s | %v | %v | %d | %v |\n",
			r.Name, r.Volume, r.Mechanism, r.LeadTime, r.SymptomShape,
			r.CauseRank, r.Found, dash(r.CauseAdmittedBy), dash(r.CauseRejectedBy), dash(strings.Join(r.FalseAdmits, ",")),
			r.RefusalExpected, r.RefusalCorrect, r.GETs, r.Pass)
	}
	return b.String()
}

// logsSection compares the log shapes' headline metrics, rendering nothing for a scorecard without them.
func logsSection(byLogs map[string]AggregateMetrics) string {
	if len(byLogs) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("## By log shape\n\n")
	b.WriteString("| logs | scored | AC@1 | AC@3 | MRR | false_admit_rate | refusal_accuracy | known_gaps |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, shape := range slices.Sorted(maps.Keys(byLogs)) {
		agg := roundAggregate(byLogs[shape])
		fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s | %d |\n", shape, agg.Scored,
			fmtF(agg.AC1), fmtF(agg.AC3), fmtF(agg.MRR), fmtF(agg.FalseAdmitRate), fmtF(agg.RefusalAccuracy), agg.KnownGaps)
	}
	b.WriteString("\n")
	return b.String()
}

// gapSection lists the documented limitations, so a reader sees what the tool cannot do
// beside what it can. An empty set renders nothing.
func gapSection(rows []ScenarioScore) string {
	var b strings.Builder
	for _, r := range rows {
		if r.KnownGap == "" {
			continue
		}
		if b.Len() == 0 {
			b.WriteString("## Known gaps\n\nShapes the tools cannot serve today. " +
				"Excluded from AC@k; the harness fails if one starts working.\n\n")
			b.WriteString("| name | shape | rank | rejected_by | gap |\n")
			b.WriteString("| --- | --- | --- | --- | --- |\n")
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %s | %s |\n",
			r.Name, r.SymptomShape, r.CauseRank, dash(r.CauseRejectedBy), r.KnownGap)
	}
	if b.Len() > 0 {
		b.WriteString("\n")
	}
	return b.String()
}

// dash renders an empty cell as "-" so the table reads as a grid.
func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// roundAggregate rounds every rate to four decimals. Fixed precision is what
// makes the committed artifacts byte-identical across runs and platforms.
func roundAggregate(a AggregateMetrics) AggregateMetrics {
	a.AC1 = round4(a.AC1)
	a.AC3 = round4(a.AC3)
	a.AC5 = round4(a.AC5)
	a.MRR = round4(a.MRR)
	a.FalseAdmitRate = round4(a.FalseAdmitRate)
	a.RefusalAccuracy = round4(a.RefusalAccuracy)
	return a
}

// round4 rounds to four decimals via integer arithmetic — same result on every
// platform, unlike a %.4f round-trip which depends on the runtime's formatter.
func round4(x float64) float64 {
	scaled := x * 10000
	if scaled >= 0 {
		scaled += 0.5
	} else {
		scaled -= 0.5
	}
	return float64(int64(scaled)) / 10000
}

// fmtF renders a rate at fixed four-decimal precision.
func fmtF(x float64) string {
	return fmt.Sprintf("%.4f", x)
}
