package stack

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// scorecard is the report TestScorecard prints every field of, beside it as .md. It runs
// without the stack tag or docker: it only reads a report TestBenchmark or TestFleet wrote.
var (
	scorecard = flag.String("scorecard", "", "report whose every field is written beside it as .md")
	baseline  = flag.String("baseline", "", "an earlier report the scorecard's is compared with, beside it as .vs-<baseline>.md")
)

func TestScorecard(t *testing.T) {
	if *scorecard == "" {
		t.Skip("set -scorecard")
	}
	report := readReport(t, *scorecard)
	md := strings.TrimSuffix(*scorecard, ".json") + ".md"
	require.NoError(t, os.WriteFile(md, []byte(everyField(report)), 0o644))
	if *baseline != "" {
		vs := strings.TrimSuffix(*scorecard, ".json") + ".vs-" + strings.TrimSuffix(filepath.Base(*baseline), ".json") + ".md"
		require.NoError(t, os.WriteFile(vs, []byte(compare(readReport(t, *baseline), report)), 0o644))
	}
}

func readReport(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var r map[string]any
	require.NoError(t, json.Unmarshal(data, &r))
	return r
}
