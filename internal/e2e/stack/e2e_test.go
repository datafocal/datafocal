//go:build stack

package stack

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/selector"
)

// deployedFlush is deploy/k8s/base's DATAFOCAL_FLUSH_INTERVAL.
const deployedFlush = 10 * time.Second

// TestSeededLogsRoundTrip is the whole system on the seeded corpus: the scenario written
// as log files, collected by the collector process into MinIO, and answered by the
// querier process. Every line must come back once, and the incident must still walk
// to its seeded cause after tailing, parsing and the object store.
func TestSeededLogsRoundTrip(t *testing.T) {
	s := newStack(t, deployedFlush)
	written := s.writeLogs(t, seedcorpus.ScenarioAll, 1, 0)
	lines := readLines(t, s.logs)

	collector := s.startCollector(t)
	s.waitRows(t, totalLines(written), 2*time.Minute, collector)
	collector.stop(t)
	// The querier then loads the catalog from a snapshot, which must not change an answer.
	require.NoError(t, catalog.NewSnapshotter(catalog.New(s.store, tenant), catalog.SnapshotConfig{MinCommits: 1}).Snapshot(t.Context()))
	_, base := s.startQuerier(t)

	t.Run("every line is counted once per service", func(t *testing.T) {
		start, _ := seedcorpus.IncidentWindow()
		_, end := seedcorpus.HealthyWindow()
		env, _ := call[tools.AggregateResponse](t, base, "aggregate", tools.AggregateRequest{
			Selector: selector.Selector{Time: timeSpec(start, end)},
			Measure:  "count", GroupBy: "service", Bins: "all",
		})
		got := map[string]int{}
		for _, series := range env.Data.Series {
			for _, v := range series.Values {
				got[series.Key] += int(*v)
			}
		}
		assert.Equal(t, written.Lines, got)
		assert.Equal(t, 1.0, env.Meta.Coverage)
	})

	t.Run("a trace id finds every line that carries it", func(t *testing.T) {
		traceID := lines.firstValue("gateway", "trace_id")
		env, _ := call[tools.SearchResponse](t, base, "search_logs", tools.SearchRequest{
			Selector: selector.Selector{Where: []selector.Predicate{{Field: "trace_id", Op: selector.OpEq, Value: traceID}}},
		})
		var messages []string
		for _, l := range env.Data.Lines {
			messages = append(messages, l.Message)
		}
		assert.ElementsMatch(t, lines.messagesWith("trace_id", traceID), messages)

		trace, _ := call[tools.TraceResponse](t, base, "get_trace", tools.TraceRequest{Field: "trace_id", Value: traceID})
		assert.Positive(t, trace.Data.Total, "get_trace found no spans for a trace search_logs found")
	})

	t.Run("the incident walks to its seeded cause", func(t *testing.T) {
		req := tools.RCAWalkRequest{
			Selector: selector.Selector{Time: timeSpec(seedcorpus.IncidentWindow())},
			Symptom:  tools.SeriesRef{PatternID: lines.patternID("slow query ")},
			Entity:   tools.EntityRef{Field: "host", Value: seedcorpus.SaturatedHost},
			MinRho:   0.5,
		}
		env, _ := call[tools.RCAWalkResponse](t, base, "rca_walk", req)
		var chain []string
		for _, hop := range env.Data.Chain {
			chain = append(chain, lines.patterns[hop.PatternTo])
		}
		if !assert.Lenf(t, chain, len(seedcorpus.RCAExpectedChain), "chain %q", chain) {
			for _, rej := range env.Data.Rejected {
				t.Logf("rejected %q by %s (rho %.2f, lag %.0fs)", lines.patterns[rej.PatternTo], rej.RejectedBy, rej.Rho, rej.LagSeconds)
			}
			t.FailNow()
		}
		for i, prefix := range seedcorpus.RCAExpectedChain {
			assert.Truef(t, strings.HasPrefix(chain[i], prefix), "chain[%d] = %q, want a pattern starting %q", i, chain[i], prefix)
		}

		viaMCP := callMCP[tools.RCAWalkResponse](t, base, "rca_walk", req)
		assert.Equal(t, env.Data, viaMCP.Data, "HTTP and MCP answer alike")
	})
}

// TestCollectorRestartKeepsEveryLineOnce kills the collector mid-ingest with SIGKILL, so
// nothing is flushed and no offset saved, then restarts it on the same state dir. The
// catalog must end holding every line exactly once.
func TestCollectorRestartKeepsEveryLineOnce(t *testing.T) {
	s := newStack(t, deployedFlush)
	written := s.writeLogs(t, seedcorpus.ScenarioAll, 4, 0)
	want := totalLines(written)

	first := s.startCollector(t)
	require.Eventually(t, func() bool {
		rows, _, err := s.catalogRows(t.Context())
		return err == nil && rows > 0
	}, time.Minute, 10*time.Millisecond, "the collector sealed nothing")
	first.kill()
	rows, _, err := s.catalogRows(t.Context())
	require.NoError(t, err)
	require.Lessf(t, rows, want, "the collector finished before it was killed, so the restart recovered nothing")
	t.Logf("killed the collector at %d of %d rows", rows, want)

	second := s.startCollector(t)
	s.waitRows(t, want, 3*time.Minute, second)
	// A duplicate can only land after the last missing line does, so it is a flush
	// later that the count is final.
	second.stop(t)
	rows, _, err = s.catalogRows(t.Context())
	require.NoError(t, err)
	assert.Equal(t, want, rows, "rows after a clean stop")
}

// TestAppendedLineIsSearchable appends one line to a followed file while both processes
// run, and measures how long until search_logs returns it.
func TestAppendedLineIsSearchable(t *testing.T) {
	s := newStack(t, deployedFlush)
	collector := s.startCollector(t)
	_, base := s.startQuerier(t)

	probe := fmt.Sprintf("probe-%d", time.Now().UnixNano())
	line, err := json.Marshal(map[string]string{
		"ts": time.Now().UTC().Format(time.RFC3339Nano), "level": "info", "service": "probe",
		"message": "freshness probe", "probe_id": probe,
	})
	require.NoError(t, err)
	written := time.Now()
	require.NoError(t, os.WriteFile(filepath.Join(s.logs, "probe.log"), append(line, '\n'), 0o644))

	req := tools.SearchRequest{Selector: selector.Selector{
		Time:  selector.TimeSpec{From: "now-15m", To: "now+1m"},
		Where: []selector.Predicate{{Field: "probe_id", Op: selector.OpEq, Value: probe}},
	}}
	bound := 3 * deployedFlush
	require.Eventuallyf(t, func() bool {
		env, _ := call[tools.SearchResponse](t, base, "search_logs", req)
		return len(env.Data.Lines) == 1
	}, bound, 200*time.Millisecond, "line not searchable within %s:\n%s", bound, collector.tail(20))
	t.Logf("write to searchable: %s (flush interval %s)", time.Since(written).Round(100*time.Millisecond), deployedFlush)
}

func timeSpec(start, end time.Time) selector.TimeSpec {
	return selector.TimeSpec{From: start.UTC().Format(time.RFC3339), To: end.UTC().Format(time.RFC3339)}
}

// logLines is every line the test wrote, decoded, with the pattern text behind each
// pattern id the collector derives from a message.
type logLines struct {
	rows     []map[string]string
	patterns map[string]string
}

func readLines(t *testing.T, dir string) logLines {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	require.NoError(t, err)
	l := logLines{patterns: map[string]string{}}
	for _, path := range paths {
		f, err := os.Open(path)
		require.NoError(t, err)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			var row map[string]string
			require.NoError(t, json.Unmarshal(scanner.Bytes(), &row))
			l.add(row)
		}
		require.NoError(t, scanner.Err())
		require.NoError(t, f.Close())
	}
	return l
}

func (l *logLines) add(row map[string]string) {
	l.rows = append(l.rows, row)
	text := pattern.Mask(row["message"])
	l.patterns[pattern.ID(text)] = text
}

func (l logLines) firstValue(service, field string) string {
	for _, row := range l.rows {
		if row["service"] == service && row[field] != "" {
			return row[field]
		}
	}
	panic(fmt.Sprintf("no %s line carries %s", service, field))
}

func (l logLines) messagesWith(field, value string) []string {
	var out []string
	for _, row := range l.rows {
		if row[field] == value {
			out = append(out, row["message"])
		}
	}
	return out
}

func (l logLines) patternID(prefix string) string {
	for id, text := range l.patterns {
		if strings.HasPrefix(text, prefix) {
			return id
		}
	}
	panic(fmt.Sprintf("no pattern starts %q", prefix))
}
