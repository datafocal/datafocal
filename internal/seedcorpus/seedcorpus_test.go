package seedcorpus

import (
	"bufio"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/value"
)

// TestWriteLogsParsesBackToTheEvents reads every written line through the collector's
// JSON parser: each must come back as its event, with the stream's service and labels
// as fields, shifted, and the second copy an hour after the first.
func TestWriteLogsParsesBackToTheEvents(t *testing.T) {
	dir := t.TempDir()
	const shift = 90 * 24 * time.Hour
	_, err := WriteLogs(dir, ScenarioAll, 2, shift)
	require.NoError(t, err)

	gens, err := streamsFor(ScenarioAll)
	require.NoError(t, err)
	for _, gen := range gens {
		s := gen()
		sort.SliceStable(s.events, func(i, j int) bool { return s.events[i].Timestamp.Before(s.events[j].Timestamp) })
		var want []map[string]string
		for c := range 2 {
			for _, ev := range s.events {
				want = append(want, eventFields(s, ev, shift+time.Duration(c)*time.Hour))
			}
		}
		assert.Equalf(t, want, parsedFields(t, filepath.Join(dir, streamFileName(s))), "stream %s %v", s.service, s.labels)
	}
}

func eventFields(s stream, ev segment.Record, shift time.Duration) map[string]string {
	fields := map[string]string{segment.ColService: s.service, segment.ColTimestamp: ev.Timestamp.Add(shift).UTC().Format(time.RFC3339Nano)}
	maps.Copy(fields, s.labels)
	for _, col := range ev.Columns {
		fields[col.Name] = value.Format(col.Type, col.Value)
	}
	return fields
}

func parsedFields(t *testing.T, path string) []map[string]string {
	t.Helper()
	parser, err := logformat.New(logformat.Config{Format: "json"})
	require.NoError(t, err)
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var got []map[string]string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		r, ok := parser.Parse(scanner.Text())
		require.True(t, ok, "a JSON line is a record of its own")
		fields := map[string]string{segment.ColTimestamp: r.Timestamp.UTC().Format(time.RFC3339Nano)}
		for _, col := range r.Columns {
			fields[col.Name] = value.Format(col.Type, col.Value)
		}
		got = append(got, fields)
	}
	require.NoError(t, scanner.Err())
	return got
}
