package tools_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
)

// sealDescriptor builds a real descriptor from events, so the zone maps and the row
// count come from the same seal a collector would produce.
func sealDescriptor(t *testing.T, events []segment.Record) segment.Descriptor {
	t.Helper()
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := segment.NewBuilder(segment.BuilderConfig{
		Tenant:    "acme",
		Stream:    segment.Stream{Service: "checkout"},
		CreatedAt: ts,
	})
	for _, ev := range events {
		require.NoError(t, b.Append(ev, segment.ParsedPattern{PatternID: "t1", Pattern: "x"}))
	}
	sealed := b.Seal()
	return sealed.Descriptor
}

// TestDescriptorGroupKeyRequiresEveryRow: a bound proves the rows it saw agree, not
// that it saw every row. A column missing from some records must not key the whole
// segment, or the scan answers those rows with a value they never held.
func TestDescriptorGroupKeyRequiresEveryRow(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	bothLevels := []segment.Record{
		segment.LogRecord(ts, "error", "boom", nil),
		segment.LogRecord(ts.Add(time.Second), "error", "bang", nil),
	}
	oneLevel := []segment.Record{
		segment.LogRecord(ts, "error", "boom", nil),
		segment.LogRecord(ts.Add(time.Second), "", "quiet", nil),
	}

	tests := []struct {
		name    string
		events  []segment.Record
		field   string
		wantKey string
		wantOK  bool
	}{
		{name: "every row carries the column", events: bothLevels, field: "level", wantKey: "error", wantOK: true},
		{name: "one row is missing the column", events: oneLevel, field: "level", wantOK: false},
		{name: "service rides the stream, so every row carries it", events: oneLevel, field: "service", wantKey: "checkout", wantOK: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, ok := tools.ExportDescriptorGroupKey(sealDescriptor(t, tc.events), tc.field)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantKey, key)
		})
	}
}

// TestEntityRelationRequiresEveryRow: entityOwned means the segment is the entity's
// own telemetry, which a column present on only some of its rows cannot establish.
func TestEntityRelationRequiresEveryRow(t *testing.T) {
	ts := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	dense := sealDescriptor(t, []segment.Record{
		segment.LogRecord(ts, "", "a", map[string]string{"host": "h1"}),
		segment.LogRecord(ts.Add(time.Second), "", "b", map[string]string{"host": "h1"}),
	})
	sparse := sealDescriptor(t, []segment.Record{
		segment.LogRecord(ts, "", "a", map[string]string{"host": "h1"}),
		segment.LogRecord(ts.Add(time.Second), "", "b", nil),
	})

	assert.Equal(t, tools.ExportEntityOwned, tools.ExportEntityRelationFor(dense, "host", "h1"))
	assert.Equal(t, tools.ExportEntityIndeterminate, tools.ExportEntityRelationFor(sparse, "host", "h1"))
}
