package series_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/series"
)

// TestLoggedLogsEachRead: a read through Logged logs one step of what it asked for and read.
func TestLoggedLogsEachRead(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	r := series.Logged(series.Fake{ReadFn: func(series.Request) (series.Result, error) {
		return series.Result{Cost: query.Cost{GETs: 2, BytesRead: 10, Segments: 1}}, nil
	}})
	_, err := r.Read(context.Background(), series.Request{GroupBy: "path", Measures: make([]series.FilteredMeasure, 3)})
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &got))
	assert.IsType(t, 0.0, got["duration_ms"])
	for _, k := range []string{"time", "level", "duration_ms", "bins"} {
		delete(got, k)
	}
	assert.Equal(t, map[string]any{"msg": "series read", "measure": "count", "group_by": "path", "measures": 3.0,
		"segments": 1.0, "gets": 2.0, "bytes_read": 10.0, "footers_decoded": 0.0}, got)
}
