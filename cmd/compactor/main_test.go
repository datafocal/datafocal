package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/config"
)

// TestLoadConfig defaults the tenant, interval, settle period, lookback and window, refuses a
// window that does not divide an hour, takes --root over
// DATAFOCAL_ROOT and --settle over the default, and the summary's sketch columns from the
// querier's variable.
func TestLoadConfig(t *testing.T) {
	t.Setenv("DATAFOCAL_ROOT", "/env")
	t.Setenv("DATAFOCAL_SUMMARY_SKETCH_COLUMNS", "duration_ms,trace_id")
	t.Setenv("DATAFOCAL_CONFIG_FILE", "")
	cfg, err := loadConfig([]string{"--root", "/flag", "--settle", "30s"})
	require.NoError(t, err)
	want := settings{Tenant: "local", Store: config.Store{Root: "/flag"}, Interval: config.Duration(time.Minute),
		Settle: config.Duration(30 * time.Second), Lookback: config.Duration(24 * time.Hour), Window: config.Duration(5 * time.Minute),
		CatalogGrace: config.Duration(15 * time.Minute)}
	want.Summary.SketchColumns = []string{"duration_ms", "trace_id"}
	assert.Equal(t, want, cfg)

	_, err = loadConfig([]string{"--window", "7m"})
	assert.ErrorContains(t, err, "does not divide an hour")
}

// TestLoop runs a pass at once and another each interval, each settled that long before it
// starts, and returns when stopped mid-pass; a pass past several intervals is followed at once
// by one more, never overlapping it, and then the interval resumes.
func TestLoop(t *testing.T) {
	type span struct{ start, end time.Time }
	// passes runs loop until its third pass, the first taking first, recording each.
	passes := func(interval, first time.Duration) (started time.Time, settled []time.Time, spans []span) {
		ctx, stop := context.WithCancel(t.Context())
		started = time.Now()
		loop(ctx, func(_ context.Context, at time.Time) error {
			s := span{start: time.Now()}
			settled = append(settled, at)
			if len(settled) == 1 {
				time.Sleep(first)
			}
			s.end = time.Now()
			spans = append(spans, s)
			if len(settled) == 3 {
				stop()
				return context.Canceled
			}
			return nil
		}, interval, time.Hour)
		return started, settled, spans
	}

	started, settled, spans := passes(10*time.Millisecond, 0)
	require.Len(t, settled, 3)
	assert.WithinDuration(t, started.Add(-time.Hour), settled[0], 5*time.Millisecond)
	assert.GreaterOrEqual(t, spans[2].start.Sub(spans[0].start), 15*time.Millisecond)

	_, _, spans = passes(50*time.Millisecond, 170*time.Millisecond)
	require.Len(t, spans, 3)
	for i := 1; i < len(spans); i++ {
		assert.False(t, spans[i].start.Before(spans[i-1].end), "pass %d overlaps the one before", i)
	}
	assert.Less(t, spans[1].start.Sub(spans[0].end), 15*time.Millisecond, "the long pass is followed at once")
	assert.Greater(t, spans[2].start.Sub(spans[1].end), 15*time.Millisecond, "by one pass, not one per missed interval")
}
