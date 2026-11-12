package selector

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectorResolve(t *testing.T) {
	now := time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC)
	at := func(h, m int) time.Time { return time.Date(2026, 7, 19, h, m, 0, 0, time.UTC) }
	where := []Predicate{{Field: "service", Op: OpEq, Value: "api"}}

	tests := []struct {
		name    string
		in      TimeSpec
		want    Window
		wantErr bool
	}{
		{name: "relative bounds", in: TimeSpec{From: "now-10m", To: "now"}, want: Window{From: at(11, 50), To: at(12, 0)}},
		{name: "relative plus", in: TimeSpec{From: "now", To: "now+1h30m"}, want: Window{From: at(12, 0), To: at(13, 30)}},
		{name: "relative days", in: TimeSpec{From: "now-1d12h", To: "now-90m"}, want: Window{From: at(12, 0).Add(-36 * time.Hour), To: at(10, 30)}},
		{name: "relative weeks", in: TimeSpec{From: "now-1w", To: "now-7d"}, want: Window{From: at(12, 0).Add(-7 * 24 * time.Hour), To: at(12, 0).Add(-7 * 24 * time.Hour)}},
		{name: "absolute rfc3339 normalized to utc", in: TimeSpec{From: "2026-07-19T09:00:00-03:00"}, want: Window{From: at(12, 0)}},
		{name: "absolute rfc3339nano", in: TimeSpec{To: "2026-07-19T12:00:00.5Z"}, want: Window{To: at(12, 0).Add(500 * time.Millisecond)}},
		{name: "open range stays open", in: TimeSpec{}, want: Window{}},
		{name: "garbage bound errors", in: TimeSpec{From: "yesterday"}, wantErr: true},
		{name: "bad relative duration errors", in: TimeSpec{From: "now-10q"}, wantErr: true},
		{name: "relative without sign errors", in: TimeSpec{To: "now10m"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Selector{Time: tt.in, Where: where}.Resolve(now)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, Resolved{Window: tt.want, Where: where}, got)
		})
	}
	_, err := Selector{Time: TimeSpec{From: "now-7x"}}.Resolve(now)
	assert.ErrorContains(t, err, `unknown unit "x"`)
}
