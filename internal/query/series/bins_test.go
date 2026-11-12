package series

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/summary"
)

func TestParseBins(t *testing.T) {
	day := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		spec    string
		from    time.Time
		to      time.Time
		want    Bins
		wantErr bool
	}{
		{
			name: "empty spec (single bin covering whole window)",
			spec: "",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			want: Bins{
				Width: time.Hour,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "15 second bins",
			spec: "15s",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC),
			want: Bins{
				Width: 15 * time.Second,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "1 minute bins",
			spec: "1m",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			want: Bins{
				Width: time.Minute,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "5 minute bins",
			spec: "5m",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			want: Bins{
				Width: 5 * time.Minute,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "1 hour bins",
			spec: "1h",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			want: Bins{
				Width: time.Hour,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "auto bin width for span < 1h (should be 15s)",
			spec: "auto",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			want: Bins{
				Width: 15 * time.Second,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "auto bin width for span < 6h (should be 1m)",
			spec: "auto",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
			want: Bins{
				Width: time.Minute,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "auto bin width for span < 24h (should be 5m)",
			spec: "auto",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			want: Bins{
				Width: 5 * time.Minute,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "auto bin width for span < 7d (should be 1h)",
			spec: "auto",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			want: Bins{
				Width: time.Hour,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "auto bin width for span >= 7d (should be 1d)",
			spec: "auto",
			from: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:   time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			want: Bins{
				Width: 24 * time.Hour,
				From:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				To:    time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{name: "10 minute bins", spec: "10m", from: day, to: day.Add(time.Hour), want: Bins{Width: 10 * time.Minute, From: day, To: day.Add(time.Hour)}},
		{
			name:    "invalid bin spec",
			spec:    "1w",
			from:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:      time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			wantErr: true,
		},
		{
			name:    "bin width under 15s",
			spec:    "10s",
			from:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:      time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseBins(tt.spec, tt.from, tt.to)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseBins() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return // Error case validated
			}
			if got.Width != tt.want.Width {
				t.Errorf("ParseBins() Width = %v, want %v", got.Width, tt.want.Width)
			}
			if got.From != tt.want.From {
				t.Errorf("ParseBins() From = %v, want %v", got.From, tt.want.From)
			}
			if got.To != tt.want.To {
				t.Errorf("ParseBins() To = %v, want %v", got.To, tt.want.To)
			}
		})
	}
}

func TestBinStart(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)
	bins := Bins{
		Width: time.Minute,
		From:  from,
		To:    to,
	}

	tests := []struct {
		name string
		ts   time.Time
		want int64
	}{
		{
			name: "timestamp exactly on bin boundary",
			ts:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name: "timestamp within first bin",
			ts:   time.Date(2024, 1, 1, 0, 0, 30, 0, time.UTC),
			want: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name: "timestamp on second bin boundary",
			ts:   time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC),
			want: time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name: "timestamp at end of window",
			ts:   time.Date(2024, 1, 1, 0, 59, 59, 0, time.UTC),
			want: time.Date(2024, 1, 1, 0, 59, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name: "timestamp before From (clamped to first bin)",
			ts:   time.Date(2023, 12, 31, 23, 0, 0, 0, time.UTC),
			want: from.UnixMilli(),
		},
		{
			name: "timestamp at or after To (clamped to last bin)",
			ts:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			want: time.Date(2024, 1, 1, 0, 59, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name: "timestamp far after To (clamped to last bin)",
			ts:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			want: time.Date(2024, 1, 1, 0, 59, 0, 0, time.UTC).UnixMilli(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := bins.BinStart(tt.ts)
			if got != tt.want {
				t.Errorf("BinStart() = %d, want %d (diff: %d ms)", got, tt.want, got-tt.want)
			}
		})
	}
}

func TestBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		from      time.Time
		to        time.Time
		width     time.Duration
		wantCount int
		wantFirst int64
		wantLast  int64
	}{
		{
			name:      "1 minute bins over 1 hour (60 bins)",
			from:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			width:     time.Minute,
			wantCount: 60,
			wantFirst: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
			wantLast:  time.Date(2024, 1, 1, 0, 59, 0, 0, time.UTC).UnixMilli(),
		},
		{
			name:      "15 second bins over 1 minute (4 bins)",
			from:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC),
			width:     15 * time.Second,
			wantCount: 4,
			wantFirst: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
			wantLast:  time.Date(2024, 1, 1, 0, 0, 45, 0, time.UTC).UnixMilli(),
		},
		{
			name:      "single bin (from == to)",
			from:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			width:     time.Minute,
			wantCount: 0,
			wantFirst: 0,
			wantLast:  0,
		},
		{
			name:      "5 minute bins over 30 minutes (6 bins)",
			from:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:        time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
			width:     5 * time.Minute,
			wantCount: 6,
			wantFirst: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
			wantLast:  time.Date(2024, 1, 1, 0, 25, 0, 0, time.UTC).UnixMilli(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bins := Bins{
				Width: tt.width,
				From:  tt.from,
				To:    tt.to,
			}
			got := bins.Boundaries()
			if len(got) != tt.wantCount {
				t.Errorf("Boundaries() count = %d, want %d", len(got), tt.wantCount)
			}
			if len(got) > 0 {
				if got[0] != tt.wantFirst {
					t.Errorf("Boundaries() first = %d, want %d", got[0], tt.wantFirst)
				}
				if got[len(got)-1] != tt.wantLast {
					t.Errorf("Boundaries() last = %d, want %d", got[len(got)-1], tt.wantLast)
				}
			}

			// Verify sorted and evenly spaced
			if len(got) > 1 {
				expectedSpacing := tt.width.Milliseconds()
				for i := 1; i < len(got); i++ {
					if got[i]-got[i-1] != expectedSpacing {
						t.Errorf("Boundaries() spacing at index %d: got %d, want %d", i, got[i]-got[i-1], expectedSpacing)
					}
				}
			}
		})
	}
}

// TestParseBinsRejectsTooManyBins: a named width the window cannot afford is a
// bad request, not a shorter window. Capping the bin count silently answered the
// first MaxBins of the span under the caller's own From and To.
func TestParseBinsRejectsTooManyBins(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		spec    string
		to      time.Time
		wantErr bool
	}{
		{"15s over a month", "15s", from.AddDate(0, 1, 0), true},
		{"1h over a century", "1h", from.AddDate(100, 0, 0), true},
		{"15s over an hour", "15s", from.Add(time.Hour), false},
		{"auto always fits", "auto", from.AddDate(100, 0, 0), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bins, err := ParseBins(tc.spec, from, tc.to)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "limit")
				return
			}
			require.NoError(t, err)
			assert.LessOrEqual(t, bins.NumBins(), int64(MaxBins))
		})
	}
}

// TestGridForCoarsensToFitTheWindow: with no width named, a window too long for
// the summary's grid gets a coarser grid covering all of it — not the summary's
// grid over the first slice of it.
func TestGridForCoarsensToFitTheWindow(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := from.AddDate(100, 0, 0)

	bins, err := GridFor("", summary.DefaultBinWidth, selector.Window{From: from, To: to})
	require.NoError(t, err)

	assert.LessOrEqual(t, bins.NumBins(), int64(MaxBins))
	last := bins.Boundaries()[bins.NumBins()-1]
	assert.False(t, time.UnixMilli(last).Add(bins.Width).Before(to),
		"the last bin must reach the end of the requested window")
}
