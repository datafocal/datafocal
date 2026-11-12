package selector

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Window is a resolved absolute time window, where a zero bound is an open bound.
type Window struct {
	From time.Time
	To   time.Time
}

// Resolve anchors relative bounds at now and parses absolute ones, the one place a
// selector's time is resolved.
func (s Selector) Resolve(now time.Time) (Resolved, error) {
	from, err := resolveBound(s.Time.From, now)
	if err != nil {
		return Resolved{}, fmt.Errorf("time.from: %w", err)
	}
	to, err := resolveBound(s.Time.To, now)
	if err != nil {
		return Resolved{}, fmt.Errorf("time.to: %w", err)
	}
	return Resolved{Window: Window{From: from, To: to}, Where: s.Where}, nil
}

// Contains reports whether t falls inside the half-open window [From, To), the reading
// bins and the summary share.
func (w Window) Contains(t time.Time) bool {
	if !w.From.IsZero() && t.Before(w.From) {
		return false
	}
	if !w.To.IsZero() && !t.Before(w.To) {
		return false
	}
	return true
}

// Bounded reports whether both ends are closed, which a full-text match scan requires.
func (w Window) Bounded() bool {
	return !w.From.IsZero() && !w.To.IsZero()
}

// Duration is the window's width, or 0 when either bound is open.
func (w Window) Duration() time.Duration {
	if w.From.IsZero() || w.To.IsZero() {
		return 0
	}
	return w.To.Sub(w.From)
}

// resolveBound accepts "", RFC3339, RFC3339Nano, "now", or "now±<duration>", returning UTC.
func resolveBound(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	rest, ok := strings.CutPrefix(s, "now")
	if !ok {
		return parseAbsolute(s)
	}
	if rest == "" {
		return now.UTC(), nil
	}
	var sign time.Duration
	switch rest[0] {
	case '-':
		sign = -1
	case '+':
		sign = 1
	default:
		return time.Time{}, fmt.Errorf("relative time %q must be now, now-<dur>, or now+<dur>", s)
	}
	d, err := parseDuration(rest[1:])
	if err != nil {
		return time.Time{}, fmt.Errorf("relative time %q: %w", s, err)
	}
	return now.Add(sign * d).UTC(), nil
}

var dayTerms = regexp.MustCompile(`([0-9]*\.?[0-9]+)([dw])`)

// parseDuration is time.ParseDuration with the d (24h) and w (7d) terms it lacks, as in 1d12h.
func parseDuration(s string) (time.Duration, error) {
	hours := dayTerms.ReplaceAllStringFunc(s, func(term string) string {
		n, _ := strconv.ParseFloat(term[:len(term)-1], 64)
		perUnit := map[byte]float64{'d': 24, 'w': 7 * 24}[term[len(term)-1]]
		return strconv.FormatFloat(n*perUnit, 'f', -1, 64) + "h"
	})
	return time.ParseDuration(hours)
}

// parseAbsolute relies on time.Parse accepting fractional seconds under RFC3339.
func parseAbsolute(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("cannot parse time %q (want RFC3339 or now[±duration])", s)
	}
	return t.UTC(), nil
}

// Relative reports whether a bound is anchored at now, so resolving it reads the clock.
func (t TimeSpec) Relative() bool {
	return strings.HasPrefix(strings.TrimSpace(t.From), "now") || strings.HasPrefix(strings.TrimSpace(t.To), "now")
}
