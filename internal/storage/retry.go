package storage

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Retry calls op until it returns nil or ErrExists, or ctx ends, backing off from 100ms to 30s.
func Retry(ctx context.Context, op func() error) error {
	delay := 100 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil || errors.Is(err, ErrExists) {
			return err
		}
		slog.Default().WarnContext(ctx, "retrying storage call", "attempt", attempt, "error", err, "backoff_ms", delay.Milliseconds())
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return err
		}
		delay = min(2*delay, 30*time.Second)
	}
}
