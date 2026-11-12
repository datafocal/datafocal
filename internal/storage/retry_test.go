package storage

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var waits []time.Duration
		last := time.Now()
		attempts := 0
		err := Retry(t.Context(), func() error {
			attempts++
			if attempts > 1 {
				waits = append(waits, time.Since(last))
			}
			last = time.Now()
			if attempts == 12 {
				return nil
			}
			return errors.New("offline")
		})
		assert.NoError(t, err)
		assert.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 6400 * time.Millisecond, 12800 * time.Millisecond, 25600 * time.Millisecond, 30 * time.Second, 30 * time.Second}, waits)
	})
}
