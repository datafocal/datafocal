package collector

import (
	"time"

	"github.com/datafocal/datafocal/internal/segment"
)

type FlushPolicy struct {
	MaxAge   time.Duration
	MaxBytes int64
	MaxRows  int

	// AlignTo keeps a builder per bucket of this width, zero for none.
	AlignTo time.Duration
}

// bucketOf is ts's AlignTo-wide slot, 0 when unaligned; log timestamps are post-epoch, so
// truncating division needs no sign handling.
func (p FlushPolicy) bucketOf(ts time.Time) int64 {
	if p.AlignTo <= 0 {
		return 0
	}
	return ts.UnixMilli() / p.AlignTo.Milliseconds()
}

func (p FlushPolicy) ShouldFlush(stats segment.Stats, now time.Time) bool {
	if stats.Rows == 0 {
		return false
	}
	if p.MaxRows > 0 && stats.Rows >= p.MaxRows {
		return true
	}
	if p.MaxBytes > 0 && stats.EstimatedBytes >= p.MaxBytes {
		return true
	}
	if p.MaxAge > 0 && now.Sub(stats.CreatedAt) >= p.MaxAge {
		return true
	}
	return false
}
