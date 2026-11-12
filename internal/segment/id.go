package segment

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// NewID derives a segment's object id. createdAt (nanosecond seal time) is the
// uniqueness token: without it two full segments in one stream that share a
// boundary-second min timestamp and row count would hash identically, and the
// second write would silently overwrite the first's object (they'd share a key).
func NewID(tenant string, stream Stream, tr TimeRange, rows int, createdAt time.Time) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%s/%s/%s/%s/%d/%d",
		tenant, stream.Service, stream.Fingerprint(), tr.Min.UTC(), rows, createdAt.UnixNano()))
	return hex.EncodeToString(sum[:16])
}
