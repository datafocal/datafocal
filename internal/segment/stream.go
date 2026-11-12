package segment

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

func (s Stream) Fingerprint() string {
	parts := make([]string, 0, len(s.Labels)+1)
	parts = append(parts, "service="+s.Service)
	for key, value := range s.Labels {
		parts = append(parts, key+"="+value)
	}
	sort.Strings(parts)

	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:8])
}
