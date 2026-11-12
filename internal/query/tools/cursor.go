package tools

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/selector"
)

type patternsCursor struct {
	Snapshot string `json:"snap"`
	Sort     string `json:"sort"`
	Selector string `json:"sel"`
	Offset   int    `json:"off"`
}

func encodePatternsCursor(c patternsCursor) string {
	b, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodePatternsCursor(s string) (patternsCursor, error) {
	if s == "" {
		return patternsCursor{}, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return patternsCursor{}, query.Invalidf("list_patterns: invalid cursor")
	}
	var c patternsCursor
	if err := json.Unmarshal(b, &c); err != nil || c.Offset < 0 {
		return patternsCursor{}, query.Invalidf("list_patterns: invalid cursor")
	}
	return c, nil
}

// matchingPatternsCursor decodes req's cursor, rejecting one produced by a different
// sort or selector.
func matchingPatternsCursor(req PatternsRequest, selFP string) (patternsCursor, error) {
	cur, err := decodePatternsCursor(req.Cursor)
	if err != nil || req.Cursor == "" {
		return cur, err
	}
	if cur.Sort != req.Sort {
		return cur, query.Invalidf("list_patterns: cursor sort %q does not match request sort %q", cur.Sort, req.Sort)
	}
	if cur.Selector != selFP {
		return cur, query.Invalidf("list_patterns: cursor does not match request selector (time or filters changed)")
	}
	return cur, nil
}

// selectorFingerprint is a short, stable digest of the request selector, stored
// in the cursor so a later page can prove it is resuming the same query. It is
// computed from the raw (pre-resolve) selector so relative time bounds like
// "now-1h" fingerprint identically across pages.
func selectorFingerprint(sel selector.Selector) string {
	b, _ := json.Marshal(sel)
	sum := sha256.Sum256(b)
	return base64.RawURLEncoding.EncodeToString(sum[:8])
}
