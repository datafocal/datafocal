// Package query holds the tool response, its meta and cost, and the typed request errors.
package query

import "encoding/json"

// Envelope wraps every tool response with the agent-critical metadata every
// caller needs to reason about the answer — how fresh, how complete, how
// expensive. The Skeleton stage introduces it;
// later tools return the same shape.
type Envelope[T any] struct {
	Data T    `json:"data"`
	Meta Meta `json:"meta"`
}

// Meta is the envelope sidecar: the snapshot the answer was computed against, how
// much of the work the answer needed actually got done (Coverage), whether
// sketches were used (Approx ⇒ bounded error), what it cost, and a rough token
// estimate.
//
// Coverage is 1.0 for a complete answer and only a complete answer. Pruning does
// not lower it — a row group its footer proves cannot match is work the answer
// never needed. What lowers it is work the answer needed and did not get: a
// truncating limit, a deadline reached mid-merge, a segment whose footer carries no
// sketch for the requested measure. An agent reading Coverage < 1 should widen or
// re-issue.
type Meta struct {
	Snapshot string  `json:"snapshot"`         // opaque pin: catalog version + summary version
	Cursor   string  `json:"cursor,omitempty"` // opaque, resumable pagination
	Coverage float64 `json:"coverage"`         // 0..1 of the needed work that was done; 1.0 = complete
	Approx   bool    `json:"approx"`           // used sketches ⇒ bounded error
	Cost     Cost    `json:"cost"`             //
	Tokens   int     `json:"token_estimate"`   //
	Cached   bool    `json:"cached,omitempty"` // served by the answer cache or another caller's computation

	// Definitions is the version of the definitions an SLO tool's answer read.
	Definitions string `json:"definitions,omitempty"`
}

// AsCached is e as the answer cache serves it again: cached, and costing nothing, since serving
// it reads nothing.
func (e Envelope[T]) AsCached() any {
	e.Meta.Cached, e.Meta.Cost = true, Cost{}
	return e
}

// Cost is the per-call resource accounting: object-store GETs and bytes read on
// the read path. BytesRead is encoded bytes off the wire, not decoded value bytes,
// so it tracks what the object store bills.
type Cost struct {
	// GETs is object-store requests this call actually made, not reads it attempted:
	// a footer the cache served reached no object and is not counted. The difference
	// is what the caller is charged, which is the number §5a #8 shows an owner.
	GETs      int   `json:"gets"`
	BytesRead int64 `json:"bytes_read"`
	// FootersDecoded is the footers this call read and decoded, the ones a cache did not hold.
	FootersDecoded int `json:"footers_decoded,omitempty"`
	// Segments is the segments in scope after pruning, each once per plan that kept it.
	Segments int `json:"segments,omitempty"`
}

// EstimateTokens is a rough token estimate for a tool response (~4 bytes per
// token), used to fill Meta.Tokens. It lives here, beside the field it feeds, so
// every tool reports the estimate the same way rather than each keeping its own
// copy. A response that cannot marshal is reported as 0 rather than failing the
// call — the estimate is advisory metadata, not part of the answer.
func EstimateTokens[T any](resp T) int { return TokensOf(EncodedSize(resp)) }

// EncodedSize is v's JSON length, counted as encoded rather than kept; 0 if it does not encode.
func EncodedSize[T any](v T) int {
	var n byteCount
	if err := json.NewEncoder(&n).Encode(v); err != nil {
		return 0
	}
	return int(n) - 1 // less the newline Encode ends with
}

// TokensOf is the token estimate of a response n bytes long.
func TokensOf(n int) int { return (n + 3) / 4 }

// byteCount is the bytes written to it.
type byteCount int

func (c *byteCount) Write(p []byte) (int, error) {
	*c += byteCount(len(p))
	return len(p), nil
}
