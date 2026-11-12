package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/datafocal/datafocal/internal/query"
)

// AnswerCacheBytes caps the encoded answers the registry caches.
const AnswerCacheBytes = 64 << 20

// Caching is whether a tool's answer to a call is served again to the same call.
type Caching int

const (
	// CacheBySnapshot is a tool whose answer is a function of its args and the snapshot.
	CacheBySnapshot Caching = iota + 1
	// NoCache is a tool whose answer reads more than its args and the snapshot.
	NoCache
)

// answerCache caches tools' encoded answers by tool, args and snapshot, bounded by bytes alone since an
// answer is never stale and an old snapshot's fall out as the least recently used.
type answerCache struct {
	snapshot  func(context.Context) (string, error)
	cache     *lru.Cache[[sha256.Size]byte, []byte]
	bytes     atomic.Int64
	computing *singleflight.Group

	hits, shared, misses, evictions, notStored atomic.Int64
}

func newAnswers(snapshot func(context.Context) (string, error)) *answerCache {
	a := &answerCache{snapshot: snapshot, computing: new(singleflight.Group)}
	// NewWithEvict only errors on a non-positive size.
	cache, _ := lru.NewWithEvict(math.MaxInt, func(_ [sha256.Size]byte, answer []byte) {
		a.bytes.Add(-int64(len(answer)))
		a.evictions.Add(1)
	})
	a.cache = cache
	return a
}

// answer is td's answer to raw, served from the cache or another caller's computation of the same
// call when its tool is CacheBySnapshot.
func (a *answerCache) answer(ctx context.Context, td ToolDef, raw json.RawMessage) (any, error) {
	o := outcomeOf(ctx)
	o.noteVars(raw)
	if td.Caching == NoCache {
		o.cache = "off"
		return td.Handler(ctx, raw)
	}
	key, err := a.key(ctx, td.Name, raw)
	if err != nil {
		return nil, err
	}
	o.cacheKey = hex.EncodeToString(key[:6])
	if hit, ok := a.cache.Get(key); ok {
		a.hits.Add(1)
		o.cache, o.answerBytes = "hit", len(hit)
		return json.RawMessage(hit), nil
	}
	return a.compute(ctx, td, raw, key)
}

// compute is td's answer to raw, computed once however many callers ask for it at once.
func (a *answerCache) compute(ctx context.Context, td ToolDef, raw json.RawMessage, key [sha256.Size]byte) (any, error) {
	var mine any
	var led bool
	var notStored string
	flight := a.computing.DoChan(string(key[:]), func() (_ any, err error) {
		// A computation that finished between the caller's miss and this one has filled the cache.
		if answer, ok := a.cache.Peek(key); ok {
			return answer, nil
		}
		led = true
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic: %v", p)
			}
		}()
		if mine, err = td.Handler(ctx, raw); err != nil {
			return nil, err
		}
		var answer []byte
		answer, notStored, err = a.keep(ctx, key, mine)
		return answer, err
	})
	select {
	case r := <-flight:
		return a.served(outcomeOf(ctx), r, led, mine, notStored)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// served is a computation's answer to the caller that led it, its own mine, or to one that
// waited on it, the encoding the leader kept, and how each was served.
func (a *answerCache) served(o *outcome, r singleflight.Result, led bool, mine any, notStored string) (any, error) {
	answer, _ := r.Val.([]byte)
	o.cache, o.answerBytes = "shared", len(answer)
	switch {
	case !led:
		a.shared.Add(1)
		if r.Err == nil {
			return json.RawMessage(answer), nil
		}
	case r.Err != nil:
		a.notStored.Add(1)
		o.cache, o.cacheReason = "not_stored", "failed"
	case notStored != "":
		a.notStored.Add(1)
		o.cache, o.cacheReason = "not_stored", notStored
		return mine, nil
	default:
		a.misses.Add(1)
		o.cache = "miss"
		return mine, nil
	}
	return nil, r.Err
}

// key is the tool, its args with their keys sorted, and the snapshot they pin or else the current one.
func (a *answerCache) key(ctx context.Context, tool string, raw json.RawMessage) ([sha256.Size]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var args any
	if err := dec.Decode(&args); err != nil {
		return [sha256.Size]byte{}, query.Invalidf("decode request: %v", err)
	}
	fields, _ := args.(map[string]any)
	snapshot, _ := fields["snapshot"].(string)
	if snapshot == "" {
		var err error
		if snapshot, err = a.snapshot(ctx); err != nil {
			return [sha256.Size]byte{}, err
		}
	}
	canonical, err := json.Marshal(args)
	return sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s", tool, snapshot, canonical)), err
}

// keep encodes v as a hit serves it and caches it, unless the call read the clock, failed or ended,
// or the answer is over a tenth of the cache, which it says why.
func (a *answerCache) keep(ctx context.Context, key [sha256.Size]byte, v any) (answer []byte, notStored string, err error) {
	answer, err = json.Marshal(v.(interface{ AsCached() any }).AsCached())
	switch {
	case err != nil:
		return nil, "", err
	case query.CallOf(ctx).ReadClock.Load():
		return answer, "clock", nil
	case ctx.Err() != nil:
		return answer, "failed", nil
	case len(answer) > AnswerCacheBytes/10:
		return answer, "oversize", nil
	}
	// Concurrent adds of one answer must count its bytes once.
	if present, _ := a.cache.ContainsOrAdd(key, answer); !present {
		a.bytes.Add(int64(len(answer)))
	}
	for a.bytes.Load() > AnswerCacheBytes {
		if _, _, ok := a.cache.RemoveOldest(); !ok {
			break
		}
	}
	return answer, "", nil
}

func (a *answerCache) stats() query.CacheStats {
	return query.CacheStats{Entries: a.cache.Len(), Bytes: a.bytes.Load(), CapBytes: AnswerCacheBytes,
		Hits: a.hits.Load(), Shared: a.shared.Load(), Misses: a.misses.Load(), Evictions: a.evictions.Load(), NotStored: a.notStored.Load()}
}
