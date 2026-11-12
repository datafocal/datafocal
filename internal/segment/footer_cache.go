package segment

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	lru "github.com/hashicorp/golang-lru/v2"
	"golang.org/x/sync/singleflight"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/sketch"
	"github.com/datafocal/datafocal/internal/storage"
)

const (
	// FooterCacheBytes caps the decoded footers a reader caches.
	FooterCacheBytes = 256 << 20
	// ChunkCacheBytes caps the decoded column chunks it caches, since decoding a chunk is most
	// of what a repeated query spends.
	ChunkCacheBytes = 128 << 20
	// ChunkStageBytes caps the chunks one request holds for the cache until it ends.
	ChunkStageBytes = 16 << 20
	// filterCacheEntries caps the filter sections it holds, each far larger than a footer.
	filterCacheEntries = 512
)

// footerCache is a Reader holding an LRU of sealed, immutable footers by object key, so
// pruning and planning repeat no footer GETs, and LRUs of the filter sections and the
// merged segments' filters objects planning reads. A miss already being read is waited
// on, not read again.
type footerCache struct {
	inner   storeReader
	footers *lru.Cache[string, Footer]
	filters *lru.Cache[filterKey, *sketch.XorFilter]
	grams   *lru.Cache[string, Grams]
	merged  *lru.Cache[string, map[string]*sketch.XorFilter]
	chunks  *lru.Cache[chunkKey, ColumnChunk]
	budget  int64
	bytes   atomic.Int64
	// chunkBytes is the decoded bytes chunks holds, kept under ChunkCacheBytes.
	chunkBytes atomic.Int64
	// The chunks served from the cache, waited on and read, and evicted, since it was made.
	chunkHits, chunkShared, chunkReads, chunkEvictions atomic.Int64
	// reading makes concurrent misses for one object one read: the first caller reads and
	// bills it, and the rest wait and are billed nothing, as for a hit.
	reading singleflight.Group
}

// chunkKey names one column's chunk of one row group in one segment object.
type chunkKey struct {
	object   string
	rowGroup int
	column   string
}

// filterKey names one column's filter section in one segment object.
type filterKey struct {
	object, column string
}

// CachingReader is a Reader that reports what its chunk cache holds and has counted.
type CachingReader interface {
	Reader
	ChunkCache() query.CacheStats
}

// NewFooterCache returns a Reader over the segment objects in store that caches their
// footers and filter sections; it is safe for concurrent use.
func NewFooterCache(store storage.Store) CachingReader {
	c := &footerCache{inner: storeReader{store: store}, budget: FooterCacheBytes}
	// New only errors on a non-positive size, and the sizes are positive constants.
	footers, err := lru.NewWithEvict[string, Footer](math.MaxInt, func(_ string, f Footer) {
		c.bytes.Add(-f.size)
	})
	if err != nil {
		panic("segment: footer cache size: " + err.Error())
	}
	filters, err := lru.New[filterKey, *sketch.XorFilter](filterCacheEntries)
	if err != nil {
		panic("segment: filter cache size: " + err.Error())
	}
	merged, err := lru.New[string, map[string]*sketch.XorFilter](filterCacheEntries)
	if err != nil {
		panic("segment: filters object cache size: " + err.Error())
	}
	grams, err := lru.New[string, Grams](4096)
	if err != nil {
		panic("segment: grams cache size: " + err.Error())
	}
	chunks, err := lru.NewWithEvict[chunkKey, ColumnChunk](math.MaxInt, func(_ chunkKey, chunk ColumnChunk) {
		c.chunkBytes.Add(-chunk.size())
		c.chunkEvictions.Add(1)
	})
	if err != nil {
		panic("segment: chunk cache size: " + err.Error())
	}
	c.grams, c.chunks = grams, chunks
	c.footers, c.filters, c.merged = footers, filters, merged
	return c
}

// Footer serves a hit unbilled, since it reached no object and costs the caller nothing.
func (c *footerCache) Footer(ctx context.Context, loc ObjectLocation) (Footer, Cost, error) {
	if f, ok := c.footers.Get(loc.Key); ok {
		return f, Cost{}, nil
	}
	var cost Cost
	read := func() (any, error) {
		// A read that finished between the miss above and this one has filled the cache.
		if f, ok := c.footers.Get(loc.Key); ok {
			return f, nil
		}
		f, readCost, err := c.fill(ctx, loc)
		cost = readCost
		return f, err
	}
	select {
	case r := <-c.reading.DoChan(loc.Key, read):
		if r.Err == nil || cost != (Cost{}) || !r.Shared {
			f, _ := r.Val.(Footer)
			return f, cost, r.Err
		}
		// A waiter reads for itself, since the reader's failure may be its own cancelled context.
		f, err := read()
		footer, _ := f.(Footer)
		return footer, cost, err
	case <-ctx.Done():
		return Footer{}, Cost{}, ctx.Err()
	}
}

// fill reads a missed footer from the inner Reader and caches it.
func (c *footerCache) fill(ctx context.Context, loc ObjectLocation) (Footer, Cost, error) {
	f, cost, err := c.inner.Footer(ctx, loc)
	if err != nil {
		return Footer{}, cost, err
	}
	if f.size > c.budget {
		return f, cost, nil
	}
	// Concurrent misses for the same object must charge its bytes only once.
	if present, _ := c.footers.ContainsOrAdd(loc.Key, f); !present {
		c.bytes.Add(f.size)
	}
	for c.bytes.Load() > c.budget {
		if _, _, ok := c.footers.RemoveOldest(); !ok {
			break
		}
	}
	return f, cost, nil
}

// Columns serves each chunk cached whole unbilled, charging ctx's memory budget for it as a read
// would, and reads the rest. A chunk read whole is staged under a ctx from CacheChunksAfter and
// cached when it ends, so a request's own reads and costs are an uncached reader's.
func (c *footerCache) Columns(ctx context.Context, loc ObjectLocation, footer Footer, rowGroup int, rows []int, cols ...string) (map[string]ColumnChunk, Cost, error) {
	out := make(map[string]ColumnChunk, len(cols))
	call := query.CallOf(ctx)
	var missing []string
	for _, col := range cols {
		chunk, ok := c.chunks.Get(chunkKey{loc.Key, rowGroup, col})
		if !ok {
			missing = append(missing, col)
			continue
		}
		if err := query.ChargeMem(ctx, chunk.size()); err != nil {
			return nil, Cost{}, err
		}
		out[col] = chunk
	}
	call.ChunksHit.Add(int64(len(out)))
	c.chunkHits.Add(int64(len(out)))
	if len(missing) == 0 {
		return out, Cost{}, nil
	}
	var read map[string]ColumnChunk
	var cost Cost
	var err error
	if rows == nil {
		read, cost, err = c.readWhole(ctx, loc, footer, rowGroup, missing, out)
	} else {
		read, cost, err = c.inner.Columns(ctx, loc, footer, rowGroup, rows, missing...)
	}
	if err != nil {
		return nil, cost, err
	}
	call.ChunksRead.Add(int64(len(read)))
	c.chunkReads.Add(int64(len(read)))
	stage, _ := ctx.Value(chunkStageKey{}).(*chunkStage)
	for col, chunk := range read {
		out[col] = chunk
		if stage != nil && rows == nil {
			stage.add(c, chunkKey{loc.Key, rowGroup, col}, chunk)
		}
	}
	return out, cost, nil
}

// readWhole reads cols' chunks whole, waiting into out, charged as for a hit, for those another
// request is reading, and takes cols in order so that two requests never wait on each other.
func (c *footerCache) readWhole(ctx context.Context, loc ObjectLocation, footer Footer, rowGroup int, cols []string, out map[string]ColumnChunk) (read map[string]ColumnChunk, cost Cost, err error) {
	ready, call := make(chan struct{}), query.CallOf(ctx)
	var mine []string
	var flights []<-chan singleflight.Result
	// The reads this call leads end before it does, so a later miss is not served by them.
	defer func() {
		close(ready)
		for _, flight := range flights {
			<-flight
		}
	}()
	for _, col := range slices.Sorted(slices.Values(cols)) {
		leading := make(chan struct{})
		flight := c.reading.DoChan(fmt.Sprintf("%s\x00%d\x00%s", loc.Key, rowGroup, col), func() (any, error) {
			close(leading)
			<-ready
			chunk, ok := read[col]
			return readChunk{chunk, ok, call}, err
		})
		select {
		case <-leading:
			mine, flights = append(mine, col), append(flights, flight)
		case r := <-flight:
			// A waiter reads for itself when the read failed, which may be the reader's cancelled
			// context, or was its own request's, whose cost stays an uncached reader's.
			got, _ := r.Val.(readChunk)
			if r.Err != nil || got.by == call {
				mine = append(mine, col)
				continue
			}
			if err := c.share(ctx, col, got, out); err != nil {
				return nil, Cost{}, err
			}
		case <-ctx.Done():
			return nil, Cost{}, ctx.Err()
		}
	}
	return c.inner.Columns(ctx, loc, footer, rowGroup, nil, mine...)
}

// share puts a chunk another request read into out, charging ctx for it as for a hit.
func (c *footerCache) share(ctx context.Context, col string, got readChunk, out map[string]ColumnChunk) error {
	if !got.ok {
		return nil
	}
	if err := query.ChargeMem(ctx, got.chunk.size()); err != nil {
		return err
	}
	out[col] = got.chunk
	query.CallOf(ctx).ChunksShared.Add(1)
	c.chunkShared.Add(1)
	return nil
}

// readChunk is a chunk read whole for the requests waiting on it, ok when the row group has it,
// and the call that read it.
type readChunk struct {
	chunk ColumnChunk
	ok    bool
	by    *query.Call
}

// ChunkCache counts a chunk read by the request leading it as a miss.
func (c *footerCache) ChunkCache() query.CacheStats {
	return query.CacheStats{Entries: c.chunks.Len(), Bytes: c.chunkBytes.Load(), CapBytes: ChunkCacheBytes,
		Hits: c.chunkHits.Load(), Shared: c.chunkShared.Load(), Misses: c.chunkReads.Load(), Evictions: c.chunkEvictions.Load()}
}

type chunkStageKey struct{}

// chunkStage is the chunks a request read whole, for the cache once it ends.
type chunkStage struct {
	mu     sync.Mutex
	cache  *footerCache
	chunks map[chunkKey]ColumnChunk
	bytes  int64
}

func (s *chunkStage) add(cache *footerCache, key chunkKey, chunk ColumnChunk) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.chunks[key]; ok || s.bytes+chunk.size() > ChunkStageBytes {
		return
	}
	s.cache, s.chunks[key], s.bytes = cache, chunk, s.bytes+chunk.size()
}

// CacheChunksAfter returns ctx, under which a caching Reader stages the chunks it reads whole,
// and done, which caches them.
func CacheChunksAfter(ctx context.Context) (_ context.Context, done func()) {
	stage := &chunkStage{chunks: map[chunkKey]ColumnChunk{}}
	return context.WithValue(ctx, chunkStageKey{}, stage), func() {
		stage.mu.Lock()
		defer stage.mu.Unlock()
		for key, chunk := range stage.chunks {
			stage.cache.keep(key, chunk)
		}
	}
}

// keep caches a chunk read whole, evicting the least recently used past ChunkCacheBytes.
func (c *footerCache) keep(key chunkKey, chunk ColumnChunk) {
	if chunk.size() > ChunkCacheBytes {
		return
	}
	// Concurrent misses for the same chunk must charge its bytes only once.
	if present, _ := c.chunks.ContainsOrAdd(key, chunk); !present {
		c.chunkBytes.Add(chunk.size())
	}
	for c.chunkBytes.Load() > ChunkCacheBytes {
		if _, _, ok := c.chunks.RemoveOldest(); !ok {
			break
		}
	}
}

func (c *footerCache) Records(ctx context.Context, loc ObjectLocation) ([]Record, Footer, Cost, error) {
	return c.inner.Records(ctx, loc)
}

func (c *footerCache) RowGroupRecords(ctx context.Context, loc ObjectLocation, footer Footer, rowGroup int) ([]Record, Cost, error) {
	return c.inner.RowGroupRecords(ctx, loc, footer, rowGroup)
}

// Rollup passes through: the summary reads a rollup once, and the planner only for a needle.
func (c *footerCache) Rollup(ctx context.Context, loc ObjectLocation) ([]byte, Cost, error) {
	return c.inner.Rollup(ctx, loc)
}

// Filters serves a cached filters object unbilled, as Footer does a cached footer.
func (c *footerCache) Filters(ctx context.Context, loc ObjectLocation) (map[string]*sketch.XorFilter, Cost, error) {
	if filters, ok := c.merged.Get(loc.Key); ok {
		return filters, Cost{}, nil
	}
	return c.fillMerged(ctx, loc)
}

// fillMerged reads a missed filters object from the inner Reader and caches it.
func (c *footerCache) fillMerged(ctx context.Context, loc ObjectLocation) (map[string]*sketch.XorFilter, Cost, error) {
	filters, cost, err := c.inner.Filters(ctx, loc)
	if err == nil {
		c.merged.Add(loc.Key, filters)
	}
	return filters, cost, err
}

// MayContain serves a cached filter section unbilled, as Footer does a cached footer.
func (c *footerCache) MayContain(ctx context.Context, loc ObjectLocation, footer Footer, column, value string) (filtered, may bool, cost Cost, err error) {
	key := filterKey{object: loc.Key, column: column}
	filter, ok := c.filters.Get(key)
	if !ok {
		// Concurrent misses for one section are one read, billed to the first caller.
		v, err, _ := c.reading.Do(loc.Key+"\x00"+column, func() (any, error) {
			if filter, ok := c.filters.Get(key); ok {
				return filter, nil
			}
			filter, readCost, err := c.fillFilter(ctx, loc, footer, column)
			cost = readCost
			return filter, err
		})
		if err != nil {
			return false, true, cost, err
		}
		filter, _ = v.(*sketch.XorFilter)
	}
	return filter != nil, filter == nil || filter.MayContain(value), cost, nil
}

// fillFilter reads a missed filter section from the inner Reader and caches it.
func (c *footerCache) fillFilter(ctx context.Context, loc ObjectLocation, footer Footer, column string) (*sketch.XorFilter, Cost, error) {
	filter, cost, err := c.inner.filter(ctx, loc, footer, column)
	if err != nil || filter == nil {
		return nil, cost, err
	}
	c.filters.Add(filterKey{object: loc.Key, column: column}, filter)
	return filter, cost, nil
}

func (c *footerCache) Grams(ctx context.Context, loc ObjectLocation, footer Footer) (Grams, Cost, error) {
	if grams, ok := c.grams.Get(loc.Key); ok {
		return grams, Cost{}, nil
	}
	return c.fillGrams(ctx, loc, footer)
}

func (c *footerCache) fillGrams(ctx context.Context, loc ObjectLocation, footer Footer) (Grams, Cost, error) {
	grams, cost, err := c.inner.Grams(ctx, loc, footer)
	if err == nil {
		c.grams.Add(loc.Key, grams)
	}
	return grams, cost, err
}
