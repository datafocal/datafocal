package segment

import (
	"context"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/value"
)

func TestFooterCacheByteBudget(t *testing.T) {
	sealed, _ := buildTestSegment(t, 10)
	obj, err := sealed.Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)
	region, err := footerRegionLen(obj, int64(len(obj)))
	require.NoError(t, err)
	raw, err := zstdDecoder.DecodeAll(obj[int64(len(obj))-region:len(obj)-trailerLen], nil)
	require.NoError(t, err)
	assert.Equal(t, heldSize(t, raw, footer), footer.size)
	sealed.PatternProfile.Name = strings.Repeat("x", int(footer.size)*4)
	large, err := sealed.Encode()
	require.NoError(t, err)
	objects := map[string][]byte{"first": obj, "second": obj, "third": obj, "last": obj, "large": large}
	cache := NewFooterCache(storage.Fake{GetSuffixFn: func(key string, n int64) ([]byte, error) {
		object := objects[key]
		return object[max(0, int64(len(object))-n):], nil
	}}).(*footerCache)
	cache.budget = 2 * footer.size
	ctx := context.Background()
	for _, key := range []string{"first", "second", "third", "last", "first"} {
		got, cost, err := cache.Footer(ctx, ObjectLocation{Key: key})
		require.NoError(t, err)
		assert.Equal(t, footer, got)
		assert.Equal(t, Cost{GETs: 1}, cost, key)
	}
	for range 2 {
		got, cost, err := cache.Footer(ctx, ObjectLocation{Key: "large"})
		require.NoError(t, err)
		assert.Greater(t, got.size, cache.budget)
		assert.Equal(t, Cost{GETs: 1}, cost)
	}
	for _, key := range []string{"last", "first"} {
		_, cost, err := cache.Footer(ctx, ObjectLocation{Key: key})
		require.NoError(t, err)
		assert.Equal(t, Cost{}, cost, key)
	}
	assert.Equal(t, cache.budget, cache.bytes.Load())
}

// heldSize is what decoding raw into f holds: raw, with each distinct-count section swapped
// for the bytes its sketches hold.
func heldSize(t *testing.T, raw []byte, f Footer) int64 {
	t.Helper()
	size := int64(len(raw))
	for _, v := range append([]Footer{f}, slices.Collect(maps.Values(f.services))...) {
		size -= 4 // the section's entry count
		for column, sketch := range v.DistinctCounts {
			encoded, err := sketch.MarshalBinary()
			require.NoError(t, err)
			size += sketch.Size() - int64(4+len(column)+4+len(encoded))
		}
		for name, stats := range v.fields {
			if v.DistinctCounts[name] != nil {
				continue // shared with the view's distinct counts, encoded as none
			}
			encoded, err := stats.Distinct.MarshalBinary()
			require.NoError(t, err)
			size += stats.Distinct.Size() - int64(len(encoded))
		}
	}
	return size
}

func TestFooterCacheConcurrentByteAccounting(t *testing.T) {
	sealed, _ := buildTestSegment(t, 10)
	obj, err := sealed.Encode()
	require.NoError(t, err)
	footer, err := footerFromTail(obj)
	require.NoError(t, err)
	cache := NewFooterCache(storage.Fake{GetSuffixFn: func(_ string, n int64) ([]byte, error) {
		return obj[max(0, int64(len(obj))-n):], nil
	}}).(*footerCache)
	cache.budget = 2 * footer.size
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i := range 128 {
		workers.Go(func() {
			<-start
			_, _, err := cache.Footer(context.Background(), ObjectLocation{Key: fmt.Sprint(i % 8)})
			assert.NoError(t, err)
		})
	}
	close(start)
	workers.Wait()
	var total int64
	for _, f := range cache.footers.Values() {
		total += f.size
	}
	assert.Equal(t, total, cache.bytes.Load())
	assert.LessOrEqual(t, total, cache.budget)
}

// TestFooterCacheReadsAConcurrentMissOnce: many callers missing one footer at once make one
// GET between them, billed to one caller, and every caller gets the footer.
func TestFooterCacheReadsAConcurrentMissOnce(t *testing.T) {
	obj, err := Sealed{Records: []Record{LogRecord(time.Unix(0, 0), "info", "x", nil)}}.Encode()
	require.NoError(t, err)
	var gets atomic.Int64
	cache := NewFooterCache(storage.Fake{GetSuffixFn: func(_ string, n int64) ([]byte, error) {
		gets.Add(1)
		time.Sleep(time.Millisecond) // long enough for the other callers to miss too
		return obj[max(0, int64(len(obj))-n):], nil
	}})
	start := make(chan struct{})
	var (
		workers sync.WaitGroup
		billed  atomic.Int64
	)
	for range 64 {
		workers.Go(func() {
			<-start
			_, cost, err := cache.Footer(context.Background(), ObjectLocation{Key: "seg"})
			assert.NoError(t, err)
			billed.Add(int64(cost.GETs))
		})
	}
	close(start)
	workers.Wait()
	assert.Equal(t, int64(2), gets.Load(), "one footer read: its trailer, then its region")
	assert.Equal(t, int64(1), billed.Load())
}

// TestFooterCacheHoldsDecodedChunks: a chunk a request reads whole is cached when the request
// ends, not before, so the request's own reads cost what an uncached reader's do and a later
// request reads it from memory with no GET; one read for some rows is not kept.
func TestFooterCacheHoldsDecodedChunks(t *testing.T) {
	sealed, _ := buildTestSegment(t, 600)
	obj, err := sealed.Encode()
	require.NoError(t, err)
	var gets atomic.Int64
	cache := NewFooterCache(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn: func(_ string, off, n int64) ([]byte, error) {
			gets.Add(1)
			return obj[off : off+n], nil
		},
	})
	loc := ObjectLocation{Key: "seg"}
	footer, _, err := cache.Footer(context.Background(), loc)
	require.NoError(t, err)

	first, done := CacheChunksAfter(context.Background())
	read, _, err := cache.Columns(first, loc, footer, 0, nil, "service")
	require.NoError(t, err)
	_, _, err = cache.Columns(first, loc, footer, 0, nil, "service")
	require.NoError(t, err)
	assert.Equal(t, int64(2), gets.Load(), "a request's own reads are not served from what it staged")
	_, _, err = cache.Columns(first, loc, footer, 0, []int{1}, "level")
	require.NoError(t, err)
	done()

	second, _ := CacheChunksAfter(context.Background())
	again, cost, err := cache.Columns(second, loc, footer, 0, nil, "service")
	require.NoError(t, err)
	assert.Equal(t, int64(3), gets.Load(), "the next request reads it from memory")
	assert.Zero(t, cost.GETs)
	assert.Equal(t, read["service"].Rows(), again["service"].Rows())
	_, _, err = cache.Columns(second, loc, footer, 0, []int{1}, "level")
	require.NoError(t, err)
	assert.Equal(t, int64(4), gets.Load(), "a chunk read for some rows is not kept")
}

// awaited is a context that says when it is first waited on.
type awaited struct {
	context.Context
	once  sync.Once
	waits chan struct{}
}

func (a *awaited) Done() <-chan struct{} {
	a.once.Do(func() { close(a.waits) })
	return a.Context.Done()
}

// TestFooterCacheSharesAChunkInFlight: a request missing a chunk another is reading waits for
// that read rather than making its own, and is charged its memory as for a hit.
func TestFooterCacheSharesAChunkInFlight(t *testing.T) {
	sealed, _ := buildTestSegment(t, 600)
	obj, err := sealed.Encode()
	require.NoError(t, err)
	var gets atomic.Int64
	entered, release := make(chan struct{}, 2), make(chan struct{})
	cache := NewFooterCache(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn: func(_ string, off, n int64) ([]byte, error) {
			gets.Add(1)
			entered <- struct{}{}
			<-release
			return obj[off : off+n], nil
		},
	})
	loc := ObjectLocation{Key: "seg"}
	footer, _, err := cache.Footer(context.Background(), loc)
	require.NoError(t, err)
	const budget = 1 << 30
	read := make(chan map[string]ColumnChunk, 2)
	columns := func(ctx context.Context) {
		chunks, _, err := cache.Columns(ctx, loc, footer, 0, nil, "service")
		assert.NoError(t, err)
		read <- chunks
	}
	reader, readerCall := query.WithCall(query.WithMemBudget(context.Background(), budget))
	waiting := &awaited{Context: context.Background(), waits: make(chan struct{})}
	waiter, waiterCall := query.WithCall(query.WithMemBudget(waiting, budget))
	go columns(reader)
	<-entered
	go columns(waiter)
	<-waiting.waits // the waiter is selecting on the read in flight
	close(release)
	size := (<-read)["service"].size()
	assert.Equal(t, (<-read)["service"].size(), size)
	assert.Equal(t, int64(1), gets.Load())
	assert.Equal(t, []int64{1, 0, 0, 1}, []int64{readerCall.ChunksRead.Load(), readerCall.ChunksShared.Load(), waiterCall.ChunksRead.Load(), waiterCall.ChunksShared.Load()})
	for _, ctx := range []context.Context{reader, waiter} {
		assert.NoError(t, query.ChargeMem(ctx, budget-size), "each was charged the chunk")
		assert.Error(t, query.ChargeMem(ctx, 1))
	}
}

// TestReader: the oracle is an in-memory decode of the same object, and a chunk
// bills the footer's byte range, since that is what Cost.BytesRead reports; a
// column the row group lacks is left out unread.
func TestReader(t *testing.T) {
	sealed, _ := buildTestSegment(t, 600)
	obj, err := encodeTable(sealed.table, nil, sealed.PatternProfile, sealed.patterns, 1<<10)
	require.NoError(t, err)
	wantFooter, err := footerFromTail(obj)
	require.NoError(t, err)
	values, err := readColumn(obj, wantFooter, 1, "service")
	require.NoError(t, err)
	br, _ := wantFooter.Offsets.Get(1, "service")

	ctx := context.Background()
	rd := NewReader(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return obj[off : off+n], nil },
	})
	footer, footerCost, err := rd.Footer(ctx, ObjectLocation{})
	require.NoError(t, err)
	columns, columnCost, err := rd.Columns(ctx, ObjectLocation{}, footer, 1, nil, "service", "absent")
	require.NoError(t, err)

	assert.Equal(t, Cost{GETs: 1}, footerCost, "an uncached read reaches the store and is billed")
	assert.Equal(t, wantFooter, footer)
	assert.Equal(t, []string{"service"}, slices.Collect(maps.Keys(columns)))
	assert.Equal(t, values, columns["service"].Rows())
	assert.Equal(t, Cost{GETs: 1, Bytes: br.Length}, columnCost)
}

// TestReaderChargesTheMemBudget: a request whose reads decode more than its budget fails
// too_large while a concurrent one under budget reads the same columns, and a footer the
// cache serves charges nothing.
func TestReaderChargesTheMemBudget(t *testing.T) {
	sealed, _ := buildTestSegment(t, 600)
	obj, err := sealed.Encode()
	require.NoError(t, err)
	rd := NewFooterCache(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return obj[off : off+n], nil },
	})
	loc := ObjectLocation{Key: "seg"}
	footer, _, err := rd.Footer(context.Background(), loc)
	require.NoError(t, err)

	read := func(budget int64) error {
		ctx := query.WithMemBudget(context.Background(), budget)
		if _, _, err := rd.Footer(ctx, loc); err != nil {
			return err
		}
		_, _, err := rd.Columns(ctx, loc, footer, 0, nil, "service", "message")
		return err
	}
	var over, under error
	var readers sync.WaitGroup
	readers.Go(func() { over = read(64) })
	readers.Go(func() { under = read(64 << 20) })
	readers.Wait()

	var apiErr *query.APIError
	require.ErrorAs(t, over, &apiErr)
	assert.Equal(t, query.ErrCodeTooLarge, apiErr.Code)
	assert.NoError(t, under)
	_, _, err = rd.Footer(query.WithMemBudget(context.Background(), 1), loc)
	assert.NoError(t, err, "a cached footer charges nothing")
}

// TestReaderRejectsCorruptTrailer: a trailer's footer length comes off disk untrusted,
// so one longer than the object fails rather than sizing a read past it.
func TestReaderRejectsCorruptTrailer(t *testing.T) {
	sealed, _ := buildTestSegment(t, 10)
	obj, err := sealed.Encode()
	require.NoError(t, err)
	binary.BigEndian.PutUint64(obj[len(obj)-trailerLen:], uint64(len(obj)))

	for name, object := range map[string][]byte{"too short": obj[len(obj)-4:], "footer longer than object": obj} {
		rd := NewReader(storage.Fake{GetSuffixFn: func(_ string, n int64) ([]byte, error) {
			return object[max(0, int64(len(object))-n):], nil
		}})
		_, _, err := rd.Footer(context.Background(), ObjectLocation{})
		assert.Error(t, err, name)
	}
}

// TestMayContain: of a segment's string columns only the one with too many values to
// count and no free text gets a filter section, which the cache reads once and which
// admits every value the column holds.
func TestMayContain(t *testing.T) {
	const orders = 50000
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	records := make([]Record, orders)
	for i := range records {
		records[i] = Record{Timestamp: base.Add(time.Duration(i) * time.Millisecond), Columns: []Column{
			{Name: "error", Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("order %d timed out", i)}},
			{Name: "order_id", Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("o-%d", i)}},
			{Name: "region", Type: value.TypeString, Value: value.Value{String: fmt.Sprintf("r-%d", i%5)}},
		}}
	}
	obj, err := Sealed{Records: records}.Encode()
	require.NoError(t, err)
	rd := NewFooterCache(storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return obj[max(0, int64(len(obj))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return obj[off : off+n], nil },
	})
	ctx, loc := context.Background(), ObjectLocation{Key: "seg"}
	footer, _, err := rd.Footer(ctx, loc)
	require.NoError(t, err)

	assert.Equal(t, []bool{false, true, false}, []bool{footer.HasFilter("error"), footer.HasFilter("order_id"), footer.HasFilter("region")})
	filtered, may, cost, err := rd.MayContain(ctx, loc, footer, "region", "r-9")
	require.NoError(t, err)
	assert.Equal(t, []any{false, true, Cost{}}, []any{filtered, may, cost}, "an unfiltered column reads nothing")

	var total Cost
	missed := 0
	for i := range orders {
		filtered, may, cost, err := rd.MayContain(ctx, loc, footer, "order_id", fmt.Sprintf("o-%d", i))
		require.NoError(t, err)
		if !filtered || !may {
			missed++
		}
		total.GETs, total.Bytes = total.GETs+cost.GETs, total.Bytes+cost.Bytes
	}
	assert.Zero(t, missed, "false negatives")
	assert.Equal(t, Cost{GETs: 1, Bytes: footer.filters["order_id"].Length}, total, "the cache reads the section once")
}

// TestRecordsReturnsTheSealedRecords reads an int, a list null on most rows and a column only
// the first rows hold back into the records the segment sealed, over row groups holding no
// chunk of a column none of their rows hold.
func TestRecordsReturnsTheSealedRecords(t *testing.T) {
	base := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	b := NewBuilder(BuilderConfig{Tenant: "acme", Stream: Stream{Service: "checkout"}, CreatedAt: base})
	for i := range 300 {
		cols := []Column{{Name: "attempt", Type: value.TypeInt, Value: value.Value{Int: int64(i)}}}
		if i%3 == 0 {
			cols = append(cols, Column{Name: "tags", Type: value.TypeString | value.TypeRepeated, Value: value.Value{List: []value.Value{{String: fmt.Sprint(i)}}}})
		}
		if i < 5 {
			cols = append(cols, Column{Name: "early", Type: value.TypeString, Value: value.Value{String: fmt.Sprint(i)}})
		}
		require.NoError(t, b.Append(Record{Timestamp: base.Add(time.Duration(i) * time.Millisecond), Columns: cols}, ParsedPattern{PatternID: fmt.Sprint(i % 4)}))
	}
	sealed := b.Seal()
	obj, err := encodeTable(sealed.table, nil, sealed.PatternProfile, sealed.patterns, 256)
	require.NoError(t, err)
	store := storage.NewFS(t.TempDir())
	require.NoError(t, store.Put(context.Background(), "seg", obj))

	got, footer, _, err := NewReader(store).Records(context.Background(), ObjectLocation{Key: "seg"})
	require.NoError(t, err)
	assert.Equal(t, rowsOf(sealed.table), got)
	require.Greater(t, footer.RowGroups, 2)
	var holding []int
	for g := range footer.RowGroups {
		if _, ok := footer.Offsets.Get(g, "early"); ok {
			holding = append(holding, g)
		}
	}
	assert.Equal(t, []int{0}, holding, "the groups holding a chunk of early")
}
