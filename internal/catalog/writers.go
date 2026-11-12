package catalog

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/storage"
)

// bucketWidth is the span of the store's clock a writer bucket holds.
const bucketWidth = 5 * time.Minute

// listSlack is how far before its last listing a load lists again, past which a commit is put
// again at a later time, so one still in flight when a load listed is read by the next.
const listSlack = time.Minute

// writerKey is a commit's key, ordered within its bucket by when it was put, so a load lists
// only what is new.
func (c *storeCatalog) writerKey(at time.Time, writer string, seq int) string {
	return fmt.Sprintf("%s%s/%013d-%s-%020d.json", c.writers, bucketOf(at), at.UnixMilli(), writer, seq)
}

func bucketOf(t time.Time) string { return t.UTC().Truncate(bucketWidth).Format("20060102T1504") }

// storeNow is the object store's clock where it tells one, whatever this host's clock says.
func (c *storeCatalog) storeNow() time.Time {
	if clock, ok := c.store.(interface{ Now() time.Time }); ok {
		return clock.Now()
	}
	return time.Now()
}

// originOf is the writer and seq of a commit at <writers><bucket>/<ms>-<writer>-<seq>.json, or at
// <writers><bucket>/<writer>/<seq>.json as commits were keyed before.
func (c *storeCatalog) originOf(key string) (origin, bool) {
	parts := strings.Split(strings.TrimPrefix(key, c.writers), "/")
	if len(parts) == 3 {
		seq, err := seqOf(parts[2])
		return origin{Writer: parts[1], Seq: seq}, err == nil
	}
	_, rest, ok := strings.Cut(strings.TrimSuffix(path.Base(key), ".json"), "-")
	i := strings.LastIndex(rest, "-")
	if len(parts) != 2 || !ok || i < 0 {
		return origin{}, false
	}
	seq, err := seqOf(rest[i+1:])
	return origin{Writer: rest[:i], Seq: seq}, err == nil
}

// appendAsWriter commits body at this writer's next seq, keyed by the store clock, and again at a
// later time if it landed more than listSlack after, as a reader may have listed past the first.
func (c *storeCatalog) appendAsWriter(ctx context.Context, body []byte) error {
	if c.seq < 0 {
		if err := c.recoverSeq(ctx); err != nil {
			return err
		}
	}
	for {
		at := c.storeNow()
		err := c.putCommit(ctx, c.writerKey(at, c.writer, c.seq), body)
		switch {
		case errors.Is(err, storage.ErrExists):
			c.seq++
		case err != nil:
			return err
		case c.storeNow().Sub(at) <= listSlack:
			c.seq++
			return nil
		}
	}
}

// recoverSeq finds this writer's next seq past the snapshot's count and its newest key, listing its
// own keys a bucket at a time back to the later of its last run's start, which each run marks, and
// the snapshot's bucket, and one past the newest it finds, as the clock may step back. It commits
// that newest again in the current bucket, in case the last run died before doing so.
func (c *storeCatalog) recoverSeq(ctx context.Context) error {
	snap, err := c.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	now, runs := c.storeNow(), strings.TrimSuffix(c.writers, "w/")+"runs/"+c.writer+"/"
	var started, keys []string
	if err := storage.Retry(ctx, func() (err error) { started, err = c.store.List(ctx, runs, ""); return err }); err != nil {
		return err
	}
	mark := func() error { return c.store.PutIfAbsent(ctx, runs+bucketOf(now.Add(-bucketWidth)), nil) }
	if err := storage.Retry(ctx, mark); !errors.Is(cmp.Or(err, storage.ErrExists), storage.ErrExists) {
		return fmt.Errorf("catalog: mark writer run: %w", err)
	}
	seq, oldest, newest := snap.Counts[c.writer], bucketOf(now.Add(-bucketWidth)), ""
	if len(started) > 0 {
		oldest = max(path.Base(started[len(started)-1]), snap.Bucket)
	}
	for at := now; bucketOf(at) >= oldest; at = at.Add(-bucketWidth) {
		if err := storage.Retry(ctx, func() (err error) {
			keys, err = c.store.List(ctx, c.writers+bucketOf(at)+"/", "")
			return err
		}); err != nil {
			return err
		}
		if key, o := c.newestOf(keys); key != "" && o.Seq >= seq {
			seq, newest, oldest = o.Seq+1, key, max(oldest, bucketOf(at.Add(-bucketWidth)))
		}
	}
	c.seq = seq
	if newest == "" || c.landedSince(newest, now.Add(-listSlack)) {
		return nil
	}
	body, err := c.store.GetSuffix(ctx, newest, math.MaxInt64)
	if err == nil {
		c.seq--
		err = c.appendAsWriter(ctx, body)
	}
	if err != nil {
		c.seq = -1
	}
	return err
}

// newestOf is this writer's commit of the highest seq among keys, and its origin.
func (c *storeCatalog) newestOf(keys []string) (newest string, at origin) {
	at.Seq = -1
	for _, key := range keys {
		if o, _ := c.originOf(key); o.Writer == c.writer && o.Seq > at.Seq {
			newest, at = key, o
		}
	}
	return newest, at
}

// landedSince reports whether the commit at key was put at or after t; one keyed before this layout
// has no time and is never.
func (c *storeCatalog) landedSince(key string, t time.Time) bool {
	ms, _, _ := strings.Cut(path.Base(key), "-")
	n, err := strconv.ParseInt(ms, 10, 64)
	return err == nil && !time.UnixMilli(n).Before(t)
}

// readWriters reads the writer commits put since its last listing less listSlack, each writer's
// through its first one missing.
func (c *storeCatalog) readWriters(ctx context.Context) error {
	now := c.storeNow()
	keys, err := c.store.List(ctx, c.writers, cmp.Or(c.cursor, c.writers+c.bucket))
	if err != nil {
		return fmt.Errorf("catalog: list writer commits: %w", err)
	}
	// A writer's keys list in the order the store clock put them, which steps back when its
	// offset is read again from a server's whole-second date, so they are read in seq order:
	// read by key, a commit keyed before the one ahead of it would be skipped, and its
	// writer's every later one with it.
	type listed struct {
		key string
		origin
		ok bool
	}
	commits := make([]listed, len(keys))
	for i, key := range keys {
		o, ok := c.originOf(key)
		commits[i] = listed{key, o, ok}
	}
	slices.SortStableFunc(commits, func(a, b listed) int { return cmp.Compare(a.Seq, b.Seq) })
	counts := maps.Clone(c.counts)
	var read []entry
	for _, l := range commits {
		key, o := l.key, l.origin
		if !l.ok || o.Seq != counts[o.Writer] {
			continue
		}
		ds, err := c.readCommits(ctx, nil, []string{key})
		if err != nil {
			return err
		}
		for _, d := range ds {
			read = append(read, entry{origin: o, D: d})
		}
		counts[o.Writer]++
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range read {
		c.add(e)
	}
	from := now.Add(-listSlack)
	c.counts, c.bucket, c.cursor = counts, bucketOf(from), fmt.Sprintf("%s%s/%013d", c.writers, bucketOf(from), from.UnixMilli())
	return nil
}
