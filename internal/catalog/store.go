package catalog

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/klauspost/compress/zstd"
	"log/slog"
	"maps"
	"math"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// ErrSnapshotExpired is Prune's error for a version a snapshot has folded away.
var ErrSnapshotExpired = &query.APIError{Code: "snapshot_expired", Message: "the pinned catalog version was folded into a newer snapshot",
	Hint: "re-pin via /v1/describe", Retryable: true}

// storeCatalog keeps each appended descriptor as one immutable commit object under
// <tenant>/catalog/v1/, named by its 20-digit seq so keys list in append order.
type storeCatalog struct {
	store          storage.Store
	prefix         string
	writers        string
	snapshotPrefix string
	newestKey      string
	// retention is how long after its last row a descriptor is still read; 0 keeps every one.
	retention time.Duration
	// writer is who Append commits as, and seq its next commit, -1 until it has found it.
	writer string
	seq    int

	appending sync.Mutex
	loading   sync.Mutex // loads alone write counts, table, loaded and bucket, so read them unlocked
	mu        sync.RWMutex
	// entries is every descriptor read and kept, with where it was committed, and loaded the
	// key of the last v1 commit they came from, which the next load lists after.
	entries []entry
	loaded  string
	// counts is each writer's commits read with none missing before them and the v1 sequence's
	// descriptors, nil before the first load; floors those below which a version is expired.
	counts, floors map[string]int
	supersededBy   map[string][]origin
	// table is the snapshot's writers versions index; bucket the oldest the next load lists.
	table  []string
	bucket string
	// cursor is the writer key the next load lists after, "" to list from bucket.
	cursor string
	// last is the latest commit key this catalog has seen, which an Append lists after
	// rather than listing every commit there is; "" before it has seen any.
	last string
	// loadedAt is when the last load was called, so a load waiting on one called since skips it.
	loadedAt time.Time
	// grace is how long a version keeps answering once it is no longer the newest, and reads
	// the versions loads reached and when, from which horizon finds the oldest still owed.
	grace  time.Duration
	now    func() time.Time
	maxAge time.Duration
	reads  []versionRead
}

// origin is where a descriptor was committed: its writer's seq-th commit, or its v1 position.
type origin struct {
	Writer string `json:"w,omitempty"`
	Seq    int    `json:"s"`
}

type entry struct {
	origin
	D segment.Descriptor `json:"d"`
}

type versionRead struct {
	at     time.Time
	counts map[string]int
}

// Config is how long a catalog reads descriptors and keeps answering superseded versions.
type Config struct {
	// Retention is how long after its last row a descriptor is still read; 0 keeps every one.
	Retention time.Duration
	// Grace is how long a version answers after a newer one is read, before the descriptors a
	// later commit supersedes are dropped and it answers ErrSnapshotExpired; 0 drops them at once.
	Grace time.Duration
	// Now is the clock grace is measured on; nil is time.Now.
	Now func() time.Time
	// Writer is whose log Append commits to, "" the shared v1 sequence; no two processes share one.
	Writer string
	// MaxAge is how old a load Version may answer from before it loads again; 0 loads every call.
	MaxAge time.Duration
}

// DefaultGrace outlasts an agent's session pinned to one version.
const DefaultGrace = 15 * time.Minute

// New returns a Catalog of tenant's descriptors kept in store.
func New(store storage.Store, tenant string) Catalog {
	return NewWithRetention(store, tenant, 0)
}

// NewWithRetention is New reading no descriptor whose rows ended more than retention ago,
// since a bucket lifecycle rule may have deleted its object.
func NewWithRetention(store storage.Store, tenant string, retention time.Duration) Catalog {
	return NewWithConfig(store, tenant, Config{Retention: retention, Grace: DefaultGrace})
}

// NewWithConfig returns a Catalog of tenant's descriptors kept in store, as cfg says.
func NewWithConfig(store storage.Store, tenant string, cfg Config) Catalog {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &storeCatalog{store: store, prefix: tenant + "/catalog/v1/", snapshotPrefix: tenant + "/catalog/snapshot/v3/",
		newestKey: tenant + "/catalog/newest-snapshot", retention: cfg.Retention, supersededBy: map[string][]origin{},
		grace: cfg.Grace, now: cfg.Now, maxAge: cfg.MaxAge, writers: tenant + "/catalog/w/", writer: cfg.Writer, seq: -1}
}

func (c *storeCatalog) commitKey(seq int) string {
	return fmt.Sprintf("%s%020d.json", c.prefix, seq)
}

// snapshotKey is where the snapshot of commits 0 through seq lives, under its own prefix
// so a commit listing never returns one.
func (c *storeCatalog) snapshotKey(seq int) string {
	return fmt.Sprintf("%s%020d.json.zst", c.snapshotPrefix, seq)
}

// seqOf is the seq a commit or snapshot key is named by.
func seqOf(key string) (int, error) {
	name, _, _ := strings.Cut(path.Base(key), ".")
	seq, err := strconv.Atoi(name)
	if err != nil {
		return 0, fmt.Errorf("catalog: parse key %s: %w", key, err)
	}
	return seq, nil
}

// Append claims the seq after the last listed commit with a create-only put. Losing that
// seq means another writer committed it, so Append lists past it and claims the next one;
// every attempt is a higher seq, so it retries only while other writers keep winning. It
// lists from the last commit it saw, since every commit before that is taken.
func (c *storeCatalog) Append(ctx context.Context, ds ...segment.Descriptor) error {
	body, err := encodeCommit(ds)
	if err != nil {
		return err
	}
	if c.writer != "" {
		c.appending.Lock()
		defer c.appending.Unlock()
		return c.appendAsWriter(ctx, body)
	}
	c.mu.Lock()
	after := c.last
	c.mu.Unlock()
	for {
		var keys []string
		err := storage.Retry(ctx, func() error {
			var err error
			keys, err = c.store.List(ctx, c.prefix, after)
			return err
		})
		if err != nil {
			return fmt.Errorf("catalog: list commits: %w", err)
		}
		if len(keys) > 0 {
			after = keys[len(keys)-1]
		}
		key, err := c.keyAfter(after)
		if err != nil {
			return err
		}
		err = c.putCommit(ctx, key, body)
		if errors.Is(err, storage.ErrExists) {
			after = key
			continue
		}
		if err != nil {
			return fmt.Errorf("catalog: put commit: %w", err)
		}
		c.saw(key)
		return nil
	}
}

// putCommit recognizes a write that landed before its response failed, so a retry
// cannot publish the same descriptors at a second seq.
func (c *storeCatalog) putCommit(ctx context.Context, key string, body []byte) error { //nolint:chatter // Read-back resolves an ambiguous create-only write.
	failed := false
	err := storage.Retry(ctx, func() error {
		err := c.store.PutIfAbsent(ctx, key, body)
		if err != nil && !errors.Is(err, storage.ErrExists) {
			failed = true
		}
		return err
	})
	if !errors.Is(err, storage.ErrExists) || !failed {
		return err
	}
	var stored []byte
	if err := storage.Retry(ctx, func() error {
		var err error
		stored, err = c.store.GetSuffix(ctx, key, math.MaxInt64)
		return err
	}); err != nil {
		return fmt.Errorf("catalog: read commit: %w", err)
	}
	if bytes.Equal(stored, body) {
		return nil
	}
	return storage.ErrExists
}

// saw records key as seen when it is later than the last commit seen, keys being
// zero-padded so they order as their seqs do.
func (c *storeCatalog) saw(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if key > c.last {
		c.last = key
	}
}

// encodeCommit is one descriptor as its JSON object, as every commit was before commits
// held several, and several as a JSON array of them.
func encodeCommit(ds []segment.Descriptor) ([]byte, error) {
	var body []byte
	var err error
	switch len(ds) {
	case 0:
		return nil, errors.New("catalog: a commit needs a descriptor")
	case 1:
		body, err = json.Marshal(ds[0])
	default:
		body, err = json.Marshal(ds)
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: marshal descriptors: %w", err)
	}
	return body, nil
}

// decodeCommit reads a commit encodeCommit wrote.
func decodeCommit(body []byte) ([]segment.Descriptor, error) {
	if trimmed := bytes.TrimLeft(body, " \t\r\n"); len(trimmed) > 0 && trimmed[0] == '[' {
		var ds []segment.Descriptor
		err := json.Unmarshal(body, &ds)
		return ds, err
	}
	var d segment.Descriptor
	err := json.Unmarshal(body, &d)
	return []segment.Descriptor{d}, err
}

// keyAfter returns the key of the commit that follows the commit at key, or of the
// first commit when key is empty.
func (c *storeCatalog) keyAfter(key string) (string, error) {
	if key == "" {
		return c.commitKey(0), nil
	}
	seq, err := seqOf(key)
	if err != nil {
		return "", err
	}
	return c.commitKey(seq + 1), nil
}

// Version is what the tenant's catalog holds now, so a version read now stays
// reproducible as more segments are appended.
func (c *storeCatalog) Version(ctx context.Context) (Version, error) {
	c.loading.Lock()
	fresh := !c.loadedAt.IsZero() && c.now().Sub(c.loadedAt) < c.maxAge
	c.loading.Unlock()
	if !fresh {
		if err := c.load(ctx, false); err != nil {
			return "", err
		}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return versionOf(c.table, c.counts), nil
}

// Prune keeps the descriptors v holds that overlap sel.Window, that sel.Where cannot rule
// out, that none of them supersedes and that are within retention, so a snapshot reads
// each row through one descriptor, ordered by their rows' first time and then ID.
func (c *storeCatalog) Prune(ctx context.Context, v Version, sel selector.Resolved) ([]segment.Descriptor, error) {
	p, err := c.resolve(ctx, v)
	if err != nil {
		return nil, err
	}
	if cutoff := c.cutoff(); sel.Window.From.Before(cutoff) {
		sel.Window.From = cutoff
	}

	where := selector.ParsePredicates(sel.Where)
	var out []segment.Descriptor
	seen := map[string]bool{}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, e := range c.entries {
		if !p.holds(e.origin) || c.superseded(e.D.ID, p) || seen[e.D.ID] {
			continue
		}
		seen[e.D.ID] = true
		if !overlaps(e.D, sel.Window) || !matchesWhere(e.D, where) {
			continue
		}
		out = append(out, e.D)
	}
	slices.SortFunc(out, func(a, b segment.Descriptor) int {
		return cmp.Or(a.TimeRange.Min.Compare(b.TimeRange.Min), strings.Compare(a.ID, b.ID))
	})
	return out, nil
}

// load caches every commit past the cache, or on a cold cache past the newest snapshot, whose
// writers table it reads again when table asks.
func (c *storeCatalog) load(ctx context.Context, table bool) error {
	called := c.now()
	c.loading.Lock()
	defer c.loading.Unlock()
	if !table && c.loadedAt.After(called) {
		return nil
	}
	if c.counts == nil || table {
		snap, err := c.loadSnapshot(ctx)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.apply(snap)
		c.mu.Unlock()
	}
	keys, err := c.store.List(ctx, c.prefix, c.loaded)
	if err != nil {
		return fmt.Errorf("catalog: list commits: %w", err)
	}
	commits, err := c.readCommits(ctx, nil, keys)
	if err != nil {
		return err
	}
	c.mu.Lock()
	for _, d := range commits {
		c.add(entry{origin: origin{Seq: c.counts[""]}, D: d})
		c.counts[""]++
	}
	if len(keys) > 0 {
		c.loaded = keys[len(keys)-1]
	}
	c.mu.Unlock()
	c.saw(c.loaded)
	if err := c.readWriters(ctx); err != nil {
		return err
	}
	c.loadedAt = called
	c.collect()
	return nil
}

// apply caches snap as a cold load's start, or a warm one's longer writers table; callers hold c.mu.
func (c *storeCatalog) apply(snap snapshot) {
	if c.counts != nil {
		if len(snap.Writers) > len(c.table) {
			c.table = snap.Writers
		}
		return
	}
	for _, e := range snap.Entries {
		c.add(e)
	}
	c.counts, c.floors = maps.Collect(maps.All(snap.Counts)), maps.Collect(maps.All(snap.Floors))
	c.table, c.loaded, c.bucket = snap.Writers, snap.V1, snap.Bucket
}

// resolve is the version v names, read past what this catalog has loaded when v holds more,
// or ErrSnapshotExpired when its descriptors were folded away or it is past reading.
func (c *storeCatalog) resolve(ctx context.Context, v Version) (pin, error) {
	p, indexed, err := v.parse()
	if err != nil {
		return nil, ErrSnapshotExpired
	}
	for loaded := false; ; loaded = true {
		c.mu.RLock()
		unknown := len(indexed) > len(c.table)
		for i, n := range indexed[:min(len(indexed), len(c.table))] {
			p[c.table[i]] = n
		}
		behind, expired := unknown || p.exceeds(c.counts), pin(c.floors).exceeds(p)
		c.mu.RUnlock()
		switch {
		case !behind && expired, behind && loaded:
			return nil, ErrSnapshotExpired
		case !behind:
			return p, nil
		}
		if err := c.load(ctx, unknown); err != nil {
			return nil, err
		}
	}
}

// overlaps keeps a segment unless it lies wholly to one side of the window, treating a
// zero descriptor bound as unknown.
func overlaps(d segment.Descriptor, w selector.Window) bool {
	if !w.From.IsZero() && !d.TimeRange.Max.IsZero() && d.TimeRange.Max.Before(w.From) {
		return false
	}
	return w.To.IsZero() || d.TimeRange.Min.IsZero() || !d.TimeRange.Min.After(w.To)
}

// cutoff is the time before which a descriptor's rows are past retention, or zero when
// every descriptor is kept.
func (c *storeCatalog) cutoff() time.Time {
	if c.retention == 0 {
		return time.Time{}
	}
	return time.Now().Add(-c.retention)
}

// superseded reports whether a commit p holds supersedes id; callers hold c.mu.
func (c *storeCatalog) superseded(id string, p pin) bool {
	return slices.ContainsFunc(c.supersededBy[id], p.holds)
}

// add caches e, recording what it supersedes; callers hold c.mu.
func (c *storeCatalog) add(e entry) {
	c.entries = append(c.entries, e)
	for _, id := range e.D.Supersedes {
		c.supersededBy[id] = append(c.supersededBy[id], e.origin)
	}
}

// readCommits appends the descriptors of the commits at keys to commits.
func (c *storeCatalog) readCommits(ctx context.Context, commits []segment.Descriptor, keys []string) ([]segment.Descriptor, error) {
	for _, key := range keys {
		body, err := c.store.GetSuffix(ctx, key, math.MaxInt64)
		if err != nil {
			return nil, fmt.Errorf("catalog: read commit: %w", err)
		}
		ds, err := decodeCommit(body)
		if err != nil {
			return nil, fmt.Errorf("catalog: parse commit %s: %w", key, err)
		}
		commits = append(commits, ds...)
	}
	return commits, nil
}

// newestSnapshot is the key the snapshot pointer names, read so a load never lists the
// snapshots, or "" when there is none.
func (c *storeCatalog) newestSnapshot(ctx context.Context) (string, error) {
	keys, err := c.store.List(ctx, c.newestKey, "")
	if err != nil {
		return "", fmt.Errorf("catalog: list snapshot pointer: %w", err)
	}
	if len(keys) == 0 {
		return "", nil
	}
	body, err := c.store.GetSuffix(ctx, c.newestKey, math.MaxInt64)
	if err != nil {
		return "", fmt.Errorf("catalog: read snapshot pointer: %w", err)
	}
	return string(body), nil
}

// loadSnapshot reads the newest snapshot, a v2 one as the v1 sequence's, or none when there
// is none or a lifecycle rule has expired it, since the commits hold the whole history.
func (c *storeCatalog) loadSnapshot(ctx context.Context) (snapshot, error) {
	key, err := c.newestSnapshot(ctx)
	if err != nil || key == "" {
		return snapshot{Seq: -1}, err
	}
	body, err := c.store.GetSuffix(ctx, key, math.MaxInt64)
	if err != nil {
		if keys, listErr := c.store.List(ctx, key, ""); listErr == nil && len(keys) == 0 {
			slog.Default().WarnContext(ctx, "catalog snapshot expired, reading every commit", "key", key)
			return snapshot{Seq: -1}, nil
		}
		return snapshot{}, fmt.Errorf("catalog: read snapshot: %w", err)
	}
	// Streamed into the parse by a decoder of its own: a tenant's snapshot runs to hundreds of
	// MB of JSON, which decompressed whole stayed held by the shared decoder that last wrote it.
	dec, err := zstd.NewReader(bytes.NewReader(body), zstd.WithDecoderConcurrency(1))
	if err != nil {
		return snapshot{}, fmt.Errorf("catalog: decompress snapshot %s: %w", key, err)
	}
	defer dec.Close()
	var snap snapshot
	if err := json.NewDecoder(dec).Decode(&snap); err != nil {
		return snapshot{}, fmt.Errorf("catalog: read snapshot %s: %w", key, err)
	}
	if strings.HasPrefix(key, c.snapshotPrefix) {
		return snap, nil
	}
	seq, err := seqOf(key)
	if err != nil {
		return snapshot{}, err
	}
	// A v2 snapshot that folded nothing away answers every version before it, and one from
	// before snapshots kept their floor none.
	end := snap.Base + len(snap.Descriptors)
	v1 := snapshot{Seq: seq, Counts: map[string]int{"": end}, V1: c.commitKey(seq)}
	if snap.Base > 0 {
		v1.Floors = map[string]int{"": cmp.Or(snap.Floor, end)}
	}
	for i, d := range snap.Descriptors {
		v1.Entries = append(v1.Entries, entry{origin: origin{Seq: snap.Base + i}, D: d})
	}
	return v1, nil
}

// matchesWhere admits a descriptor unless some predicate the descriptor can decide
// rejects it.
func matchesWhere(d segment.Descriptor, where []selector.ParsedPredicate) bool {
	for _, p := range where {
		if !matches(d, p) {
			return false
		}
	}
	return true
}

// matches is matchesWhere for one predicate, or any of a group's alternatives.
func matches(d segment.Descriptor, p selector.ParsedPredicate) bool {
	if len(p.Any) > 0 {
		return slices.ContainsFunc(p.Any, func(alt selector.ParsedPredicate) bool { return matches(d, alt) })
	}
	if p.Field == segment.ColService {
		return matchesService(d, p)
	}
	v, ok := d.Stream.Labels[p.Field]
	return !ok || matchOp(v, true, p)
}

// matchesService decides a service predicate from the services the rows name, never the
// stream, because one collector's segment holds every service on its node. Any row may
// name none, which the descriptor does not record.
func matchesService(d segment.Descriptor, p selector.ParsedPredicate) bool {
	return matchOp("", false, p) || slices.ContainsFunc(d.Services, func(service string) bool { return matchOp(service, true, p) })
}

// matchOp defers to selector.ParsedPredicate.Matches so pruning and the scan agree on each
// operator, and admits an unknown operator for Compile to reject where the caller can
// be told.
func matchOp(value string, present bool, p selector.ParsedPredicate) bool {
	if !slices.Contains(selector.AllOps, p.Op) {
		return true
	}
	return p.Matches(value, present)
}
