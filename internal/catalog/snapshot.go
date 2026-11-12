package catalog

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

var zstdEncoder, _ = zstd.NewWriter(nil)

// Snapshotter folds catalog commits into snapshots, so a cold load reads one snapshot and
// the commits after it, keeping planning cost bounded by snapshot size rather than
// descriptor count.
type Snapshotter interface {
	// Snapshot folds the commits past the newest snapshot into a new one, when there are
	// at least MinCommits of them, leaving out, when the catalog has a retention, the
	// descriptors they supersede and those past it.
	Snapshot(ctx context.Context) error
}

// snapshot is a snapshot's body, as the catalog's same-named fields; Base, Floor and Descriptors are v2's.
type snapshot struct {
	Seq     int            `json:"seq"`
	Entries []entry        `json:"entries,omitempty"`
	Counts  map[string]int `json:"counts,omitempty"`
	Floors  map[string]int `json:"floors,omitempty"`
	Writers []string       `json:"writers,omitempty"`
	V1      string         `json:"v1,omitempty"`
	Bucket  string         `json:"bucket,omitempty"`

	Base        int                  `json:"base,omitempty"`
	Floor       int                  `json:"floor,omitempty"`
	Descriptors []segment.Descriptor `json:"descriptors,omitempty"`
}

// SnapshotConfig is when a Snapshotter writes.
type SnapshotConfig struct {
	MinCommits int // commits past the newest snapshot before another is written; 0 uses 1000
}

type snapshotter struct {
	cat        *storeCatalog
	minCommits int
}

// NewSnapshotter returns a Snapshotter of cat, which New or NewWithRetention returned.
func NewSnapshotter(cat Catalog, cfg SnapshotConfig) Snapshotter {
	return &snapshotter{cat: cat.(*storeCatalog), minCommits: cmp.Or(cfg.MinCommits, 1000)}
}

// Snapshot writes the snapshot create-only, so of two snapshotters racing to the same
// seq one writes it and the other finds it written, then points readers at it.
func (s *snapshotter) Snapshot(ctx context.Context) error {
	newest, err := s.cat.loadSnapshot(ctx)
	if err != nil {
		return err
	}
	if err := s.cat.load(ctx, false); err != nil {
		return err
	}
	snap := s.cat.fold(newest.Writers)
	// A tenant's first snapshot lands at once; a day bounds how long commits wait on the lifecycle rule.
	stale, _ := time.Parse("20060102T1504", newest.Bucket)
	if added := sum(maps.Values(snap.Counts)) - sum(maps.Values(newest.Counts)); added < s.minCommits && (added == 0 || newest.Seq >= 0 && s.cat.storeNow().Sub(stale) < 24*time.Hour) {
		return nil
	}
	snap.Seq = newest.Seq + 1
	raw, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("catalog: marshal snapshot: %w", err)
	}
	body, key := zstdEncoder.EncodeAll(raw, nil), s.cat.snapshotKey(snap.Seq)
	err = storage.Retry(ctx, func() error { return s.cat.store.PutIfAbsent(ctx, key, body) })
	if err != nil && !errors.Is(err, storage.ErrExists) {
		return fmt.Errorf("catalog: put snapshot: %w", err)
	}
	if err := storage.Retry(ctx, func() error { return s.cat.store.Put(ctx, s.cat.newestKey, []byte(key)) }); err != nil {
		return fmt.Errorf("catalog: put snapshot pointer: %w", err)
	}
	return nil
}

// fold is the cache as a snapshot less what kept drops, its writers table's indexes kept from table.
func (c *storeCatalog) fold(table []string) snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	kept, horizon := c.kept()
	snap := snapshot{Entries: kept, Counts: maps.Clone(c.counts), Floors: maps.Clone(c.floors), Writers: slices.Clone(table), V1: c.loaded, Bucket: c.bucket}
	for w, n := range horizon {
		snap.Floors[w] = max(snap.Floors[w], n)
	}
	for _, w := range slices.Sorted(maps.Keys(c.counts)) {
		if w != "" && !slices.Contains(table, w) {
			snap.Writers = append(snap.Writers, w)
		}
	}
	return snap
}

// collect drops from the cache what kept leaves out, so a catalog's memory holds the versions it
// still answers rather than every descriptor it ever read.
func (c *storeCatalog) collect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads = append(c.reads, versionRead{at: c.now(), counts: maps.Clone(c.counts)})
	kept, horizon := c.kept()
	if len(kept) == len(c.entries) {
		return
	}
	at := pin(horizon)
	for _, e := range c.entries {
		if c.superseded(e.D.ID, at) {
			delete(c.supersededBy, e.D.ID)
		}
	}
	for w, n := range horizon {
		c.floors[w] = max(c.floors[w], n)
	}
	c.entries = kept
}

// kept is the cached descriptors less those before the horizon that a commit before it
// supersedes or that end past retention, which no version from the horizon on reads, and the
// horizon: the newest version read grace ago. Callers hold c.mu.
func (c *storeCatalog) kept() ([]entry, map[string]int) {
	var horizon map[string]int
	if i := slices.IndexFunc(c.reads, func(r versionRead) bool { return r.at.After(c.now().Add(-c.grace)) }); i != 0 {
		if i < 0 {
			i = len(c.reads)
		}
		horizon, c.reads = c.reads[i-1].counts, c.reads[i-1:]
	}
	at, live := pin(horizon), selector.Window{From: c.cutoff()}
	kept := make([]entry, 0, len(c.entries))
	for _, e := range c.entries {
		if !at.holds(e.origin) || !c.superseded(e.D.ID, at) && (c.retention == 0 || overlaps(e.D, live)) {
			kept = append(kept, e)
		}
	}
	return kept, horizon
}
