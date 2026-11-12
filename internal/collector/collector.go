package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/storage"
)

// patternBudget is the distinct pattern_ids a service holds before its derivation degrades.
const patternBudget = 10_000 //nolint:dupdefault // a pattern budget, unrelated to topology's node cap

type Dependencies struct {
	Store         storage.Store
	CatalogWriter catalog.Catalog
	Clock         Clock
	Logger        *slog.Logger
}

// maxSealing bounds the batches sealing in the background at once, each holding its rows.
const maxSealing = 2

// maxOpenBuckets bounds the alignment buckets a batch holds open, which catching up on
// files read at different speeds fills where tailing live logs fills one or two.
const maxOpenBuckets = 16

// Collector parses records into a batch that Flush saves, per
// docs/architecture.md#how-a-log-line-becomes-a-segment-the-write-path.
type Collector interface {
	Collect(ctx context.Context, r segment.Record) (CollectResult, error)
	// Cut ends the open batch as a size seal would and reports like Collect, so sealing
	// in the background it returns without waiting for the batch to land.
	Cut(ctx context.Context) (CollectResult, error)
	// Flush saves the open batch, waits for any seal in the background, and returns the
	// segments that became durable during the call.
	Flush(ctx context.Context) ([]segment.Descriptor, error)
}

type collector struct {
	mu   sync.Mutex
	cfg  Config
	deps Dependencies
	// builders is the open batch, a builder per alignment bucket its records fall in, so
	// each seals as a segment inside one bucket; unaligned, everything is bucket 0.
	builders map[int64]*segment.Builder
	// openedAt is when the open batch began, at the last cut: its age, and every
	// builder's CreatedAt.
	openedAt time.Time
	policy   FlushPolicy
	pattern  pattern.Pattern
	// sealing is the batches sealing in the background, oldest first; failed is the first
	// background seal's error, which every later call returns.
	sealing []*sealing
	failed  error
}

// sealing is one batch's background seal, whose outcome is set once done is closed.
type sealing struct {
	done  chan struct{}
	descs []segment.Descriptor
	err   error
}

// CollectResult reports the batches one Collect cut, two when a batch at maxOpenBuckets
// is cut before a record that then fills the fresh one, and those it found durable.
type CollectResult struct {
	// Descriptors are the segments that became durable during the call, in the order
	// their batches were cut, a batch's in bucket order: the batches it cut when sealing
	// inline, and in the background ones an earlier call may have cut.
	Descriptors []segment.Descriptor
	Cut         int
	// Durable is how many cut batches became durable during the call, oldest first.
	Durable int
	// RecordSealed is set when the record itself is in the last batch the call cut,
	// which a size cut includes and a cut at maxOpenBuckets does not; a caller saving
	// its read position at a seal needs to know which side of it the record fell.
	RecordSealed bool
}

func New(cfg Config, deps Dependencies) (Collector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if deps.Store == nil {
		return nil, errors.New("object store is required")
	}
	if deps.CatalogWriter == nil {
		return nil, errors.New("catalog writer is required")
	}
	if deps.Clock == nil {
		deps.Clock = NewSystemClock()
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	p := pattern.New(patternBudget)
	return &collector{
		cfg:      cfg,
		deps:     deps,
		builders: map[int64]*segment.Builder{},
		openedAt: deps.Clock.Now(),
		policy:   cfg.FlushPolicy(),
		pattern:  p,
	}, nil
}

// Collect may only buffer the record, so call Flush at clean end-of-input to save the remaining records.
func (c *collector) Collect(ctx context.Context, r segment.Record) (CollectResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if r.Timestamp.IsZero() {
		r.Timestamp = c.deps.Clock.Now()
	}

	var result CollectResult
	if err := c.reap(ctx, &result, maxSealing); err != nil {
		return CollectResult{}, err
	}
	bucket := c.policy.bucketOf(r.Timestamp)
	if _, open := c.builders[bucket]; !open && len(c.builders) >= maxOpenBuckets {
		if err := c.cut(ctx, &result); err != nil {
			return CollectResult{}, err
		}
	}

	b := c.builders[bucket]
	if b == nil {
		b = c.newBuilder(c.openedAt)
		c.builders[bucket] = b
	}
	if err := b.Append(r, c.pattern.Derive(r.Columns)); err != nil {
		return CollectResult{}, fmt.Errorf("append record: %w", err)
	}

	if !c.policy.ShouldFlush(c.openStats(), c.deps.Clock.Now()) {
		return result, nil
	}

	if err := c.cut(ctx, &result); err != nil {
		return CollectResult{}, err
	}
	result.RecordSealed = true
	return result, nil
}

func (c *collector) Cut(ctx context.Context) (CollectResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result CollectResult
	if err := c.reap(ctx, &result, maxSealing); err != nil {
		return CollectResult{}, err
	}
	if c.openStats().Rows == 0 {
		return result, nil
	}
	if err := c.cut(ctx, &result); err != nil {
		return CollectResult{}, err
	}
	return result, nil
}

// Flush retains, sealing inline, the builders of a batch it failed to seal.
func (c *collector) Flush(ctx context.Context) ([]segment.Descriptor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.cfg.SealInBackground {
		return c.flushLocked(ctx)
	}
	var result CollectResult
	if c.openStats().Rows > 0 {
		if err := c.cut(ctx, &result); err != nil {
			return nil, err
		}
	}
	if err := c.reap(ctx, &result, 0); err != nil {
		return nil, err
	}
	return result.Descriptors, nil
}

// openStats is the open batch's rows and bytes across its builders, aged from openedAt.
func (c *collector) openStats() segment.Stats {
	stats := segment.Stats{CreatedAt: c.openedAt}
	for _, b := range c.builders {
		stats.Rows += b.Stats().Rows
		stats.EstimatedBytes += b.Stats().EstimatedBytes
	}
	return stats
}

// cut ends the open batch: sealing it now, or in the background once fewer than maxSealing
// are, each committing after the one before it, so segments commit in the order cut.
func (c *collector) cut(ctx context.Context, result *CollectResult) error {
	if !c.cfg.SealInBackground {
		descriptors, err := c.flushLocked(ctx)
		if err != nil {
			return err
		}
		result.Cut++
		result.Durable++
		result.Descriptors = append(result.Descriptors, descriptors...)
		return nil
	}
	if err := c.reap(ctx, result, maxSealing-1); err != nil {
		return err
	}
	batch, profile := c.builders, c.pattern.Footer()
	c.builders, c.openedAt = map[int64]*segment.Builder{}, c.deps.Clock.Now()
	s := &sealing{done: make(chan struct{})}
	var before *sealing
	if len(c.sealing) > 0 {
		before = c.sealing[len(c.sealing)-1]
	}
	c.sealing = append(c.sealing, s)
	result.Cut++
	// Not cancelled with the call, so a shutdown still lands the batch it cut.
	sealCtx := context.WithoutCancel(ctx)
	go func() {
		defer close(s.done)
		s.descs, s.err = c.seal(sealCtx, batch, profile, before)
	}()
	return nil
}

// reap adds the background seals' segments to result once they are durable, oldest first,
// waiting for them while more than running seal.
func (c *collector) reap(ctx context.Context, result *CollectResult, running int) error {
	if c.failed != nil {
		return c.failed
	}
	for len(c.sealing) > 0 {
		s := c.sealing[0]
		if len(c.sealing) <= running {
			select {
			case <-s.done:
			default:
				return nil
			}
		}
		select {
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.sealing = c.sealing[1:]
		if s.err != nil {
			c.failed = s.err
			return s.err
		}
		result.Durable++
		result.Descriptors = append(result.Descriptors, s.descs...)
	}
	return nil
}

// flushLocked seals the open batch, keeping all of it when that fails, since none of it
// is visible until its one catalog commit.
func (c *collector) flushLocked(ctx context.Context) ([]segment.Descriptor, error) {
	if len(c.builders) == 0 {
		return nil, nil
	}
	descriptors, err := c.seal(ctx, c.builders, c.pattern.Footer(), nil)
	if err != nil {
		return nil, err
	}
	c.builders, c.openedAt = map[int64]*segment.Builder{}, c.deps.Clock.Now()
	return descriptors, nil
}

// seal publishes batch's segments in one catalog commit after putting them all, so a crash
// before it leaves nothing visible, and after the seal before, if any, commits, which it
// fails with; it reads nothing a Collect changes, so it can run beside one.
func (c *collector) seal(ctx context.Context, batch map[int64]*segment.Builder, profile segment.PatternProfile, before *sealing) ([]segment.Descriptor, error) {
	retry := c.cfg.SealRetry
	if retry == 0 {
		retry = 10 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, retry)
	defer cancel()
	start, rows, bytes := time.Now(), 0, 0
	descriptors := make([]segment.Descriptor, 0, len(batch))
	for _, bucket := range slices.Sorted(maps.Keys(batch)) {
		sealed := batch[bucket].Seal()
		sealed.PatternProfile = profile
		body, err := sealed.Encode()
		if err != nil {
			return nil, fmt.Errorf("encode segment: %w", err)
		}
		key := segment.BuildSegmentKey(c.cfg.Tenant, segment.TierRaw, sealed.Descriptor)
		if err := storage.Retry(ctx, func() error { return c.deps.Store.Put(ctx, key, body) }); err != nil {
			return nil, fmt.Errorf("put segment %s: %w", key, err)
		}
		descriptor := sealed.Descriptor
		descriptor.Object = segment.ObjectLocation{Key: key}
		descriptors = append(descriptors, descriptor)
		rows, bytes = rows+descriptor.Rows, bytes+len(body)
	}
	if before != nil {
		if <-before.done; before.err != nil {
			return nil, before.err
		}
	}
	if err := c.deps.CatalogWriter.Append(ctx, descriptors...); err != nil {
		return nil, fmt.Errorf("append catalog descriptors: %w", err)
	}
	for _, d := range descriptors {
		c.deps.Logger.Debug("flushed segment", "segment_id", d.ID, "rows", d.Rows, "object", d.Object.Key)
	}
	c.deps.Logger.Info("sealed batch", "segments", len(descriptors), "rows", rows, "bytes", bytes,
		"duration_ms", float64(time.Since(start).Microseconds())/1000)
	return descriptors, nil
}

// stream is the configured stream identity as the segment and storage layers
// take it. Both the writer and the builder need it, and it must be identical in
// each — the object key is derived from its fingerprint.
func (c *collector) stream() segment.Stream {
	return segment.Stream{
		Service: c.cfg.Stream.Service,
		Labels:  c.cfg.Stream.Labels,
	}
}

func (c *collector) newBuilder(now time.Time) *segment.Builder {
	return segment.NewBuilder(segment.BuilderConfig{
		Tenant:    c.cfg.Tenant,
		Stream:    c.stream(),
		CreatedAt: now,
	})
}
