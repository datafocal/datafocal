package tools

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// keepAlive is how often a follow sends an event with no lines, so an idle proxy keeps it open.
const keepAlive = 15 * time.Second

// tail is a querier's live tail: while anyone follows it, it reads each raw segment the
// catalog gains whole, once, and queues its lines for every follower.
type tail struct {
	catalog catalog.Catalog
	store   storage.Store
	every   time.Duration

	mu        sync.Mutex
	followers map[chan []LogLine]<-chan struct{} // to each follower's done
	stop      context.CancelFunc
	stopped   chan struct{} // closed once the running tail has stopped
}

// follow calls emit with no lines once attached and every keepAlive, and with each batch of
// new lines, in time order, until ctx is done.
func (t *tail) follow(ctx context.Context, emit func([]LogLine) error) error {
	lines := make(chan []LogLine, 16)
	if err := t.join(ctx, lines); err != nil {
		return err
	}
	defer t.leave(lines)
	ticker := time.NewTicker(keepAlive)
	defer ticker.Stop()
	for batch := []LogLine(nil); ; {
		if err := emit(batch); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			batch = nil
		case batch = <-lines:
		}
	}
}

// join adds a follower, starting the tail from the catalog as it stands for the first.
func (t *tail) join(ctx context.Context, lines chan []LogLine) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.followers) == 0 {
		seen, _, err := t.raw(ctx, nil)
		if err != nil {
			return err
		}
		run, stop := context.WithCancel(context.WithoutCancel(ctx))
		stopped := make(chan struct{})
		t.stop, t.stopped = stop, stopped
		go func() {
			defer close(stopped)
			t.run(run, seen)
		}()
	}
	t.followers[lines] = ctx.Done()
	return nil
}

// leave removes a follower, stopping the tail after the last and waiting until it has.
func (t *tail) leave(lines chan []LogLine) {
	t.mu.Lock()
	delete(t.followers, lines)
	last, stop, stopped := len(t.followers) == 0, t.stop, t.stopped
	t.mu.Unlock()
	if last {
		stop()
		<-stopped
	}
}

// timedLine is a line the tail read, by its time.
type timedLine struct {
	at   time.Time
	line LogLine
}

// run sends the lines of the raw segments the catalog gains on each tick until ctx is done.
func (t *tail) run(ctx context.Context, seen map[string]bool) {
	ticker := time.NewTicker(t.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		seen = t.tick(ctx, seen)
	}
}

// tick sends the lines of the raw segments not in seen, returning those the catalog holds.
func (t *tail) tick(ctx context.Context, seen map[string]bool) map[string]bool {
	now, fresh, err := t.raw(ctx, seen)
	if err != nil {
		slog.Default().Warn("live tail: read catalog", "error", err)
		return seen
	}
	var read []timedLine
	for _, d := range fresh {
		if read, err = t.read(ctx, d, read); err != nil {
			slog.Default().Warn("live tail: read segment", "segment", d.ID, "error", err)
		}
	}
	slices.SortStableFunc(read, func(a, b timedLine) int { return a.at.Compare(b.at) })
	if len(read) > 0 && ctx.Err() == nil {
		lines := make([]LogLine, len(read))
		for i, r := range read {
			lines[i] = r.line
		}
		t.send(ctx, lines)
	}
	return now
}

// raw is the ids of the catalog's raw segments, and those of them not in seen; a merged
// segment holds lines its inputs already sent.
func (t *tail) raw(ctx context.Context, seen map[string]bool) (map[string]bool, []segment.Descriptor, error) {
	v, err := t.catalog.Version(ctx)
	if err != nil {
		return nil, nil, err
	}
	descs, err := t.catalog.Prune(ctx, v, selector.Resolved{})
	if err != nil {
		return nil, nil, err
	}
	ids := map[string]bool{}
	var fresh []segment.Descriptor
	for _, d := range descs {
		if len(d.Supersedes) == 0 {
			ids[d.ID] = true
			if !seen[d.ID] {
				fresh = append(fresh, d)
			}
		}
	}
	return ids, fresh, nil
}

// read appends d's lines to read, read whole in one GET as compaction reads a raw input.
func (t *tail) read(ctx context.Context, d segment.Descriptor, read []timedLine) ([]timedLine, error) {
	body, err := t.store.GetSuffix(ctx, d.Object.Key, math.MaxInt64)
	if err != nil {
		return read, err
	}
	inMemory := storage.Fake{
		GetSuffixFn: func(_ string, n int64) ([]byte, error) { return body[max(0, int64(len(body))-n):], nil },
		GetRangeFn:  func(_ string, off, n int64) ([]byte, error) { return body[off : off+n], nil },
	}
	records, _, _, err := segment.NewReader(inMemory).Records(ctx, d.Object)
	for i, r := range records {
		l := LogLine{Timestamp: r.Timestamp.Format(time.RFC3339Nano), Level: r.Text(segment.ColLevel),
			Message: r.Text(segment.ColMessage), PatternID: r.Text(segment.ColPatternID),
			Service: cmp.Or(r.Text(segment.ColService), d.Stream.Service), Segment: d.ID, Row: i, Attributes: map[string]string{}}
		for _, c := range r.Columns {
			if !slices.Contains(lineFields, c.Name) {
				l.Attributes[c.Name] = r.Text(c.Name)
			}
		}
		read = append(read, timedLine{r.Timestamp, l})
	}
	return read, err
}

// send queues lines for every follower, skipping one that has gone.
func (t *tail) send(ctx context.Context, lines []LogLine) {
	t.mu.Lock()
	followers := maps.Clone(t.followers)
	t.mu.Unlock()
	for f, done := range followers {
		select {
		case f <- lines:
		case <-done:
		case <-ctx.Done():
		}
	}
}
