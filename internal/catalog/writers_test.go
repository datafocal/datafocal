package catalog_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// clockStore is a store telling the time by its own clock, as an object store's responses do.
type clockStore struct {
	storage.Store
	now func() time.Time
}

func (s clockStore) Now() time.Time { return s.now() }

func writerDesc(writer string, i int) segment.Descriptor {
	at := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	return desc(fmt.Sprint(writer, "-", i), "checkout", at, at.Add(time.Minute))
}

// TestWritersReplicasAnswerAPinAlike: two replicas loading at different moments while writers
// commit, each to its own log, answer one pin with the same descriptors in the same order, and
// neither a commit landing after it nor one landing late before a writer's next is in it.
func TestWritersReplicasAnswerAPinAlike(t *testing.T) {
	ctx := t.Context()
	store := storage.NewFS(t.TempDir())
	var wg sync.WaitGroup
	for _, w := range []string{"node-b", "node-a", "node-c"} {
		writer := catalog.NewWithConfig(store, "acme", catalog.Config{Writer: w})
		wg.Go(func() {
			for i := range 20 {
				assert.NoError(t, writer.Append(ctx, writerDesc(w, i)))
			}
		})
	}
	first := catalog.New(store, "acme")
	var pins []catalog.Version
	var answers [][]segment.Descriptor
	for range 5 {
		v, err := first.Version(ctx)
		require.NoError(t, err)
		got, err := first.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		pins, answers = append(pins, v), append(answers, got)
	}
	wg.Wait()
	second := catalog.New(store, "acme")
	for i, v := range pins {
		got, err := second.Prune(ctx, v, selector.Resolved{})
		require.NoError(t, err)
		assert.Equal(t, answers[i], got, "pin %s", v)
	}
	v, err := second.Version(ctx)
	require.NoError(t, err)
	all, err := second.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Len(t, all, 60)
}

// TestWritersLateCommit: a writer's commit that lands after a pin is read is not in it, a
// commit seen while the one before it is still missing is in no pin until that one lands, and a
// later pin holds both.
func TestWritersLateCommit(t *testing.T) {
	ctx := t.Context()
	fs := storage.NewFS(t.TempDir())
	writer := catalog.NewWithConfig(fs, "acme", catalog.Config{Writer: "node-a"})
	require.NoError(t, writer.Append(ctx, writerDesc("node-a", 0)))
	now := time.Now()
	reader := catalog.NewWithConfig(fs, "acme", catalog.Config{Now: func() time.Time { return now }})
	before, err := reader.Version(ctx)
	require.NoError(t, err)

	require.NoError(t, writer.Append(ctx, writerDesc("node-a", 1)))
	require.NoError(t, writer.Append(ctx, writerDesc("node-a", 2)))
	keys, err := fs.List(ctx, "acme/catalog/w/", "")
	require.NoError(t, err)
	require.Len(t, keys, 3)
	second := keys[1]
	hidden := storage.Fake{
		GetSuffixFn: func(key string, n int64) ([]byte, error) { return fs.GetSuffix(ctx, key, n) },
		ListFn: func(prefix, after string) ([]string, error) {
			keys, err := fs.List(ctx, prefix, after)
			return slices.DeleteFunc(keys, func(k string) bool { return k == second }), err
		},
	}
	gap := catalog.New(hidden, "acme")
	held, err := gap.Version(ctx)
	require.NoError(t, err)
	got, err := gap.Prune(ctx, held, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a-0"}, ids(got), "the third commit waits on the second")

	got, err = reader.Prune(ctx, before, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a-0"}, ids(got), "commits after the pin are not in it")
	now = now.Add(time.Second)
	after, err := reader.Version(ctx)
	require.NoError(t, err)
	got, err = reader.Prune(ctx, after, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a-0", "node-a-1", "node-a-2"}, ids(got))
}

// TestWritersOnASkewedClock: a writer whose own clock is two hours slow commits to the bucket of
// the store's clock, so a reader listing only the buckets since its snapshot still sees it.
func TestWritersOnASkewedClock(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	store := clockStore{Store: storage.NewFS(t.TempDir()), now: func() time.Time { return now }}
	reader := catalog.New(store, "acme")
	require.NoError(t, catalog.NewWithConfig(store, "acme", catalog.Config{Writer: "early"}).Append(ctx, writerDesc("early", 0)))
	require.NoError(t, catalog.NewSnapshotter(reader, catalog.SnapshotConfig{MinCommits: 1}).Snapshot(ctx))

	now = now.Add(time.Hour)
	slow := catalog.NewWithConfig(store, "acme", catalog.Config{Writer: "slow", Now: func() time.Time { return now.Add(-2 * time.Hour) }})
	require.NoError(t, slow.Append(ctx, writerDesc("slow", 0)))
	cold := catalog.New(store, "acme")
	v, err := cold.Version(ctx)
	require.NoError(t, err)
	got, err := cold.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"early-0", "slow-0"}, ids(got))
}

// TestWritersStoreClockSteppingBack: a store clock read again from a server's whole-second
// date steps back, keying a writer's commit before the one ahead of it, and a reader still
// reads that commit and every later one.
func TestWritersStoreClockSteppingBack(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 7, 18, 12, 0, 0, 25*int(time.Millisecond), time.UTC)
	store := clockStore{Store: storage.NewFS(t.TempDir()), now: func() time.Time { return now }}
	writer := catalog.NewWithConfig(store, "acme", catalog.Config{Writer: "node"})
	require.NoError(t, writer.Append(ctx, writerDesc("node", 0)))
	now = now.Truncate(time.Second) // the offset read again, a second's fraction lost
	require.NoError(t, writer.Append(ctx, writerDesc("node", 1)))
	now = now.Add(time.Second)
	require.NoError(t, writer.Append(ctx, writerDesc("node", 2)))

	reader := catalog.New(store, "acme")
	v, err := reader.Version(ctx)
	require.NoError(t, err)
	got, err := reader.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"node-0", "node-1", "node-2"}, ids(got))
}

// TestWritersResumeAfterARestart: a writer that restarts, before and after its commits are
// folded into a snapshot, continues its seq rather than reusing one, so readers count all of them.
func TestWritersResumeAfterARestart(t *testing.T) {
	ctx := t.Context()
	fs := storage.NewFS(t.TempDir())
	restart := func(i int) {
		require.NoError(t, catalog.NewWithConfig(fs, "acme", catalog.Config{Writer: "node-a"}).Append(ctx, writerDesc("node-a", i)))
	}
	restart(0)
	restart(1)
	require.NoError(t, catalog.NewSnapshotter(catalog.New(fs, "acme"), catalog.SnapshotConfig{MinCommits: 1}).Snapshot(ctx))
	restart(2)
	cat := catalog.New(fs, "acme")
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	got, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"node-a-0", "node-a-1", "node-a-2"}, ids(got))
}

// TestWritersTableOnlyGrows: a pin indexing one snapshot's writers answers the same from a later
// snapshot that a writer sorting before them joined.
func TestWritersTableOnlyGrows(t *testing.T) {
	ctx := t.Context()
	fs := storage.NewFS(t.TempDir())
	commit := func(w string, i int) {
		require.NoError(t, catalog.NewWithConfig(fs, "acme", catalog.Config{Writer: w}).Append(ctx, writerDesc(w, i)))
	}
	compactor := catalog.New(fs, "acme")
	snapshotter := catalog.NewSnapshotter(compactor, catalog.SnapshotConfig{MinCommits: 1})
	commit("node-b", 0)
	require.NoError(t, snapshotter.Snapshot(ctx))
	v, err := catalog.New(fs, "acme").Version(ctx)
	require.NoError(t, err)
	commit("node-a", 0)
	require.NoError(t, snapshotter.Snapshot(ctx))
	got, err := catalog.New(fs, "acme").Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"node-b-0"}, ids(got))
}

// TestWritersResumeAfterAnIdleRestart: a writer restarting an hour after its last commit, with no
// snapshot to bound its listing, still continues its seq rather than reusing one in a new bucket.
func TestWritersResumeAfterAnIdleRestart(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	store := clockStore{Store: storage.NewFS(t.TempDir()), now: func() time.Time { return now }}
	for i := range 2 {
		require.NoError(t, catalog.NewWithConfig(store, "acme", catalog.Config{Writer: "a"}).Append(ctx, writerDesc("a", i)))
		now = now.Add(time.Hour)
	}
	cat := catalog.New(store, "acme")
	v, err := cat.Version(ctx)
	require.NoError(t, err)
	got, err := cat.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Equal(t, []string{"a-0", "a-1"}, ids(got))
}

// lateStore runs before ahead of each create-only put, as a put still in flight would.
type lateStore struct {
	clockStore
	before func() error
}

func (s lateStore) PutIfAbsent(ctx context.Context, key string, body []byte) error {
	if err := s.before(); err != nil {
		return err
	}
	return s.clockStore.PutIfAbsent(ctx, key, body)
}

// TestWritersCommitLandingLate: a commit that lands after a reader listed past its bucket, with
// later commits after it, still reaches the reader, also when its writer dies before committing it
// again and restarts, and a pin read before it answers as it did.
func TestWritersCommitLandingLate(t *testing.T) {
	for _, dies := range []bool{false, true} {
		ctx := t.Context()
		now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
		var late func() error
		store := lateStore{clockStore{storage.NewFS(t.TempDir()), func() time.Time { return now }}, func() error {
			if f := late; f != nil {
				late = nil
				return f()
			}
			return nil
		}}
		writer, reader := catalog.NewWithConfig(store, "acme", catalog.Config{Writer: "a"}), catalog.New(store, "acme")
		require.NoError(t, writer.Append(ctx, writerDesc("a", 0)))
		before, err := reader.Version(ctx)
		require.NoError(t, err)
		writerCtx, kill := context.WithCancel(ctx)
		late = func() error {
			now = now.Add(10 * time.Minute)
			_, err := reader.Version(ctx)
			if dies {
				late = func() error { kill(); return context.Canceled }
			}
			return err
		}
		if err := writer.Append(writerCtx, writerDesc("a", 1)); dies {
			require.Error(t, err)
			writer = catalog.NewWithConfig(store, "acme", catalog.Config{Writer: "a"})
		}
		require.NoError(t, writer.Append(ctx, writerDesc("a", 2)))
		for _, cat := range []catalog.Catalog{reader, catalog.New(store, "acme")} {
			v, err := cat.Version(ctx)
			require.NoError(t, err)
			for pin, want := range map[catalog.Version][]string{before: {"a-0"}, v: {"a-0", "a-1", "a-2"}} {
				got, err := cat.Prune(ctx, pin, selector.Resolved{})
				require.NoError(t, err)
				assert.Equal(t, want, ids(got), "dies %v, pin %s", dies, pin)
			}
		}
	}
}

// listStore counts the requests a catalog makes and the keys its LISTs return.
type listStore struct {
	clockStore
	requests, listed *int
}

func (s listStore) List(ctx context.Context, prefix, after string) ([]string, error) {
	keys, err := s.clockStore.List(ctx, prefix, after)
	*s.requests, *s.listed = *s.requests+1, *s.listed+len(keys)
	return keys, err
}

func (s listStore) GetSuffix(ctx context.Context, key string, n int64) ([]byte, error) {
	*s.requests++
	return s.clockStore.GetSuffix(ctx, key, n)
}

// TestWritersLoadListsWhatIsNew: a load lists the commits put since its last listing less the
// slack, not every writer's in the buckets before it, and a Version within MaxAge reads nothing.
func TestWritersLoadListsWhatIsNew(t *testing.T) {
	ctx := t.Context()
	now := time.Date(2026, 7, 18, 12, 1, 0, 0, time.UTC)
	fs := clockStore{storage.NewFS(t.TempDir()), func() time.Time { return now }}
	for w := range 50 {
		writer := catalog.NewWithConfig(fs, "acme", catalog.Config{Writer: fmt.Sprint("node-", w)})
		require.NoError(t, writer.Append(ctx, writerDesc(fmt.Sprint("node-", w), 0)))
	}
	var requests, listed int
	reader := catalog.NewWithConfig(listStore{fs, &requests, &listed}, "acme", catalog.Config{MaxAge: 5 * time.Second, Now: func() time.Time { return now }})
	_, err := reader.Version(ctx)
	require.NoError(t, err)
	requests = 0
	_, err = reader.Version(ctx)
	require.NoError(t, err)
	assert.Zero(t, requests, "a load less than MaxAge old answers")

	now = now.Add(3 * time.Minute)
	_, err = reader.Version(ctx)
	require.NoError(t, err)
	now = now.Add(2 * time.Minute)
	require.NoError(t, catalog.NewWithConfig(fs, "acme", catalog.Config{Writer: "node-new"}).Append(ctx, writerDesc("node-new", 0)))
	listed = 0
	v, err := reader.Version(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, listed, "only the commit put since the last listing less the slack")
	got, err := reader.Prune(ctx, v, selector.Resolved{})
	require.NoError(t, err)
	assert.Len(t, got, 51)
}
