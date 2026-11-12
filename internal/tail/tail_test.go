package tail

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func appendFile(t *testing.T, path, text string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteString(text)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

// next waits for n lines, as at would return them.
func next(t *testing.T, f Follower, n int) []Line {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var lines []Line
	for range n {
		line, err := f.Next(ctx)
		require.NoError(t, err)
		lines = append(lines, at(line.Text, line.Offset))
	}
	return lines
}

// logFile returns the path to follow and the file it names, which a linked path reaches
// through a symlink from another directory, as /var/log/containers does.
func logFile(t *testing.T, linked bool) (path, target string) {
	target = filepath.Join(t.TempDir(), "app.log")
	appendFile(t, target, "")
	if !linked {
		return target, target
	}
	path = filepath.Join(t.TempDir(), "app.log")
	require.NoError(t, os.Symlink(target, path))
	return path, target
}

// at is a line as the tests compare it, without the identity of the file it came from.
func at(text string, offset int64) Line {
	return Line{Text: text, Position: Position{Offset: offset}}
}

// available returns every complete line Next can return without waiting, as at would.
func available(t *testing.T, f Follower) []Line {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var lines []Line
	for {
		line, err := f.Next(ctx)
		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
			return lines
		}
		lines = append(lines, at(line.Text, line.Offset))
	}
}

func TestNext(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(fmt.Sprintf("linked=%t", linked), func(t *testing.T) { testNext(t, linked) })
	}
}

// Each case opens initial at offset and reads what is there, then applies change to the
// file path names, checks the follower is woken and what the change means, and reads
// until it has want and nothing more.
func testNext(t *testing.T, linked bool) {
	for _, tc := range []struct {
		name    string
		initial string
		offset  int64
		change  func(t *testing.T, path string, woken func())
		means   change
		want    []Line
	}{
		{name: "lines carry the offset just past them", initial: "one\ntwo\n", want: []Line{at("one", 4), at("two", 8)}},
		{name: "offset past the end reads from the start", initial: "one\n", offset: 100, want: []Line{at("one", 4)}},
		{name: "partial line waits for its newline", initial: "one\ntw",
			change: func(t *testing.T, path string, _ func()) { appendFile(t, path, "o\n") },
			means:  grown, want: []Line{at("one", 4), at("two", 8)}},
		{name: "rotation finishes the old file first", initial: "one\n", means: grown,
			change: func(t *testing.T, path string, _ func()) {
				appendFile(t, path, "two\n")
				require.NoError(t, os.Rename(path, path+".1"))
				appendFile(t, path, "three\n")
			},
			want: []Line{at("one", 4), at("two", 8), at("three", 6)}},
		{name: "rotation reopens the new file once it is recreated", initial: "one\n", means: replaced,
			change: func(t *testing.T, path string, woken func()) {
				require.NoError(t, os.Rename(path, path+".1"))
				woken()
				appendFile(t, path, "three\n")
			},
			want: []Line{at("one", 4), at("three", 6)}},
		{name: "renamed and not yet recreated waits", initial: "one\n",
			change: func(t *testing.T, path string, _ func()) { require.NoError(t, os.Rename(path, path+".1")) },
			want:   []Line{at("one", 4)}},
		{name: "truncation reopens from the start", initial: "one\n", means: replaced,
			change: func(t *testing.T, path string, _ func()) {
				require.NoError(t, os.Truncate(path, 0))
				appendFile(t, path, "a\n")
			},
			want: []Line{at("one", 4), at("a", 2)}},
		{name: "deleted ends the follower", initial: "one\n", means: removed,
			change: func(t *testing.T, path string, _ func()) { require.NoError(t, os.Remove(path)) },
			want:   []Line{at("one", 4)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, target := logFile(t, linked)
			appendFile(t, target, tc.initial)
			w, err := NewWatcher()
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, w.Close()) })
			f, err := w.Open(path, Position{Offset: tc.offset})
			require.NoError(t, err)

			got := available(t, f)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			woken := func() {
				select {
				case <-f.(*follower).wake:
				case <-time.After(5 * time.Second):
					require.Fail(t, "not woken by the change")
				}
			}
			if tc.change != nil {
				tc.change(t, target, woken)
				woken()
			}
			means, err := f.(*follower).change()
			require.NoError(t, err)
			assert.Equal(t, tc.means, means)
			if means == removed {
				_, err := f.Next(ctx)
				require.ErrorIs(t, err, ErrRemoved)
				assert.Empty(t, w.(*watcher).wakes, "a removed file wakes nothing")
				return
			}

			for len(got) < len(tc.want) {
				line, err := f.Next(ctx)
				require.NoError(t, err)
				got = append(got, at(line.Text, line.Offset))
			}
			assert.Equal(t, tc.want, got)
			assert.Empty(t, available(t, f))
		})
	}
}

func TestResumesTheFileItStopped(t *testing.T) {
	for _, linked := range []bool{false, true} {
		for _, suspended := range []bool{false, true} {
			t.Run(fmt.Sprintf("linked=%t/suspended=%t", linked, suspended), func(t *testing.T) { testResumes(t, linked, suspended) })
		}
	}
}

// Each case reads "one\n" and the start of a line, changes the files while the follower is
// suspended or nothing follows them, then reads on, which must continue the file it read.
func testResumes(t *testing.T, linked, suspended bool) {
	rotate := func(t *testing.T, path string) {
		require.NoError(t, os.Rename(path, path+".1"))
		appendFile(t, path+".1", "o\n")
		appendFile(t, path, "three\n")
	}
	for _, tc := range []struct {
		name    string
		change  func(t *testing.T, path string)
		want    []Line
		removed bool
	}{
		{name: "unchanged", change: func(t *testing.T, path string) { appendFile(t, path, "o\n") }, want: []Line{at("two", 8)}},
		{name: "rotated, so the old file is finished first", change: rotate, want: []Line{at("two", 8), at("three", 6)}},
		{name: "rotated and deleted, so the new file is read from the start", change: func(t *testing.T, path string) {
			rotate(t, path)
			require.NoError(t, os.Remove(path+".1"))
		}, want: []Line{at("three", 6)}},
		{name: "deleted unseen by the watcher, as on an unmount, so there is nothing to resume", change: func(t *testing.T, path string) { require.NoError(t, os.Remove(path)) }, removed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, target := logFile(t, linked)
			appendFile(t, target, "one\ntw")
			w, err := NewWatcher()
			require.NoError(t, err)
			t.Cleanup(func() { assert.NoError(t, w.Close()) })
			f, err := w.Open(path, Position{})
			require.NoError(t, err)
			stopped, err := f.Next(context.Background())
			require.NoError(t, err)

			if suspended {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				require.ErrorIs(t, f.(*follower).suspend(ctx), context.Canceled)
				require.Nil(t, f.(*follower).file, "a suspended file is closed")
			}
			if tc.removed {
				require.NoError(t, w.(*watcher).notify.Remove(filepath.Dir(target)))
				if linked {
					require.NoError(t, w.(*watcher).notify.Remove(filepath.Dir(path)))
				}
			}
			tc.change(t, target)
			if tc.removed && suspended {
				f.(*follower).idleAfter = time.Millisecond
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				require.NoError(t, f.(*follower).awaitWake(ctx), "a suspended follower finds its path gone")
			}
			if !suspended {
				f, err = w.Open(path, stopped.Position)
				if tc.removed {
					require.ErrorIs(t, err, fs.ErrNotExist)
					return
				}
				require.NoError(t, err)
			}
			if tc.removed {
				_, err := f.Next(context.Background())
				require.ErrorIs(t, err, ErrRemoved)
				assert.Empty(t, w.(*watcher).wakes, "a removed file wakes nothing")
				return
			}
			assert.Equal(t, tc.want, next(t, f, len(tc.want)))
			assert.Empty(t, available(t, f))
		})
	}
}

func TestCommandResumesAfterItsCursorAndReportsAnExit(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "journalctl"), []byte("#!/bin/sh\necho \"$@\" > "+dir+"/args\n"+
		"echo '{\"__CURSOR\":\"s=1;i=2\",\"MESSAGE\":\"up\"}'\necho 'journal corrupted' >&2\nexit 1\n"), 0o755))
	t.Setenv("PATH", dir)
	f, err := OpenCommand(t.Context(), []string{"journalctl", "--follow", "--output=json"}, "--after-cursor={cursor}", "__CURSOR", Position{Cursor: "s=1;i=1"})
	require.NoError(t, err)
	line, err := f.Next(t.Context())
	require.NoError(t, err)
	assert.Equal(t, Line{Text: `{"__CURSOR":"s=1;i=2","MESSAGE":"up"}`, Position: Position{Cursor: "s=1;i=2"}}, line)
	_, err = f.Next(t.Context())
	assert.ErrorContains(t, err, "journal corrupted")
	args, err := os.ReadFile(filepath.Join(dir, "args"))
	require.NoError(t, err)
	assert.Equal(t, "--follow --output=json --after-cursor=s=1;i=1\n", string(args))
}
