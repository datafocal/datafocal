// Package tail follows log files and a command's output, returning each complete line
// with its position.
package tail

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/valyala/fastjson"
)

// Watcher opens Followers that share one filesystem watch, so an idle file costs nothing
// until the kernel reports a change to it.
type Watcher interface {
	Open(path string, from Position) (Follower, error)
	Close() error
}

// Follower returns a file's complete lines in order, following it across rotation.
type Follower interface {
	Next(ctx context.Context) (Line, error) // blocks until a complete line
}

// idleAfter is how long a follower waits for a write before it closes its file until the next.
const idleAfter = 10 * time.Second

// ErrRemoved is Next's error once its file is deleted and every line in it read; the
// follower has released the file.
var ErrRemoved = errors.New("tail: file removed")

// Line is one complete line, without its newline.
type Line struct {
	Text     string
	Position // just past this line
}

// Position is a point in one file, which rotation may since have renamed, or in a
// command's output or the kernel ring buffer.
type Position struct {
	File   FileID `json:"file"`
	Offset int64  `json:"offset"`           // in bytes
	Cursor string `json:"cursor,omitempty"` // a command's cursor
}

// FileID tells files apart across renames; the zero FileID means whichever file the path names.
type FileID struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
}

func fileID(info fs.FileInfo) FileID {
	st := info.Sys().(*syscall.Stat_t)
	return FileID{Device: uint64(st.Dev), Inode: st.Ino} //nolint:unconvert // Dev is int32 on darwin
}

type watcher struct {
	notify *fsnotify.Watcher
	done   chan struct{} // closed when dispatch returns
	mu     sync.Mutex
	wakes  map[string]chan struct{} // by cleaned path
}

// NewWatcher starts the watch; Close stops it once its followers are done.
func NewWatcher() (Watcher, error) {
	notify, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("tail: watch: %w", err)
	}
	w := &watcher{notify: notify, done: make(chan struct{}), wakes: map[string]chan struct{}{}}
	go w.dispatch()
	return w, nil
}

func (w *watcher) Close() error {
	err := w.notify.Close()
	<-w.done
	return err
}

// dispatch wakes the follower of each changed path, and every follower when the kernel
// reports an error such as dropped events, since any of them may have missed a change.
func (w *watcher) dispatch() {
	defer close(w.done)
	for {
		select {
		case event, ok := <-w.notify.Events:
			if !ok {
				return
			}
			w.wake(filepath.Clean(event.Name))
		case _, ok := <-w.notify.Errors:
			if !ok {
				return
			}
			w.wake("")
		}
	}
}

// wake signals path's follower, or every follower for an empty path.
func (w *watcher) wake(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if path != "" {
		signal(w.wakes[path])
		return
	}
	for _, wake := range w.wakes {
		signal(wake)
	}
}

// signal leaves one wake pending; a nil channel, for a file nobody follows, is skipped.
func signal(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

// Open watches the directories of path and of the file a symlink path names rather than
// the file, so a write through the link and a rename and recreate are seen.
func (w *watcher) Open(path string, from Position) (Follower, error) {
	path = filepath.Clean(path)
	target, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("tail: resolve %s: %w", path, err)
	}
	for _, dir := range []string{filepath.Dir(path), filepath.Dir(target)} {
		if err := w.notify.Add(dir); err != nil {
			return nil, fmt.Errorf("tail: watch %s: %w", dir, err)
		}
	}
	name, offset, err := resume(path, target, from)
	if err != nil {
		return nil, err
	}
	f := &follower{path: path, target: target, wake: make(chan struct{}, 1), idleAfter: idleAfter, idle: time.NewTimer(idleAfter)}
	if err := f.open(name, offset); err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.wakes[path], w.wakes[target] = f.wake, f.wake
	w.mu.Unlock()
	f.release = func() {
		w.mu.Lock()
		delete(w.wakes, path)
		delete(w.wakes, target)
		w.mu.Unlock()
	}
	return f, nil
}

// resume finds from's file in target's directory, under target or the name rotation gave
// it; once that file is gone, path is read from the start.
func resume(path, target string, from Position) (name string, offset int64, err error) {
	if from.File == (FileID{}) {
		return path, from.Offset, nil
	}
	dir := filepath.Dir(target)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", 0, fmt.Errorf("tail: list %s: %w", dir, err)
	}
	for _, entry := range entries {
		if info, err := entry.Info(); err == nil && fileID(info) == from.File {
			return filepath.Join(dir, entry.Name()), from.Offset, nil
		}
	}
	return path, 0, nil
}

type follower struct {
	path      string
	target    string // the file path named when it was opened, whose directory rotation renames within
	wake      chan struct{}
	release   func() // stops the watcher waking this follower
	idleAfter time.Duration
	idle      *time.Timer
	file      *os.File // nil while suspended
	id        FileID
	reader    *bufio.Reader
	offset    int64
	partial   []byte
}

// open reads name, which is path or the file path named before a rotation, from offset.
func (f *follower) open(name string, offset int64) error {
	file, err := os.Open(name)
	if err != nil {
		return fmt.Errorf("tail: open %s: %w", name, err)
	}
	info, err := file.Stat()
	if err != nil {
		return errors.Join(fmt.Errorf("tail: stat %s: %w", name, err), file.Close())
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return errors.Join(fmt.Errorf("tail: seek %s to %d: %w", name, offset, err), file.Close())
	}
	previous := f.file
	f.file, f.id, f.reader, f.offset, f.partial = file, fileID(info), bufio.NewReader(file), offset, nil
	if previous == nil {
		return nil
	}
	if err := previous.Close(); err != nil {
		return fmt.Errorf("tail: close replaced %s: %w", f.path, err)
	}
	return nil
}

func (f *follower) Next(ctx context.Context) (Line, error) {
	for {
		if f.file == nil {
			if err := f.reopen(); err != nil {
				return Line{}, err
			}
		}
		line, complete, err := f.readLine()
		if err != nil || complete {
			return line, err
		}
		if err := f.waitForMore(ctx); err != nil {
			return Line{}, err
		}
	}
}

// readLine returns the next complete line, or complete false at the end of what has been
// written, keeping a partly written line for the next call.
func (f *follower) readLine() (line Line, complete bool, err error) {
	chunk, err := f.reader.ReadBytes('\n')
	f.partial = append(f.partial, chunk...)
	if errors.Is(err, io.EOF) {
		return Line{}, false, nil
	}
	if err != nil {
		return Line{}, false, fmt.Errorf("tail: read %s: %w", f.path, err)
	}
	f.offset += int64(len(f.partial))
	line = Line{Text: string(f.partial[:len(f.partial)-1]), Position: Position{File: f.id, Offset: f.offset}}
	f.partial = f.partial[:0]
	return line, true, nil
}

// waitForMore returns when the file may have more to read: at once when it grew or was
// replaced, otherwise on a wake, suspended after idleAfter. A deleted file returns ErrRemoved.
func (f *follower) waitForMore(ctx context.Context) error {
	c, err := f.change()
	if err != nil {
		return err
	}
	switch c {
	case grown:
		return nil
	case replaced:
		return f.open(f.path, 0)
	case removed:
		f.release()
		if err := f.file.Close(); err != nil {
			return fmt.Errorf("tail: close removed %s: %w", f.path, err)
		}
		return ErrRemoved
	}
	f.idle.Reset(f.idleAfter)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.wake:
		return nil
	case <-f.idle.C:
		return f.suspend(ctx)
	}
}

// suspend closes the file until a wake: an open file keeps a mount propagated from the host
// busy, and a container's files are not deleted until its root is unmounted.
func (f *follower) suspend(ctx context.Context) error {
	err := f.file.Close()
	f.file = nil
	if err != nil {
		return fmt.Errorf("tail: close idle %s: %w", f.path, err)
	}
	return f.awaitWake(ctx)
}

// awaitWake returns on a wake or once path is gone, checked every idleAfter because the
// watcher reports nothing when an unmount drops its watch.
func (f *follower) awaitWake(ctx context.Context) error {
	for {
		f.idle.Reset(f.idleAfter)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.wake:
			return nil
		case <-f.idle.C:
		}
		if _, err := os.Stat(f.path); errors.Is(err, fs.ErrNotExist) {
			return nil
		}
	}
}

// reopen resumes a suspended file from the start of its partial line, finishing it first if
// rotation renamed it.
func (f *follower) reopen() error {
	name, offset, err := resume(f.path, f.target, Position{File: f.id, Offset: f.offset})
	if err == nil {
		err = f.open(name, offset)
	}
	if errors.Is(err, fs.ErrNotExist) {
		f.release()
		return ErrRemoved
	}
	return err
}

// change is what reaching the end of the open file means.
type change int

const (
	unchanged change = iota // nothing new, so wait for a wake
	grown                   // written since it was read, so read on
	replaced                // the path names another file, or this one shrank
	removed                 // deleted rather than renamed, so no path leads to it
)

// change reads the open file to its end before switching, so lines written just before a rename are not lost.
func (f *follower) change() (change, error) {
	current, err := f.file.Stat()
	if err != nil {
		return unchanged, fmt.Errorf("tail: stat %s: %w", f.path, err)
	}
	read := f.offset + int64(len(f.partial))
	if current.Size() > read {
		return grown, nil
	}
	next, err := os.Stat(f.path)
	if errors.Is(err, fs.ErrNotExist) {
		if current.Sys().(*syscall.Stat_t).Nlink == 0 {
			return removed, nil
		}
		return unchanged, nil // renamed away, and not yet recreated
	}
	if err != nil {
		return unchanged, fmt.Errorf("tail: stat %s: %w", f.path, err)
	}
	if !os.SameFile(current, next) || current.Size() < read {
		return replaced, nil
	}
	return unchanged, nil
}

// OpenCommand follows argv's output until ctx ends, one JSON object per line whose cursor
// field is its position; resuming adds resume with {cursor} replaced by from's cursor.
func OpenCommand(ctx context.Context, argv []string, resume, cursor string, from Position) (Follower, error) {
	args := slices.Clone(argv[1:])
	if from.Cursor != "" && resume != "" {
		args = append(args, strings.ReplaceAll(resume, "{cursor}", from.Cursor))
	}
	c := &command{cmd: exec.CommandContext(ctx, argv[0], args...), cursor: cursor}
	c.cmd.Stderr = &c.stderr
	out, err := c.cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("tail: %s: %w", argv[0], err)
	}
	if err := c.cmd.Start(); err != nil {
		return nil, fmt.Errorf("tail: start %s: %w", argv[0], err)
	}
	c.out = bufio.NewReader(out)
	return c, nil
}

type command struct {
	cmd    *exec.Cmd
	cursor string
	out    *bufio.Reader
	stderr bytes.Buffer
}

// Next returns the command's stderr as its error once it exits.
func (c *command) Next(ctx context.Context) (Line, error) {
	text, err := c.out.ReadString('\n')
	if err == nil {
		text = strings.TrimSuffix(text, "\n")
		return Line{Text: text, Position: Position{Cursor: fastjson.GetString([]byte(text), c.cursor)}}, nil
	}
	waitErr := c.cmd.Wait()
	if ctx.Err() != nil {
		return Line{}, ctx.Err()
	}
	return Line{}, fmt.Errorf("tail: %s exited: %w: %s", c.cmd.Path, cmp.Or(waitErr, err), strings.TrimSpace(c.stderr.String()))
}
