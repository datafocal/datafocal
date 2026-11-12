package definitions

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Directory is the definitions directory.
type Directory interface {
	// Load reads every .yaml, .yml and .json file under the directory, in path order.
	Load() ([]Document, []Error)
	// Save writes each document to the file it was loaded from, or <kind>/<name>.yaml,
	// refusing a read-only directory and a file holding other documents.
	Save(docs []Document) error
	// Writable reports whether creating and removing a file in the directory succeeded at Open.
	Writable() bool
	// Watch calls onChange once a burst of changes to the directory settles, until ctx is done.
	Watch(ctx context.Context, onChange func()) error
}

type directory struct {
	dir      string
	limits   Limits
	writable bool
}

// Open returns the directory dir, which holds nothing when dir is empty.
func Open(dir string, limits Limits) Directory {
	d := directory{dir: dir, limits: limits}
	if f, err := os.CreateTemp(dir, ".writable-*"); dir != "" && err == nil {
		_ = f.Close()
		d.writable = os.Remove(f.Name()) == nil
	}
	return d
}

func (d directory) Writable() bool { return d.writable }

func (d directory) Load() ([]Document, []Error) {
	if d.dir == "" {
		return nil, nil
	}
	var docs []Document
	var errs []Error
	err := filepath.WalkDir(d.dir, func(path string, e fs.DirEntry, err error) error {
		if err != nil || e.IsDir() || !slices.Contains([]string{".yaml", ".yml", ".json"}, filepath.Ext(path)) {
			return err
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(d.dir, path)
		more, bad := decode(content, rel)
		docs, errs = append(docs, more...), append(errs, bad...)
		return nil
	})
	if err != nil {
		errs = append(errs, Error{Path: d.dir, Message: err.Error()})
	}
	docs, bad := compile(docs, nil, d.limits)
	return docs, append(errs, bad...)
}

func (d directory) Save(docs []Document) error {
	existing, invalid := d.Load()
	held := map[string]int{}
	at := map[string]string{}
	for _, e := range existing {
		held[e.Path]++
		at[e.Kind+"/"+e.Name] = e.Path
	}
	for _, e := range invalid {
		held[e.Path]++
	}
	paths := make([]string, len(docs))
	for i, doc := range docs {
		paths[i] = filepath.Join(strings.ToLower(doc.Kind), doc.Name+".yaml")
		if path, ok := at[doc.Kind+"/"+doc.Name]; ok {
			paths[i] = path
		}
		switch {
		case !d.writable:
			return fmt.Errorf("cannot save %s: the definitions directory is read-only, so export it and commit it instead", paths[i])
		case held[paths[i]] > 1:
			return fmt.Errorf("cannot save %s %s: %s holds other documents too, so export it instead", doc.Kind, doc.Name, paths[i])
		}
	}
	for i, doc := range docs {
		if err := d.write(paths[i], doc); err != nil {
			return err
		}
	}
	return nil
}

// write writes doc to path through a temporary file, so a reader never sees half of it.
func (d directory) write(path string, doc Document) error {
	format := FormatYAML
	if filepath.Ext(path) == ".json" {
		format = FormatJSON
	}
	content, err := Format([]Document{doc}, format)
	if err != nil {
		return err
	}
	path = filepath.Join(d.dir, path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".save-*")
	if err != nil {
		return err
	}
	_, err = tmp.Write(content)
	if err = errors.Join(err, tmp.Close()); err == nil {
		err = os.Rename(tmp.Name(), path)
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("save %s: %w", path, err)
	}
	return nil
}

// settle is how long the directory must be quiet before Watch reports a change.
const settle = 200 * time.Millisecond

func (d directory) Watch(ctx context.Context, onChange func()) error {
	if d.dir == "" {
		<-ctx.Done()
		return nil
	}
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()
	watch := func(dir string) error {
		return filepath.WalkDir(dir, func(path string, e fs.DirEntry, err error) error {
			if err == nil && e.IsDir() {
				err = w.Add(path)
			}
			return err
		})
	}
	if err := watch(d.dir); err != nil {
		return err
	}
	return settled(ctx, w, watch, onChange)
}

// settled calls onChange once w's events have been quiet for settle, watching each
// directory created, until ctx is done.
func settled(ctx context.Context, w *fsnotify.Watcher, watch func(string) error, onChange func()) error {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-w.Errors:
			return err
		case e := <-w.Events:
			if e.Has(fsnotify.Create) {
				_ = watch(e.Name)
			}
			timer.Reset(settle)
		case <-timer.C:
			onChange()
		}
	}
}
