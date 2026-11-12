package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// fsStore is the dev/test object store: objects are files under root at their key,
// and ranged reads use ReadAt so they touch only the bytes they need, the same
// access pattern as ranged GETs against object storage.
type fsStore struct {
	root string
}

// NewFS returns a Store rooted at root, creating directories as objects are written.
func NewFS(root string) Store {
	return &fsStore{root: root}
}

func (s *fsStore) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	f, err := os.Open(s.pathFor(key))
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", key, err)
	}
	defer f.Close()
	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("storage: read %s at %d+%d: %w", key, offset, length, err)
	}
	return buf, nil
}

func (s *fsStore) GetSuffix(_ context.Context, key string, n int64) ([]byte, error) {
	f, err := os.Open(s.pathFor(key))
	if err != nil {
		return nil, fmt.Errorf("storage: open %s: %w", key, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("storage: stat %s: %w", key, err)
	}
	n = min(n, st.Size())
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, st.Size()-n); err != nil {
		return nil, fmt.Errorf("storage: read last %d bytes of %s: %w", n, key, err)
	}
	return buf, nil
}

// Put writes atomically (temp file + rename) so a reader never observes a
// partially written object.
func (s *fsStore) Put(_ context.Context, key string, body []byte) error {
	path := s.pathFor(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("storage: mkdir: %w", err)
	}
	return writeFileAtomic(path, body, os.Rename)
}

// PutIfAbsent publishes the object with a hard link, which fails rather than replaces
// when the key exists, so of two concurrent writers to one key exactly one wins.
func (s *fsStore) PutIfAbsent(_ context.Context, key string, body []byte) error {
	path := s.pathFor(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("storage: mkdir: %w", err)
	}
	return writeFileAtomic(path, body, func(tmp, path string) error {
		if err := os.Link(tmp, path); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return fmt.Errorf("%s: %w", key, ErrExists)
			}
			return err
		}
		return os.Remove(tmp)
	})
}

// List walks the directory holding prefix, so a missing directory is an empty listing.
func (s *fsStore) List(_ context.Context, prefix, after string) ([]string, error) {
	dir := s.pathFor(prefix[:strings.LastIndex(prefix, "/")+1])
	var keys []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return err
		}
		if key := filepath.ToSlash(rel); strings.HasPrefix(key, prefix) && key > after {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: list %s: %w", prefix, err)
	}
	slices.Sort(keys)
	return keys, nil
}

// pathFor maps an object key to a filesystem path under the store root.
func (s *fsStore) pathFor(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

// writeFileAtomic writes data to a temp file in path's directory and publishes it at path,
// syncing both, so readers and a restart after power loss see the old or the whole new object.
func writeFileAtomic(path string, data []byte, publish func(tmp, path string) error) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("storage: create temp: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("storage: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return errors.Join(fmt.Errorf("storage: sync temp: %w", err), tmp.Close(), os.Remove(tmpName))
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("storage: close temp: %w", err)
	}
	if err := publish(tmpName, path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("storage: publish temp: %w", err)
	}
	return syncDir(dir)
}

// syncDir makes a rename into dir durable, which syncing the renamed file does not.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("storage: open %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		return errors.Join(fmt.Errorf("storage: sync %s: %w", dir, err), d.Close())
	}
	return d.Close()
}
