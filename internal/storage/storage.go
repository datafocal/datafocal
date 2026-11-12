package storage

import (
	"context"
	"errors"
)

// ErrExists is PutIfAbsent's error when an object is already stored at the key.
var ErrExists = errors.New("storage: object exists")

// Store holds immutable byte objects by key, knowing nothing of what the bytes encode.
type Store interface {
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	// GetSuffix returns the last n bytes of the object, or all of it when it is shorter,
	// the one-request read a trailer-indexed format opens with (HTTP bytes=-n).
	GetSuffix(ctx context.Context, key string, n int64) ([]byte, error)
	Put(ctx context.Context, key string, body []byte) error
	// PutIfAbsent writes the object only if none is stored at key, returning ErrExists and
	// leaving the stored object intact otherwise.
	PutIfAbsent(ctx context.Context, key string, body []byte) error
	// List returns the keys that start with prefix and sort after after, ascending.
	List(ctx context.Context, prefix, after string) ([]string, error)
}
