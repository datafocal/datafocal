package storage

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreConformance runs every check against the filesystem store and, when
// DATAFOCAL_S3_ENDPOINT is set, against the existing bucket DATAFOCAL_S3_* names.
func TestStoreConformance(t *testing.T) {
	stores := map[string]func(*testing.T) Store{
		"fs": func(t *testing.T) Store { return NewFS(t.TempDir()) },
		"s3": newS3,
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			t.Run("round trip", func(t *testing.T) { checkRoundTrip(t, store) })
			t.Run("PutIfAbsent", func(t *testing.T) { checkPutIfAbsentKeepsTheStoredObject(t, store) })
			t.Run("List", func(t *testing.T) { checkListReturnsKeysAfterInOrder(t, store) })
		})
	}
}

func newS3(t *testing.T) Store {
	if os.Getenv("DATAFOCAL_S3_ENDPOINT") == "" {
		t.Skip("DATAFOCAL_S3_ENDPOINT is unset")
	}
	var cfg S3Config
	require.NoError(t, envconfig.Process("DATAFOCAL_S3", &cfg))
	store, err := NewS3(cfg)
	require.NoError(t, err)
	return store
}

func checkRoundTrip(t *testing.T, store Store) { //nolint:chatter // a store contract is a sequence of calls on one key
	ctx := context.Background()
	require.NoError(t, store.Put(ctx, "acme/2026/obj", []byte("0123456789")))

	part, err := store.GetRange(ctx, "acme/2026/obj", 3, 4)
	require.NoError(t, err)
	assert.Equal(t, []byte("3456"), part)
	tail, err := store.GetSuffix(ctx, "acme/2026/obj", 4)
	require.NoError(t, err)
	assert.Equal(t, []byte("6789"), tail)
	whole, err := store.GetSuffix(ctx, "acme/2026/obj", 64)
	require.NoError(t, err)
	assert.Equal(t, []byte("0123456789"), whole, "a suffix longer than the object is the whole object")
}

// checkPutIfAbsentKeepsTheStoredObject writes a key of its own per run, the one check a
// rerun against the same bucket would otherwise fail.
func checkPutIfAbsentKeepsTheStoredObject(t *testing.T, store Store) { //nolint:chatter // a store contract is a sequence of calls on one key
	ctx := context.Background()
	key := "acme/obj-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	require.NoError(t, store.PutIfAbsent(ctx, key, []byte("first")))

	err := store.PutIfAbsent(ctx, key, []byte("second"))
	require.ErrorIs(t, err, ErrExists)
	got, err := store.GetSuffix(ctx, key, 64)
	require.NoError(t, err)
	assert.Equal(t, []byte("first"), got)
}

func checkListReturnsKeysAfterInOrder(t *testing.T, store Store) { //nolint:chatter // a store contract is a sequence of calls on one key
	ctx := context.Background()
	for _, key := range []string{"acme/c/2", "acme/c/0", "acme/c/1", "acme/d/0", "other/c/0"} {
		require.NoError(t, store.Put(ctx, key, []byte(key)))
	}

	keys, err := store.List(ctx, "acme/c/", "")
	require.NoError(t, err)
	assert.Equal(t, []string{"acme/c/0", "acme/c/1", "acme/c/2"}, keys)
	keys, err = store.List(ctx, "acme/c/", "acme/c/0")
	require.NoError(t, err)
	assert.Equal(t, []string{"acme/c/1", "acme/c/2"}, keys)
	keys, err = store.List(ctx, "missing/", "")
	require.NoError(t, err)
	assert.Empty(t, keys)
}
