package storage

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestS3StoreTellsTheStoresTime: the store's clock is this one's moved by how far the store's
// last response was dated from it, so a client whose own clock is skewed agrees with the store.
func TestS3StoreTellsTheStoresTime(t *testing.T) {
	ahead := 2 * time.Hour
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Date", time.Now().Add(ahead).UTC().Format(http.TimeFormat))
	}))
	defer srv.Close()
	dated := &datedTransport{RoundTripper: http.DefaultTransport}
	store := &s3Store{dated: dated}
	assert.WithinDuration(t, time.Now(), store.Now(), time.Second, "before any response, this clock")
	resp, err := (&http.Client{Transport: dated}).Get(srv.URL)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.WithinDuration(t, time.Now().Add(ahead), store.Now(), 2*time.Second)
}
