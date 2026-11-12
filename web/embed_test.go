package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func get(t *testing.T, h http.Handler, target string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	return rec.Code, string(body)
}

func TestHandlerServesBuild(t *testing.T) {
	h := handler(fstest.MapFS{
		"index.html":      {Data: []byte("<html>app</html>")},
		"assets/index.js": {Data: []byte("console.log(1)")},
	})
	for _, target := range []string{"/", "/logs", "/trace/abc", "/assets/", "/../index.html"} {
		code, body := get(t, h, target)
		assert.Equal(t, http.StatusOK, code, target)
		assert.Equal(t, "<html>app</html>", body, target)
	}
	code, body := get(t, h, "/assets/index.js")
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "console.log(1)", body)
}

func TestHandlerNotBuilt(t *testing.T) {
	code, body := get(t, handler(fstest.MapFS{".gitkeep": {}}), "/logs")
	assert.Equal(t, http.StatusNotFound, code)
	assert.Equal(t, "UI not built: run make web\n", body)
}
