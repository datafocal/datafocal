// Package web serves the frontend's build output, embedded in the binary.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
)

//go:embed all:dist
var dist embed.FS

// Handler serves paths relative to the UI's root, so mount it behind http.StripPrefix.
func Handler() http.Handler {
	files, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // dist is embedded above, so Sub cannot fail
	}
	return handler(files)
}

func handler(files fs.FS) http.Handler {
	index, err := fs.ReadFile(files, "index.html")
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "UI not built: run make web", http.StatusNotFound)
		})
	}
	fileServer := http.FileServerFS(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Any path that is not a file is a route inside the single-page app.
		info, err := fs.Stat(files, path.Clean("."+r.URL.Path))
		if err != nil || info.IsDir() {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write(index)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
}
