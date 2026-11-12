// Command testserver serves the tool surface over the seeded incident corpus for
// @datafocal/api's client tests, printing its URL and serving until stdin closes.
package main

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"os"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/seedcorpus"
	"github.com/datafocal/datafocal/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "testserver:", err)
		os.Exit(1)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "datafocal-testserver-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	if _, err := seedcorpus.Build(context.Background(), dir, seedcorpus.ScenarioIncident); err != nil {
		return err
	}

	app := querier.Open(querier.Options{Catalog: catalog.New(storage.NewFS(dir), seedcorpus.Tenant), Store: storage.NewFS(dir)})
	ts := httptest.NewServer(app.Handler())
	defer ts.Close()
	if _, err := fmt.Fprintln(os.Stdout, ts.URL); err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, os.Stdin)
	return err
}
