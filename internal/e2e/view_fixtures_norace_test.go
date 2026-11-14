// The race detector slows an answer past the server's 30-second request timeout, and the PR's
// go check answers every fixture without it.
//go:build !race

package e2e

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/storage"
)

// TestViewFixtures answers each view's requests on its corpus, by a querier each, requiring no
// server error and, answered twice, the same bytes; -update (make view-fixtures) answers once
// and writes them as fixtures/engine.json, which its tests and make preview read. Logs and graph
// read TestMain's incident corpus since the factory's has no service graph or id lines.
// The AI view is written twice more: as fixtures/sources.json on every agent source in
// testdata/ai ingested together, the shapes it must read without any one field in common.
// SLOs reads the incident corpus with the gateway's SLOs, in a directory of each request's own,
// and Views with the sample view and the SLO it draws.
func TestViewFixtures(t *testing.T) {
	factory, _ := factoryCorpus(t)
	derived, err := plan.NewDerived(querier.DefaultDerived())
	require.NoError(t, err)
	views := []struct{ name, corpus, set string }{
		{"view-ai", factory, "engine"}, {"view-ai", ingestAIFixtures(t), "sources"},
		{"view-trace", factory, "engine"}, {"view-trace", corpusDir, "search"},
		{"view-logs", corpusDir, "engine"}, {"view-graph", corpusDir, "engine"},
		{"view-resources", ingestResourceFixtures(t), "engine"}, {"view-slos", corpusDir, "engine"},
		{"view-views", factory, "engine"},
	}
	sample := func(file string) string {
		doc, err := os.ReadFile(filepath.Join("..", "..", "deploy", "definitions", file))
		require.NoError(t, err)
		return string(doc)
	}
	samples := []string{sample("view/querier.yaml"), sample("slo/querier-availability.yaml")}
	for _, view := range views {
		store := storage.NewFS(view.corpus)
		t.Run(view.name+"/"+view.set, func(t *testing.T) {
			t.Parallel() // each request reads the corpus through a querier of its own
			root := filepath.Join("..", "..", "web", "packages", view.name)
			requestsFile := map[string]string{"engine": "requests.json", "sources": "requests.sources.json", "search": "requests.search.json"}[view.set]
			var requests []viewRequest
			readJSON(t, filepath.Join(root, requestsFile), &requests)
			answer := func(r viewRequest) (querier.App, []byte) {
				opts := querier.Options{Catalog: catalog.New(store, tenant), Store: store, Derived: derived}
				switch view.name {
				case "view-slos":
					opts.Definitions = definitionsDir(t, gatewaySLO, promSLO, cleanSLO)
				case "view-views":
					opts.Definitions = definitionsDir(t, samples...)
				}
				app := querier.Open(opts)
				require.NoError(t, app.Refresh(t.Context()))
				rec := httptest.NewRecorder()
				app.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/"+r.Tool, bytes.NewReader(r.Req)))
				assert.Less(t, rec.Code, http.StatusInternalServerError, "/v1/%s %s: %s", r.Tool, r.Req, rec.Body)
				return app, rec.Body.Bytes()
			}
			for i, r := range requests {
				app, body := answer(r)
				if !*updateGoldens {
					_, again := answer(r)
					assert.Equal(t, string(body), string(again), "/v1/%s %s answered twice", r.Tool, r.Req)
				}
				requests[i].Answer = body
				if r.Tool == "get_trace" {
					assertWholeTrace(t, app, r.Req, body)
				}
			}
			if *updateGoldens {
				require.NoError(t, os.MkdirAll(filepath.Join(root, "fixtures"), 0o755))
				writeLines(t, filepath.Join(root, "fixtures", view.set+".json"), requests)
			}
		})
	}
}
