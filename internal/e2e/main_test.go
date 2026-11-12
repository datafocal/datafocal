// Package e2e is the end-to-end tool suite. It builds the deterministic
// seedcorpus once per package run (in TestMain), opens a querier over it, and
// drives every registered tool through the real HTTP and MCP surfaces. The point is coverage the acceptance criteria calls out: every
// tool in Registry.Tools() is exercised against ground truth from
// internal/seedcorpus's exported fixture constants.
package e2e

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/seedcorpus"
)

// tenant is the tenant every seeded stream is written under, aliased from the
// fixture so the corpus and the assertions cannot drift to two spellings.
const tenant = seedcorpus.Tenant

// Shared corpus state, built once by TestMain and reused by every subtest.
// Package-level rather than passed through helpers because httptest servers
// spin up per-test and the corpus itself is invariant across them.
var (
	corpusDir      string
	buildResult    seedcorpus.BuildResult
	patternIDByStr map[string]string
	factoryDir     string
)

// buildFactory builds the factory scenario into a new factoryDir on its first call.
var buildFactory = sync.OnceValues(func() (seedcorpus.BuildResult, error) {
	dir, err := os.MkdirTemp("", "datafocal-e2e-factory-*")
	if err != nil {
		return seedcorpus.BuildResult{}, err
	}
	factoryDir = dir
	return seedcorpus.Build(context.Background(), dir, seedcorpus.ScenarioFactory)
})

// factoryCorpus is the factory scenario's directory and build, shared by the tests that only
// read it so the package builds it once rather than once a test.
func factoryCorpus(t *testing.T) (string, seedcorpus.BuildResult) {
	t.Helper()
	built, err := buildFactory()
	require.NoError(t, err)
	return factoryDir, built
}

// TestMain builds the corpus once into a temp dir, runs the suite, and removes
// the temp dir on the way out. A build failure is fatal — no test in this
// package can run without a corpus.
func TestMain(m *testing.M) {
	tmp, err := os.MkdirTemp("", "datafocal-e2e-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: mkdirtemp: %v\n", err)
		os.Exit(1)
	}
	corpusDir = tmp

	br, err := seedcorpus.Build(context.Background(), corpusDir, seedcorpus.ScenarioAll)
	if err != nil {
		_ = os.RemoveAll(corpusDir)
		fmt.Fprintf(os.Stderr, "e2e: build corpus: %v\n", err)
		os.Exit(1)
	}
	buildResult = br

	// Tests look patterns up by rendered string because pattern_id is a
	// content hash they cannot hard-code.
	patternIDByStr = make(map[string]string, len(br.Patterns))
	for id, tmpl := range br.Patterns {
		patternIDByStr[tmpl] = id
	}

	code := m.Run()
	_ = os.RemoveAll(corpusDir)
	_ = os.RemoveAll(factoryDir)
	os.Exit(code)
}
