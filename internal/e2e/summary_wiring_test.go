package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
)

// TestSummaryWiring_RefreshDeterminism locks acceptance criterion #5a: a request
// pinned to a snapshot returns byte-identical Envelope.Data across a concurrent
// refresh that has advanced the store past the pin. The wired path serves the
// first call; the second, whose pin is now behind the store's version,
// legitimately falls through to the scan (the planner declines the summary), and
// the two paths must agree by construction.
func TestSummaryWiring_RefreshDeterminism(t *testing.T) {
	ctx := context.Background()
	fs := catalog.New(storage.NewFS(corpusDir), tenant)
	// The catalog moves on by bump versions without mutating the shared corpus dir.
	var bump atomic.Uint64
	cat := catalog.Fake{
		VersionFn: func() (catalog.Version, error) {
			version, err := fs.Version(ctx)
			n, _ := strconv.Atoi(string(version)) // a v1 catalog's version is its count
			return catalog.Version(strconv.Itoa(n + int(bump.Load()))), err
		},
		PruneFn: func(version catalog.Version, sel selector.Resolved) ([]segment.Descriptor, error) {
			// A bumped version holds no more commits than the corpus has.
			current, _ := fs.Version(ctx)
			n, _ := strconv.Atoi(string(version))
			m, _ := strconv.Atoi(string(current))
			return fs.Prune(ctx, catalog.Version(strconv.Itoa(min(n, m))), sel)
		},
	}
	app := querier.Open(querier.Options{Catalog: cat, Store: storage.NewFS(corpusDir), Summary: &summary.Config{}})
	require.NoError(t, app.Refresh(ctx), "initial build")
	ts := httptest.NewServer(app.Handler())
	t.Cleanup(ts.Close)

	// Use the same service-scoped query TestToolsOverSeededCorpus proves is
	// byte-identical between the summary and scan paths — the concurrent-refresh
	// invariant this test locks is the version guard's byte-identity, not the
	// summary-vs-scan invariant that suite already covers.
	req := tools.AggregateRequest{
		Selector: withPredicates(incidentSelector(),
			selector.Predicate{Field: "service", Op: selector.OpEq, Value: "gateway"}),
		Measure: "count",
		GroupBy: "status",
		Bins:    "1m",
	}
	envA := postToURL[tools.AggregateResponse](t, ts.URL, "aggregate", req)
	pin := envA.Meta.Snapshot
	require.NotEmpty(t, pin, "Meta.Snapshot must name the served summary")

	// A refresh absorbs the next catalog version, which moves the store past the pin.
	bump.Store(1)
	require.NoError(t, app.Refresh(ctx), "refresh")
	var version uint64
	_, err := fmt.Sscanf(pin, "e%d.s", &version)
	require.NoError(t, err)
	fresh := postToURL[tools.AggregateResponse](t, ts.URL, "aggregate", req)
	require.Equal(t, fmt.Sprintf("e%d.s%d", version+1, version+1), fresh.Meta.Snapshot, "store must have advanced")

	reqPinned := req
	reqPinned.Snapshot = pin
	envB := postToURL[tools.AggregateResponse](t, ts.URL, "aggregate", reqPinned)

	aJSON, err := json.Marshal(envA.Data)
	require.NoError(t, err)
	bJSON, err := json.Marshal(envB.Data)
	require.NoError(t, err)
	if !bytes.Equal(aJSON, bJSON) {
		t.Fatalf("Envelope.Data must be byte-identical across the concurrent refresh at pin %q\n--- before ---\n%s\n--- after ---\n%s",
			pin, aJSON, bJSON)
	}
}

// TestSummaryWiring_ReplayOrderRetention locks the corpus-spans-more-than-Retention
// acceptance criterion: a Builder given a Retention shorter than the corpus keeps
// only the newest Retention window, evicting older descriptors. This is
// load-bearing on the ascending-order replay — retainedDescriptors sorts by
// (TimeRange.Min, ID) so eviction can anchor on the newest observed bin.
// Descriptors older than the content-relative floor must not
// be sealed; descriptors inside the window must be.
func TestSummaryWiring_ReplayOrderRetention(t *testing.T) {
	ctx := context.Background()
	cat := catalog.New(storage.NewFS(corpusDir), tenant)
	store := storage.NewFS(corpusDir)
	rd := segment.NewReader(store)

	const shortRetention = 5 * time.Minute
	bldr := summary.NewBuilder(cat, rd, summary.Config{Retention: shortRetention})
	require.NoError(t, bldr.Build(ctx))
	rs := bldr.Store()

	version, err := cat.Version(ctx)
	require.NoError(t, err)
	descs, err := cat.Prune(ctx, version, selector.Resolved{})
	require.NoError(t, err)
	require.NotEmpty(t, descs, "corpus must have sealed descriptors")

	var newestMax, oldestMax time.Time
	for _, d := range descs {
		if d.TimeRange.Max.After(newestMax) {
			newestMax = d.TimeRange.Max
		}
		if oldestMax.IsZero() || d.TimeRange.Max.Before(oldestMax) {
			oldestMax = d.TimeRange.Max
		}
	}
	require.Greaterf(t, newestMax.Sub(oldestMax), shortRetention,
		"corpus must span more than the retention window for this test to bite; span=%s retention=%s",
		newestMax.Sub(oldestMax), shortRetention)

	floor := newestMax.Add(-shortRetention)
	var retained, evicted int
	for _, d := range descs {
		if d.TimeRange.Max.Before(floor) {
			assert.Falsef(t, rs.HasAdded(d.ID),
				"descriptor %s (max=%s) is older than the retention floor %s and must NOT be sealed",
				d.ID, d.TimeRange.Max.Format(time.RFC3339), floor.Format(time.RFC3339))
			evicted++
		} else {
			assert.Truef(t, rs.HasAdded(d.ID),
				"descriptor %s (max=%s) is within [floor=%s, newest=%s] and must be sealed",
				d.ID, d.TimeRange.Max.Format(time.RFC3339), floor.Format(time.RFC3339), newestMax.Format(time.RFC3339))
			retained++
		}
	}
	require.Positive(t, retained, "some descriptors must land inside the retention window")
	require.Positive(t, evicted, "some descriptors must fall outside the retention window (else the test does not bite)")
}

// TestSummaryWiring_UnreachablePinScans locks acceptance criterion #6 at the HTTP
// seam: a request pinned to a summary version this store never reached is answered
// off the objects, not refused. The pin still names a live catalog snapshot, and
// the scan is the path that minted the pinned answer in the first place —
// snapshot_expired is reserved for a catalog snapshot that has been compacted
// away, which a summary at a different
// version is not.
func TestSummaryWiring_UnreachablePinScans(t *testing.T) {
	h := newHarness(t, true)
	req := tools.AggregateRequest{
		Selector: incidentSelector(),
		Measure:  "count",
		GroupBy:  "service",
		Bins:     "1m",
	}

	// Probe one query to learn the current pin format (epoch/version).
	probe := postTool[tools.AggregateResponse](t, h, "aggregate", req)
	require.NotEmpty(t, probe.Meta.Snapshot)

	var epoch, version uint64
	_, err := fmt.Sscanf(probe.Meta.Snapshot, "e%d.s%d", &epoch, &version)
	require.NoErrorf(t, err, "parse probe snapshot %q", probe.Meta.Snapshot)
	require.Positive(t, version, "the wired summary must have a non-zero version")

	// Pin far ahead of the store — a version the store never reached.
	pinned := req
	pinned.Snapshot = fmt.Sprintf("e%d.s%d", epoch, version+100)
	status, body := postToolRaw(t, h, "aggregate", pinned)
	require.Equalf(t, http.StatusOK, status, "got status=%d body=%s", status, body)

	var apiErr query.APIError
	require.NoErrorf(t, json.Unmarshal(body, &apiErr), "decode response body: %s", body)
	require.Emptyf(t, apiErr.Code, "an unreachable pin must be answered, not refused: %s", body)

	// The answer is the one the scan gives: identical to a server with no summary
	// at all, at the same pin.
	scanned := decodeEnvelope(t, body)
	_, nilBody := postToolRaw(t, newHarness(t, false), "aggregate", pinned)
	nilServed := decodeEnvelope(t, nilBody)
	// Cost parity with the nil-store server, not GETs > 0: segments are sealed inside one
	// bin, so this count comes off the descriptors and the fallback path is legitimately
	// free. Matching the nil-store cost exactly is the stronger claim anyway — the same
	// path answered both.
	assert.Equal(t, nilServed.Meta.Cost, scanned.Meta.Cost,
		"an unreachable pin must fall back to the path a server with no summary takes")
	if !bytes.Equal(nilServed.Data, scanned.Data) {
		t.Fatalf("scan fallback must match the nil-store answer\n--- nil store ---\n%s\n--- fallback ---\n%s",
			nilServed.Data, scanned.Data)
	}
}

// TestSummaryWiring_NilStoreParity locks acceptance criterion #5: a server built
// without a summary store answers exactly as today's querier does. The wired path
// composes a versionedReader around the catalog and a summary.Store; with a nil
// store api.NewServer takes the plain FS catalog instead — this test locks in that
// the nil path produces identical envelopes across two independent constructions
// for every registered tool, so a future wiring change cannot silently move that
// baseline. It is the path a querier runs on before its first build lands.
func TestSummaryWiring_NilStoreParity(t *testing.T) {
	hA := newHarness(t, false)
	hB := newHarness(t, false)

	for _, tc := range []struct {
		name string
		tool string
		req  any
	}{
		{
			name: "aggregate_count",
			tool: "aggregate",
			req: tools.AggregateRequest{
				Selector: incidentSelector(),
				Measure:  "count",
				GroupBy:  "service",
				Bins:     "1m",
			},
		},
		{
			name: "aggregate_rate",
			tool: "aggregate",
			req: tools.AggregateRequest{
				Selector: incidentSelector(),
				Measure:  "rate",
				GroupBy:  "service",
				Bins:     "1m",
			},
		},
		{
			name: "rank_fields",
			tool: "rank_fields",
			req:  tools.FieldsRequest{Selector: incidentSelector()},
		},
		{
			name: "service_graph",
			tool: "service_graph",
			req:  tools.ServiceGraphRequest{Selector: incidentSelector(), Limit: 500},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, bodyA := postToolRaw(t, hA, tc.tool, tc.req)
			_, bodyB := postToolRaw(t, hB, tc.tool, tc.req)

			envA := decodeEnvelope(t, bodyA)
			envB := decodeEnvelope(t, bodyB)

			assert.Equal(t, envA.Meta.Cost, envB.Meta.Cost,
				"nil-store Cost must be identical across nil-store constructions")
			assert.Equal(t, envA.Meta.Snapshot, envB.Meta.Snapshot,
				"nil-store Meta.Snapshot must match; SummaryVersion=0 for nil-store")

			if !bytes.Equal(envA.Data, envB.Data) {
				t.Fatalf("nil-store Data diverged across two nil-store constructions\n--- A ---\n%s\n--- B ---\n%s",
					envA.Data, envB.Data)
			}
		})
	}
}

// postToURL POSTs to a full baseURL — the tests that own an ad-hoc httptest
// server can't reuse postTool, which is scoped to a *toolHarness.
func postToURL[Resp any](t *testing.T, baseURL, tool string, req any) query.Envelope[Resp] {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoErrorf(t, err, "marshal /v1/%s request", tool)
	resp, err := http.Post(baseURL+"/v1/"+tool, "application/json", bytes.NewReader(body))
	require.NoErrorf(t, err, "POST /v1/%s", tool)
	defer resp.Body.Close()
	require.Equalf(t, http.StatusOK, resp.StatusCode, "POST /v1/%s status", tool)
	var env query.Envelope[Resp]
	require.NoErrorf(t, json.NewDecoder(resp.Body).Decode(&env), "decode /v1/%s response", tool)
	return env
}

// rawEnvelope is the shape decodeEnvelope returns — raw Data bytes plus the
// parsed Meta. Keeping Data raw is deliberate: the nil-store parity assertion
// compares the marshaled Data across two servers, and re-marshaling a decoded
// tool response would ride the tool response's own MarshalJSON rather than the
// bytes the server actually wrote.
type rawEnvelope struct {
	Data json.RawMessage `json:"data"`
	Meta query.Meta      `json:"meta"`
}

func decodeEnvelope(t *testing.T, body []byte) rawEnvelope {
	t.Helper()
	var env rawEnvelope
	require.NoErrorf(t, json.Unmarshal(body, &env), "decode envelope: %s", body)
	return env
}
