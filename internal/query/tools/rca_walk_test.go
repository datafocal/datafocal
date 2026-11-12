package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/datafocal/datafocal/internal/topology"
)

var rcaBase = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

const rcaBinWidth = time.Minute
const rcaSnapshot = "e1.s1"

func newRCAStore(t *testing.T, segs ...summary.Segment) summary.Store {
	t.Helper()
	rs, err := summary.NewStore(summary.Config{
		BinWidth:  rcaBinWidth,
		Retention: 365 * 24 * time.Hour,
		Topology:  topology.Config{MinEdgeWeight: 0.01},
	}, "1", rcaBase, segs...)
	require.NoError(t, err)
	return rs
}

// rcaPulseService seeds a single-bin segment under a specific service so the
// pattern→service mapping is meaningful for topology admission.
func rcaPulseService(service, patternID string, binIndex, rows int) summary.Segment {
	binStart := rcaBase.Add(time.Duration(binIndex) * rcaBinWidth).UnixMilli()
	desc := segment.Descriptor{
		ID:     fmt.Sprintf("%s-bin%d", patternID, binIndex),
		Stream: segment.Stream{Service: service},
		TimeRange: segment.TimeRange{
			Min: time.UnixMilli(binStart + 1000).UTC(),
			Max: time.UnixMilli(binStart + 2000).UTC(),
		},
		Rows: rows,
	}
	return summary.Segment{Descriptor: desc, Footer: patternFooter(service, patternID, rows)}
}

func rcaRequest(symptom string, candidates []string, opts ...func(*tools.RCAWalkRequest)) tools.RCAWalkRequest {
	refs := make([]tools.SeriesRef, len(candidates))
	for i, c := range candidates {
		refs[i] = tools.SeriesRef{PatternID: c}
	}
	req := tools.RCAWalkRequest{
		Selector: selector.Selector{
			Time: window(rcaBase, rcaBase.Add(8*rcaBinWidth)),
		},
		Symptom:       tools.SeriesRef{PatternID: symptom},
		Candidates:    refs,
		MaxLagSeconds: 300,
		MinRho:        0.3,
		Snapshot:      rcaSnapshot,
	}
	for _, o := range opts {
		o(&req)
	}
	return req
}

// rcaPulse is one seeded single-bin segment: a service emitting a pattern in a bin.
type rcaPulse struct {
	service   string
	patternID string
	bin       int
	rows      int
}

// TestRCAWalkGates drives the gate on a synthetic summary with no segment reader, so
// every admission and rejection is decided by the store's own topology and the
// correlate step alone. Each case states the whole chain and the whole rejection set:
// a per-field check would not notice a candidate that leaked into the wrong one.
//
// Rho and LagSeconds are zeroed before the compare. They are computed from the seeded
// shape, and the case that turns on their values ("ordered by lag") says so through
// the chain's order rather than through a float this table would have to hard-code.
func TestRCAWalkGates(t *testing.T) {
	const symptom = "tmpl-s"

	noEdge := func(to string) tools.RCARejection {
		return tools.RCARejection{PatternFrom: symptom, PatternTo: to, RejectedBy: "no_attribution"}
	}

	tests := []struct {
		name         string
		pulses       []rcaPulse
		edges        [][2]string
		candidates   []string
		opts         []func(*tools.RCAWalkRequest)
		wantChain    []string
		wantRejected []tools.RCARejection
		wantCoverage float64
	}{
		{
			// A and B lead the symptom by 2 and 1 bins under services that share an
			// edge with it (so lag -120s and -60s); C leads by 3 under a service that
			// does not.
			name: "topology edge admits, its absence rejects",
			pulses: []rcaPulse{
				{"svc-s", symptom, 4, 10},
				{"svc-a", "tmpl-a", 2, 10},
				{"svc-b", "tmpl-b", 3, 10},
				{"svc-c", "tmpl-c", 1, 10},
			},
			edges:        [][2]string{{"svc-s", "svc-a"}, {"svc-s", "svc-b"}},
			candidates:   []string{"tmpl-a", "tmpl-b", "tmpl-c"},
			wantChain:    []string{"tmpl-a", "tmpl-b"},
			wantRejected: []tools.RCARejection{noEdge("tmpl-c")},
			wantCoverage: 1.0,
		},
		{
			name: "no admission path leaves an empty chain",
			pulses: []rcaPulse{
				{"svc-s", symptom, 4, 10},
				{"svc-d", "tmpl-d", 2, 10},
			},
			candidates:   []string{"tmpl-d"},
			wantChain:    []string{},
			wantRejected: []tools.RCARejection{noEdge("tmpl-d")},
			wantCoverage: 1.0,
		},
		{
			// The edge is present, so the below-rho drop has to come first: a candidate
			// that never moves with the symptom is not evidence, whatever the graph says.
			name: "flat candidate is dropped before the topology check",
			pulses: []rcaPulse{
				{"svc-s", symptom, 4, 10},
				{"svc-flat", "tmpl-flat", 0, 5}, {"svc-flat", "tmpl-flat", 1, 5},
				{"svc-flat", "tmpl-flat", 2, 5}, {"svc-flat", "tmpl-flat", 3, 5},
				{"svc-flat", "tmpl-flat", 4, 5}, {"svc-flat", "tmpl-flat", 5, 5},
				{"svc-flat", "tmpl-flat", 6, 5}, {"svc-flat", "tmpl-flat", 7, 5},
			},
			edges:      [][2]string{{"svc-s", "svc-flat"}},
			candidates: []string{"tmpl-flat"},
			opts:       []func(*tools.RCAWalkRequest){func(r *tools.RCAWalkRequest) { r.MinRho = 0.5 }},
			wantChain:  []string{},
			wantRejected: []tools.RCARejection{
				{PatternFrom: symptom, PatternTo: "tmpl-flat", RejectedBy: "below_rho"},
			},
			wantCoverage: 1.0,
		},
		{
			// Symptom at bin 6; X, Y, Z lead it by 4, 3, 2 bins, which CrossCorrelate
			// reports as lag -240s, -180s, -120s — a series that leads the symptom lags
			// it negatively. Ascending order therefore reads origin-first: the candidate
			// that moved earliest, and so is furthest up the causal path, comes first.
			name: "chain is ordered by lag ascending, earliest leader first",
			pulses: []rcaPulse{
				{"svc-s", symptom, 6, 10},
				{"svc-x", "tmpl-x", 2, 10},
				{"svc-y", "tmpl-y", 3, 10},
				{"svc-z", "tmpl-z", 4, 10},
			},
			edges:        [][2]string{{"svc-s", "svc-x"}, {"svc-s", "svc-y"}, {"svc-s", "svc-z"}},
			candidates:   []string{"tmpl-x", "tmpl-y", "tmpl-z"},
			wantChain:    []string{"tmpl-x", "tmpl-y", "tmpl-z"},
			wantRejected: []tools.RCARejection{},
			wantCoverage: 1.0,
		},
		{
			// Volume ranking would pick tmpl-loud at its baseline rate over tmpl-quiet, which appears
			// from nothing like a pattern that only fires when something breaks.
			name: "auto-picked candidates are ranked by signal, not volume",
			pulses: []rcaPulse{
				{"svc-s", symptom, 4, 10},
				// Background: 200 lines a bin before and during the window.
				{"svc-loud", "tmpl-loud", -4, 200}, {"svc-loud", "tmpl-loud", 2, 200},
				// Appeared: nothing before, a trickle now.
				{"svc-quiet", "tmpl-quiet", 3, 5},
			},
			opts:         []func(*tools.RCAWalkRequest){func(r *tools.RCAWalkRequest) { r.MaxCandidates = 1 }},
			wantChain:    []string{},
			wantRejected: []tools.RCARejection{noEdge("tmpl-quiet")},
			wantCoverage: 1.0 / 2.0,
		},
		{
			// Four patterns with identical evidence: the id decides which two survive the
			// cut, not the map iteration order they were ranked from.
			name: "score ties are cut by pattern id",
			pulses: []rcaPulse{
				{"svc-s", symptom, 4, 10},
				{"svc-a", "tmpl-a", 2, 10}, {"svc-b", "tmpl-b", 2, 10},
				{"svc-c", "tmpl-c", 2, 10}, {"svc-d", "tmpl-d", 2, 10},
			},
			opts:         []func(*tools.RCAWalkRequest){func(r *tools.RCAWalkRequest) { r.MaxCandidates = 2 }},
			wantChain:    []string{},
			wantRejected: []tools.RCARejection{noEdge("tmpl-a"), noEdge("tmpl-b")},
			wantCoverage: 2.0 / 4.0,
		},
		{
			// A 5000-line happy path that never changes must lose the single candidate slot to a cause that logs 3 lines only once it breaks.
			name: "the quiet cause outranks the chatty happy path",
			pulses: []rcaPulse{
				{"svc-s", symptom, 5, 100},
				{"svc-chatty", "tmpl-chatty", -6, 5000}, {"svc-chatty", "tmpl-chatty", -4, 5000},
				{"svc-chatty", "tmpl-chatty", -2, 5000}, {"svc-chatty", "tmpl-chatty", 1, 5000},
				{"svc-chatty", "tmpl-chatty", 3, 5000}, {"svc-chatty", "tmpl-chatty", 5, 5000},
				{"svc-broken", "tmpl-broken", 3, 3},
			},
			edges:        [][2]string{{"svc-s", "svc-chatty"}, {"svc-s", "svc-broken"}},
			opts:         []func(*tools.RCAWalkRequest){func(r *tools.RCAWalkRequest) { r.MaxCandidates = 1 }},
			wantChain:    []string{"tmpl-broken"},
			wantRejected: []tools.RCARejection{},
			wantCoverage: 1.0 / 2.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := rcaSeedStore(t, tc.pulses, tc.edges)
			req := rcaRequest(symptom, tc.candidates, tc.opts...)
			env, err := toolsOver(catalog.Fake{}, segment.Fake{}, store).RCAWalk(context.Background(), req)
			require.NoError(t, err)
			again, err := toolsOver(catalog.Fake{}, segment.Fake{}, store).RCAWalk(context.Background(), req)
			require.NoError(t, err)
			b1, err := json.Marshal(env.Data)
			require.NoError(t, err)
			b2, err := json.Marshal(again.Data)
			require.NoError(t, err)
			assert.Equal(t, string(b1), string(b2), "same call + same snapshot must produce byte-identical output")

			chain := make([]string, 0, len(env.Data.Chain))
			for _, h := range env.Data.Chain {
				assert.NotEmpty(t, h.AdmittedBy, "every hop records the gate that admitted it")
				chain = append(chain, h.PatternTo)
			}
			assert.Equal(t, tc.wantChain, chain)

			rejected := make([]tools.RCARejection, 0, len(env.Data.Rejected))
			for _, r := range env.Data.Rejected {
				r.Rho, r.LagSeconds = 0, 0
				rejected = append(rejected, r)
			}
			assert.Equal(t, tc.wantRejected, rejected)

			assert.InDelta(t, tc.wantCoverage, env.Meta.Coverage, 1e-9)
			assert.Equal(t, rcaSnapshot, env.Meta.Snapshot)
		})
	}
}

// rcaSeedStore is a summary holding the given pulses and service topology edges.
func rcaSeedStore(t *testing.T, pulses []rcaPulse, edges [][2]string) summary.Store {
	t.Helper()
	var segs []summary.Segment
	for _, p := range pulses {
		segs = append(segs, rcaPulseService(p.service, p.patternID, p.bin, p.rows))
	}
	for _, e := range edges {
		segs = append(segs, seedTopologyEdge(e[0], e[1], rcaBase)...)
	}
	return newRCAStore(t, segs...)
}

// rcaVerdict is a walk's outcome reduced to what the gate decided, which stays fixed while rho and pattern ids vary.
type rcaVerdict struct {
	Chain    []rcaVerdictHop
	Rejected []tools.RejectReason
}

// rcaVerdictHop is one admitted hop: the gate that admitted it and the node it names.
type rcaVerdictHop struct {
	By, To string
}

func rcaVerdictOf(resp tools.RCAWalkResponse) rcaVerdict {
	var v rcaVerdict
	for _, h := range resp.Chain {
		v.Chain = append(v.Chain, rcaVerdictHop{By: h.AdmittedBy, To: h.To})
	}
	for _, r := range resp.Rejected {
		v.Rejected = append(v.Rejected, r.RejectedBy)
	}
	return v
}

// rcaRamp is n events a second apart from at, with attrs(i) as the i-th event's attributes when attrs is set.
func rcaRamp(at time.Time, n int, level, msg string, attrs func(i int) map[string]string) []segment.Record {
	events := make([]segment.Record, n)
	for i := range events {
		var attrsI map[string]string
		if attrs != nil {
			attrsI = attrs(i)
		}
		events[i] = segment.LogRecord(at.Add(time.Duration(i)*time.Second), level, msg, attrsI)
	}
	return events
}

// rcaPlace writes a scheduler record placing pod on host-7.
func rcaPlace(t *testing.T, fsStore storage.Store, cat catalog.Catalog, at time.Time, pod string) {
	t.Helper()
	writeSegment(t, fsStore, cat, "scheduler", []segment.Record{segment.LogRecord(at, "info", "placed pod on host", map[string]string{"pod": pod, "host": "host-7"})})
}

// rcaPatternIDs returns the pattern each named service emits in w, failing if one has none.
func rcaPatternIDs(t *testing.T, cr catalog.Catalog, rd segment.Reader, w selector.TimeSpec, services ...string) []string {
	t.Helper()
	patterns, err := toolsOver(cr, rd, nil).Patterns(context.Background(), tools.PatternsRequest{
		Selector: selector.Selector{Time: w}, Limit: 100,
	})
	require.NoError(t, err)
	ids := make([]string, len(services))
	for i, svc := range services {
		for _, p := range patterns.Data.Patterns {
			if p.Service == svc {
				ids[i] = p.PatternID
			}
		}
		require.NotEmpty(t, ids[i], "service %s must emit a pattern", svc)
	}
	return ids
}

// TestRCAWalkAttribution: a candidate under a service placed on the requested entity is
// admitted via the attribution gate even without a topology edge, and a storage failure
// while attributing fails the call rather than rejecting as no_attribution.
func TestRCAWalkAttribution(t *testing.T) {
	readErr := errors.New("object store unavailable")
	tests := []struct {
		name        string
		failColumns bool
		wantErr     error
		want        rcaVerdict
	}{
		{name: "placement admits without an edge", want: rcaVerdict{Chain: []rcaVerdictHop{{By: "attribution", To: "svc-a"}}}},
		{name: "column read failure surfaces", failColumns: true, wantErr: readErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, fsStore, cat := newCorpus(t)
			rcaPlace(t, fsStore, cat, rcaBase.Add(time.Minute), "svc-a-0")
			cr, rd := readers(dir)
			// Symptom svc-s pulses at bin 4 and candidate svc-a at bin 2, with no topology edge between them.
			rs := rcaSeedStore(t, []rcaPulse{{"svc-s", "tmpl-s", 4, 10}, {"svc-a", "tmpl-a", 2, 10}}, nil)
			if tc.failColumns {
				// Footers read through while every column read fails, as an object store erroring mid-scan would.
				through := rd
				rd = segment.Fake{
					FooterFn: func(loc segment.ObjectLocation) (segment.Footer, segment.Cost, error) {
						return through.Footer(context.Background(), loc)
					},
					ColumnsFn: func(segment.ObjectLocation, segment.Footer, int, []int, []string) (map[string]segment.ColumnChunk, segment.Cost, error) {
						return nil, segment.Cost{}, readErr
					},
				}
			}

			env, err := toolsOver(cr, rd, rs).RCAWalk(context.Background(),
				rcaRequest("tmpl-s", []string{"tmpl-a"}, func(r *tools.RCAWalkRequest) {
					r.Entity = tools.EntityRef{Field: "host", Value: "host-7"}
					r.Snapshot = ""
				}))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, rcaVerdictOf(env.Data))
			// This is the one path where rca_walk reads data the summary cannot serve, so its GETs are billed.
			assert.NotZero(t, env.Meta.Cost.GETs, "the attribution gate's placement scan must be billed")
		})
	}
}

func TestRCAWalkEmptyPatternIsInvalid(t *testing.T) {
	_, err := toolsOver(catalog.Fake{}, segment.Fake{}, newRCAStore(t)).RCAWalk(context.Background(),
		rcaRequest("", []string{"tmpl-a"}))

	var invalid *query.InvalidRequestError
	require.ErrorAs(t, err, &invalid)
}

// seedTopologyEdge registers a co-occurrence edge between two services by
// observing descriptors with overlapping trace HLLs.
func seedTopologyEdge(from, to string, base time.Time) []summary.Segment {
	binStart := base.UnixMilli()
	descFrom := segment.Descriptor{
		ID:     from + "-" + to + "-topo-from",
		Stream: segment.Stream{Service: from},
		TimeRange: segment.TimeRange{
			Min: time.UnixMilli(binStart).UTC(),
			Max: time.UnixMilli(binStart + 60000).UTC(),
		},
		Rows: 100,
	}
	descTo := segment.Descriptor{
		ID:     from + "-" + to + "-topo-to",
		Stream: segment.Stream{Service: to},
		TimeRange: segment.TimeRange{
			Min: time.UnixMilli(binStart).UTC(),
			Max: time.UnixMilli(binStart + 60000).UTC(),
		},
		Rows: 100,
	}
	return []summary.Segment{
		{Descriptor: descFrom, Footer: sgFooterWithTraceHLL(from, from+"-"+to, 0, 200)},
		{Descriptor: descTo, Footer: sgFooterWithTraceHLL(to, from+"-"+to, 0, 200)},
	}
}

// TestRCAWalkBoundsTheScopedScan: a filtered or measure-valued series cannot come from
// the summary — the count cube keys on (service, pattern, bin) and carries no dimension
// to filter on — so it scans. An unbounded scan over a wide window is the cost
// reserved for an explicit drill, so the walk caps it and refuses rather
// than reading past the cap.
//
// It refuses rather than truncating because a correlation computed over some of the
// segments is a different number from the one asked for, not an approximation of it.
func TestRCAWalkBoundsTheScopedScan(t *testing.T) {
	dir, fsStore, cat := newCorpus(t)
	// Six single-segment streams carry the same pattern, giving the scoped read six segments.
	for i := range 6 {
		writeSegment(t, fsStore, cat, fmt.Sprintf("svc-%d", i), []segment.Record{segment.LogRecord(rcaBase.Add(time.Duration(i)*time.Minute), "info", "handled a request", map[string]string{"status": "503"})})
	}
	cr, rd := readers(dir)
	w := window(rcaBase, rcaBase.Add(10*time.Minute))
	// The six streams share one pattern, so its id is whichever one the corpus holds.
	patterns, err := toolsOver(cr, rd, nil).Patterns(context.Background(), tools.PatternsRequest{
		Selector: selector.Selector{Time: w}, Limit: 100,
	})
	require.NoError(t, err)
	require.Len(t, patterns.Data.Patterns, 1, "the six streams must share one pattern")
	tmpl := patterns.Data.Patterns[0].PatternID

	filtered := []selector.Predicate{{Field: "status", Op: selector.OpEq, Value: "503"}}
	walk := func(cap int) error {
		_, err := toolsOver(cr, rd, nil).RCAWalk(context.Background(), tools.RCAWalkRequest{
			Selector:        selector.Selector{Time: w},
			Symptom:         tools.SeriesRef{PatternID: tmpl, Where: filtered},
			Candidates:      []tools.SeriesRef{{PatternID: tmpl}},
			MaxScanSegments: cap,
		})
		return err
	}

	var apiErr *query.APIError
	require.ErrorAs(t, walk(2), &apiErr, "a scoped read past the cap must refuse")
	assert.Equal(t, query.ErrCodeScanTooLarge, apiErr.Code)
	assert.NotEmpty(t, apiErr.Hint, "the refusal must carry the re-plan")

	assert.NoError(t, walk(64), "the same walk inside the cap must answer")
	assert.NoError(t, walk(-1), "a negative cap is the explicit opt-out")
}

// TestRCAWalkScopedScanReadsOnlyPatternBearingSegments pins that a filtered walk's cost tracks
// the streams emitting its series, which the descriptor's pattern list allows without a read.
func TestRCAWalkScopedScanReadsOnlyPatternBearingSegments(t *testing.T) {
	run := func(decoys int) query.Cost {
		dir, fsStore, cat := newCorpus(t)
		writeSegment(t, fsStore, cat, "subject", []segment.Record{
			segment.LogRecord(rcaBase.Add(time.Minute), "info", "subject handled a request", map[string]string{"status": "503"}),
			segment.LogRecord(rcaBase.Add(2*time.Minute), "info", "subject handled a request", map[string]string{"status": "200"}),
		})
		// Noise streams in the same window, emitting a different pattern.
		for i := range decoys {
			writeSegment(t, fsStore, cat, fmt.Sprintf("noise-%d", i), []segment.Record{segment.LogRecord(rcaBase.Add(time.Duration(i+2)*time.Second), "info", "noise did something else entirely", nil)})
		}
		cr, rd := readers(dir)
		w := window(rcaBase, rcaBase.Add(10*time.Minute))
		subject := rcaPatternIDs(t, cr, rd, w, "subject")[0]

		env, err := toolsOver(cr, rd, nil).RCAWalk(context.Background(), tools.RCAWalkRequest{
			Selector: selector.Selector{Time: w},
			Symptom: tools.SeriesRef{PatternID: subject, Where: []selector.Predicate{
				{Field: "status", Op: selector.OpEq, Value: "503"},
			}},
			Candidates: []tools.SeriesRef{{PatternID: subject}},
		})
		require.NoError(t, err)
		return env.Meta.Cost
	}

	small, large := run(2), run(10)
	// BytesRead is the quantity narrowing controls, and the walk's other stages make it
	// the only honest one to assert on: patternStats needs every pattern's owning
	// service and topologyView needs the whole graph, so both read one footer per segment
	// in the window and their GETs grow with it by design. Footer reads bill a GET and no
	// bytes, so every byte here came from a column read — and a column read of a segment
	// that does not carry the series is exactly what narrowing removes.
	assert.Positive(t, small.BytesRead, "the scoped series must actually have been scanned")
	assert.Equal(t, small.BytesRead, large.BytesRead,
		"column reads must not grow with streams that do not carry the pattern: 2 decoys=%d bytes vs 10=%d",
		small.BytesRead, large.BytesRead)
}

// TestRCAWalkAdmitsTheEntitysOwnTelemetry: a resource's own readings are evidence about
// the resource, and the attribution gate cannot say so — it asks which workload owns a
// share of host-7, and host-7 owns no share of itself. So the host's disk series
// correlated with the symptom and was rejected for having no share, which is the gate
// failing to express what the series is rather than the series being irrelevant.
//
// The entity gate reads it off the descriptors: a segment whose entity column holds one
// value is that entity's telemetry. That costs no reads and answers the same warm or
// cold, and the hop is labelled with the resource rather than the stream that reported
// it, because the node is the resource.
func TestRCAWalkAdmitsTheEntitysOwnTelemetry(t *testing.T) {
	const entity = "host-7"
	admitted := rcaVerdict{Chain: []rcaVerdictHop{{By: "entity", To: entity}}}
	rejected := func(r tools.RejectReason) rcaVerdict { return rcaVerdict{Rejected: []tools.RejectReason{r}} }
	// One stream reporting many hosts, so host-7 is inside the range and the manifest cannot settle it.
	spansManyHosts := func(i int) string { return fmt.Sprintf("host-%d", i+5) }

	tests := []struct {
		name string
		// rowHost gives the per-row host value, or nil to leave rows without one.
		rowHost func(i int) string
		// streamHost records the host as a stream label instead, when set.
		streamHost string
		// exporter adds a second service emitting the disk pattern over two hosts, and scopes both series to their own service under a cap of 4.
		exporter bool
		want     rcaVerdict
	}{
		{name: "stamped on every row", rowHost: func(int) string { return entity }, want: admitted},
		{name: "recorded once as a stream label", streamHost: entity, want: admitted},
		{name: "a stream labelled with another entity is unrelated", streamHost: "host-1", want: rejected(tools.RejectNoAttribution)},
		{name: "rows spanning several entities are settled by reading them", rowHost: spansManyHosts, want: admitted},
		{
			// The series reads fit the bound and only the confirmation, which must consider the exporter too, does not.
			name: "past the scan bound the check stays unresolved", rowHost: spansManyHosts, exporter: true,
			want: rejected(tools.RejectEntityIndeterminate),
		},
		{
			// host-0..host-3 cannot contain host-7, so the manifest settles it and nothing is read.
			name:    "rows excluding the entity need no read",
			rowHost: func(i int) string { return fmt.Sprintf("host-%d", i) }, want: rejected(tools.RejectNoAttribution),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir, fsStore, cat := newCorpus(t)
			// Both series ramp together over four bins so they clear the rho floor.
			for bin := range 4 {
				at := rcaBase.Add(time.Duration(bin) * time.Minute)
				writeSegment(t, fsStore, cat, "gateway", rcaRamp(at, bin+1, "error", "gateway returned a failure", nil))
				var hostAttrs func(int) map[string]string
				if tc.rowHost != nil {
					hostAttrs = func(i int) map[string]string { return map[string]string{"host": tc.rowHost(i)} }
				}
				var labels map[string]string
				if tc.streamHost != "" {
					labels = map[string]string{"host": tc.streamHost}
				}
				writeLabeledSegment(t, fsStore, cat, "host", labels, rcaRamp(at, bin+1, "warn", "disk saturated somewhat", hostAttrs))
				if tc.exporter {
					writeSegment(t, fsStore, cat, "exporter", rcaRamp(at.Add(30*time.Second), 2, "warn", "disk saturated somewhat",
						func(i int) map[string]string { return map[string]string{"host": []string{"host-5", "host-8"}[i]} }))
				}
			}
			cr, rd := readers(dir)
			w := window(rcaBase, rcaBase.Add(4*time.Minute))
			ids := rcaPatternIDs(t, cr, rd, w, "gateway", "host")

			req := tools.RCAWalkRequest{
				Selector:   selector.Selector{Time: w},
				Symptom:    tools.SeriesRef{PatternID: ids[0]},
				Candidates: []tools.SeriesRef{{PatternID: ids[1]}},
				Entity:     tools.EntityRef{Field: "host", Value: entity},
				MinRho:     0.5,
			}
			if tc.exporter {
				req.Symptom.Where = []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "gateway"}}
				req.Candidates[0].Where = []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "host"}}
				req.MaxScanSegments = 4
			}
			env, err := toolsOver(cr, rd, nil).RCAWalk(context.Background(), req)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rcaVerdictOf(env.Data))
		})
	}
}

// rcaPlacedDBCorpus seeds db-3-0 placed on host-7, a gateway symptom from symptom(at, bin)
// and a db-3 instance's slowness ramping over four bins, returning the two pattern ids.
func rcaPlacedDBCorpus(t *testing.T, symptom func(at time.Time, bin int) []segment.Record) (catalog.Catalog, segment.Reader, string, string) {
	t.Helper()
	dir, fsStore, cat := newCorpus(t)
	rcaPlace(t, fsStore, cat, rcaBase, "db-3-0")
	onDB := func(int) map[string]string { return map[string]string{"pod": "db-3-0"} }
	for bin := range 4 {
		at := rcaBase.Add(time.Duration(bin) * time.Minute)
		writeSegment(t, fsStore, cat, "gateway", symptom(at, bin))
		writeLabeledSegment(t, fsStore, cat, "db", map[string]string{"instance": "db-3"},
			rcaRamp(at, bin+1, "warn", "db was slow again", onDB))
	}
	cr, rd := readers(dir)
	ids := rcaPatternIDs(t, cr, rd, window(rcaBase, rcaBase.Add(4*time.Minute)), "gateway", "db")
	return cr, rd, ids[0], ids[1]
}

// TestRCAWalkNamesNodesByTheRequestedDimension: the seed runs two db instances and only
// db-3 degraded, so a chain that says "db" has dropped the thing an operator acts on.
// node_by names the dimension the chain's nodes carry.
//
// It changes labelling only. The gates key on the service either way, because that is what
// the topology graph and the attribution breakdown are keyed on, so the same candidate is
// admitted by the same gate whichever name the node ends up with.
func TestRCAWalkNamesNodesByTheRequestedDimension(t *testing.T) {
	cr, rd, symptomID, slowID := rcaPlacedDBCorpus(t, func(at time.Time, bin int) []segment.Record {
		return rcaRamp(at, bin+1, "error", "gateway returned a failure", nil)
	})

	tests := []struct {
		name   string
		nodeBy string
		wantTo string
	}{
		{name: "the default names nodes for the service", nodeBy: "", wantTo: "db"},
		{name: "node_by=instance names the instance that degraded", nodeBy: "instance", wantTo: "db-3"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// db-3-0 is placed on host-7, so attribution admits the db series and labels the hop from the node dimension.
			env, err := toolsOver(cr, rd, nil).RCAWalk(context.Background(), tools.RCAWalkRequest{
				Selector:   selector.Selector{Time: window(rcaBase, rcaBase.Add(4*time.Minute))},
				Symptom:    tools.SeriesRef{PatternID: symptomID},
				Candidates: []tools.SeriesRef{{PatternID: slowID}},
				Entity:     tools.EntityRef{Field: "host", Value: "host-7"},
				NodeBy:     tc.nodeBy,
				MinRho:     0.5,
			})
			require.NoError(t, err)
			assert.Equal(t, rcaVerdict{Chain: []rcaVerdictHop{{By: "attribution", To: tc.wantTo}}}, rcaVerdictOf(env.Data))
		})
	}
}

// TestRCAWalkPath: the path is a route through intermediate nodes, not the star of direct
// causes the chain reports, and it only goes deeper than one hop when the grid can order two
// candidates and something admits the link between them.
func TestRCAWalkPath(t *testing.T) {
	tests := []struct {
		name      string
		binA      int
		binB      int
		pairEdge  bool
		wantRoute [][2]string
		why       string
	}{
		{
			name: "ordered and admitted", binA: 2, binB: 4, pairEdge: true,
			wantRoute: [][2]string{{"svc-a", "svc-b"}, {"svc-b", "svc-s"}},
			why:       "the earliest mover reaches the symptom through the one between them",
		},
		{
			name: "ordered but no mechanism", binA: 2, binB: 4, pairEdge: false,
			wantRoute: [][2]string{{"svc-a", "svc-s"}},
			why:       "co-movement with the symptom is not evidence that the two are related",
		},
		{
			name: "same onset cannot be ordered", binA: 3, binB: 3, pairEdge: true,
			wantRoute: [][2]string{{"svc-a", "svc-s"}},
			why:       "two candidates the grid puts at the same moment have no direction between them",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			edges := [][2]string{{"svc-s", "svc-a"}, {"svc-s", "svc-b"}}
			if tc.pairEdge {
				edges = append(edges, [2]string{"svc-a", "svc-b"})
			}
			store := rcaSeedStore(t, []rcaPulse{
				{"svc-s", "tmpl-s", 6, 10}, {"svc-a", "tmpl-a", tc.binA, 10}, {"svc-b", "tmpl-b", tc.binB, 10},
			}, edges)

			env, err := toolsOver(catalog.Fake{}, segment.Fake{}, store).RCAWalk(context.Background(),
				rcaRequest("tmpl-s", []string{"tmpl-a", "tmpl-b"}))
			require.NoError(t, err)

			route := make([][2]string, 0, len(env.Data.Path))
			for _, hop := range env.Data.Path {
				assert.NotEmpty(t, hop.AdmittedBy, "every path hop states the mechanism that admitted it")
				assert.NotEmpty(t, hop.PatternFrom, "and which series it was measured from")
				assert.NotEmpty(t, hop.PatternTo)
				route = append(route, [2]string{hop.From, hop.To})
			}
			assert.Equal(t, tc.wantRoute, route, tc.why)

			chain := make([]string, 0, len(env.Data.Chain))
			for _, hop := range env.Data.Chain {
				chain = append(chain, hop.PatternTo)
			}
			assert.Equal(t, []string{"tmpl-a", "tmpl-b"}, chain,
				"both candidates stay the symptom's direct causes, ordered earliest-leader first")
		})
	}
}

// TestRCAWalkAcceptsARatioSymptom is acceptance #3: with traffic held constant the
// symptom's error rate is its error count up to a scale factor, and cross-correlation is
// scale-free, so a ratio symptom must drive the identical chain and rejections a
// filtered-count one does — the proof the gates need no ratio-specific branch. The
// unfiltered symptom is the control: its flat count correlates with nothing, the false
// negative a ratio answers.
func TestRCAWalkAcceptsARatioSymptom(t *testing.T) {
	// Four requests every bin with the 5xx share ramping, so the error rate and count climb with the same shape.
	cr, rd, symptomID, slowID := rcaPlacedDBCorpus(t, func(at time.Time, bin int) []segment.Record {
		return statusEvents(at, "gateway returned a failure", 4, bin+1)
	})

	walk := func(symptom tools.SeriesRef) (tools.RCAWalkResponse, error) {
		env, err := toolsOver(cr, rd, nil).RCAWalk(context.Background(), tools.RCAWalkRequest{
			Selector:   selector.Selector{Time: window(rcaBase, rcaBase.Add(4*time.Minute))},
			Symptom:    symptom,
			Candidates: []tools.SeriesRef{{PatternID: slowID}},
			Entity:     tools.EntityRef{Field: "host", Value: "host-7"},
			MinRho:     0.5,
			// A minute grid puts every bin's constant traffic in one cell, so the ratio has
			// no gap and lines up with the count; a finer auto grid would gap the ratio.
			Bins: "1m",
		})
		return env.Data, err
	}

	errSlice := []selector.Predicate{{Field: "status", Op: selector.OpGe, Value: "500"}}
	count, err := walk(tools.SeriesRef{PatternID: symptomID, Where: errSlice})
	require.NoError(t, err)
	ratio, err := walk(tools.SeriesRef{PatternID: symptomID, Measure: "ratio", Where: errSlice})
	require.NoError(t, err)

	assert.Equal(t, rcaVerdict{Chain: []rcaVerdictHop{{By: "attribution", To: "db"}}}, rcaVerdictOf(ratio),
		"the ratio symptom correlates with the db series and attribution admits it")
	// The two symptoms are one series up to scale, so the walk they drive is identical.
	assert.Equal(t, count.Chain, ratio.Chain, "ratio and error-count symptoms drive the same chain")
	assert.Equal(t, count.Rejected, ratio.Rejected)

	// A flat whole-pattern count must error rather than return an empty chain, because
	// "nothing correlates" and "this series never moves" call for different re-plans.
	_, plainErr := walk(tools.SeriesRef{PatternID: symptomID})
	var apiErr *query.APIError
	require.ErrorAs(t, plainErr, &apiErr,
		"a flat symptom must refuse with a typed, re-plannable error")
	assert.Equal(t, query.ErrCodeUnusableSeries, apiErr.Code)
	assert.NotEmpty(t, apiErr.Hint, "the refusal must carry the re-plan")
}
