package tools_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/storage"
	"github.com/datafocal/datafocal/internal/summary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attributeBase is minute-aligned so the default bin grid needs no snapping and
// the incident and baseline windows tile it exactly.
var attributeBase = time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)

const attributeSpan = 10 * time.Minute

// placementEvent is one line of a placement stream: the record that says a
// subject is running on an entity. The seed corpus' scheduler emits exactly this
// shape, and it is the only thing linking a service's lines to a host.
func placementEvent(ts time.Time, pod, host string) segment.Record {
	return segment.LogRecord(ts, "info", fmt.Sprintf("bound %s to %s", pod, host), map[string]string{"pod": pod, "host": host})
}

// activity writes one segment of n indistinguishable lines for service, starting
// at start. Line count is all the attribution reads, so the message shape is
// deliberately uniform.
func activity(t *testing.T, store storage.Store, cat catalog.Catalog, service string, start time.Time, n int) {
	t.Helper()
	msgs := make([]string, n)
	for i := range msgs {
		msgs[i] = fmt.Sprintf("wrote block %d", i)
	}
	writeSegment(t, store, cat, service, messageEvents(start, msgs...))
}

// seedAttributionCorpus builds the incident shape the tool exists to explain:
// two services placed on host-7 and one placed elsewhere, with analytics-export
// going from half the host's lines in the baseline window to nearly all of them
// during the incident.
//
// Counts are chosen to divide evenly (99/1 of 100, 10/10 of 20) so the reported
// percentages are exact and the expectation can be a whole-object compare.
func seedAttributionCorpus(t *testing.T) string {
	t.Helper()
	dir, store, cat := newCorpus(t)

	// Baseline window, [base-10m, base): the two host-7 services split it evenly.
	activity(t, store, cat, "analytics-export", attributeBase.Add(-5*time.Minute), 10)
	activity(t, store, cat, "db-3", attributeBase.Add(-5*time.Minute), 10)

	// Incident window, [base, base+10m). Segments start a minute in so none of
	// them touches the baseline window's closing boundary.
	incident := attributeBase.Add(time.Minute)
	writeSegment(t, store, cat, "scheduler", []segment.Record{
		placementEvent(incident, "analytics-export-0", "host-7"),
		placementEvent(incident.Add(time.Second), "db-3-0", "host-7"),
		placementEvent(incident.Add(2*time.Second), "gateway-0", "host-4"),
	})
	activity(t, store, cat, "analytics-export", incident, 99)
	activity(t, store, cat, "db-3", incident, 1)
	activity(t, store, cat, "gateway", incident, 50) // on host-4; must not appear
	return dir
}

// attributeRequest scopes the incident window over the seeded corpus.
func attributeRequest(entity string) tools.AttributeWritesRequest {
	return tools.AttributeWritesRequest{
		Selector: selector.Selector{
			Time: window(attributeBase, attributeBase.Add(attributeSpan)),
		},
		Entity: tools.EntityRef{Value: entity},
	}
}

// TestAttributeWrites pins the attribution each corpus and request yields, including the typed errors.
func TestAttributeWrites(t *testing.T) {
	nodeRequest := attributeRequest("node-2")
	nodeRequest.Entity.Field = "node"
	nodeRequest.Placement = tools.PlacementRef{Service: "nomad", SubjectField: "alloc"}

	tests := []struct {
		name string
		// corpus seeds the corpus, and nil reads through no catalog or reader at all.
		corpus func(*testing.T) string
		req    tools.AttributeWritesRequest
		want   tools.AttributeWritesResponse
		// wantKL, when set, is checked to 1e-3 with a positive MI, and both are zeroed before the whole-object compare.
		wantKL float64
		// warmBinWidth, when set, also answers from a summary of that bin width, which must agree with the scan and report Approx.
		warmBinWidth time.Duration
		wantErr      func(*testing.T, error)
	}{
		{
			// Only the services placed on host-7 appear, ranked by share, and the jump from an even split to 99/1 is anomalous.
			name:   "splits the entity by group",
			corpus: seedAttributionCorpus,
			req:    attributeRequest("host-7"),
			want: tools.AttributeWritesResponse{
				Breakdown: []stats.Share{
					{Key: "analytics-export", Lines: 99, Pct: 99},
					{Key: "db-3", Lines: 1, Pct: 1},
				},
				Anomalous: true,
				Note:      "groups resolved from the placement stream; incident vs baseline split compared",
			},
			// A smoothed 100/2 against a smoothed 11/11 pulls the raw 0.6371 down slightly.
			wantKL: 0.5966,
			// 15s bins split every activity segment (99s and 10s long), which the summary counts from rows.
			warmBinWidth: 15 * time.Second,
		},
		{
			// An entity nothing is placed on is an empty breakdown and a note, not an error.
			name:   "entity with no placements",
			corpus: seedAttributionCorpus,
			req:    attributeRequest("host-99"),
			want: tools.AttributeWritesResponse{
				Breakdown: []stats.Share{},
				Note:      `no pod found on host "host-99"`,
			},
		},
		{
			// Another placement stream and entity field attribute over that dimension instead of hosts and pods.
			name: "names the entity dimension",
			corpus: func(t *testing.T) string {
				dir, store, cat := newCorpus(t)
				incident := attributeBase.Add(time.Minute)
				writeSegment(t, store, cat, "nomad", []segment.Record{segment.LogRecord(incident, "info", "placed alloc", map[string]string{"alloc": "ingest-0", "node": "node-2"})})
				activity(t, store, cat, "ingest", incident, 8)
				return dir
			},
			req: nodeRequest,
			want: tools.AttributeWritesResponse{
				Breakdown: []stats.Share{{Key: "ingest", Lines: 8, Pct: 100}},
				Anomalous: true,
				Note:      "no baseline activity — this split appeared where none existed; groups resolved from the placement stream; incident vs baseline split compared",
			},
		},
		{
			// A corpus with no placement records is a typed error naming what was looked for.
			name: "without a placement stream",
			corpus: func(t *testing.T) string {
				dir, store, cat := newCorpus(t)
				activity(t, store, cat, "analytics-export", attributeBase.Add(time.Minute), 10)
				return dir
			},
			req: attributeRequest("host-7"),
			wantErr: func(t *testing.T, err error) {
				var apiErr *query.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Contains(t, apiErr.Message, `"scheduler"`)
				assert.Contains(t, apiErr.Message, `"pod"`)
				assert.Contains(t, apiErr.Message, `"host"`)
			},
		},
		{
			// The entity is not defaulted, because there is no sensible "some host".
			name: "entity required",
			req:  attributeRequest(""),
			wantErr: func(t *testing.T, err error) {
				var invalid *query.InvalidRequestError
				require.ErrorAs(t, err, &invalid)
				assert.Contains(t, err.Error(), "entity.value is required")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cr catalog.Catalog
			var rd segment.Reader
			if tt.corpus != nil {
				cr, rd = readers(tt.corpus(t))
			}
			ctx := context.Background()
			cold, err := toolsOver(cr, rd, nil).AttributeWrites(ctx, tt.req)
			if tt.wantErr != nil {
				tt.wantErr(t, err)
				return
			}
			require.NoError(t, err)
			assert.False(t, cold.Meta.Approx, "the scan counts rows by their own timestamps")

			if tt.warmBinWidth != 0 {
				rs := wireSummary(t, cr, rd, summary.Config{BinWidth: tt.warmBinWidth})
				warm, err := toolsOver(cr, rd, rs).AttributeWrites(ctx, tt.req)
				require.NoError(t, err)
				assert.Equal(t, cold.Data, warm.Data, "the summary must not change the attribution")
				assert.False(t, warm.Meta.Approx, "the summary counts rows by their own timestamps")
			}

			got := cold.Data
			if tt.wantKL != 0 {
				assert.InDelta(t, tt.wantKL, got.KL, 1e-3)
				assert.Positive(t, got.MI, "the window a line came from is predictable from its service")
				got.KL, got.MI = 0, 0
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestGroupFromSubject pins the replica-suffix strip. The cases that matter are
// the ones with no suffix: a bare VM or singleton container is its own group, so
// it still appears in the breakdown.
func TestGroupFromSubject(t *testing.T) {
	tests := []struct{ subject, want string }{
		{"analytics-export-0", "analytics-export"},
		{"db-3-0", "db-3"},
		{"db-3-1", "db-3"},
		{"a-b-c-99", "a-b-c"},
		{"no-digit-suffix", "no-digit-suffix"},
		{"nodash", "nodash"},
		{"trailing-", "trailing-"},
		{"-0", "-0"},
	}
	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			assert.Equal(t, tt.want, tools.ExportGroupFromSubject(tt.subject))
		})
	}
}

// TestGroupsPlacedOn asserts the reduction keeps only the requested entity and
// resolves each placed subject to its group: from what the activity was observed to
// carry when that is available, from the subject's own spelling when it is not, and
// not at all when the subject is its own group.
func TestGroupsPlacedOn(t *testing.T) {
	placements := map[string]string{
		"analytics-export-0": "host-7",
		"analytics-export-1": "host-7",
		"db-3-0":             "host-7",
		"gateway-0":          "host-4",
	}
	// What the window's activity actually carried: db's pods are under service "db",
	// which no amount of string surgery on "db-3-0" produces.
	observed := map[string]string{
		"analytics-export-0": "analytics-export",
		"analytics-export-1": "analytics-export",
		"db-3-0":             "db",
	}
	tests := []struct {
		name                   string
		observed               map[string]string
		entity, subject, group string
		want                   []string
	}{
		// The replicas collapse into their service, and db-3-0 resolves to "db".
		{"by service, observed", observed, "host-7", "pod", "service",
			[]string{"analytics-export", "db"}},
		// Nothing observed the subjects, so each group comes from the subject's own
		// spelling. "db-3" is the wrong answer — it is db's instance, not its service —
		// and it is the price of answering at all when the activity names no subject.
		{"by service, unobserved falls back to the subject's spelling", nil, "host-7", "pod", "service",
			[]string{"analytics-export", "db-3"}},
		// A subject observed under a group still loses to an exact match on itself.
		{"by pod", observed, "host-7", "pod", "pod",
			[]string{"analytics-export-0", "analytics-export-1", "db-3-0"}},
		{"absent entity", observed, "host-99", "pod", "service", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tools.ExportGroupsPlacedOn(placements, tt.observed, tt.entity, tt.subject, tt.group)
			if tt.want == nil {
				assert.Empty(t, got)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestAttributionEmptyWindows: each window can be empty on its own, and which one
// is empty is the finding. Divergence between the shapes is only meaningful once
// both windows have a shape.
func TestAttributionEmptyWindows(t *testing.T) {
	active := map[string]int64{"svc-a": 10, "svc-b": 5}

	tests := []struct {
		name               string
		incident, baseline map[string]int64
		wantAnomalous      bool
		wantNote           string
	}{
		{"both empty", map[string]int64{}, map[string]int64{}, false, ""},
		{"appeared", active, map[string]int64{"svc-a": 0}, true, "no baseline activity"},
		{"went silent", map[string]int64{"svc-a": 0}, active, true, "activity stopped"},
		{"unchanged", active, active, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tools.ExportAttribution(tt.incident, tt.baseline)

			assert.Equal(t, tt.wantAnomalous, got.Anomalous)
			assert.False(t, math.IsInf(got.KL, 1), "KL must stay marshalable")
			if tt.wantNote != "" {
				assert.Contains(t, got.Note, tt.wantNote)
			}
		})
	}
}
