package tools

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/plan"
	"github.com/datafocal/datafocal/internal/query/scan"
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/topology"
	"github.com/datafocal/datafocal/internal/value"
)

const (
	defaultMaxCandidates       = 32
	maxMaxCandidates           = 128
	defaultMinRho              = 0.5
	defaultMinAttributionShare = 0.20
	defaultMaxLagSeconds       = 300
	// defaultMaxScanSegments and defaultMaxScanBytes bound what any one scoped series
	// read may touch. A walk whose symptom or candidate carries a filter or a measure
	// cannot be served from the summary — the count cube has no dimension to filter on
	// (summary.CountKey) — so it scans, and an unbounded scan over a wide window is the
	// cost reserved for an explicit drill and forbidden on the always-on path.
	//
	// The segment ceiling follows the L0 flush cadence: at 30-60s a stream seals ~60
	// segments an hour, so 2048 covers a service with ~20 replicas over an incident
	// hour, or one stream over a day, where its predecessor of 256 fit four streams.
	defaultMaxScanSegments = 2048
	// The byte ceiling is the one that holds when the segments are not L0: compaction
	// takes them to 128-256 MB, where 2048 of them is terabytes.
	// 4 GB is about four seconds at the ≥1 GB/s/worker scan target.
	defaultMaxScanBytes = int64(4) << 30
)

// RCAWalkRequest is the wire request for the rca_walk tool.
type RCAWalkRequest struct {
	Selector selector.Selector `json:"selector"`
	// Symptom carries a measure and its own predicates, so it flows through the same
	// resolveSeries path a correlate series does: a ratio symptom (measure=ratio, the
	// numerator in its predicates) samples an error-rate and is correlated against
	// without any change to the gates, which are agnostic to how the series was measured.
	Symptom    SeriesRef   `json:"symptom" jsonschema:"the observed symptom series; the walk starts from its owning node. Carries a measure and its own predicates, so a symptom can be the 5xx slice of a pattern, or its error rate via measure=ratio, rather than the whole pattern"`
	Candidates []SeriesRef `json:"candidates,omitempty" jsonschema:"optional explicit candidate series; empty means auto-pick top-K high-signal patterns in the window as plain counts"`

	MaxCandidates int     `json:"max_candidates,omitempty" jsonschema:"cap on the auto-picked candidate set; defaults to 32, max 128"`
	MaxLagSeconds int     `json:"max_lag_seconds,omitempty" jsonschema:"± lag search for each correlate call; clamped to the window"`
	MinRho        float64 `json:"min_rho,omitempty" jsonschema:"floor on |rho|; candidates below it are dropped before gating; defaults to 0.5"`

	MinAttributionShare float64 `json:"min_attribution_share,omitempty" jsonschema:"attribution gate threshold; a group must own at least this fraction on the entity to corroborate; defaults to 0.20"`

	// MaxScanSegments bounds the segments one scoped series read may touch. Series the
	// summary can serve are unaffected — the bound is on reads, not on the window — so
	// raising it only widens what a filtered or measure-valued series may cost.
	MaxScanSegments int `json:"max_scan_segments,omitempty" jsonschema:"cap on segments a filtered or measure-valued series may scan; defaults to 2048, negative means uncapped"`

	// MaxScanBytes bounds the same read by the estimated bytes in scope, which is the
	// limit that binds once compaction makes a segment 128-256 MB. Defaults to 4 GB;
	// negative is uncapped, and whichever bound trips first refuses.
	MaxScanBytes int64 `json:"max_scan_bytes,omitempty" jsonschema:"cap on estimated bytes a scoped series read may scan; defaults to 4 GB, negative means uncapped"`

	// NodeBy is the dimension the chain's nodes are named for: the service by default, or
	// any dimension the stream carries, such as the instance. It changes what a hop is
	// labelled with and nothing about which hops are admitted — the gates key on the
	// service either way, because that is what the topology graph and the attribution
	// breakdown are keyed on.
	NodeBy string `json:"node_by,omitempty" jsonschema:"dimension naming chain nodes: service (default) or a stream label such as instance"`

	Bins      string       `json:"bins,omitempty" jsonschema:"grid width; empty takes the summary's own width"`
	Entity    EntityRef    `json:"entity,omitempty" jsonschema:"entity to attribute against for the attribution fallback gate"`
	Placement PlacementRef `json:"placement,omitempty"`
	Snapshot  string       `json:"snapshot,omitempty"`
}

// RCAWalkResponse holds the deterministic causal chain and every rejected candidate.
type RCAWalkResponse struct {
	Symptom string `json:"symptom"`
	// Patterns holds the text of the symptom and every pattern named in a hop or rejection.
	Patterns map[string]string `json:"patterns"`
	// Chain is the symptom's direct causes, ranked: every candidate the gate admitted
	// against the symptom itself, each carrying the mechanism that admitted it.
	Chain []RCAHop `json:"chain"`
	// Path is the route from the origin to the symptom, one hop per node.
	//
	// It answers a different question from Chain and the two diverge in both directions: a
	// candidate admitted only against another candidate reaches the symptom through it and
	// so appears here and not there, while a window whose candidates all link directly to
	// the symptom and not to each other has a Chain and no Path. Where they overlap, the
	// last hop of the Path is also in the Chain.
	Path     []RCAHop       `json:"path,omitempty"`
	Rejected []RCARejection `json:"rejected,omitempty"`
}

// RCAHop is one link in the causal chain.
type RCAHop struct {
	From             string     `json:"from"`
	To               string     `json:"to"`
	PatternFrom      string     `json:"pattern_from"`
	PatternTo        string     `json:"pattern_to"`
	Rho              float64    `json:"rho"`
	LagSeconds       float64    `json:"lag_seconds" jsonschema:"peak-correlation offset; negative means the candidate leads the symptom, which is the direction a cause points"`
	AdmittedBy       string     `json:"admitted_by" jsonschema:"topology|entity|attribution — the gate that admitted this hop"`
	TopologyEdge     *GraphEdge `json:"topology_edge,omitempty"`
	AttributionShare float64    `json:"attribution_share,omitempty"`
}

// RejectReason names the gate that declined a candidate. It is a type rather than a
// bare string so a construction site cannot invent one by typo, and RejectReasons is
// the single list the wire schema and the coverage test both read.
type RejectReason string

const (
	// RejectBelowRho: |rho| under the floor, so the co-movement is not evidence.
	RejectBelowRho RejectReason = "below_rho"
	// RejectSameNode: the candidate sits on the symptom's own node, so the link is a
	// restatement of the symptom rather than a cause of it.
	RejectSameNode RejectReason = "same_node"
	// RejectNoAttribution: not a co-tenant of the entity and below the share floor.
	RejectNoAttribution RejectReason = "no_attribution"
	// RejectEntityIndeterminate: the entity check could not decide, which is more
	// actionable than a share it never measured.
	RejectEntityIndeterminate RejectReason = "entity_indeterminate"
)

// RejectReasons is every reason the walk can report, in wire order; the scorecard pins
// the schema tag to it and asserts every listed reason is still reachable.
var RejectReasons = []RejectReason{
	RejectBelowRho,
	RejectSameNode,
	RejectNoAttribution,
	RejectEntityIndeterminate,
}

// RCARejection records a candidate the gate dropped and the reason.
type RCARejection struct {
	PatternFrom string       `json:"pattern_from"`
	PatternTo   string       `json:"pattern_to"`
	Rho         float64      `json:"rho"`
	LagSeconds  float64      `json:"lag_seconds"`
	RejectedBy  RejectReason `json:"rejected_by" jsonschema:"below_rho|same_node|no_attribution|entity_indeterminate"`
}

type candidateLink struct {
	symptomPattern   string
	candidatePattern string
	rho              float64
	lagSeconds       float64
}

type admittedLink struct {
	candidateLink
	admittedBy       string
	topologyEdge     *GraphEdge
	attributionShare float64
}

// validateRCAWalk checks req names a symptom and resolves its selector to a bounded window.
func validateRCAWalk(ctx context.Context, req RCAWalkRequest) (selector.Resolved, error) {
	if req.Symptom.PatternID == "" {
		return selector.Resolved{}, query.Invalidf("rca_walk: symptom.pattern_id is required")
	}
	sel, err := validateSelector(ctx, "rca_walk", req.Selector)
	if err != nil {
		return sel, err
	}
	if !sel.Window.Bounded() {
		return sel, query.Invalidf("rca_walk: a bounded time window is required")
	}
	return sel, nil
}

// RCAWalk correlates candidate series with the symptom and checks their supporting
// relationships to build the direct-cause Chain and the transitive Path.
func (t toolSet) RCAWalk(ctx context.Context, req RCAWalkRequest) (query.Envelope[RCAWalkResponse], error) {
	var zero query.Envelope[RCAWalkResponse]

	sel, err := validateRCAWalk(ctx, req)
	if err != nil {
		return zero, err
	}

	snap, err := t.resolveSnapshot(ctx, t.Summary, "rca_walk", req.Snapshot)
	if err != nil {
		return zero, err
	}
	bins, err := series.GridFor(req.Bins, t.summaryWidth(), sel.Window)
	if err != nil {
		return zero, query.Invalidf("rca_walk: %v", err)
	}

	minRho := cmp.Or(req.MinRho, defaultMinRho)
	maxLag := cmp.Or(req.MaxLagSeconds, defaultMaxLagSeconds)
	maxCandidates := min(cmp.Or(req.MaxCandidates, defaultMaxCandidates), maxMaxCandidates)
	// A negative limit is the explicit opt-out, so 0 can stay "unset, take the default".
	scope := readScope{snap: snap, sel: sel, bins: bins, bound: series.Bound{
		Segments: max(cmp.Or(req.MaxScanSegments, defaultMaxScanSegments), 0),
		Bytes:    max(cmp.Or(req.MaxScanBytes, defaultMaxScanBytes), 0),
	}}

	stats, statsCost, err := t.patternStats(ctx, snap, sel, bins, cmp.Or(req.NodeBy, defaultGroupBy))
	if err != nil {
		return zero, err
	}

	candidates, coverage := rcaCandidates(req, stats, maxCandidates)

	links, series, correlateCost, err := t.correlateCandidates(ctx, scope, req.Symptom, candidates, maxLag)
	if err != nil {
		return zero, err
	}

	rhoFiltered, rejected := partitionByRho(links, minRho)

	edgeIndex, topoCost, err := t.rcaEdges(ctx, snap, sel)
	if err != nil {
		return zero, err
	}

	spec := resolveAttributionSpec(req.Entity, req.Placement, defaultGroupBy)
	minAttrShare := cmp.Or(req.MinAttributionShare, defaultMinAttributionShare)

	entityRel, err := t.entityPatterns(ctx, snap, sel, spec)
	if err != nil {
		return zero, err
	}
	// Reads rows to settle, in entityRel, the linked candidates the catalog left indeterminate.
	entityCost, err := t.confirmEntityRows(ctx, scope, spec, entityRel, rhoFiltered)
	if err != nil {
		return zero, err
	}

	admission, err := t.admitLinks(ctx, admissionInput{
		snapshot:            snap,
		candidates:          rhoFiltered,
		topologyEdges:       edgeIndex,
		serviceByPattern:    stats.serviceByPattern,
		linesByService:      serviceLineTotals(stats),
		selector:            sel,
		attributionSpec:     spec,
		minAttributionShare: minAttrShare,
		entityRelations:     entityRel,
	})
	if err != nil {
		return zero, err
	}
	rejected = append(rejected, admission.rejected...)

	sortRejections(rejected)

	resp := RCAWalkResponse{
		Symptom: req.Symptom.PatternID,
		Chain:   rankDirectCauses(req.Symptom.PatternID, spec.entityValue, admission.admitted, stats.nodeByPattern),
		Path: rcaPath(pathInput{
			symptom:     req.Symptom.PatternID,
			candidates:  candidates,
			series:      series,
			links:       links,
			admitted:    admission.admitted,
			nodes:       stats.nodeByPattern,
			services:    stats.serviceByPattern,
			edges:       edgeIndex,
			entityRel:   entityRel,
			attribution: attributionShares(admission.attribution),
			minRho:      minRho,
			minShare:    minAttrShare,
			entity:      spec.entityValue,
			binWidth:    bins.Width,
			maxLag:      maxLag,
		}),
		Rejected: rejected,
	}

	return rcaEnvelope(snap, addCost(statsCost, correlateCost, topoCost, entityCost, admission.cost), coverage, resp.withPatternTexts(stats.textByPattern)), nil
}

func (resp RCAWalkResponse) withPatternTexts(texts map[string]string) RCAWalkResponse {
	resp.Patterns = map[string]string{resp.Symptom: texts[resp.Symptom]}
	for _, hop := range append(slices.Clone(resp.Chain), resp.Path...) {
		resp.Patterns[hop.PatternFrom] = texts[hop.PatternFrom]
		resp.Patterns[hop.PatternTo] = texts[hop.PatternTo]
	}
	for _, rejection := range resp.Rejected {
		resp.Patterns[rejection.PatternFrom] = texts[rejection.PatternFrom]
		resp.Patterns[rejection.PatternTo] = texts[rejection.PatternTo]
	}
	return resp
}

// pathInput is everything rcaPath needs. It is a struct because the alternative is a
// fifteen-parameter call whose argument order is its own hazard.
type pathInput struct {
	symptom     string
	candidates  []SeriesRef
	series      [][]float64
	links       []candidateLink
	admitted    []admittedLink
	nodes       map[string]string
	services    map[string]string
	edges       map[edgeKey]topology.Edge
	entityRel   map[string]entityRelation
	attribution map[string]float64
	minRho      float64
	minShare    float64
	entity      string
	binWidth    time.Duration
	// maxLag is the lag sweep in seconds, which pathEdges clamps to the series length.
	maxLag int
}

// rcaPath builds the route from the origin to the symptom over pattern-pair edges grouped
// by node, ordered by onset so the graph is a DAG and weighted by -log|rho|.
func rcaPath(in pathInput) []RCAHop {
	if len(in.admitted) == 0 {
		return nil
	}

	// Onset puts the symptom at zero, so a candidate that leads it has a negative lag and
	// only patterns at or before it can be its causes.
	onset := map[string]float64{in.symptom: 0}
	for _, link := range in.links {
		onset[link.candidatePattern] = link.lagSeconds
	}

	return in.bestRoute(in.pathEdges(onset), onset)
}

// pathEdges collects the strongest admitted edge between each ordered pair of nodes.
//
// The symptom's own admitted links come in already gated. Candidate-to-candidate links are
// gated here by the same three mechanisms, read for a pair rather than for the symptom: a
// topology edge between their services, one of them being the entity the other sits on, or
// both holding a share of it — co-tenancy, which is the seed's own story about db-3 and
// analytics-export sharing host-7.
func (in pathInput) pathEdges(onset map[string]float64) map[edgeKey]RCAHop {
	best := make(map[edgeKey]RCAHop)
	for _, a := range in.admitted {
		keepStrongest(best, RCAHop{
			From:        in.nodeName(a.candidatePattern),
			To:          in.nodeName(in.symptom),
			Rho:         a.rho,
			LagSeconds:  a.lagSeconds,
			PatternFrom: a.candidatePattern,
			PatternTo:   in.symptom,
			AdmittedBy:  a.admittedBy,
		})
	}

	// Admitted links imply candidates, so series holds the symptom and is non-empty.
	maxLag := clampLagBins(in.maxLag, in.binWidth, len(in.series[0]))
	for i := range in.candidates {
		for j := range in.candidates {
			if e, ok := in.pairEdge(i, j, onset, maxLag); ok {
				keepStrongest(best, e)
			}
		}
	}
	return best
}

// keepStrongest records e unless it is a self-loop or a stronger edge already joins its nodes.
func keepStrongest(best map[edgeKey]RCAHop, e RCAHop) {
	key := edgeKey{e.From, e.To}
	if cur, ok := best[key]; e.From == e.To || (ok && math.Abs(cur.Rho) >= math.Abs(e.Rho)) {
		return
	}
	best[key] = e
}

// pairEdge is the link from candidate i to the later candidate j, when a mechanism gates
// it and their series correlate at least minRho.
func (in pathInput) pairEdge(i, j int, onset map[string]float64, maxLag int) (RCAHop, bool) {
	from, to := in.candidates[i].PatternID, in.candidates[j].PatternID
	oi, oki := onset[from]
	oj, okj := onset[to]
	if !oki || !okj || oi >= oj {
		return RCAHop{}, false
	}
	mechanism, ok := in.pairMechanism(from, to)
	if !ok {
		return RCAHop{}, false
	}
	// Series index 0 is the symptom, so candidate i is at i+1.
	result := stats.CrossCorrelate(in.series[i+1], in.series[j+1], in.binWidth, maxLag)
	if math.Abs(result.Rho) < in.minRho {
		return RCAHop{}, false
	}
	return RCAHop{
		From:        in.nodeName(from),
		To:          in.nodeName(to),
		Rho:         result.Rho,
		LagSeconds:  result.LagDuration.Seconds(),
		PatternFrom: from,
		PatternTo:   to,
		AdmittedBy:  mechanism,
	}, true
}

// nodeName is the vertex a pattern belongs to, preferring the entity when the pattern is
// the entity's own telemetry — the node is then the resource, not the stream reporting it.
func (in pathInput) nodeName(pattern string) string {
	if in.entityRel[pattern] == entityOwned {
		return in.entity
	}
	return cmp.Or(in.nodes[pattern], pattern)
}

// Mechanism names reported on hops and admissions: how a link between two
// candidates was justified.
const (
	mechanismTopology    = string(topology.EdgePlacement)
	mechanismEntity      = "entity"
	mechanismAttribution = "attribution"
)

// pairMechanism is the gate for a link between two candidates.
func (in pathInput) pairMechanism(from, to string) (string, bool) {
	svcFrom, svcTo := in.services[from], in.services[to]
	if _, ok := in.edges[makeEdgeKey(svcFrom, svcTo)]; ok {
		return mechanismTopology, true
	}
	fromIsEntity := in.entityRel[from] == entityOwned
	toIsEntity := in.entityRel[to] == entityOwned
	switch {
	case fromIsEntity && in.attribution[svcTo] >= in.minShare,
		toIsEntity && in.attribution[svcFrom] >= in.minShare:
		return mechanismEntity, true
	case in.attribution[svcFrom] >= in.minShare && in.attribution[svcTo] >= in.minShare:
		return mechanismAttribution, true
	}
	return "", false
}

// bestRoute walks the nodes in onset order and returns the highest-scoring route into the
// symptom's node, as hops ordered origin-first.
func (in pathInput) bestRoute(edges map[edgeKey]RCAHop, onset map[string]float64) []RCAHop {
	nodeOnset, order := in.nodeOnsets(onset)
	via := cheapestRoutes(order, nodeOnset, edges)
	var hops []RCAHop
	for e, ok := via[in.nodeName(in.symptom)]; ok; e, ok = via[e.From] {
		hops = append(hops, e)
	}
	slices.Reverse(hops)
	return hops
}

// nodeOnsets is each node's earliest pattern onset — when it first moved — and the
// nodes in that order, ties broken by name.
func (in pathInput) nodeOnsets(onset map[string]float64) (map[string]float64, []string) {
	nodeOnset := make(map[string]float64)
	for tmpl, at := range onset {
		node := in.nodeName(tmpl)
		if cur, ok := nodeOnset[node]; !ok || at < cur {
			nodeOnset[node] = at
		}
	}
	order := slices.Sorted(maps.Keys(nodeOnset))
	slices.SortStableFunc(order, func(a, b string) int {
		return cmp.Or(cmp.Compare(nodeOnset[a], nodeOnset[b]), cmp.Compare(a, b))
	})
	return nodeOnset, order
}

// cheapestRoutes maps each reachable node to the last hop of its lowest-scoring route,
// scored as the accumulated -log|rho| and starting only from the earliest onset.
func cheapestRoutes(order []string, nodeOnset map[string]float64, edges map[edgeKey]RCAHop) map[string]RCAHop {
	score := make(map[string]float64, len(order))
	via := make(map[string]RCAHop, len(order))
	for _, node := range order {
		for _, prev := range order {
			e, ok := edges[edgeKey{prev, node}]
			_, reached := via[prev]
			if !ok || nodeOnset[prev] >= nodeOnset[node] || (!reached && nodeOnset[prev] != nodeOnset[order[0]]) {
				continue
			}
			cost := score[prev] - math.Log(math.Abs(e.Rho))
			if cur, seen := score[node]; !seen || cost < cur {
				score[node], via[node] = cost, e
			}
		}
	}
	return via
}

// attributionShares flattens the breakdown into service -> fraction, or nil when
// attribution never resolved.
func attributionShares(resp *AttributeWritesResponse) map[string]float64 {
	if resp == nil {
		return nil
	}
	out := make(map[string]float64, len(resp.Breakdown))
	for _, s := range resp.Breakdown {
		out[s.Key] = s.Pct / 100.0
	}
	return out
}

// rcaEdges builds the window's topology edge index.
//
// The graph's shape comes off the configured store rather than the gated one, for the
// same reason service_graph reads it there: the edge-weight floor is process
// configuration, so a request that may not read the summary's contents must still draw
// the same graph as one that may.
func (t toolSet) rcaEdges(ctx context.Context, snap snapshot, sel selector.Resolved) (map[edgeKey]topology.Edge, query.Cost, error) {
	filter := topology.Filter{From: sel.Window.From.UnixMilli(), To: sel.Window.To.UnixMilli()}
	view, cost, err := t.topologyView(ctx, snap, sel, t.topologyConfig(), filter)
	if err != nil {
		return nil, cost, err
	}
	return buildEdgeIndex(view), cost, nil
}

// partitionByRho splits correlated links into the ones worth gating and the ones whose
// co-movement is too weak to be evidence of anything. This runs before either gate
// because it is the cheap test: a candidate that does not move with the symptom needs
// no topology lookup or attribution scan to be dismissed.
func partitionByRho(links []candidateLink, minRho float64) ([]candidateLink, []RCARejection) {
	kept := make([]candidateLink, 0, len(links))
	var rejected []RCARejection
	for _, link := range links {
		if math.Abs(link.rho) < minRho {
			rejected = append(rejected, link.reject(RejectBelowRho))
			continue
		}
		kept = append(kept, link)
	}
	return kept, rejected
}

// rcaCandidates resolves the candidate set and its Coverage, which falls below one only
// when auto-picking cut the window's patterns at maxCandidates.
func rcaCandidates(req RCAWalkRequest, stats rcaPatternStats, maxCandidates int) ([]SeriesRef, float64) {
	if len(req.Candidates) > 0 {
		// Sorted so the same request always correlates in the same order.
		refs := slices.Clone(req.Candidates)
		slices.SortStableFunc(refs, func(a, b SeriesRef) int {
			return cmp.Compare(a.PatternID, b.PatternID)
		})
		return refs, 1.0
	}
	ids, considered := pickHighSignalCandidates(stats, req.Symptom.PatternID, maxCandidates)
	candidates := make([]SeriesRef, len(ids))
	for i, id := range ids {
		candidates[i] = SeriesRef{PatternID: id}
	}
	coverage := 1.0
	if considered > len(candidates) {
		coverage = float64(len(candidates)) / float64(considered)
	}
	return candidates, coverage
}

// pickHighSignalCandidates ranks by stats.Score rather than volume, because a pattern that
// logs only on failure is the most diagnostic and least voluminous.
func pickHighSignalCandidates(stats rcaPatternStats, symptomID string, max int) ([]string, int) {
	total := int64(0)
	for _, lines := range stats.linesByPattern {
		total += lines
	}

	type entry struct {
		id    string
		score float64
	}
	entries := make([]entry, 0, len(stats.linesByPattern))
	for id, lines := range stats.linesByPattern {
		if id == symptomID {
			continue
		}
		entries = append(entries, entry{id: id, score: stats.signalOf(id, lines, total)})
	}
	// The pattern id breaks score ties so the cut is deterministic.
	slices.SortFunc(entries, func(a, b entry) int {
		return cmp.Or(cmp.Compare(b.score, a.score), cmp.Compare(a.id, b.id))
	})

	considered := len(entries)
	result := make([]string, 0, min(len(entries), max))
	for _, e := range entries[:cap(result)] {
		result = append(result, e.id)
	}
	slices.Sort(result)
	return result, considered
}

// signalOf scores one pattern from the two windows' rates and its share of the window.
func (s rcaPatternStats) signalOf(id string, lines, total int64) float64 {
	ev := stats.Evidence{Lines: lines, TotalLines: total}
	if s.windowDuration > 0 {
		ev.Rate = float64(lines) / s.windowDuration.Seconds()
		ev.PrevRate = float64(s.baselineLinesByPattern[id]) / s.windowDuration.Seconds()
	}
	// Absent from a baseline window we did read is a previous rate of zero, not an unknown
	// one: the read covered that span and this pattern was not in it. That distinction is
	// the whole signal for a pattern that logs only when something breaks — calling its
	// previous rate unknown suppresses the rate-of-change term and scores "started
	// happening" the same as "always happening". FirstObservation is for a service new to
	// the window, whose every pattern would otherwise rise from nothing.
	if _, ok := s.baselineServices[s.serviceByPattern[id]]; !ok {
		ev.FirstObservation = true
	}
	return stats.Score(ev)
}

// checkSymptomVariance refuses a symptom the walk cannot reason about: fewer than two
// points carries no lag, and a constant carries no correlation. Returning zero rho for
// every candidate instead would report "nothing correlates" when the truth is that the
// question cannot be answered as posed.
func checkSymptomVariance(series []float64, symptom SeriesRef) error {
	points, first, varies := seriesShape(series)

	if points < 2 {
		return query.NewAPIError(query.ErrCodeUnusableSeries, fmt.Sprintf(
			"symptom series %q has %d usable bins, and a lag needs at least two",
			symptom.PatternID, points)).
			WithHint("widen the time range or narrow the bin width")
	}
	if varies {
		return nil
	}
	// A ratio pinned at 0 or 1 is the mis-scoped case: the numerator matched none or all
	// of the scope's rows, which is what happens when masking splits the outcomes apart.
	if isRatio(symptom.Measure) {
		return query.NewAPIError(query.ErrCodeUnusableSeries, fmt.Sprintf(
			"symptom ratio is a constant %g in every bin, so nothing correlates against it",
			first)).
			WithHint("a ratio's denominator is this pattern's own count, so the numerator " +
				"matching all or none of its rows pins it flat. This happens when the " +
				"outcome is in the message text and masking splits the outcomes into " +
				"separate patterns: pick a pattern whose rows carry both outcomes, or " +
				"use a filtered count on the slice instead")
	}
	return query.NewAPIError(query.ErrCodeUnusableSeries, fmt.Sprintf(
		"symptom series %q is a constant %g in every bin, so nothing correlates against it",
		symptom.PatternID, first)).
		WithHint("pick a series that moves during the window")
}

// seriesShape counts a series' usable points and reports its first value and whether any
// other differs. NaN is a gap rather than a value, so it is skipped.
func seriesShape(series []float64) (points int, first float64, varies bool) {
	for _, v := range series {
		if math.IsNaN(v) {
			continue
		}
		if points == 0 {
			first = v
		} else if v != first {
			varies = true
		}
		points++
	}
	return points, first, varies
}

func (t toolSet) correlateCandidates(ctx context.Context, scope readScope, symptom SeriesRef, candidates []SeriesRef,
	maxLagSeconds int) ([]candidateLink, [][]float64, query.Cost, error) {

	if len(candidates) == 0 {
		return nil, nil, query.Cost{}, nil
	}

	// The batch goes in whole because resolveSeries scopes each ref by its own
	// predicates; split, the symptom's filter would push every candidate onto the scan.
	series, cost, err := t.resolveSeries(ctx, scope, append([]SeriesRef{symptom}, candidates...))
	if err != nil {
		return nil, nil, cost, fmt.Errorf("rca_walk: %w", err)
	}

	symptomSeries := series[0]
	if err := checkSymptomVariance(symptomSeries, symptom); err != nil {
		return nil, nil, cost, err
	}
	maxLag := clampLagBins(maxLagSeconds, scope.bins.Width, len(symptomSeries))

	links := make([]candidateLink, 0, len(candidates))
	for i, cand := range candidates {
		result := stats.CrossCorrelate(symptomSeries, series[i+1], scope.bins.Width, maxLag)
		links = append(links, candidateLink{
			symptomPattern:   symptom.PatternID,
			candidatePattern: cand.PatternID,
			rho:              result.Rho,
			lagSeconds:       result.LagDuration.Seconds(),
		})
	}
	return links, series, cost, nil
}

// entityRelation is what the catalog can say about a series and the requested entity.
type entityRelation int

const (
	// entityUnrelated: nothing carrying this series is about the entity.
	entityUnrelated entityRelation = iota
	// entityIndeterminate: the segments carrying it do cover the entity, among others, so
	// whether these particular rows are about it cannot be decided without reading them.
	entityIndeterminate
	// entityOwned: the series is the entity's own telemetry.
	entityOwned
)

// entityPatterns flags a resource's own telemetry, for which ownership is meaningless, and
// keeps "cannot tell" distinct from "not the entity".
func (t toolSet) entityPatterns(ctx context.Context, snap snapshot, sel selector.Resolved, spec attributeSpec) (map[string]entityRelation, error) {
	if spec.entityValue == "" {
		return nil, nil
	}
	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return nil, fmt.Errorf("rca_walk: prune for entity telemetry: %w", err)
	}
	out := make(map[string]entityRelation)
	for _, d := range descs {
		rel := entityRelationFor(d, spec.entityField, spec.entityValue)
		if rel == entityUnrelated {
			continue
		}
		for _, tmpl := range d.PatternIDs {
			// Proof from any one segment settles it: a pattern emitted about the entity
			// somewhere is the entity's telemetry, whatever other segments also carry it.
			if rel > out[tmpl] {
				out[tmpl] = rel
			}
		}
	}
	return out, nil
}

// confirmEntityRows reads entity and pattern columns together since only a row carrying
// both proves ownership, and past the scan bound it leaves the relation indeterminate.
func (t toolSet) confirmEntityRows(ctx context.Context, scope readScope, spec attributeSpec,
	rel map[string]entityRelation, links []candidateLink) (query.Cost, error) {

	var cost query.Cost
	pending := indeterminateCandidates(links, rel)
	if len(pending) == 0 {
		return cost, nil
	}

	all, err := t.Planner.Segments(ctx, scope.snap.catalog, scope.sel)
	if err != nil {
		return cost, fmt.Errorf("rca_walk: prune for entity rows: %w", err)
	}
	descs := undecidedSegments(all, spec, pending)
	// Both halves of the bound apply: a compacted segment is fifty times an L0 one, so
	// a count alone does not say what this will read.
	if scope.bound.Exceeds(descs) {
		return cost, nil
	}

	p, err := t.footers(ctx, descs)
	if err != nil {
		return p.Cost, fmt.Errorf("rca_walk: entity footers: %w", err)
	}
	proven := make(map[string]struct{})
	columns := []string{spec.entityField, segment.ColPatternID}
	rows := t.Scanner.Scan(ctx, p, scan.Query{Columns: columns, Require: columns})
	for rows.Next() {
		entity, _ := rows.Row().Text(spec.entityField)
		pattern, _ := rows.Row().Text(segment.ColPatternID)
		if _, want := pending[pattern]; want && entity == spec.entityValue {
			proven[strings.Clone(pattern)] = struct{}{}
		}
	}
	if cost = addCost(p.Cost, rows.Cost()); rows.Err() != nil {
		return cost, fmt.Errorf("rca_walk: %w", rows.Err())
	}

	settleEntityRelations(rel, pending, proven)
	return cost, nil
}

// settleEntityRelations marks each pending pattern owned if proven and unrelated
// otherwise, since every segment that could have proven it was read.
func settleEntityRelations(rel map[string]entityRelation, pending, proven map[string]struct{}) {
	for tmpl := range pending {
		if _, ok := proven[tmpl]; ok {
			rel[tmpl] = entityOwned
			continue
		}
		rel[tmpl] = entityUnrelated
	}
}

// indeterminateCandidates are the linked candidate patterns whose relation to the
// entity the manifest left indeterminate.
func indeterminateCandidates(links []candidateLink, rel map[string]entityRelation) map[string]struct{} {
	pending := make(map[string]struct{})
	for _, link := range links {
		if rel[link.candidatePattern] == entityIndeterminate {
			pending[link.candidatePattern] = struct{}{}
		}
	}
	return pending
}

// undecidedSegments are the segments the manifest could not settle that carry one of the
// pending patterns — the only ones worth reading.
func undecidedSegments(descs []segment.Descriptor, spec attributeSpec, pending map[string]struct{}) []segment.Descriptor {
	out := make([]segment.Descriptor, 0, len(descs))
	for _, d := range descs {
		if entityRelationFor(d, spec.entityField, spec.entityValue) != entityIndeterminate {
			continue
		}
		if slices.ContainsFunc(d.PatternIDs, func(id string) bool {
			_, ok := pending[id]
			return ok
		}) {
			out = append(out, d)
		}
	}
	return out
}

// entityRelationFor decides what one segment says about the entity.
//
// A stream label is one value for every row by construction — an agent that labels its
// stream host=host-7 is stating that about the whole segment — so it settles the question
// outright. Failing that, the rows' own column answers through its zone map: a range of
// one value is the data agreeing the segment is about that entity alone, and a range
// spanning several means the segment covers the entity among others, which the catalog
// cannot narrow further. Only a range that excludes the value proves the segment is not
// about it.
//
// Checking the label as well as the column is what makes this independent of where a
// deployment records the resource. Reading only the column missed the ordinary case of an
// agent that stamps the host once per stream rather than once per line.
func entityRelationFor(d segment.Descriptor, field, text string) entityRelation {
	if labelled, ok := d.Stream.Labels[field]; ok {
		if labelled == text {
			return entityOwned
		}
		return entityUnrelated
	}
	zm, ok := d.ZoneMaps[field]
	if !ok {
		return entityUnrelated
	}
	typed, err := value.Parse(zm.Type, text)
	if err != nil {
		return entityUnrelated
	}
	// Owned needs every row on the entity, not just agreeing extremes: a column absent
	// from some rows leaves them out of the bound entirely (ZoneMap.SoleValue).
	switch sole, uniform := zm.SoleValue(d.Rows); {
	case uniform && value.Compare(zm.Type, sole, typed) == 0:
		return entityOwned
	case value.Compare(zm.Type, zm.Min, typed) <= 0 && value.Compare(zm.Type, typed, zm.Max) <= 0:
		return entityIndeterminate
	default:
		return entityUnrelated
	}
}

// rcaPatternStats is what the walk needs to know about the window's patterns
// before it correlates anything: how many lines each one carried, which is how
// candidates are ranked, and which service owns it, which is what a hop is labelled
// with and what the topology gate looks an edge up by.
type rcaPatternStats struct {
	textByPattern          map[string]string
	linesByPattern         map[string]int64
	baselineLinesByPattern map[string]int64
	windowDuration         time.Duration
	serviceByPattern       map[string]string
	// nodeByPattern identifies the instance shown in a hop, falling back to its service.
	nodeByPattern map[string]string
	// baselineServices are the services with lines in the baseline window.
	baselineServices map[string]struct{}
}

// patternStats falls back to segment footers because the store answers nothing before its
// first build or on version drift, and would report "no cause found" without looking.
func (t toolSet) patternStats(ctx context.Context, snap snapshot, sel selector.Resolved, bins series.Bins,
	nodeBy string) (rcaPatternStats, query.Cost, error) {

	stats, cost, err := t.patternLines(ctx, snap, sel, bins)
	if err != nil {
		return rcaPatternStats{}, cost, err
	}

	// The window before this one, read the same way. Candidate ranking is a comparison
	// against it, so a pattern that was already running at this volume scores as the
	// background it is.
	base := baselineGrid(bins)
	prior, priorCost, err := t.patternLines(ctx, snap, shiftSelector(sel, base), base)
	if err != nil {
		return rcaPatternStats{}, cost, err
	}
	cost = addCost(cost, priorCost)
	stats.baselineLinesByPattern = prior.linesByPattern
	stats.baselineServices = make(map[string]struct{}, len(prior.serviceByPattern))
	for _, svc := range prior.serviceByPattern {
		stats.baselineServices[svc] = struct{}{}
	}
	stats.windowDuration = bins.To.Sub(bins.From)

	// Nodes are named for the service unless the caller asked for a finer dimension, and
	// only that finer case needs the catalog: the summary keys counts on the service and
	// carries nothing below it.
	stats.nodeByPattern = stats.serviceByPattern
	if nodeBy != defaultGroupBy {
		node, err := t.patternNodes(ctx, snap, sel, nodeBy)
		if err != nil {
			return rcaPatternStats{}, cost, err
		}
		// A pattern the catalog cannot label at the finer dimension keeps its service, so
		// asking for instances never blanks out the nodes that have none.
		stats.nodeByPattern = make(map[string]string, len(stats.serviceByPattern))
		for tmpl, svc := range stats.serviceByPattern {
			stats.nodeByPattern[tmpl] = cmp.Or(node[tmpl], svc)
		}
	}
	return stats, cost, nil
}

// patternLines totals each pattern's lines over the window and labels it with the
// service that emitted most of them, from the summary when one is readable and from
// segment footers when one is not.
func (t toolSet) patternLines(ctx context.Context, snap snapshot, sel selector.Resolved, bins series.Bins) (rcaPatternStats, query.Cost, error) {
	if store := t.store(snap); store != nil {
		texts := make(map[string]string)
		lines := make(map[string]int64)
		byService := make(map[string]map[string]int64)
		for _, p := range store.CountSlice("", bins.From.UnixMilli(), bins.To.UnixMilli()) {
			texts[p.Key.PatternID], _ = store.PatternText(p.Key.PatternID)
			lines[p.Key.PatternID] += p.Lines
			addWeight(byService, p.Key.PatternID, p.Key.Service, p.Lines)
		}
		return rcaPatternStats{textByPattern: texts, linesByPattern: lines, serviceByPattern: dominantLabels(byService)}, query.Cost{}, nil
	}

	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return rcaPatternStats{}, query.Cost{}, fmt.Errorf("rca_walk: prune for pattern stats: %w", err)
	}
	byPattern, cost, err := t.aggregatePatterns(ctx, descs)
	if err != nil {
		return rcaPatternStats{}, cost, fmt.Errorf("rca_walk: pattern stats: %w", err)
	}
	texts := make(map[string]string, len(byPattern))
	lines := make(map[string]int64, len(byPattern))
	byService := make(map[string]map[string]int64, len(byPattern))
	for tmpl, agg := range byPattern {
		texts[tmpl] = agg.text
		lines[tmpl] = agg.count
		byService[tmpl] = agg.byService
	}
	return rcaPatternStats{textByPattern: texts, linesByPattern: lines, serviceByPattern: dominantLabels(byService)}, cost, nil
}

// patternNodes labels each pattern with a caller-named dimension finer than the service,
// since only the operator knows whether "db" or "db-3" is the actionable granularity.
func (t toolSet) patternNodes(ctx context.Context, snap snapshot, sel selector.Resolved, nodeBy string) (map[string]string, error) {
	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return nil, fmt.Errorf("rca_walk: prune for node labels: %w", err)
	}
	// Weighted by rows so a pattern spanning two streams is attributed where most of it
	// came from, the same rule the service label uses.
	byNode := make(map[string]map[string]int64)
	for _, d := range descs {
		node, ok := plan.SegmentValue(d, nodeBy)
		if !ok {
			continue
		}
		for _, tmpl := range d.PatternIDs {
			addWeight(byNode, tmpl, node, int64(d.Rows))
		}
	}
	return dominantLabels(byNode), nil
}

func addWeight(m map[string]map[string]int64, key, value string, weight int64) {
	inner, ok := m[key]
	if !ok {
		inner = make(map[string]int64)
		m[key] = inner
	}
	inner[value] += weight
}

// dominantLabels labels each pattern with its top value by list_patterns' tie-break rule.
func dominantLabels(weights map[string]map[string]int64) map[string]string {
	out := make(map[string]string, len(weights))
	for key, inner := range weights {
		out[key] = dominantService(inner)
	}
	return out
}

type edgeKey struct{ a, b string }

func makeEdgeKey(from, to string) edgeKey {
	if from > to {
		from, to = to, from
	}
	return edgeKey{from, to}
}

func buildEdgeIndex(view topology.View) map[edgeKey]topology.Edge {
	idx := make(map[edgeKey]topology.Edge, len(view.Edges))
	for _, e := range view.Edges {
		key := makeEdgeKey(e.From, e.To)
		if existing, ok := idx[key]; !ok || e.Weight > existing.Weight {
			idx[key] = e
		}
	}
	return idx
}

type admissionInput struct {
	snapshot            snapshot
	candidates          []candidateLink
	topologyEdges       map[edgeKey]topology.Edge
	serviceByPattern    map[string]string
	linesByService      map[string]int64
	selector            selector.Resolved
	attributionSpec     attributeSpec
	minAttributionShare float64
	entityRelations     map[string]entityRelation
}

type admissionResult struct {
	admitted    []admittedLink
	rejected    []RCARejection
	cost        query.Cost
	attribution *AttributeWritesResponse
}

// admitLinks keeps candidates supported by topology, entity ownership or attribution.
func (t toolSet) admitLinks(ctx context.Context, input admissionInput) (admissionResult, error) {
	result := admissionResult{
		admitted: make([]admittedLink, 0, len(input.candidates)),
	}
	// Only compute attribution once per call, so its placement-stream scan is billed once too.
	attributed := false
	for _, link := range input.candidates {
		a, reason, decided := admitWithoutAttribution(link, input)
		if !decided {
			if !attributed {
				attributed = true
				var err error
				if result.attribution, result.cost, err = t.entityAttribution(ctx, input); err != nil {
					return admissionResult{}, err
				}
			}
			a, reason = admitByAttribution(link, input, result.attribution)
		}
		if reason != "" {
			result.rejected = append(result.rejected, link.reject(reason))
			continue
		}
		result.admitted = append(result.admitted, a)
	}
	return result, nil
}

// admitWithoutAttribution decides a link by node, topology and entity ownership alone, or reports it undecided.
func admitWithoutAttribution(link candidateLink, input admissionInput) (a admittedLink, reason RejectReason, decided bool) {
	svcFrom := input.serviceByPattern[link.symptomPattern]
	svcTo := input.serviceByPattern[link.candidatePattern]

	// A candidate on the symptom's own node is not a cause of it, yet always correlates:
	// a complementary slice of the same stream moves at |rho| = 1 by construction.
	if svcFrom != "" && svcFrom == svcTo {
		return admittedLink{}, RejectSameNode, true
	}

	if edge, ok := input.topologyEdges[makeEdgeKey(svcFrom, svcTo)]; ok {
		return admittedLink{
			candidateLink: link,
			admittedBy:    mechanismTopology,
			topologyEdge: &GraphEdge{
				From:   edge.From,
				To:     edge.To,
				Method: string(edge.Method),
				Weight: edge.Weight,
			},
		}, "", true
	}

	// The entity's own telemetry needs no attribution to corroborate it, and being the
	// resource is a stronger statement than owning a share of it.
	if input.entityRelations[link.candidatePattern] == entityOwned {
		return admittedLink{
			candidateLink: link,
			admittedBy:    mechanismEntity,
		}, "", true
	}
	return admittedLink{}, "", false
}

// admitByAttribution decides a link from the entity's attribution, nil when there is none.
func admitByAttribution(link candidateLink, input admissionInput, attr *AttributeWritesResponse) (admittedLink, RejectReason) {
	// An indeterminate entity check is reported as itself even when attribution also
	// declines, because it is the more actionable of the two: the caller can narrow the
	// stream or ask for a row-level check, whereas "does not own a share" invites them
	// to conclude something the data never said.
	reasonNoAttribution := RejectNoAttribution
	if input.entityRelations[link.candidatePattern] == entityIndeterminate {
		reasonNoAttribution = RejectEntityIndeterminate
	}
	if attr == nil || len(attr.Breakdown) == 0 {
		return admittedLink{}, reasonNoAttribution
	}

	// Co-tenancy comes first because it is a topology fact rather than a magnitude one: the
	// graph only carries workload→resource edges a stream labelled itself with (Observe).
	svcFrom, svcTo := input.serviceByPattern[link.symptomPattern], input.serviceByPattern[link.candidatePattern]
	admitted := admittedLink{candidateLink: link, attributionShare: attributionShare(attr.Breakdown, svcTo, input.linesByService)}
	switch {
	case onEntity(attr.Breakdown, svcFrom) && onEntity(attr.Breakdown, svcTo):
		admitted.admittedBy = mechanismTopology
		admitted.topologyEdge = &GraphEdge{
			From:   svcTo,
			To:     input.attributionSpec.entityValue,
			Method: string(topology.EdgePlacement),
			Weight: placementEdgeWeight,
		}
		return admitted, ""
	case admitted.attributionShare >= input.minAttributionShare:
		admitted.admittedBy = mechanismAttribution
		return admitted, ""
	}
	return admittedLink{}, reasonNoAttribution
}

// entityAttribution returns the entity's attribution, nil when the window has no placement
// stream; any other failure is not an answer about the data, so it is returned.
func (t toolSet) entityAttribution(ctx context.Context, input admissionInput) (*AttributeWritesResponse, query.Cost, error) {
	spec := input.attributionSpec
	resp, _, cost, err := t.attributionForEntity(ctx, input.snapshot, input.selector, spec)
	var apiErr *query.APIError
	switch {
	case err == nil:
		return &resp, cost, nil
	case errors.As(err, &apiErr) && apiErr.Code == query.ErrCodeUnknownField:
		return nil, cost, nil
	}
	return nil, cost, fmt.Errorf("rca_walk attribution on %s %q: %w", spec.entityField, spec.entityValue, err)
}

// attributionShare is the fraction of the service's own window volume (serviceLines)
// that lands on the entity, not its fraction of the entity's lines. The absolute form
// rejected a cause for being quiet, since a pattern that only logs on failure owns
// little of a busy host however completely it sits on it.
func attributionShare(breakdown []stats.Share, service string, serviceLines map[string]int64) float64 {
	i := slices.IndexFunc(breakdown, func(s stats.Share) bool { return s.Key == service })
	if i < 0 {
		return 0
	}
	// With no window total to divide by, fall back to the entity-relative form.
	if total := serviceLines[service]; total > 0 {
		return min(float64(breakdown[i].Lines)/float64(total), 1)
	}
	return breakdown[i].Pct / 100
}

// onEntity reports whether the service appears in the entity's breakdown at all, which
// is the structural question of co-tenancy rather than the magnitude one of share.
func onEntity(breakdown []stats.Share, service string) bool {
	return service != "" && slices.ContainsFunc(breakdown, func(s stats.Share) bool {
		return s.Key == service && s.Lines > 0
	})
}

// serviceLineTotals sums each service's window lines from the pattern stats, which is
// the denominator attributionShare divides by.
func serviceLineTotals(stats rcaPatternStats) map[string]int64 {
	out := make(map[string]int64, len(stats.linesByPattern))
	for tmpl, lines := range stats.linesByPattern {
		out[stats.serviceByPattern[tmpl]] += lines
	}
	return out
}

// rankDirectCauses orders the admitted links, each one hop from the symptom, by lag
// ascending then |rho| then pattern id, so the same admitted set always ranks the
// same. A hop the entity gate admitted reaches the resource itself, so it
// is labelled with entity rather than with the stream that reported it.
func rankDirectCauses(symptomID, entity string, admitted []admittedLink, patternSvc map[string]string) []RCAHop {
	if len(admitted) == 0 {
		return nil
	}

	slices.SortStableFunc(admitted, func(a, b admittedLink) int {
		return cmp.Or(cmp.Compare(a.lagSeconds, b.lagSeconds), cmp.Compare(math.Abs(b.rho), math.Abs(a.rho)),
			cmp.Compare(a.candidatePattern, b.candidatePattern))
	})

	chain := make([]RCAHop, 0, len(admitted))
	visited := make(map[string]struct{})
	visited[symptomID] = struct{}{}

	for _, a := range admitted {
		if _, seen := visited[a.candidatePattern]; seen {
			continue
		}
		visited[a.candidatePattern] = struct{}{}

		to := cmp.Or(patternSvc[a.candidatePattern], a.candidatePattern)
		if a.admittedBy == mechanismEntity {
			to = entity
		}
		hop := RCAHop{
			From:             cmp.Or(patternSvc[a.symptomPattern], a.symptomPattern),
			To:               to,
			PatternFrom:      a.symptomPattern,
			PatternTo:        a.candidatePattern,
			Rho:              a.rho,
			LagSeconds:       a.lagSeconds,
			AdmittedBy:       a.admittedBy,
			TopologyEdge:     a.topologyEdge,
			AttributionShare: a.attributionShare,
		}
		chain = append(chain, hop)
	}
	return chain
}

func sortRejections(rejected []RCARejection) {
	slices.SortFunc(rejected, func(a, b RCARejection) int {
		return cmp.Or(cmp.Compare(a.RejectedBy, b.RejectedBy), cmp.Compare(a.PatternFrom, b.PatternFrom),
			cmp.Compare(a.PatternTo, b.PatternTo))
	})
}

func (l candidateLink) reject(by RejectReason) RCARejection {
	return RCARejection{PatternFrom: l.symptomPattern, PatternTo: l.candidatePattern, Rho: l.rho, LagSeconds: l.lagSeconds, RejectedBy: by}
}

// rcaEnvelope wraps a response. Approx is unconditional: the topology gate's edge
// weights come from sketch inclusion-exclusion (service_graph), so no hop the graph
// admitted is exact, and that subsumes whatever the attribution gate's own totals
// reported. coverage is below 1 when max_candidates cut the auto-picked candidate
// set — the walk then reasoned over part of the window's patterns, and a caller
// acting on an empty chain needs to know it did.
func rcaEnvelope(snap snapshot, cost query.Cost, coverage float64, resp RCAWalkResponse) query.Envelope[RCAWalkResponse] {
	return query.Envelope[RCAWalkResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Approx:   true,
			Coverage: coverage,
			Cost:     cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}
}
