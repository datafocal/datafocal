package tools

import (
	"cmp"
	"context"
	"fmt"
	"maps"
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
)

// anomalyThreshold is the KL divergence above which the incident split counts as
// a shift rather than noise. 0.5 nat is roughly "one group moved from a fifth of
// the traffic to half of it" — enough to be worth a human's attention and coarse
// enough not to fire on sampling jitter. It is a starting point, not a derived
// bound; the §11 anomaly jobs replace it with a per-series baseline.
const anomalyThreshold = 0.5

// Defaults for the placement seam and the attribution dimensions. They spell the
// Kubernetes-shaped deployment because that is what the seed corpus emits, and
// they are defaults rather than constants precisely so nothing else has to be.
const (
	defaultEntityField      = "host"
	defaultGroupBy          = segment.ColService
	defaultPlacementService = "scheduler"
	defaultSubjectField     = "pod"
)

// EntityRef names the thing whose activity is being attributed. Field is the
// dimension — "host", "node", "broker", "volume" — and Value the instance.
//
// The field is named rather than assumed so one implementation answers "who is
// writing this disk", "who is filling this queue", and "who is saturating this
// link". Empty Field defaults to "host".
type EntityRef struct {
	Field string `json:"field,omitempty" jsonschema:"Entity dimension; defaults to host"`
	Value string `json:"value" jsonschema:"Entity instance, e.g. host-7"`
}

// PlacementRef says where the subject→entity association is recorded, for
// deployments whose activity logs do not carry the entity field on every line.
//
// The design derives the association from the topology graph. The graph exists,
// but it learns placements from this same scan — topology.Graph.Add sees only
// footers, and a placement in log content is not in a footer — so reading the
// stream is the primitive underneath both tools rather than a stand-in for one.
//
// All three fields are named rather than assumed, so one implementation reads a
// Kubernetes scheduler's pod→host, a Nomad alloc→client, or a Kafka rebalance
// log's consumer_group→broker. The defaults spell the Kubernetes case because that
// is what the seed corpus emits.
type PlacementRef struct {
	Service      string `json:"service,omitempty" jsonschema:"Service emitting placement records; defaults to scheduler"`
	SubjectField string `json:"subject_field,omitempty" jsonschema:"Field naming the placed workload; defaults to pod"`
	EntityField  string `json:"entity_field,omitempty" jsonschema:"Field naming the resource the workload sits on; defaults to host"`
}

// AttributeWritesRequest is the wire request for the attribute_writes tool. It
// resolves which groups are active on an entity during an incident window and
// compares that split against the preceding window to say whether it is
// anomalous.
//
// Scope beyond the entity — a resource, a level, a subsystem — goes in
// Selector.Where like every other tool's filter, rather than in a tool-specific
// field that only this tool knows how to apply.
type AttributeWritesRequest struct {
	Selector selector.Selector `json:"selector"`
	Entity   EntityRef         `json:"entity"`
	// GroupBy is the dimension the activity is attributed across. Defaults to
	// "service", which is also the one value the summary keys counts on and can
	// therefore answer without a scan.
	GroupBy   string       `json:"group_by,omitempty" jsonschema:"Dimension to attribute across; defaults to service"`
	Placement PlacementRef `json:"placement,omitempty"`
	// There is deliberately no bins field. The answer is one share per group over
	// the whole window, so a finer grid would change nothing but the cost
	// (totalsGrid). A caller who wants the shape over time asks aggregate.
	Snapshot string `json:"snapshot,omitempty"`
}

// AttributeWritesResponse holds the breakdown with anomaly detection.
type AttributeWritesResponse struct {
	Breakdown []stats.Share `json:"breakdown"`
	Anomalous bool          `json:"anomalous"`
	KL        float64       `json:"kl"`
	MI        float64       `json:"mi"`
	Note      string        `json:"note"`
}

// attributeSpec is the request's dimensions after defaults are applied, so the
// rest of the tool reads one resolved shape instead of re-deriving each fallback.
type attributeSpec struct {
	entityField      string
	entityValue      string
	groupBy          string
	placementService string
	subjectField     string
}

func (r AttributeWritesRequest) spec() (attributeSpec, error) {
	if r.Entity.Value == "" {
		return attributeSpec{}, query.Invalidf("attribute_writes: entity.value is required — the host, node, or volume being attributed")
	}
	return resolveAttributionSpec(r.Entity, r.Placement, cmp.Or(r.GroupBy, defaultGroupBy)), nil
}

// resolveAttributionSpec applies the same placement defaults for attribution, service graphs and RCA.
// An explicit entity field wins over the placement field; groupBy stays caller-owned because graphs do not group attribution.
func resolveAttributionSpec(entity EntityRef, placement PlacementRef, groupBy string) attributeSpec {
	return attributeSpec{
		entityField:      cmp.Or(entity.Field, placement.EntityField, defaultEntityField),
		entityValue:      entity.Value,
		groupBy:          groupBy,
		placementService: cmp.Or(placement.Service, defaultPlacementService),
		subjectField:     cmp.Or(placement.SubjectField, defaultSubjectField),
	}
}

// AttributeWrites is the attribute_writes tool entry point. It resolves which
// groups sit on the requested entity, totals their lines over the incident window
// and the window immediately before it, and reports the incident split plus how
// far it diverged from that baseline.
func (t toolSet) AttributeWrites(ctx context.Context, req AttributeWritesRequest) (query.Envelope[AttributeWritesResponse], error) {
	var zero query.Envelope[AttributeWritesResponse]

	spec, err := req.spec()
	if err != nil {
		return zero, err
	}
	sel, err := validateSelector(ctx, "attribute_writes", req.Selector)
	if err != nil {
		return zero, err
	}
	if !sel.Window.Bounded() {
		return zero, query.Invalidf("attribute_writes: a bounded time window is required")
	}
	snap, err := t.resolveSnapshot(ctx, t.Summary, "attribute_writes", req.Snapshot)
	if err != nil {
		return zero, err
	}

	resp, approx, cost, err := t.attributionForEntity(ctx, snap, sel, spec)
	if err != nil {
		return zero, err
	}
	return query.Envelope[AttributeWritesResponse]{
		Data: resp,
		Meta: query.Meta{Snapshot: snap.String(), Approx: approx, Coverage: 1.0, Cost: cost, Tokens: query.EstimateTokens(resp)},
	}, nil
}

// attributionForEntity resolves which groups sit on the requested entity, totals
// their lines over the incident and baseline windows, and returns the attribution
// breakdown with anomaly detection. Both AttributeWrites and the rca_walk
// admission gate call this one implementation.
func (t toolSet) attributionForEntity(ctx context.Context, snap snapshot, sel selector.Resolved,
	spec attributeSpec) (resp AttributeWritesResponse, approx bool, cost query.Cost, err error) {

	var zero AttributeWritesResponse
	groups, cost, err := t.groupsOnEntity(ctx, snap, sel, spec)
	if err != nil {
		return zero, false, cost, err
	}
	if len(groups) == 0 {
		return AttributeWritesResponse{
			Breakdown: []stats.Share{},
			Note:      fmt.Sprintf("no %s found on %s %q", spec.subjectField, spec.entityField, spec.entityValue),
		}, false, cost, nil
	}

	// The incident reads the request's own window and the baseline the same span just
	// before it.
	bins := totalsGrid(t.summaryWidth(), sel.Window)
	base := baselineGrid(bins)
	windows := []struct {
		sel  selector.Resolved
		bins series.Bins
	}{{sel, bins}, {shiftSelector(sel, base), base}}
	totals := make([]map[string]int64, len(windows))
	for i, w := range windows {
		res, err := t.Series.Read(ctx, totalsRequest(snap, w.sel, spec.groupBy, w.bins))
		cost = addCost(cost, res.Cost)
		if err != nil {
			return zero, false, cost, fmt.Errorf("attribute_writes: counts: %w", err)
		}
		totals[i], approx = keptTotals(res.Values, groups), approx || res.Approx
	}
	return attribution(totals[0], totals[1]), approx, cost, nil
}

// attribution turns the two windows' totals into the response: the incident
// split, how far it diverged from the baseline, and whether that counts as
// anomalous.
func attribution(incident, baseline map[string]int64) AttributeWritesResponse {
	resp := AttributeWritesResponse{
		Breakdown: stats.Breakdown(incident),
		Note:      "groups resolved from the placement stream; incident vs baseline split compared",
	}

	// A window with no lines has no shape to compare, so the divergence between
	// the two is not the answer — the appearance or disappearance of the activity
	// itself is.
	switch incidentLines, baselineLines := totalOf(incident), totalOf(baseline); {
	case incidentLines == 0 && baselineLines == 0:
		return resp
	case baselineLines == 0:
		resp.Anomalous = true
		resp.Note = "no baseline activity — this split appeared where none existed; " + resp.Note
		return resp
	case incidentLines == 0:
		resp.Anomalous = true
		resp.Note = "activity stopped — the groups on this entity went silent; " + resp.Note
		return resp
	}

	resp.KL = stats.Divergence(incident, baseline)
	resp.MI = stats.MutualInformation(jointDistribution(incident, baseline))
	resp.Anomalous = resp.KL > anomalyThreshold
	return resp
}

// groupsOnEntity resolves which GroupBy values have a subject placed on the
// requested entity, sorted so the same placements always yield the same list.
func (t toolSet) groupsOnEntity(ctx context.Context, snap snapshot, sel selector.Resolved, spec attributeSpec) ([]string, query.Cost, error) {
	placements, segments, cost, err := t.readPlacements(ctx, snap, "attribute_writes", sel, spec)
	if err != nil {
		return nil, cost, err
	}
	// An absent placement stream is fatal here and only here: attribution has no
	// other way to learn which groups sit on the entity, whereas service_graph
	// simply has fewer edges to draw.
	if segments == 0 {
		return nil, cost, query.NewAPIError(query.ErrCodeUnknownField,
			fmt.Sprintf("attribute_writes: no %q segments carrying both %q and %q in the window",
				spec.placementService, spec.subjectField, spec.entityField)).
			WithHint("attribution needs a stream mapping subjects to entities; name it via placement.service and placement.subject_field, or attribute over a dimension the activity logs carry directly")
	}
	observed, err := t.observeSubjectGroups(ctx, snap, sel, spec, placements)
	if err != nil {
		return nil, cost, err
	}
	return groupsPlacedOn(placements, observed, spec.entityValue, spec.subjectField, spec.groupBy), cost, nil
}

// groupsPlacedOn reduces a subject→entity map to the sorted groups sitting on
// one entity.
//
// When groupBy is the subject field itself (e.g. group_by=pod, subject_field=pod)
// the subject is its own group and is kept raw. Otherwise the group is whichever
// value observeSubjectGroups saw alongside that subject in the window's activity.
//
// groupFromSubject is the fallback for a subject the activity never named: it reads
// the group out of the subject's own spelling. That is a guess — it is right only
// when a workload's group name is its subject minus a replica suffix — so it is
// reached only when the data could not answer. Preferring it to dropping the subject
// is what keeps deployments whose activity does not stamp the subject field present
// in the breakdown instead of silently absent from it.
func groupsPlacedOn(placements, observed map[string]string, entity, subjectField, groupBy string) []string {
	on := make(map[string]struct{})
	for subject, placed := range placements {
		if placed != entity {
			continue
		}
		if groupBy == subjectField {
			on[subject] = struct{}{}
			continue
		}
		if group, ok := observed[subject]; ok {
			on[group] = struct{}{}
			continue
		}
		on[groupFromSubject(subject)] = struct{}{}
	}
	return slices.Sorted(maps.Keys(on))
}

// observeSubjectGroups maps each placed subject to the group it belongs to, using only
// stream identity off the catalog: the pruned descriptors' services and labels. It
// reads no segment data, so attribution's cost still tracks the placement stream and
// not the corpus (TestCostAttributeWritesBoundedByPlacementStream).
//
// It exists because a subject's group is not recoverable from its spelling, and the
// seed's two host-7 co-tenants prove both directions. Pod "analytics-export-0" belongs
// to service "analytics-export", which stripping the replica suffix happens to
// produce. Pod "db-3-0" belongs to service "db", and stripping produces "db-3" — db's
// instance label, which no segment's service is spelled as (seedcorpus/gen.go:22). The
// second one totalled zero lines and handed analytics-export a host it drives 42% of.
//
// Stream identity closes exactly that gap, because it is where the ambiguity lives:
// the collector partitions segments by (service, labels), so a name that is an
// instance rather than a service is still a name the catalog holds, on the same
// descriptor that names the service. A subject whose derived name matches neither is
// left out for the caller's fallback to guess at — resolving what the catalog knows
// and no more.
func (t toolSet) observeSubjectGroups(ctx context.Context, snap snapshot,
	sel selector.Resolved, spec attributeSpec, placements map[string]string) (map[string]string, error) {
	// The subject is its own group, so there is nothing to resolve.
	if spec.groupBy == spec.subjectField {
		return nil, nil
	}

	descs, err := t.Planner.Segments(ctx, snap.catalog, sel)
	if err != nil {
		return nil, fmt.Errorf("attribute_writes: prune for subject groups: %w", err)
	}

	groups := newSubjectGroups(descs, spec.groupBy)
	observed := make(map[string]string)
	for subject, placed := range placements {
		if placed != spec.entityValue {
			continue
		}
		if group, ok := groups.resolve(subject); ok {
			observed[subject] = group
		}
	}
	return observed, nil
}

// subjectGroups resolves a subject to the group of the stream that names it.
type subjectGroups struct {
	// groups are the group dimension's real values in this window; byLabel maps every
	// other stream-label value to the group of the stream carrying it.
	groups  map[string]struct{}
	byLabel map[string]string
}

// newSubjectGroups indexes descs by groupBy with the first descriptor winning, so a label
// value two streams share resolves the same way for a given snapshot.
func newSubjectGroups(descs []segment.Descriptor, groupBy string) subjectGroups {
	g := subjectGroups{groups: make(map[string]struct{}, len(descs)), byLabel: make(map[string]string)}
	for _, d := range descs {
		group, ok := plan.SegmentValue(d, groupBy)
		if !ok {
			continue
		}
		g.groups[group] = struct{}{}
		for name, value := range d.Stream.Labels {
			if _, seen := g.byLabel[value]; name != groupBy && value != "" && !seen {
				g.byLabel[value] = group
			}
		}
	}
	return g
}

func (g subjectGroups) resolve(subject string) (string, bool) {
	// A derived name that is already a value of the group dimension needs no
	// resolving: that is the case the replica-suffix rule gets right.
	if derived := groupFromSubject(subject); derived != subject {
		if _, ok := g.groups[derived]; ok {
			return derived, true
		}
		if group, ok := g.byLabel[derived]; ok {
			return group, true
		}
	}
	// The subject may itself be a stream label — the seed's gateway and jobs pods
	// are spelled exactly like their instance.
	group, ok := g.byLabel[subject]
	return group, ok
}

// readPlacements scans the placement service's segments for subject→entity
// records. It reads only those two columns, so its cost tracks the placement
// stream's size and not the corpus'.
//
// It reports how many row groups carried both columns alongside the map, because
// "no placement stream in this window" means different things to different
// callers and is theirs to interpret.
func (t toolSet) readPlacements(ctx context.Context, snap snapshot, toolName string,
	sel selector.Resolved, spec attributeSpec) (placements map[string]string, segments int, cost query.Cost, err error) {
	placement := selector.Resolved{
		Window: sel.Window,
		Where:  []selector.Predicate{{Field: segment.ColService, Op: selector.OpEq, Value: spec.placementService}},
	}
	p, err := t.Planner.Prune(ctx, snap.catalog, placement)
	if err != nil {
		return nil, 0, query.Cost{}, fmt.Errorf("%s: placement plan: %w", toolName, err)
	}
	columns := []string{spec.subjectField, spec.entityField}
	for _, wi := range p.Items {
		_, subjects := wi.Footer.Offsets.Get(wi.RowGroup, spec.subjectField)
		_, entities := wi.Footer.Offsets.Get(wi.RowGroup, spec.entityField)
		if subjects && entities {
			segments++
		}
	}
	// Each subject keeps the entity its latest record names.
	placed := make(map[string]string)
	rows := t.Scanner.Scan(ctx, p, scan.Query{Columns: columns, Require: columns})
	for rows.Next() {
		subject, _ := rows.Row().Text(spec.subjectField)
		entity, _ := rows.Row().Text(spec.entityField)
		if subject, entity = strings.TrimSpace(subject), strings.TrimSpace(entity); subject != "" && entity != "" {
			placed[strings.Clone(subject)] = strings.Clone(entity)
		}
	}
	cost = addCost(p.Cost, rows.Cost())
	if err := rows.Err(); err != nil {
		return nil, segments, cost, fmt.Errorf("%s: read placement columns: %w", toolName, err)
	}
	return placed, segments, cost, nil
}

// groupFromSubject maps a placed subject to the group it belongs to, dropping a
// trailing "-<digits>" replica suffix when there is one.
//
// A subject that does not carry one is its own group. Returning it unchanged
// rather than discarding it is what keeps non-Kubernetes deployments — VMs, bare
// metal, singleton containers — present in the breakdown instead of silently
// absent from it.
func groupFromSubject(subject string) string {
	i := strings.LastIndexByte(subject, '-')
	if i <= 0 || i == len(subject)-1 {
		return subject
	}
	for _, ch := range subject[i+1:] {
		if ch < '0' || ch > '9' {
			return subject
		}
	}
	return subject[:i]
}

// totalsRequest counts each group's lines over bins.
func totalsRequest(snap snapshot, sel selector.Resolved, groupBy string, bins series.Bins) series.Request {
	return series.Request{
		Version:  snap.catalog,
		Summary:  snap.summaryServes(),
		Selector: sel,
		Window:   bins.Window(),
		Measure:  series.Measure{Kind: series.KindCount},
		GroupBy:  groupBy,
		Bins:     bins,
	}
}

// keptTotals sums each kept group's values, zero for a group with none.
func keptTotals(values series.Values, keep []string) map[string]int64 {
	totals := make(map[string]int64, len(keep))
	for _, group := range keep {
		sum := 0.0
		for _, v := range values[group] {
			sum += v
		}
		totals[group] = int64(sum)
	}
	return totals
}

// totalsGrid is a single bin spanning the whole window.
//
// The totals sum every bin anyway, so a finer grid buys nothing and costs real
// reads: the scan shortcut that answers a contained segment from its descriptor
// alone needs the segment to fit inside one bin (countable). With the default
// 15s grid, every longer segment cost a footer plus a ts-column GET for a number the
// catalog already knew.
//
// The window is first snapped to the summary's own width so the single bin is a
// whole number of summary points, which is what Store.CanServe asks for.
func totalsGrid(width time.Duration, w selector.Window) series.Bins {
	snapped := series.Bins{Width: width, From: w.From, To: w.To}.Snap()
	if span := snapped.To.Sub(snapped.From); span > 0 {
		snapped.Width = span
	}
	return snapped
}

// baselineGrid is the same-width window immediately preceding bins: the
// comparison the anomaly test needs is the same groups over the same span of
// time, just before the incident.
func baselineGrid(bins series.Bins) series.Bins {
	span := bins.To.Sub(bins.From)
	return series.Bins{Width: bins.Width, From: bins.From.Add(-span), To: bins.From}
}

// shiftSelector re-points a selector at another window, keeping its predicates,
// so the baseline read prunes on the baseline's own time range.
func shiftSelector(sel selector.Resolved, bins series.Bins) selector.Resolved {
	sel.Window = selector.Window{From: bins.From, To: bins.To}
	return sel
}

func totalOf(counts map[string]int64) int64 {
	total := int64(0)
	for _, c := range counts {
		total += c
	}
	return total
}

// jointDistribution builds the (group × window) joint over both breakdowns, which
// is what MutualInformation scores: how much knowing the group tells you which
// window a line came from. A split that shifted hard between the two windows
// carries more information than one that held steady.
func jointDistribution(incident, baseline map[string]int64) [][]float64 {
	groups := slices.Sorted(maps.Keys(union(incident, baseline)))
	matrix := make([][]float64, len(groups))
	total := float64(totalOf(incident) + totalOf(baseline))
	for i, group := range groups {
		matrix[i] = []float64{float64(incident[group]) / total, float64(baseline[group]) / total}
	}
	return matrix
}

func union(a, b map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(a)+len(b))
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}
