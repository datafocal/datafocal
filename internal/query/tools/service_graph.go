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
	"github.com/datafocal/datafocal/internal/query/series"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/stats"
	"github.com/datafocal/datafocal/internal/topology"
)

// ServiceGraphRequest is the wire request for the service_graph tool.
type ServiceGraphRequest struct {
	Selector  selector.Selector `json:"selector"`
	Limit     int               `json:"limit,omitempty" jsonschema:"Max edges returned; defaults to 200, max 2000"`
	MinWeight float64           `json:"min_weight,omitempty" jsonschema:"Minimum edge weight override; 0 uses graph default"`
	Placement PlacementRef      `json:"placement,omitempty"`
	// Correlation adds an edge between each pair of the most-moving workloads whose counts correlate.
	Correlation   bool   `json:"correlation,omitempty"`
	MaxCorrelated int    `json:"max_correlated,omitempty" jsonschema:"Workloads correlated pairwise; defaults to 12, max 32"`
	GroupBy       string `json:"group_by,omitempty" jsonschema:"Stream label whose value groups nodes, or auto for service.group then the name's first segment two or more nodes share"`
	Focus         string `json:"focus,omitempty" jsonschema:"Node ID whose neighbourhood is returned, the rest folded into a node per group"`
	Hops          int    `json:"hops,omitempty" jsonschema:"Hops out from focus; defaults to 1, max 3"`
	MaxNodes      int    `json:"max_nodes,omitempty" jsonschema:"Max nodes when grouping or focusing, the lightest folded into other; defaults to 400, max 2000"`
	GroupByID     string `json:"group_by_id,omitempty" jsonschema:"Id column whose traces call edges are drawn from; defaults to the id the most services share"`
	SharedLimit   int    `json:"shared_limit,omitempty" jsonschema:"Shared values listed per co-occurrence edge; defaults to 3, max 10"`
}

// GraphNode is one vertex in the service_graph response, with rate in rows per second,
// error ratio and p95 over the window when it is a service's workload node.
type GraphNode struct {
	ID         string  `json:"id"`
	Kind       string  `json:"kind"`
	Rate       float64 `json:"rate,omitempty"`
	ErrorRatio float64 `json:"error_ratio,omitempty"`
	P95Ms      float64 `json:"p95_ms,omitempty"`
	// Members, MemberIDs (sorted) and Group are set on a node that nodes were folded into, whose Rate is its members' sum and ErrorRatio their mean by rate.
	Members   int      `json:"members,omitempty"`
	MemberIDs []string `json:"member_ids,omitempty"`
	Group     string   `json:"group,omitempty"`
}

// GraphEdge is one directed edge in the service_graph response.
type GraphEdge struct {
	From       string  `json:"from"`
	To         string  `json:"to"`
	Method     string  `json:"method"`
	Weight     float64 `json:"weight"`
	Confidence float64 `json:"confidence"`
	// Rho and LagSeconds are set on correlation edges only; a positive lag means From leads.
	Rho        float64 `json:"rho,omitempty"`
	LagSeconds float64 `json:"lag_seconds,omitempty"`
	// Calls, ErrorRatio, P50Ms and P95Ms are set on a call edge: the calls in the sampled
	// traces scaled to the window's, and the share of callee spans erring and their durations.
	Calls      int64   `json:"calls,omitempty"`
	ErrorRatio float64 `json:"error_ratio,omitempty"`
	P50Ms      int64   `json:"p50_ms,omitempty"`
	P95Ms      int64   `json:"p95_ms,omitempty"`
	// Shared is set on a co-occurrence edge between two services: the values of a correlation
	// column both hold, under one name or two, the most lines first.
	Shared []SharedValue `json:"shared,omitempty"`
}

// SharedValue is one value two services' lines share and about how many lines of each hold
// it, Field being From's name for the column: a service whose footers do not count the
// column is credited a line an hour.
type SharedValue struct {
	Field     string `json:"field"`
	Value     string `json:"value"`
	FromLines int64  `json:"from_lines"`
	ToLines   int64  `json:"to_lines"`
}

// ServiceGraphResponse is the wire response for service_graph.
type ServiceGraphResponse struct {
	Nodes []GraphNode `json:"nodes"`
	Edges []GraphEdge `json:"edges"`
	// Merged is how many nodes MaxNodes folded into the other node.
	Merged int `json:"merged,omitempty"`
}

const (
	defaultServiceGraphLimit = 200
	maxServiceGraphLimit     = 2000
	// placementEdgeWeight is what one workload→resource placement seen in the window
	// is worth. The scan collapses repeated records for the same pair, so the weight
	// says the placement held during the window, not how often it was logged.
	placementEdgeWeight = 1.0
	edgeCorrelation     = "correlation"
	edgeCall            = "call"
	callSample          = 100
	// callHorizon is how far either side of its first line a sampled trace is read.
	callHorizon          = time.Minute
	defaultSharedLimit   = 3
	defaultMaxCorrelated = 12
	maxMaxCorrelated     = 32
	defaultMaxNodes      = 400
	maxMaxNodes          = 2000
	maxFocusHops         = 3
	autoGroupBy          = "auto"
	autoGroupLabel       = "service.group"
	nodeGroup            = "group"
	otherNode            = "other"
)

// ServiceGraph is the entry point for the service_graph tool. It reads a topology
// snapshot from the summary store, adds the workload→resource placements recorded in
// the window, and returns the nodes and edges inside the selector's time window and
// service scope.
//
// It is not a hot-artifact-only tool. The summary graph learns placements from stream
// labels, which a log line like "scheduled {workload} on {resource}" does not carry,
// so the placement stream is scanned for the rest — two columns over one small
// service, billed in Cost. Co-occurrence weights come from sketch
// inclusion-exclusion, so Approx is always true; Coverage drops below 1 only when
// limit truncates the edge set.
//
// No summary is a cold cache, not a refusal: the graph is folded from the window's
// footers instead, billing the GETs it takes (topologyView).
func (t toolSet) ServiceGraph(ctx context.Context, req ServiceGraphRequest) (query.Envelope[ServiceGraphResponse], error) {
	var zero query.Envelope[ServiceGraphResponse]

	sel, err := validateSelector(ctx, "service_graph", req.Selector)
	if err != nil {
		return zero, err
	}

	snap, err := t.resolveSnapshot(ctx, t.Summary, "service_graph", "")
	if err != nil {
		return zero, err
	}

	// The topology shape comes off the configured store, not the gated one: a graph
	// this request folds itself must use the same workload label the
	// warm path would have, whether or not it may read the warm path's contents.
	topoCfg := t.topologyConfig()
	scope, err := t.graphScope(ctx, snap, sel, cmp.Or(topoCfg.WorkloadLabel, topology.DefaultWorkloadLabel))
	if err != nil {
		return zero, err
	}

	spec := resolveAttributionSpec(EntityRef{}, req.Placement, "")
	// A window with no placement records is not an error here — it costs the graph
	// its workload and resource nodes and leaves the service layer intact.
	placements, _, cost, err := t.readPlacements(ctx, snap, "service_graph", sel, spec)
	if err != nil {
		return zero, err
	}

	filter := topology.Filter{
		MinWeight: req.MinWeight,
		Nodes:     slices.Sorted(maps.Keys(scope)),
	}
	if !sel.Window.From.IsZero() {
		filter.From = sel.Window.From.UnixMilli()
	}
	if !sel.Window.To.IsZero() {
		filter.To = sel.Window.To.UnixMilli()
	}

	view, foldCost, err := t.topologyView(ctx, snap, sel, topoCfg, filter)
	if err != nil {
		return zero, err
	}
	cost = addCost(cost, foldCost)
	correlated, corrCost, err := t.correlationEdges(ctx, snap, sel, scope, req)
	if err != nil {
		return zero, err
	}
	cost = addCost(cost, corrCost)
	calls, callMeta, err := t.callEdges(ctx, req, view, scope)
	if err != nil {
		return zero, err
	}
	cost = addCost(cost, callMeta.Cost)
	edges, kinds := mergePlacementEdges(view, placements, append(correlated, calls...), scope, req.MinWeight)

	resp, coverage, metricsCost, err := t.graphResponse(ctx, snap, sel, edges, kinds, req)
	cost = addCost(cost, metricsCost)
	if err != nil {
		return zero, err
	}
	sharedValues(view, resp.Edges, min(cmp.Or(req.SharedLimit, defaultSharedLimit), topology.MaxSharedValues))

	return query.Envelope[ServiceGraphResponse]{
		Data: resp,
		Meta: query.Meta{
			Snapshot: snap.String(),
			Coverage: min(coverage, callMeta.Coverage),
			Approx:   true,
			Cost:     cost,
			Tokens:   query.EstimateTokens(resp),
		},
	}, nil
}

// sharedValues sets each kept co-occurrence edge's Shared from the graph's edge between the
// same two services, its first limit values.
func sharedValues(view topology.View, edges []GraphEdge, limit int) {
	shared := make(map[[2]string][]topology.SharedValue, len(view.Edges))
	for _, e := range view.Edges {
		if e.Method == topology.EdgeCoOccurrence {
			shared[[2]string{e.From, e.To}] = e.Shared
		}
	}
	for i, e := range edges {
		if e.Method != string(topology.EdgeCoOccurrence) {
			continue
		}
		values := shared[[2]string{e.From, e.To}]
		for _, v := range values[:min(len(values), limit)] {
			edges[i].Shared = append(edges[i].Shared, SharedValue{Field: v.Field, Value: v.Value, FromLines: v.Lines[0], ToLines: v.Lines[1]})
		}
	}
}

// limitEdges keeps the first limit edges, defaulted and capped, and reports the fraction
// kept as coverage.
func limitEdges(edges []GraphEdge, limit int) ([]GraphEdge, float64) {
	if limit <= 0 {
		limit = defaultServiceGraphLimit
	}
	limit = min(limit, maxServiceGraphLimit)
	if len(edges) <= limit {
		return edges, 1.0
	}
	return edges[:limit], float64(limit) / float64(len(edges))
}

// graphResponse limits a grouped or focused graph's edges only once they are bundled, so an
// edge folded away never costs a kept one its place.
func (t toolSet) graphResponse(ctx context.Context, snap snapshot, sel selector.Resolved,
	edges []GraphEdge, kinds map[string]topology.NodeKind, req ServiceGraphRequest) (ServiceGraphResponse, float64, query.Cost, error) {
	shaped := req.GroupBy != "" || req.Focus != "" || req.MaxNodes > 0
	coverage := 1.0
	if !shaped {
		edges, coverage = limitEdges(edges, req.Limit)
	}
	resp := ServiceGraphResponse{Nodes: graphNodes(edges, kinds), Edges: edges}
	cost, err := t.nodeMetrics(ctx, snap, sel, &resp.Nodes)
	if err != nil || !shaped {
		return resp, coverage, cost, err
	}

	// With Focus, the nodes within Hops of it stay and the rest fold into a node per group;
	// with only GroupBy, every node does.
	if req.GroupBy != "" || req.Focus != "" {
		near, err := focusNodes(resp.Nodes, resp.Edges, req.Focus, min(max(req.Hops, 1), maxFocusHops))
		if err != nil {
			return resp, coverage, cost, err
		}
		if req.Focus != "" && len(near) == 1 {
			resp.Nodes = slices.DeleteFunc(resp.Nodes, func(n GraphNode) bool { return n.ID != req.Focus })
			resp.Edges = []GraphEdge{}
			return resp, coverage, cost, nil
		}
		groups, err := t.nodeGroups(ctx, snap, sel, req.GroupBy)
		if err != nil {
			return resp, coverage, cost, err
		}
		resp = groupNodes(resp, near, groups)
	}
	resp = limitNodes(resp, req.MaxNodes)
	resp.Edges, coverage = limitEdges(resp.Edges, req.Limit)
	return resp, coverage, cost, nil
}

// focusNodes is the set of nodes within hops of focus along edges either way, nil without a focus.
func focusNodes(nodes []GraphNode, edges []GraphEdge, focus string, hops int) (map[string]bool, error) {
	if focus == "" {
		return nil, nil
	}
	neighbours := make(map[string][]string)
	for _, e := range edges {
		neighbours[e.From] = append(neighbours[e.From], e.To)
		neighbours[e.To] = append(neighbours[e.To], e.From)
	}
	if !slices.ContainsFunc(nodes, func(n GraphNode) bool { return n.ID == focus }) {
		return nil, query.Invalidf("service_graph: focus %q is not a node in the window", focus)
	}
	near := map[string]bool{focus: true}
	frontier := []string{focus}
	for range hops {
		var next []string
		for _, id := range frontier {
			for _, n := range neighbours[id] {
				if !near[n] {
					near[n] = true
					next = append(next, n)
				}
			}
		}
		frontier = next
	}
	return near, nil
}

// nodeGroups maps each node a stream in the window declares, as its service or workload, to
// the stream's value for column.
func (t toolSet) nodeGroups(ctx context.Context, snap snapshot, sel selector.Resolved, column string) (map[string]string, error) {
	if column == "" || column == autoGroupBy {
		column = autoGroupLabel
	}
	descs, err := t.Planner.Segments(ctx, snap.catalog, selector.Resolved{Window: sel.Window})
	if err != nil {
		return nil, fmt.Errorf("service_graph: prune for groups: %w", err)
	}
	workloadLabel := cmp.Or(t.topologyConfig().WorkloadLabel, topology.DefaultWorkloadLabel)
	groups := make(map[string]string)
	for _, d := range descs {
		group := d.Stream.Labels[column]
		if group == "" {
			continue
		}
		for _, id := range []string{d.Stream.Service, d.Stream.Labels[workloadLabel]} {
			if _, ok := groups[id]; id != "" && !ok {
				groups[id] = group
			}
		}
	}
	return groups, nil
}

// groupNodes folds every node not near into a node for its group: the one groups names, else
// its name up to the first '-', '.' or '/' when another node's starts the same.
func groupNodes(g ServiceGraphResponse, near map[string]bool, groups map[string]string) ServiceGraphResponse {
	prefixed := make(map[string]int, len(g.Nodes))
	for _, n := range g.Nodes {
		prefixed[namePrefix(n.ID)]++
	}
	into := make(map[string]GraphNode, len(g.Nodes))
	for _, n := range g.Nodes {
		if prefix := namePrefix(n.ID); !near[n.ID] && (groups[n.ID] != "" || prefixed[prefix] > 1) {
			group := cmp.Or(groups[n.ID], prefix)
			// The prefix keeps a group from taking the ID of a node kept beside it.
			into[n.ID] = GraphNode{ID: nodeGroup + ":" + group, Kind: nodeGroup, Group: group}
		}
	}
	return collapse(g, into)
}

// fold adds n to the node it folds into: its members, its rate, and its errors, so the
// node's error ratio is its members' by the lines each logged.
func fold(into *GraphNode, n GraphNode) {
	into.Members += max(n.Members, 1)
	into.MemberIDs = appendMembers(into.MemberIDs, n)
	if rate := into.Rate + n.Rate; rate > 0 {
		into.ErrorRatio = (into.ErrorRatio*into.Rate + n.ErrorRatio*n.Rate) / rate
	}
	into.Rate += n.Rate
}

func namePrefix(id string) string {
	if i := strings.IndexAny(id, "-./"); i > 0 {
		return id[:i]
	}
	return id
}

// limitNodes folds the nodes with the least edge weight into one other node until at most
// maxNodes, defaulted and capped, remain.
func limitNodes(g ServiceGraphResponse, maxNodes int) ServiceGraphResponse {
	if maxNodes <= 0 {
		maxNodes = defaultMaxNodes
	}
	maxNodes = min(maxNodes, maxMaxNodes)
	if len(g.Nodes) <= maxNodes {
		return g
	}
	weights := make(map[string]float64, len(g.Nodes))
	for _, e := range g.Edges {
		weights[e.From] += e.Weight
		weights[e.To] += e.Weight
	}
	ranked := slices.Clone(g.Nodes)
	slices.SortStableFunc(ranked, func(a, b GraphNode) int { return cmp.Compare(weights[b.ID], weights[a.ID]) })
	into := make(map[string]GraphNode, len(ranked)-maxNodes+1)
	for _, n := range ranked[maxNodes-1:] {
		into[n.ID] = GraphNode{ID: otherNode, Kind: nodeGroup}
	}
	g = collapse(g, into)
	g.Merged = len(into)
	return g
}

// collapse folds each node into the node into maps it to, adding up and listing members and
// rates and weighting error ratios by rate, and bundles the edges between the result: weights and
// calls summed, the error ratio by calls, the slowest latencies, the lowest confidence, the methods joined.
func collapse(g ServiceGraphResponse, into map[string]GraphNode) ServiceGraphResponse {
	at := make(map[string]int, len(g.Nodes))
	nodes := make([]GraphNode, 0, len(g.Nodes))
	for _, n := range g.Nodes {
		target, folded := into[n.ID]
		if !folded {
			target = n
		}
		i, ok := at[target.ID]
		if !ok {
			i, at[target.ID] = len(nodes), len(nodes)
			nodes = append(nodes, target)
		}
		if folded {
			fold(&nodes[i], n)
		}
	}
	for _, n := range nodes {
		slices.Sort(n.MemberIDs)
	}
	slices.SortFunc(nodes, compareNodes)

	rename := func(id string) string { return cmp.Or(into[id].ID, id) }
	bundles := make(map[[2]string]int, len(g.Edges))
	edges := make([]GraphEdge, 0, len(g.Edges))
	for _, e := range g.Edges {
		e.From, e.To = rename(e.From), rename(e.To)
		i, ok := bundles[[2]string{e.From, e.To}]
		if !ok {
			bundles[[2]string{e.From, e.To}] = len(edges)
			edges = append(edges, e)
			continue
		}
		b := &edges[i]
		b.Weight += e.Weight
		if calls := b.Calls + e.Calls; calls > 0 {
			b.ErrorRatio = (b.ErrorRatio*float64(b.Calls) + e.ErrorRatio*float64(e.Calls)) / float64(calls)
		}
		b.Calls, b.P50Ms, b.P95Ms = b.Calls+e.Calls, max(b.P50Ms, e.P50Ms), max(b.P95Ms, e.P95Ms)
		b.Confidence = min(b.Confidence, e.Confidence)
		b.Rho, b.LagSeconds = 0, 0
		if methods := strings.Split(b.Method, ","); !slices.Contains(methods, e.Method) {
			b.Method = strings.Join(slices.Sorted(slices.Values(append(methods, e.Method))), ",")
		}
	}
	slices.SortFunc(edges, compareEdges)
	return ServiceGraphResponse{Nodes: nodes, Edges: edges, Merged: g.Merged}
}

// appendMembers appends to ids the nodes n stands for: its members, or n when it has none.
func appendMembers(ids []string, n GraphNode) []string {
	if n.MemberIDs == nil {
		return append(ids, n.ID)
	}
	return append(ids, n.MemberIDs...)
}

// topologyView returns the window's topology snapshot: from the summary when one is
// wired, and by folding the window's segment footers into a graph of the same shape
// when it is not.
//
// The two agree by construction. The graph stores each edge's weight per bin and
// Snapshot sums only the bins the filter admits, so folding exactly the segments
// the window prunes to contributes exactly the bins the summary would have.
// Adding a segment is idempotent — placement weight is set rather than
// incremented, trace sketches merge register-wise — so the fold is a function of
// the snapshot, not of the order the footers arrived in.
//
// Footers are the cheapest artifact this can come from: co-occurrence weight
// derives from the segments' trace sketches, which live nowhere else. A cold cache
// costs one GET per pruned segment and no data reads, billed rather than hidden.
func (t toolSet) topologyView(ctx context.Context, snap snapshot, sel selector.Resolved, cfg topology.Config,
	filter topology.Filter) (topology.View, query.Cost, error) {
	var cost query.Cost
	store := t.store(snap)
	// A named lower bound has to be covered, not merely non-nil: a store that never
	// absorbed the older end of the window has no bins there, and Snapshot sums the
	// bins it has. That reads as a window in which those services did not talk to
	// each other, which is a wrong answer rather than a slow one — so fold footers.
	//
	// A window with no lower bound (filter.From zero) is left to the store. It asks
	// for all of time, which no store covers and a footer fold over every descriptor
	// the tenant ever wrote would answer literally; a graph is declared approximate
	// either way (Approx below), so the store's window is a reasonable reading of an
	// unbounded one and the fold is not worth its cost.
	if store != nil && (filter.From == 0 || store.CompleteFrom(filter.From)) {
		return store.TopologyView(filter), cost, nil
	}

	// The service predicates are dropped for the same reason graphScope drops them:
	// an edge is evidence about both its endpoints, so pruning to one side's segments
	// would lose the other side's contribution to the shared-value overlap. The
	// filter's node scope, applied by Edges, is what narrows the result.
	descs, err := t.Planner.Segments(ctx, snap.catalog, selector.Resolved{Window: hourWindow(sel.Window)})
	if err != nil {
		return topology.View{}, cost, fmt.Errorf("service_graph: prune for topology: %w", err)
	}
	p, err := t.footers(ctx, descs)
	if err != nil {
		return topology.View{}, p.Cost, fmt.Errorf("service_graph: topology footers: %w", err)
	}
	graph := topology.New(cfg)
	for d, footer := range p.Footers() {
		if err := graph.Add(d, footer); err != nil {
			return topology.View{}, p.Cost, fmt.Errorf("service_graph: topology add %s: %w", d.ID, err)
		}
	}
	return graph.Edges(filter), p.Cost, nil
}

// hourWindow widens w to the whole hours the topology's bins cover.
func hourWindow(w selector.Window) selector.Window {
	if !w.From.IsZero() {
		w.From = w.From.Truncate(time.Hour)
	}
	if !w.To.IsZero() {
		w.To = w.To.Add(-time.Nanosecond).Truncate(time.Hour).Add(time.Hour)
	}
	return w
}

// topologyConfig is the configured summary's topology shape, the defaults without one.
func (t toolSet) topologyConfig() topology.Config {
	if t.Summary == nil {
		return topology.Config{}
	}
	return t.Summary.TopologyConfig()
}

// mergePlacementEdges combines the graph's edges with the scanned
// workload→resource placements and the derived edges and returns them ordered heaviest
// first, along with each endpoint's node kind. A co-occurrence edge between two services
// call edges touch is dropped: the calls say how they are linked.
//
// Ordering by weight is what makes truncation mean something: a caller who asks
// for 20 edges of a 500-edge graph wants the 20 strongest relationships, not the
// 20 whose method sorts first. Ties fall back to the graph's own (method, from,
// to) order so the result stays byte-identical across calls.
func mergePlacementEdges(view topology.View, placements map[string]string, derived []GraphEdge,
	scope map[string]struct{}, minWeight float64) ([]GraphEdge, map[string]topology.NodeKind) {
	kinds := make(map[string]topology.NodeKind, len(view.Nodes)+len(placements)*2)
	for _, n := range view.Nodes {
		kinds[n.ID] = n.Kind
	}
	edges := viewEdges(view, derived, len(placements))

	// The graph applies the caller's floor to its own edges; apply it here too so a
	// raised floor does not let scanned edges past a bar the others had to clear.
	if minWeight <= placementEdgeWeight {
		for _, workload := range slices.Sorted(maps.Keys(placements)) {
			resource := placements[workload]
			if len(scope) > 0 && !inGraphScope(scope, workload, resource) {
				continue
			}
			kinds[workload] = topology.NodeWorkload
			kinds[resource] = topology.NodeResource
			edges = append(edges, GraphEdge{
				From: workload, To: resource, Method: string(topology.EdgePlacement), Weight: placementEdgeWeight,
				Confidence: 1,
			})
		}
	}
	edges = append(edges, derived...)

	slices.SortFunc(edges, compareEdges)
	return edges, kinds
}

// viewEdges is view's edges but the co-occurrence ones between two services derived's call
// edges touch, with room for extra more.
func viewEdges(view topology.View, derived []GraphEdge, extra int) []GraphEdge {
	called := map[string]bool{}
	for _, e := range derived {
		called[e.From], called[e.To] = called[e.From] || e.Method == edgeCall, called[e.To] || e.Method == edgeCall
	}
	edges := make([]GraphEdge, 0, len(view.Edges)+extra)
	for _, e := range view.Edges {
		if e.Method == topology.EdgeCoOccurrence && called[e.From] && called[e.To] {
			continue
		}
		edges = append(edges, GraphEdge{
			From: e.From, To: e.To, Method: string(e.Method), Weight: e.Weight, Confidence: e.Confidence,
		})
	}
	return edges
}

// idCarriers is the services sharing each correlation column's values in view's edges.
func idCarriers(view topology.View) map[string]map[string]bool {
	carriers := map[string]map[string]bool{}
	carry := func(column, service string) {
		if carriers[column] == nil {
			carriers[column] = map[string]bool{}
		}
		carriers[column][service] = true
	}
	for _, e := range view.Edges {
		for _, f := range e.Fields {
			carry(f[0], e.From)
			carry(f[1], e.To)
		}
	}
	return carriers
}

// compareEdges orders edges heaviest first, then by (method, from, to).
func compareEdges(a, b GraphEdge) int {
	if c := cmp.Compare(b.Weight, a.Weight); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Method, b.Method); c != 0 {
		return c
	}
	if c := cmp.Compare(a.From, b.From); c != 0 {
		return c
	}
	return cmp.Compare(a.To, b.To)
}

// callEdges is an edge from each service to each it calls in up to callSample traces of the
// whole window, so the sample matches the trace count it is scaled by, scope only filtering
// the edges.
func (t toolSet) callEdges(ctx context.Context, req ServiceGraphRequest, view topology.View, scope map[string]struct{}) ([]GraphEdge, query.Meta, error) {
	column, traces, cost, err := t.callColumn(ctx, req, view)
	if err != nil || column == "" {
		return nil, query.Meta{Coverage: 1, Cost: cost}, err
	}
	sample, meta, err := t.sampleTraces(ctx, selector.Selector{Time: req.Selector.Time}, column, callSample, sampling{horizon: callHorizon})
	meta.Cost = addCost(meta.Cost, cost)
	if err != nil || len(sample) == 0 {
		return nil, meta, err
	}
	callees := calleeSpans(sample, scope)
	scale := float64(max(traces, int64(len(sample)))) / float64(len(sample))
	edges := make([]GraphEdge, 0, len(callees))
	for pair, c := range callees {
		slices.Sort(c.durs)
		calls := int64(math.Round(float64(len(c.durs)) * scale))
		edges = append(edges, GraphEdge{From: pair[0], To: pair[1], Method: edgeCall, Weight: float64(calls), Confidence: 1,
			Calls: calls, ErrorRatio: float64(c.errors) / float64(len(c.durs)), P50Ms: nearestRank(c.durs, 0.5), P95Ms: nearestRank(c.durs, 0.95)})
	}
	return edges, meta, nil
}

// calleeSpans is the errors and durations of the sampled spans whose parent is in another
// service, under the services of the caller and the callee, those scope keeps.
func calleeSpans(sample [][]Span, scope map[string]struct{}) map[[2]string]*shapeNode {
	nodes := map[string]*shapeNode{}
	for _, spans := range sample {
		mergeTrace(nodes, spans)
	}
	callees := map[[2]string]*shapeNode{}
	for _, n := range nodes {
		parent := nodes[n.parent]
		if parent == nil || parent.service == n.service || len(scope) > 0 && !inGraphScope(scope, parent.service, n.service) {
			continue
		}
		pair := [2]string{parent.service, n.service}
		if callees[pair] == nil {
			callees[pair] = &shapeNode{}
		}
		callees[pair].errors += n.errors
		callees[pair].durs = append(callees[pair].durs, n.durs...)
	}
	return callees
}

// callColumn is req's GroupByID, else the id column whose values the most services in view
// share, the one with the most distinct values on a tie, and its distinct values in the window.
func (t toolSet) callColumn(ctx context.Context, req ServiceGraphRequest, view topology.View) (string, int64, query.Cost, error) {
	carriers := idCarriers(view)
	if req.GroupByID != "" {
		carriers = map[string]map[string]bool{req.GroupByID: nil}
	}
	if len(carriers) == 0 {
		return "", 0, query.Cost{}, nil
	}
	fields, err := t.RankFields(ctx, FieldsRequest{Selector: selector.Selector{Time: req.Selector.Time}})
	if err != nil {
		return "", 0, fields.Meta.Cost, err
	}
	distinct := make(map[string]int64, len(fields.Data.Fields))
	for _, f := range fields.Data.Fields {
		distinct[f.Name] = f.Cardinality
	}
	column := ""
	for _, c := range slices.Sorted(maps.Keys(carriers)) {
		if column == "" || cmp.Or(cmp.Compare(len(carriers[c]), len(carriers[column])), cmp.Compare(distinct[c], distinct[column])) > 0 {
			column = c
		}
	}
	return column, distinct[column], fields.Meta.Cost, nil
}

// correlationEdges ranks services by their strongest pattern as rca_walk ranks
// candidates, so the graph and a walk over the same window agree on who moved.
func (t toolSet) correlationEdges(ctx context.Context, snap snapshot, sel selector.Resolved,
	graphScope map[string]struct{}, req ServiceGraphRequest) ([]GraphEdge, query.Cost, error) {
	if !req.Correlation {
		return nil, query.Cost{}, nil
	}
	scope := selector.Resolved{Window: sel.Window}
	descs, err := t.Planner.Segments(ctx, snap.catalog, scope)
	if err != nil {
		return nil, query.Cost{}, fmt.Errorf("service_graph: prune for correlation: %w", err)
	}
	from, to := binWindow(ctx, descs, sel.Window)
	bins, err := series.GridFor("", t.summaryWidth(), selector.Window{From: from, To: to})
	if err != nil {
		return nil, query.Cost{}, query.Invalidf("service_graph: %v", err)
	}
	patterns, cost, err := t.patternStats(ctx, snap, scope, bins, defaultGroupBy)
	if err != nil {
		return nil, cost, err
	}
	nodes, refs := movingServices(patterns, graphScope, min(cmp.Or(req.MaxCorrelated, defaultMaxCorrelated), maxMaxCorrelated))
	if len(nodes) < 2 {
		return nil, cost, nil
	}
	counts, readCost, err := t.resolveSeries(ctx, readScope{snap: snap, sel: scope, bins: bins,
		bound: series.Bound{Segments: defaultMaxScanSegments, Bytes: defaultMaxScanBytes}}, refs)
	cost = addCost(cost, readCost)
	if err != nil {
		return nil, cost, fmt.Errorf("service_graph: correlation series: %w", err)
	}

	return correlatePairs(nodes, counts, bins.Width, max(defaultMinRho, req.MinWeight)), cost, nil
}

// correlatePairs is an edge for each pair of nodes whose counts are both stats.Supported and
// correlate at least minRho, from the leader to the follower.
func correlatePairs(nodes []string, counts [][]float64, width time.Duration, minRho float64) []GraphEdge {
	maxLag := clampLagBins(defaultMaxLagSeconds, width, len(counts[0]))
	var edges []GraphEdge
	for i := range nodes {
		if !stats.Supported(counts[i]) {
			continue
		}
		for j := i + 1; j < len(nodes); j++ {
			if !stats.Supported(counts[j]) {
				continue
			}
			r := stats.CrossCorrelate(counts[i], counts[j], width, maxLag)
			if math.Abs(r.Rho) < minRho {
				continue
			}
			e := GraphEdge{From: nodes[i], To: nodes[j], Method: edgeCorrelation, Weight: math.Abs(r.Rho),
				Confidence: math.Abs(r.Rho), Rho: r.Rho, LagSeconds: r.LagDuration.Seconds()}
			if e.LagSeconds < 0 {
				e.From, e.To, e.LagSeconds = e.To, e.From, -e.LagSeconds
			}
			edges = append(edges, e)
		}
	}
	return edges
}

// movingServices is the limit services in scope whose patterns signal most, ties broken by
// name, sorted by name, with each one's strongest pattern: what moved, which the service's
// total would dilute.
func movingServices(patterns rcaPatternStats, scope map[string]struct{}, limit int) ([]string, []SeriesRef) {
	total := int64(0)
	for _, lines := range patterns.linesByPattern {
		total += lines
	}
	type strongest struct {
		pattern string
		signal  float64
	}
	best := make(map[string]strongest)
	for _, id := range slices.Sorted(maps.Keys(patterns.linesByPattern)) {
		node := patterns.serviceByPattern[id]
		if _, ok := scope[node]; len(scope) > 0 && !ok {
			continue
		}
		signal := patterns.signalOf(id, patterns.linesByPattern[id], total)
		if cur, ok := best[node]; !ok || signal > cur.signal {
			best[node] = strongest{id, signal}
		}
	}
	nodes := slices.Sorted(maps.Keys(best))
	slices.SortStableFunc(nodes, func(a, b string) int { return cmp.Compare(best[b].signal, best[a].signal) })
	nodes = nodes[:min(len(nodes), limit)]
	slices.Sort(nodes)
	refs := make([]SeriesRef, len(nodes))
	for i, node := range nodes {
		refs[i] = SeriesRef{PatternID: best[node].pattern}
	}
	return nodes, refs
}

// graphNodes starts with the endpoints of the kept edges, sorted (Kind, ID) to mirror
// the graph's own Snapshot order.
func graphNodes(edges []GraphEdge, kinds map[string]topology.NodeKind) []GraphNode {
	seen := make(map[string]struct{}, len(edges)*2)
	nodes := make([]GraphNode, 0, len(edges)*2)
	for _, e := range edges {
		for _, id := range []string{e.From, e.To} {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			// Only a correlation endpoint has no kind: a service, which the graph may not hold.
			nodes = append(nodes, GraphNode{ID: id, Kind: string(cmp.Or(kinds[id], topology.NodeWorkload))})
		}
	}
	slices.SortFunc(nodes, compareNodes)
	return nodes
}

func compareNodes(a, b GraphNode) int {
	if c := cmp.Compare(a.Kind, b.Kind); c != 0 {
		return c
	}
	return cmp.Compare(a.ID, b.ID)
}

// nodeMetrics sets each workload node's rate, error ratio and p95 from the series
// aggregate reads for that service over the window as one bin, so the two agree.
func (t toolSet) nodeMetrics(ctx context.Context, snap snapshot, sel selector.Resolved, nodes *[]GraphNode) (query.Cost, error) {
	var cost query.Cost
	graphScope, err := t.graphScope(ctx, snap, sel, cmp.Or(t.topologyConfig().WorkloadLabel, topology.DefaultWorkloadLabel))
	if err != nil {
		return cost, err
	}
	scope := selector.Resolved{Window: sel.Window}
	descs, err := t.Planner.Segments(ctx, snap.catalog, scope)
	if err != nil {
		return cost, fmt.Errorf("service_graph: prune for node metrics: %w", err)
	}
	from, to := binWindow(ctx, descs, sel.Window)
	bins, err := series.ParseBins("", from, to)
	if err != nil {
		return cost, err
	}
	read := func(measure series.Measure, numerator []selector.Predicate, window selector.Window) (map[string]float64, error) {
		values, readCost, err := t.serviceValues(ctx, snap, series.Request{
			Selector: scope, Window: window, Measure: measure, Numerator: numerator, Bins: bins,
		})
		cost = addCost(cost, readCost)
		return values, err
	}

	rates, err := read(series.Measure{Kind: series.KindRate}, nil, sel.Window)
	if err != nil {
		return cost, err
	}
	// The error rule is get_trace's (isErrorLevel), and a ratio's rows are its bins' as in aggregate.
	errorLevel := []selector.Predicate{{Field: segment.ColLevel, Op: selector.OpEq, Value: "error"}}
	errorRatios, err := read(series.Measure{Kind: series.KindRatio}, errorLevel, bins.Window())
	if err != nil {
		return cost, err
	}
	// A window with no duration_ms distribution leaves every node without a p95.
	p95s, err := read(series.Measure{Kind: series.KindQuantile, Field: segment.ColDurationMs, Quantile: 0.95}, nil, sel.Window)
	var apiErr *query.APIError
	if err != nil && (!errors.As(err, &apiErr) || apiErr.Code != query.ErrCodeUnknownField) {
		return cost, err
	}

	*nodes = applyNodeMetrics(*nodes, rates, errorRatios, p95s, graphScope)
	return cost, nil
}

// applyNodeMetrics includes logged services in scope and sets each workload's metrics.
func applyNodeMetrics(nodes []GraphNode, rates, errorRatios, p95s map[string]float64, graphScope map[string]struct{}) []GraphNode {
	services := slices.Sorted(maps.Keys(rates))
	if len(graphScope) > 0 {
		services = slices.DeleteFunc(services, func(service string) bool {
			_, ok := graphScope[service]
			return !ok
		})
	}
	for _, service := range services {
		if !slices.ContainsFunc(nodes, func(n GraphNode) bool { return n.ID == service }) {
			nodes = append(nodes, GraphNode{ID: service, Kind: string(topology.NodeWorkload)})
		}
	}
	slices.SortFunc(nodes, compareNodes)
	for i, n := range nodes {
		if n.Kind != string(topology.NodeWorkload) {
			continue
		}
		nodes[i].Rate, nodes[i].ErrorRatio, nodes[i].P95Ms = rates[n.ID], errorRatios[n.ID], p95s[n.ID]
	}
	return nodes
}

// serviceValues reads req for every service as aggregate would, keyed by service to
// its value in req's one bin.
func (t toolSet) serviceValues(ctx context.Context, snap snapshot, req series.Request) (map[string]float64, query.Cost, error) {
	req.Version, req.Summary, req.GroupBy = snap.catalog, snap.summaryServes(), segment.ColService
	req.Bound = series.Bound{Segments: defaultMaxScanSegments, Bytes: defaultMaxScanBytes}
	res, err := t.Series.Read(ctx, req)
	if err != nil {
		return nil, res.Cost, fmt.Errorf("service_graph: node %s: %w", req.Measure.Kind, err)
	}
	byService := make(map[string]float64, len(res.Values))
	for service, byBin := range res.Values {
		byService[service] = byBin[req.Bins.From.UnixMilli()]
	}
	return byService, res.Cost, nil
}

// graphScope expands the selector's service predicates into the node IDs that
// belong to those services: the service itself, plus every workload a stream of
// that service declared itself to be.
//
// The expansion is what makes a service scope answer "where does db run". Nodes
// are named by their own identity — a workload node is a workload, not its
// service — so the bare service name matches only the co-occurrence layer and
// hides the placements underneath. An empty scope means every edge.
//
// Ownership comes from the workload label the stream set, never from the
// workload's name: stripping "-0" off "db-3-0" is a StatefulSet convention that a
// Kafka consumer group or a Lambda ARN does not share, and guessing wrong scopes
// the query to the wrong workload silently.
//
// The resource is deliberately left out of the scope: the workload's own edge
// already carries it as an endpoint, and adding it would pull in every unrelated
// workload sharing that resource under the either-endpoint rule.
func (t toolSet) graphScope(ctx context.Context, snap snapshot, sel selector.Resolved, workloadLabel string) (map[string]struct{}, error) {
	services := make(map[string]struct{})
	for _, p := range sel.Where {
		if p.Field == segment.ColService && p.Op == selector.OpEq {
			services[p.Value] = struct{}{}
		}
	}
	if len(services) == 0 {
		return nil, nil
	}

	// The descriptors come from the snapshot the caller already resolved, and Prune
	// reads them without a data GET. The service predicates are dropped from the
	// pruning selector on purpose — several of them are a union here and Prune would
	// AND them into nothing — so the match happens below, over descriptors in hand.
	descs, err := t.Planner.Segments(ctx, snap.catalog, selector.Resolved{Window: sel.Window})
	if err != nil {
		return nil, fmt.Errorf("service_graph: prune for service scope: %w", err)
	}
	// services stays the read side and scope the write side: a workload whose ID
	// happens to equal some other service's name would otherwise start matching
	// descriptors mid-loop, making the scope depend on descriptor order.
	scope := maps.Clone(services)
	for _, desc := range descs {
		if !slices.ContainsFunc(desc.Services, func(s string) bool { _, ok := services[s]; return ok }) {
			continue
		}
		if workload := desc.Stream.Labels[workloadLabel]; workload != "" {
			scope[workload] = struct{}{}
		}
	}
	return scope, nil
}

// inGraphScope reports whether either endpoint is in scope, matching the graph's
// own either-endpoint rule (topology.Filter.Nodes).
func inGraphScope(wanted map[string]struct{}, from, to string) bool {
	if _, ok := wanted[from]; ok {
		return true
	}
	_, ok := wanted[to]
	return ok
}
