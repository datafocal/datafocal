// Package topology is the graph of workloads, resources and the edges between them,
// learned from segment footers.
package topology

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/sketch"
)

// NodeKind identifies what a topology graph node represents.
//
// Two kinds, not one platform's org chart: a workload does work, a resource is
// something workloads contend for. Kubernetes spells the pair pod and node, Nomad
// allocation and client, Snowflake model and virtual warehouse, Kafka consumer
// group and broker, Airflow task and worker pool. The inference the graph exists
// to support — these two workloads share this resource, and the resource is
// saturated — is identical in every one of them, so the kind names the role and
// the pipeline's own vocabulary stays in the node ID.
type NodeKind string

const (
	// NodeWorkload is a thing that does work: a service, a pod, a job, a task.
	NodeWorkload NodeKind = "workload"
	// NodeResource is a thing workloads contend for: a host, a broker, a warehouse.
	NodeResource NodeKind = "resource"
)

// EdgeMethod identifies how a topology edge was inferred.
type EdgeMethod string

const (
	// EdgeCoOccurrence marks an edge derived from shared correlation values in the same bin.
	EdgeCoOccurrence EdgeMethod = "co-occurrence"
	// EdgePlacement marks an edge derived from a declared workload→resource
	// placement, whether a stream declared its own or a placement stream recorded it.
	EdgePlacement EdgeMethod = "topology"
	// EdgeReference marks an edge from a line naming the service it called, in a column
	// whose values name services, from the line's own service or the column naming the caller.
	EdgeReference EdgeMethod = "reference"
)

// Node is one vertex in the topology graph.
type Node struct {
	ID      string
	Kind    NodeKind
	LastBin int64
}

// Edge is one directed edge in the topology graph.
type Edge struct {
	From    string
	To      string
	Method  EdgeMethod
	Weight  float64
	LastBin int64
	// Confidence is 0..1: a co-occurrence edge's shared values over the fewer of its two
	// services' distinct values, summed over the window's bins, and 1 for a placement.
	Confidence float64
	// Fields are the correlation columns, From's then To's and sorted, whose values both
	// services share: under one name, or under two sharing most of their values.
	Fields [][2]string
	// Shared is the values of Fields both services' lines hold, the most lines first and at
	// most MaxSharedValues: a value some footer counts, with its counted lines, plus one an
	// hour where the sketch of segments that do not count the column may hold it.
	Shared []SharedValue
}

// SharedValue is one value two services' lines share and how many lines of each hold it,
// From's then To's as in Fields, Field being From's name for the column.
type SharedValue struct {
	Field string
	Value string
	Lines [2]int64
}

// View is the read-side result of Edges: a deterministically-sorted
// snapshot of nodes and edges matching a Filter.
type View struct {
	Nodes []Node
	Edges []Edge
}

// Filter is the read-side selector passed to Edges.
type Filter struct {
	// From is the inclusive unix-ms bound, counting overlapping hours and leaving zero open.
	From int64
	// To is the exclusive unix-ms bound, with zero leaving the upper bound open.
	To int64
	// Nodes restricts results to edges with either endpoint in this set. Nil means
	// all.
	//
	// These are node IDs, not service names: a workload or resource node is named by
	// its own identity, so scoping "the db service" means naming db's workloads too.
	// The service_graph tool does that expansion from the stream labels before it
	// calls here.
	Nodes []string
	// MinWeight overrides Config.MinEdgeWeight for this call when nonzero.
	MinWeight float64
}

// Config is the topology graph's shape, where every field defaults when zero.
type Config struct {
	MinEdgeWeight float64
	MaxNodes      int
	MaxEdges      int
	// WorkloadLabel and ResourceLabel name the stream labels a stream uses to
	// declare its own placement. They are named rather than fixed because the pair
	// is whatever the pipeline contends over: pod/node on Kubernetes, alloc/client
	// on Nomad, model/warehouse on Snowflake, consumer_group/broker on Kafka. The
	// defaults spell the Kubernetes case because it is the most common, not because
	// the graph assumes it.
	WorkloadLabel string
	ResourceLabel string
}

const binWidth = time.Hour

type sketchKey struct{ service, column string }

const (
	// DefaultMinEdgeWeight keeps an edge carrying one shared value or one placement bin
	// over the window, a caller-facing floor rather than the noise guard in SharedValues.
	DefaultMinEdgeWeight = 1.0
	// DefaultMaxNodes caps the node map so a runaway trace set cannot
	// unboundedly inflate the artifact.
	DefaultMaxNodes = 10_000 //nolint:dupdefault // a topology node cap, unrelated to collector's pattern budget
	// DefaultMaxEdges caps the (edge, bin) records held, so 200k is ~8k edges over a day.
	DefaultMaxEdges = 200_000
	// coOccurrenceSigmas is the headroom, in standard errors of the estimate's own
	// spread, an overlap needs before it counts as evidence rather than noise.
	coOccurrenceSigmas = 3.0
	// DefaultWorkloadLabel and DefaultResourceLabel spell the Kubernetes pair,
	// which is the deployment shape most tenants arrive with. A tenant on any other
	// pipeline names its own via Config.
	DefaultWorkloadLabel = "pod"
	DefaultResourceLabel = "host"
	// MaxSharedValues is the most values an edge's Shared lists.
	MaxSharedValues = 10
)

func (c Config) withDefaults() Config {
	if c.MinEdgeWeight <= 0 {
		c.MinEdgeWeight = DefaultMinEdgeWeight
	}
	if c.MaxNodes <= 0 {
		c.MaxNodes = DefaultMaxNodes
	}
	if c.MaxEdges <= 0 {
		c.MaxEdges = DefaultMaxEdges
	}
	c.WorkloadLabel = cmp.Or(c.WorkloadLabel, DefaultWorkloadLabel)
	c.ResourceLabel = cmp.Or(c.ResourceLabel, DefaultResourceLabel)
	return c
}

// edgeKey identifies one bin's contribution to an edge. The bin is part of the
// identity because a window query has to sum the bins it asked for and nothing
// else — a single all-time counter cannot answer "who talked to whom at 14:50".
type edgeKey struct {
	from   string
	to     string
	method EdgeMethod
	bin    int64
}

// edgeBin is one bin's weight for one edge, and for a co-occurrence edge the smaller
// of its two services' distinct values in the bin, which bounds that weight, and the
// columns, From's then To's, whose values the two share.
type edgeBin struct {
	weight  float64
	smaller float64
	fields  [][2]string
}

// binValues is one service's values of one column in a bin: each segment's lines of each
// value its footer counts, set by segment ID so adding a segment twice counts it once, and
// the values of the segments whose footers count none, as a column is counted only at two
// lines a value or more.
type binValues struct {
	counted   map[string]map[string]uint64
	uncounted *sketch.HLL
}

// valueSketch caches the distinct-value estimate to avoid a register scan for every peer comparison.
type valueSketch struct {
	values   *sketch.HLL
	distinct float64
	// segments is each segment's distinct values, set by segment ID, and recurs whether the
	// column is an id only by its values whose values recur across them, as an enum's do.
	segments map[string]float64
	recurs   bool
}

// Graph is the service-topology artifact, with edges from streams declaring their own
// placement (EdgePlacement), from correlation values services share in a bin
// (EdgeCoOccurrence) and from lines naming the services they called (EdgeReference).
type Graph interface {
	// Add incorporates one sealed segment into every bin its time range overlaps, idempotently.
	Add(desc segment.Descriptor, footer segment.Footer) error
	// Edges returns a deterministically-sorted copy of the nodes and edges that match
	// filter, each edge weighted by the bins inside the window and no others.
	Edges(filter Filter) View
	// Evict drops nodes, edge-bins, and correlation sketches older than retentionStart.
	Evict(retentionStart int64)
}

type graph struct {
	cfg   Config
	nodes map[string]*Node
	// weights holds one bin's weight for one edge. Each entry is set outright,
	// never added to, so the graph is a function of which segments were observed
	// and not of the order they arrived in.
	weights map[edgeKey]edgeBin
	// sketches indexes accumulated correlation sketches by hour and service/column, so
	// Add walks only the bins it is merging into.
	sketches map[int64]map[sketchKey]*valueSketch
	// stale holds, by bin, the service/column pairs an Add merged into since their
	// overlaps were last computed, which Edges recomputes before it reads. mu serializes
	// that, since concurrent readers share the summary store's read lock.
	stale map[int64]map[sketchKey]bool
	// values holds, by bin and service/column, the lines of each value the bin's segments hold.
	values map[int64]map[sketchKey]*binValues
	// referenced holds, by bin and service/column, each segment's lines of each value of a
	// column that may name an endpoint, and pairs each segment's value pairs by service.
	referenced map[int64]map[sketchKey]map[string]map[string]uint64
	pairs      map[int64]map[string]map[string]map[segment.ValuePair]uint64
	// services is every service a footer named, what a referenced value resolves to.
	services map[string]bool
	// labels holds, by bin, the columns whose values are one label every service carrying
	// them shares, which draw no co-occurrence.
	labels map[int64]map[string]bool
	mu     sync.Mutex
}

// New returns an empty Graph with hourly bins.
func New(cfg Config) Graph {
	return &graph{
		cfg:      cfg.withDefaults(),
		nodes:    make(map[string]*Node),
		weights:  make(map[edgeKey]edgeBin),
		sketches: make(map[int64]map[sketchKey]*valueSketch),
		stale:    make(map[int64]map[sketchKey]bool),
		values:   make(map[int64]map[sketchKey]*binValues),

		referenced: make(map[int64]map[sketchKey]map[string]map[string]uint64),
		pairs:      make(map[int64]map[string]map[string]map[segment.ValuePair]uint64),
		services:   make(map[string]bool),
		labels:     make(map[int64]map[string]bool),
	}
}

func (g *graph) Add(desc segment.Descriptor, footer segment.Footer) error {
	bins, err := desc.TimeRange.Bins(binWidth)
	if err != nil {
		return err
	}
	for _, bin := range bins {
		if err := g.add(desc, footer, bin.Start); err != nil {
			return err
		}
	}
	return nil
}

// add records the placement a stream declares about itself and folds the segment's
// correlation values into the emitting service's sketch for bin, marking that service's overlaps
// in the bin stale.
func (g *graph) add(desc segment.Descriptor, footer segment.Footer, bin int64) error {
	// A stream that labels itself with the workload it is and the resource it runs
	// on states one placement directly — the usual shape for agent-collected
	// telemetry. A placement that lives in log *content* instead ("scheduled pod
	// {pod} host {host}", which is what this repo's corpus emits) is not visible in
	// a footer at all; service_graph derives those from the placement scan.
	workload := desc.Stream.Labels[g.cfg.WorkloadLabel]
	resource := desc.Stream.Labels[g.cfg.ResourceLabel]
	if workload != "" && resource != "" {
		g.upsertNode(workload, NodeWorkload, bin)
		g.upsertNode(resource, NodeResource, bin)
		g.setWeight(workload, resource, EdgePlacement, bin, edgeBin{weight: 1})
	}

	for _, service := range footer.Services() {
		g.upsertNode(service, NodeWorkload, bin)
		g.recordReferences(desc.ID, footer, service, bin)
		for _, column := range slices.Sorted(maps.Keys(footer.ForService(service).DistinctCounts)) {
			key := sketchKey{service, column}
			if err := g.mergeValues(key, bin, desc.ID, footer.ForService(service).DistinctCounts[column]); err != nil {
				return err
			}
			if g.stale[bin] == nil {
				g.stale[bin] = map[sketchKey]bool{}
			}
			g.stale[bin][key] = true
			if err := g.recordValues(key, bin, desc.ID, footer.ForService(service)); err != nil {
				return err
			}
		}
	}
	return nil
}

// mergeValues keeps the union of every segment's values in the hour, and each segment's count.
func (g *graph) mergeValues(key sketchKey, bin int64, id string, values *sketch.HLL) error {
	byKey := g.sketches[bin]
	if byKey == nil {
		byKey = make(map[sketchKey]*valueSketch)
		g.sketches[bin] = byKey
	}
	st := byKey[key]
	if st == nil {
		st = &valueSketch{values: sketch.NewHLL(), segments: map[string]float64{}}
	}
	if err := st.values.Merge(values); err != nil {
		return fmt.Errorf("merge correlation sketch for %s: %w", key.service, err)
	}
	st.segments[id] = values.Estimate()
	byKey[key] = st
	return nil
}

// recordValues records the lines of each value of a service's column a segment's view holds
// in bin, or the column's sketch when the view does not count it.
func (g *graph) recordValues(key sketchKey, bin int64, id string, view segment.Footer) error {
	if g.values[bin] == nil {
		g.values[bin] = map[sketchKey]*binValues{}
	}
	bv := g.values[bin][key]
	if bv == nil {
		bv = &binValues{counted: map[string]map[string]uint64{}, uncounted: sketch.NewHLL()}
		g.values[bin][key] = bv
	}
	if counts, ok := view.ValueCounts(key.column); ok {
		bv.counted[id] = counts
		return nil
	}
	if err := bv.uncounted.Merge(view.DistinctCounts[key.column]); err != nil {
		return fmt.Errorf("merge uncounted values for %s: %w", key.service, err)
	}
	return nil
}

// settle refreshes pairs touched by Add, keeping the strongest column overlap per pair.
func (g *graph) settle() {
	for _, bin := range slices.Sorted(maps.Keys(g.stale)) {
		byKey := g.sketches[bin]
		keys := slices.SortedFunc(maps.Keys(byKey), func(a, b sketchKey) int {
			return cmp.Or(cmp.Compare(a.service, b.service), cmp.Compare(a.column, b.column))
		})
		regs := make(map[sketchKey][]byte, len(keys))
		changed := map[string]bool{}
		for _, key := range keys {
			regs[key], _ = byKey[key].values.Registers()
			if g.stale[bin][key] {
				byKey[key].distinct = sketch.UnionEstimate(regs[key])
				byKey[key].recurs = recurs(key.column, byKey[key])
				changed[key.service] = true
			}
		}
		g.relabel(bin, keys, regs, changed)
		maps.DeleteFunc(g.weights, func(k edgeKey, _ edgeBin) bool {
			return k.bin == bin && k.method == EdgeCoOccurrence && (changed[k.from] || changed[k.to])
		})
		for i, key := range keys {
			g.refreshSharedValues(bin, key, keys[i+1:], regs, changed)
		}
	}
	clear(g.stale)
}

// refreshSharedValues compares one column with the remaining columns in the hour.
func (g *graph) refreshSharedValues(bin int64, key sketchKey, peers []sketchKey, regs map[sketchKey][]byte, changed map[string]bool) {
	byKey := g.sketches[bin]
	for _, peer := range peers {
		if !g.comparable(bin, key, peer, changed) || !mayShare(byKey[key].distinct, byKey[peer].distinct) {
			continue
		}
		shared, ok := SharedValues(byKey[key].distinct, byKey[peer].distinct, sketch.UnionEstimate(regs[key], regs[peer]))
		if !ok {
			continue
		}
		from, to := coOccurrencePair(key.service, peer.service)
		w := g.weights[edgeKey{from, to, EdgeCoOccurrence, bin}]
		smaller := min(byKey[key].distinct, byKey[peer].distinct)
		if shared > w.weight {
			w.weight, w.smaller = shared, smaller
		}
		// Two names are one entity's when they share most of their values, as get_trace joins them.
		if key.column == peer.column || 2*shared > smaller {
			fields := [2]string{key.column, peer.column}
			if from != key.service {
				fields = [2]string{peer.column, key.column}
			}
			w.fields = append(w.fields, fields)
		}
		g.setWeight(from, to, EdgeCoOccurrence, bin, w)
	}
}

// comparable reports whether key and peer, of two services one of which changed, may share
// values: neither column is one of bin's labels.
func (g *graph) comparable(bin int64, key, peer sketchKey, changed map[string]bool) bool {
	return key.service != peer.service && (changed[key.service] || changed[peer.service]) &&
		!g.labels[bin][key.column] && !g.labels[bin][peer.column] &&
		!g.sketches[bin][key].recurs && !g.sketches[bin][peer].recurs
}

// maxRecurrence is the most the values of an id by its values may recur across a bin's
// segments: below half as many distinct values in them all as in each summed, its values
// are a few names every segment repeats (span names, steps), not fresh units of work.
const maxRecurrence = 0.5

// recurs reports whether column, an id only by its values, holds values its segments repeat.
func recurs(column string, st *valueSketch) bool {
	if segment.IsCorrelation(column) || len(st.segments) < 2 {
		return false
	}
	var sum float64
	for _, n := range st.segments {
		sum += n
	}
	return st.distinct < maxRecurrence*sum
}

// minLabelServices is the most services one value of a column can be shared by and still
// be a unit of work rather than a label of the deployment, as a run's id or a cluster's name is.
const minLabelServices = 3

// relabel sets bin's label columns, those more than minLabelServices-1 services carry whose
// values all together are one, and marks changed every service carrying a column that became
// or stopped being one, so the edges it drew or would draw are recomputed.
func (g *graph) relabel(bin int64, keys []sketchKey, regs map[sketchKey][]byte, changed map[string]bool) {
	byColumn := map[string][][]byte{}
	for _, key := range keys {
		byColumn[key.column] = append(byColumn[key.column], regs[key])
	}
	labels := map[string]bool{}
	for column, columnRegs := range byColumn {
		if len(columnRegs) >= minLabelServices && sketch.UnionEstimate(columnRegs...) < 1.5 {
			labels[column] = true
		}
	}
	for _, key := range keys {
		if labels[key.column] != g.labels[bin][key.column] {
			changed[key.service] = true
		}
	}
	g.labels[bin] = labels
}

func (g *graph) Evict(retentionStart int64) {
	retentionStart = time.UnixMilli(retentionStart).Truncate(binWidth).UnixMilli()
	maps.DeleteFunc(g.weights, func(k edgeKey, _ edgeBin) bool { return k.bin < retentionStart })
	maps.DeleteFunc(g.nodes, func(_ string, n *Node) bool { return n.LastBin < retentionStart })
	maps.DeleteFunc(g.sketches, func(bin int64, _ map[sketchKey]*valueSketch) bool { return bin < retentionStart })
	maps.DeleteFunc(g.stale, func(bin int64, _ map[sketchKey]bool) bool { return bin < retentionStart })
	maps.DeleteFunc(g.values, func(bin int64, _ map[sketchKey]*binValues) bool { return bin < retentionStart })
	maps.DeleteFunc(g.labels, func(bin int64, _ map[string]bool) bool { return bin < retentionStart })
	maps.DeleteFunc(g.referenced, func(bin int64, _ map[sketchKey]map[string]map[string]uint64) bool { return bin < retentionStart })
	maps.DeleteFunc(g.pairs, func(bin int64, _ map[string]map[string]map[segment.ValuePair]uint64) bool {
		return bin < retentionStart
	})
}

func (g *graph) Edges(filter Filter) View {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.settle()
	minWeight := filter.MinWeight
	if minWeight <= 0 {
		minWeight = g.cfg.MinEdgeWeight
	}

	edges := append(g.windowEdges(filter, minWeight), g.referenceEdges(filter, minWeight)...)

	// Sort edges deterministically: (Method, From, To).
	slices.SortFunc(edges, func(a, b Edge) int {
		if c := cmp.Compare(string(a.Method), string(b.Method)); c != 0 {
			return c
		}
		if c := cmp.Compare(a.From, b.From); c != 0 {
			return c
		}
		return cmp.Compare(a.To, b.To)
	})

	// Collect nodes referenced by matching edges.
	nodeSet := make(map[string]struct{}, len(edges)*2)
	for _, e := range edges {
		nodeSet[e.From] = struct{}{}
		nodeSet[e.To] = struct{}{}
	}

	nodes := make([]Node, 0, len(nodeSet))
	for id := range nodeSet {
		if n, ok := g.nodes[id]; ok {
			nodes = append(nodes, *n)
		}
	}

	// Sort nodes deterministically: (Kind, ID).
	slices.SortFunc(nodes, func(a, b Node) int {
		if c := cmp.Compare(string(a.Kind), string(b.Kind)); c != 0 {
			return c
		}
		return cmp.Compare(a.ID, b.ID)
	})

	return View{Nodes: nodes, Edges: edges}
}

// windowEdges sums each in-scope edge over the window's bins and keeps those reaching
// minWeight.
func (g *graph) windowEdges(filter Filter, minWeight float64) []Edge {
	scope := make(map[string]struct{}, len(filter.Nodes))
	for _, s := range filter.Nodes {
		scope[s] = struct{}{}
	}

	// Sum each edge over the bins the window asked for. An edge that has been live
	// for hours reports the part of itself that falls inside the window, so a query
	// about a past incident sees that window's traffic and not a lifetime total.
	type edgeIdent struct {
		from   string
		to     string
		method EdgeMethod
	}
	sums := make(map[edgeIdent]*Edge, len(g.weights))
	smaller := make(map[edgeIdent]float64, len(g.weights))
	fields := make(map[edgeIdent][][2]string, len(g.weights))
	for k, w := range g.weights {
		if !filter.inWindow(k.bin) {
			continue
		}
		if len(scope) > 0 && !inScope(scope, k.from, k.to) {
			continue
		}
		id := edgeIdent{from: k.from, to: k.to, method: k.method}
		e, ok := sums[id]
		if !ok {
			e = &Edge{From: k.from, To: k.to, Method: k.method, LastBin: k.bin}
			sums[id] = e
		}
		e.Weight += w.weight
		e.LastBin = max(e.LastBin, k.bin)
		smaller[id] += w.smaller
		fields[id] = append(fields[id], w.fields...)
	}

	edges := make([]Edge, 0, len(sums))
	for id, e := range sums {
		if e.Weight < minWeight {
			continue
		}
		// Both counts are estimates, so a pair sharing every value can land just past 1.
		e.Confidence = 1
		if id.method == EdgeCoOccurrence {
			e.Confidence = min(e.Weight/smaller[id], 1)
			e.Fields = slices.Compact(slices.SortedFunc(slices.Values(fields[id]), func(a, b [2]string) int {
				return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
			}))
			e.Shared = g.sharedValues(filter, *e)
		}
		edges = append(edges, *e)
	}
	return edges
}

// sharedValues is each value of e's fields both its services' lines hold in the window, the
// most lines first.
func (g *graph) sharedValues(filter Filter, e Edge) []SharedValue {
	var shared []SharedValue
	for _, f := range e.Fields {
		fromKey, toKey := sketchKey{e.From, f[0]}, sketchKey{e.To, f[1]}
		values := g.countedValues(filter, fromKey)
		maps.Copy(values, g.countedValues(filter, toKey))
		from, to := g.windowLines(filter, fromKey, values), g.windowLines(filter, toKey, values)
		for v, n := range from {
			if to[v] > 0 {
				shared = append(shared, SharedValue{Field: f[0], Value: v, Lines: [2]int64{n, to[v]}})
			}
		}
	}
	slices.SortFunc(shared, func(a, b SharedValue) int {
		return cmp.Or(cmp.Compare(b.Lines[0]+b.Lines[1], a.Lines[0]+a.Lines[1]), cmp.Compare(a.Field, b.Field), cmp.Compare(a.Value, b.Value))
	})
	return shared[:min(len(shared), MaxSharedValues)]
}

// countedValues is the set of values of a service's column its footers count in the window.
func (g *graph) countedValues(filter Filter, key sketchKey) map[string]bool {
	values := map[string]bool{}
	for bin, byKey := range g.values {
		if byKey[key] == nil || !filter.inWindow(bin) {
			continue
		}
		for _, counts := range byKey[key].counted {
			for v := range counts {
				values[v] = true
			}
		}
	}
	delete(values, "")
	return values
}

// windowLines is a service's lines of each of values in its column over the window's bins.
func (g *graph) windowLines(filter Filter, key sketchKey, values map[string]bool) map[string]int64 {
	lines := map[string]int64{}
	for bin, byKey := range g.values {
		if byKey[key] != nil && filter.inWindow(bin) {
			byKey[key].addLines(values, lines)
		}
	}
	return lines
}

// addLines adds to lines the bin's lines of each of values: those its footers count, and one
// where the segments that count none may hold it.
func (bv *binValues) addLines(values map[string]bool, lines map[string]int64) {
	for _, counts := range bv.counted {
		for v, n := range counts {
			if values[v] {
				lines[v] += int64(n)
			}
		}
	}
	for v := range values {
		if bv.uncounted.MayHold([]byte(v)) {
			lines[v]++
		}
	}
}

// inWindow reports whether bin's hour overlaps [From, To), a zero bound being open.
func (f Filter) inWindow(bin int64) bool {
	return (f.From <= 0 || bin+binWidth.Milliseconds() > f.From) && (f.To <= 0 || bin < f.To)
}

// upsertNode adds id as kind at bin, or advances LastBin if the node already
// exists.  Drops silently when MaxNodes is reached.
func (g *graph) upsertNode(id string, kind NodeKind, bin int64) {
	if n, ok := g.nodes[id]; ok {
		if bin > n.LastBin {
			n.LastBin = bin
		}
		return
	}
	if len(g.nodes) >= g.cfg.MaxNodes {
		return
	}
	g.nodes[id] = &Node{ID: id, Kind: kind, LastBin: bin}
}

// setWeight records this bin's weight for an edge, replacing whatever the bin
// held before.  Drops silently when MaxEdges is reached, or when either endpoint
// was not admitted to g.nodes (meaning MaxNodes turned it away).
func (g *graph) setWeight(from, to string, method EdgeMethod, bin int64, w edgeBin) {
	if _, ok := g.nodes[from]; !ok {
		return
	}
	if _, ok := g.nodes[to]; !ok {
		return
	}
	key := edgeKey{from: from, to: to, method: method, bin: bin}
	if _, ok := g.weights[key]; !ok && len(g.weights) >= g.cfg.MaxEdges {
		return
	}
	g.weights[key] = w
}

// inScope reports whether either endpoint of an edge is one of the wanted nodes.
// Either endpoint matching is what lets a caller name a pod and still be told
// which host it sits on.
func inScope(wanted map[string]struct{}, from, to string) bool {
	if _, ok := wanted[from]; ok {
		return true
	}
	_, ok := wanted[to]
	return ok
}

// coOccurrencePair returns (from, to) in a consistent alphabetical order so
// that (svc-a, svc-b) and (svc-b, svc-a) map to the same edge key.
func coOccurrencePair(a, b string) (from, to string) {
	if a <= b {
		return a, b
	}
	return b, a
}

// mayShare reports whether sets of a and b distinct values can pass SharedValues, sparing their
// union: unless its estimate falls short of the larger by more than the smaller, they share at
// most twice the smaller, which passes none under the noise floor of a and b alone.
func mayShare(a, b float64) bool {
	return 2*min(a, b) >= coOccurrenceSigmas*sketch.HLLStandardError*math.Sqrt(a*a+b*b)
}

// SharedValues is how many values two sets of a and b distinct values, both together,
// share, rejecting an overlap below the combined sketch noise floor as no evidence that
// they share any.
func SharedValues(a, b, both float64) (float64, bool) {
	est := a + b - both
	if est <= 0 {
		return 0, false
	}
	spread := math.Sqrt(a*a + b*b + both*both)
	if est < coOccurrenceSigmas*sketch.HLLStandardError*spread {
		return 0, false
	}
	return est, true
}
