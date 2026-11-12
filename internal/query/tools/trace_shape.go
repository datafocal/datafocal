package tools

import (
	"cmp"
	"context"
	"maps"
	"math"
	"slices"

	"github.com/datafocal/datafocal/internal/pattern"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// TraceShapeRequest is the wire request for trace_shape.
type TraceShapeRequest struct {
	Selector selector.Selector    `json:"selector" jsonschema:"the window"`
	Root     []selector.Predicate `json:"root" jsonschema:"the events that start the traces to merge"`
	Field    string               `json:"field,omitempty" jsonschema:"correlation column; defaults to trace_id"`
	Sample   int                  `json:"sample,omitempty" jsonschema:"traces to merge, earliest first; defaults to 100, at most 500"`
	Compare  string               `json:"compare,omitempty" jsonschema:"a correlation value to diff against the shape"`
	Snapshot string               `json:"snapshot,omitempty"`
}

// TraceShapeResponse is the wire response for trace_shape: the traces merged, and the
// merged tree with each parent before its children.
type TraceShapeResponse struct {
	Traces int         `json:"traces"`
	Nodes  []ShapeNode `json:"nodes"`
}

// ShapeNode is the spans sharing one path of (service, name) from the root: the share of
// traces holding one, their duration and error share, and how the compared trace differs.
type ShapeNode struct {
	Service    string  `json:"service"`
	Name       string  `json:"name"`
	Parent     int     `json:"parent"`
	Presence   float64 `json:"presence"`
	P50Ms      int64   `json:"p50_ms"`
	P95Ms      int64   `json:"p95_ms"`
	ErrorShare float64 `json:"error_share"`
	Count      int     `json:"count"`
	Diff       string  `json:"diff,omitempty" jsonschema:"extra|slower|missing"`
	InTrace    bool    `json:"in_trace,omitempty" jsonschema:"the compared trace has this operation"`
}

const (
	defaultShapeSample = 100
	maxShapeSample     = 500
	// missingPresence is the presence from which a node the compared trace lacks is missing.
	missingPresence = 0.9
)

// TraceShape samples the earliest traces starting at Root and merges their spans by path.
func (t toolSet) TraceShape(ctx context.Context, req TraceShapeRequest) (query.Envelope[TraceShapeResponse], error) {
	var zero query.Envelope[TraceShapeResponse]
	if len(req.Root) == 0 {
		return zero, query.Invalidf("trace_shape: root is required")
	}
	if req.Sample > maxShapeSample {
		return zero, query.Invalidf("trace_shape: sample is at most %d, got %d", maxShapeSample, req.Sample)
	}
	field := cmp.Or(req.Field, segment.ColTraceID)
	sel := req.Selector
	sel.Where = append(slices.Clip(sel.Where), req.Root...)
	traces, meta, err := t.sampleTraces(ctx, sel, field, cmp.Or(req.Sample, defaultShapeSample), sampling{snapshot: req.Snapshot, skip: req.Compare})
	if err != nil {
		return zero, err
	}
	var compare []Span
	if req.Compare != "" {
		if compare, err = t.readTrace(ctx, &meta, field, req.Compare, req.Selector.Time); err != nil {
			return zero, err
		}
	}

	resp := mergeShape(traces, compare, req.Compare != "")
	meta.Tokens = query.EstimateTokens(resp)
	return query.Envelope[TraceShapeResponse]{Data: resp, Meta: meta}, nil
}

// shapeNode accumulates one path's spans across the traces, and the compared trace's
// slowest span on it.
type shapeNode struct {
	service, name, parent string
	traces, errors        int
	durs                  []int64
	compared              bool
	comparedMs            int64
}

// mergeShape merges traces by path and, when compared, marks how compare differs.
func mergeShape(traces [][]Span, compare []Span, compared bool) TraceShapeResponse {
	nodes := map[string]*shapeNode{}
	for _, spans := range traces {
		mergeTrace(nodes, spans)
	}
	comparePaths := spanPaths(compare)
	for i, p := range comparePaths {
		n := pathNode(nodes, comparePaths, compare[i], p)
		n.compared = true
		if compare[i].Group == nil {
			n.comparedMs = max(n.comparedMs, compare[i].DurMs)
		}
	}

	// A path sorts after its parent's, which it extends.
	paths := slices.Sorted(maps.Keys(nodes))
	resp := TraceShapeResponse{Traces: len(traces), Nodes: make([]ShapeNode, len(paths))}
	for i, p := range paths {
		n := nodes[p]
		slices.Sort(n.durs)
		node := ShapeNode{Service: n.service, Name: n.name, Parent: -1, Count: len(n.durs),
			P50Ms: nearestRank(n.durs, 0.5), P95Ms: nearestRank(n.durs, 0.95)}
		if n.parent != "" {
			node.Parent, _ = slices.BinarySearch(paths, n.parent)
		}
		if len(n.durs) > 0 {
			node.Presence = float64(n.traces) / float64(len(traces))
			node.ErrorShare = float64(n.errors) / float64(len(n.durs))
		}
		if compared {
			node.Diff, node.InTrace = shapeDiff(node, n), n.compared
		}
		resp.Nodes[i] = node
	}
	return resp
}

// mergeTrace adds one trace's spans to the nodes on their paths, a group span adding
// its median once for each member get_trace did not return.
func mergeTrace(nodes map[string]*shapeNode, spans []Span) {
	seen := map[string]bool{}
	paths := spanPaths(spans)
	returned := map[int]int{}
	for i, s := range spans {
		if s.Parent >= 0 && paths[i] == paths[s.Parent] {
			returned[s.Parent]++
		}
	}
	for i, p := range paths {
		n := pathNode(nodes, paths, spans[i], p)
		switch g := spans[i].Group; {
		case g != nil:
			for range g.Count - returned[i] {
				n.durs = append(n.durs, g.P50Ms)
			}
		case spans[i].Error:
			n.errors++
			fallthrough
		default:
			n.durs = append(n.durs, spans[i].DurMs)
		}
		if !seen[p] {
			seen[p] = true
			n.traces++
		}
	}
}

// shapeDiff is how the compared trace differs from node, "" when it does not.
func shapeDiff(node ShapeNode, n *shapeNode) string {
	switch {
	case n.compared && len(n.durs) == 0:
		return "extra"
	case n.compared && n.comparedMs > node.P95Ms:
		return "slower"
	case !n.compared && node.Presence >= missingPresence:
		return "missing"
	}
	return ""
}

// pathNode is the node for span's path p among its trace's paths, added when new.
func pathNode(nodes map[string]*shapeNode, paths []string, span Span, p string) *shapeNode {
	n, ok := nodes[p]
	if !ok {
		n = &shapeNode{service: span.Service, name: pattern.Mask(span.Name)}
		if span.Parent >= 0 {
			n.parent = paths[span.Parent]
		}
		nodes[p] = n
	}
	return n
}

// spanPaths is each span's path from its root, a group's returned members sharing the
// group's, relying on get_trace putting a parent before its children.
func spanPaths(spans []Span) []string {
	paths := make([]string, len(spans))
	for i, s := range spans {
		if s.Parent < 0 {
			paths[i] = pathStep(s)
			continue
		}
		paths[i] = paths[s.Parent]
		if g := spans[s.Parent]; g.Group == nil || g.Service != s.Service || g.Name != s.Name {
			paths[i] += pathStep(s)
		}
	}
	return paths
}

// pathStep masks the name so a derived span, named by its log message, matches across traces.
func pathStep(s Span) string {
	return "\x00" + s.Service + "\x01" + pattern.Mask(s.Name)
}

// nearestRank is the q quantile of sorted by nearest rank, and 0 for none.
func nearestRank(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[max(0, int(math.Ceil(q*float64(len(sorted))))-1)]
}
