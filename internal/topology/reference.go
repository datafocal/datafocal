package topology

import (
	"cmp"
	"maps"
	"net"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/identity"
	"github.com/datafocal/datafocal/internal/segment"
)

// minReferenceShare is the share of a column's lines whose values must name a service for
// the column to be read as naming the services its lines called.
const minReferenceShare = 0.5

// minLinesPerReference is the fewest lines per distinct value a column naming services
// holds over the window, as a callee is named by many calls and a request's id by one.
const minLinesPerReference = 4

// recordReferences keeps a service's counted endpoint-like columns and value pairs in bin,
// set by segment ID so adding a segment twice counts it once.
func (g *graph) recordReferences(id string, footer segment.Footer, service string, bin int64) {
	g.services[service] = true
	view := footer.ForService(service)
	for _, column := range slices.Sorted(maps.Keys(footer.ZoneMaps)) {
		counts, ok := view.ValueCounts(column)
		if !ok || !segment.EndpointColumn(column) {
			continue
		}
		key := sketchKey{service, column}
		if g.referenced[bin] == nil {
			g.referenced[bin] = map[sketchKey]map[string]map[string]uint64{}
		}
		if g.referenced[bin][key] == nil {
			g.referenced[bin][key] = map[string]map[string]uint64{}
		}
		g.referenced[bin][key][id] = counts
	}
	if pairs := view.ValuePairs(); len(pairs) > 0 {
		if g.pairs[bin] == nil {
			g.pairs[bin] = map[string]map[string]map[segment.ValuePair]uint64{}
		}
		if g.pairs[bin][service] == nil {
			g.pairs[bin][service] = map[string]map[segment.ValuePair]uint64{}
		}
		g.pairs[bin][service][id] = pairs
	}
}

// reference is one way the window's lines name an edge: the emitting service and the
// columns, the caller's then the callee's, service standing for the emitter itself.
type reference struct {
	from, to, service string
	fields            [2]string
}

// referenceUse is the lines naming an edge one way, the lowest share of its columns'
// lines naming a service, and the latest bin holding one.
type referenceUse struct {
	lines      uint64
	confidence float64
	lastBin    int64
}

// referenceEdges is the window's edges named by lines. Each edge is weighted by the one
// way of naming it with the most lines, as a sidecar and ztunnel logging one call are
// two sightings of it, not two calls.
func (g *graph) referenceEdges(filter Filter, minWeight float64) []Edge {
	lines, lastBin := g.referencedLines(filter)
	resolve := g.resolver()
	shares := referenceShares(lines, resolve)
	uses := map[reference]referenceUse{}
	paired := g.pairReferences(filter, shares, resolve, uses)
	for key := range lines {
		if _, ok := shares[key]; !ok || paired[key] {
			continue
		}
		for value, n := range lines[key] {
			named := callee(key.column, value, resolve)
			if named == "" || named == key.service {
				continue
			}
			r := reference{key.service, named, key.service, [2]string{segment.ColService, key.column}}
			if endpointRole(key.column) < 0 { // the line names its caller
				r = reference{named, key.service, key.service, [2]string{key.column, segment.ColService}}
			}
			use(uses, r, n, shares[key], lastBin[key])
		}
	}
	dropHops(uses)
	return edgesOfReferences(uses, filter, minWeight)
}

// dropHops removes the references to an intermediary: where one emitter's lines name, for
// one caller, a callee in one column and another callee in a second column, and the first
// is seen calling the second, it is the hop the call went through, as a proxy, gateway or
// load balancer is, unless the second column names it too, as a call to the hop itself does.
func dropHops(uses map[reference]referenceUse) {
	forwards, callees := referenceIndex(uses)
	hop := func(r reference) bool {
		for column, named := range callees[referenceCaller{r.service, r.fields[0], r.from}] {
			if column == r.fields[1] || named[r.to] {
				continue
			}
			for to := range named {
				if forwards[[2]string{r.to, to}] {
					return true
				}
			}
		}
		return false
	}
	maps.DeleteFunc(uses, func(r reference, _ referenceUse) bool { return hop(r) })
}

// referenceCaller is one emitter's lines naming one caller in one column.
type referenceCaller struct{ service, column, from string }

// referenceIndex is the caller and callee of every reference, and by the lines naming each
// caller, the services each callee column names.
func referenceIndex(uses map[reference]referenceUse) (map[[2]string]bool, map[referenceCaller]map[string]map[string]bool) {
	forwards := map[[2]string]bool{}
	callees := map[referenceCaller]map[string]map[string]bool{}
	for r := range uses {
		forwards[[2]string{r.from, r.to}] = true
		c := referenceCaller{r.service, r.fields[0], r.from}
		if callees[c] == nil {
			callees[c] = map[string]map[string]bool{}
		}
		if callees[c][r.fields[1]] == nil {
			callees[c][r.fields[1]] = map[string]bool{}
		}
		callees[c][r.fields[1]][r.to] = true
	}
	return forwards, callees
}

// referencedLines sums each service column's lines of each value over the window's bins.
func (g *graph) referencedLines(filter Filter) (map[sketchKey]map[string]uint64, map[sketchKey]int64) {
	lines, lastBin := map[sketchKey]map[string]uint64{}, map[sketchKey]int64{}
	for bin, byKey := range g.referenced {
		if !filter.inWindow(bin) {
			continue
		}
		for key, bySegment := range byKey {
			if lines[key] == nil {
				lines[key] = map[string]uint64{}
			}
			for _, counts := range bySegment {
				for v, n := range counts {
					lines[key][v] += n
				}
			}
			lastBin[key] = max(lastBin[key], bin)
		}
	}
	return lines, lastBin
}

// referenceShares is, for each column most of whose lines name a service and whose values
// repeat as callees' names do, that share.
func referenceShares(lines map[sketchKey]map[string]uint64, resolve func(string) string) map[sketchKey]float64 {
	shares := map[sketchKey]float64{}
	for key, counts := range lines {
		var total, named uint64
		for v, n := range counts {
			total += n
			if callee(key.column, v, resolve) != "" {
				named += n
			}
		}
		if named > 0 && float64(named) >= minReferenceShare*float64(total) && total >= minLinesPerReference*uint64(len(counts)) {
			shares[key] = float64(named) / float64(total)
		}
	}
	return shares
}

// pairReferences adds the edges lines name by both ends, a caller's column and a callee's
// column of one line, and returns the columns read that way.
func (g *graph) pairReferences(filter Filter, shares map[sketchKey]float64, resolve func(string) string,
	uses map[reference]referenceUse) map[sketchKey]bool {
	paired := map[sketchKey]bool{}
	for p, sum := range g.windowPairs(filter) {
		a, b := sketchKey{p.service, p.Columns[0]}, sketchKey{p.service, p.Columns[1]}
		shareA, namedA := shares[a]
		shareB, namedB := shares[b]
		from, to, fields, ok := orient(p.ValuePair, resolve)
		if !namedA || !namedB || !ok {
			continue
		}
		paired[a], paired[b] = true, true
		if from != to {
			use(uses, reference{from, to, p.service, fields}, sum.lines, min(shareA, shareB), sum.lastBin)
		}
	}
	return paired
}

// servicePair is one service's lines of one value pair.
type servicePair struct {
	service string
	segment.ValuePair
}

// windowPairs sums each service's lines of each value pair over the window's bins.
func (g *graph) windowPairs(filter Filter) map[servicePair]referenceUse {
	sums := map[servicePair]referenceUse{}
	for bin, byService := range g.pairs {
		if !filter.inWindow(bin) {
			continue
		}
		for service, bySegment := range byService {
			for _, pairs := range bySegment {
				for p, n := range pairs {
					key := servicePair{service, p}
					sums[key] = referenceUse{lines: sums[key].lines + n, lastBin: max(sums[key].lastBin, bin)}
				}
			}
		}
	}
	return sums
}

// orient is the caller and callee a value pair names, its caller's column first, when one
// column's name says it is the source and the other's the destination.
func orient(p segment.ValuePair, resolve func(string) string) (from, to string, fields [2]string, ok bool) {
	a, b := endpointRole(p.Columns[0]), endpointRole(p.Columns[1])
	i, j := 0, 1
	switch {
	case a < 0 && b > 0:
	case a > 0 && b < 0:
		i, j = 1, 0
	default:
		return "", "", fields, false
	}
	from, to = resolve(p.Values[i]), resolve(p.Values[j])
	return from, to, [2]string{p.Columns[i], p.Columns[j]}, from != "" && to != ""
}

// sourceWords and destinationWords are the words of a column name saying which end of a
// call it names, as src.workload and upstream_cluster do.
var (
	sourceWords      = []string{"src", "source", "client", "downstream", "from", "caller", "origin"}
	destinationWords = []string{"dst", "dest", "destination", "upstream", "server", "backend", "target", "to", "callee", "peer"}
)

// endpointRole is -1 for a column naming a call's source, 1 for its destination and 0 when
// its name says neither or both.
func endpointRole(column string) int {
	role := 0
	for _, word := range strings.FieldsFunc(strings.ToLower(column), func(r rune) bool { return r == '.' || r == '_' || r == '-' }) {
		switch {
		case slices.Contains(sourceWords, word):
			role--
		case slices.Contains(destinationWords, word):
			role++
		}
	}
	return cmp.Compare(role, 0)
}

// use folds one reference's lines in, keeping its latest bin and lowest confidence.
func use(uses map[reference]referenceUse, r reference, lines uint64, confidence float64, bin int64) {
	u, seen := uses[r]
	if !seen || confidence < u.confidence {
		u.confidence = confidence
	}
	u.lines += lines
	u.lastBin = max(u.lastBin, bin)
	uses[r] = u
}

// edgesOfReferences is an edge per caller and callee, weighted by its heaviest reference.
func edgesOfReferences(uses map[reference]referenceUse, filter Filter, minWeight float64) []Edge {
	scope := make(map[string]struct{}, len(filter.Nodes))
	for _, s := range filter.Nodes {
		scope[s] = struct{}{}
	}
	byEnds := map[[2]string]*Edge{}
	for r, u := range uses {
		if len(scope) > 0 && !inScope(scope, r.from, r.to) {
			continue
		}
		e := byEnds[[2]string{r.from, r.to}]
		if e == nil {
			e = &Edge{From: r.from, To: r.to, Method: EdgeReference}
			byEnds[[2]string{r.from, r.to}] = e
		}
		e.Weight = max(e.Weight, float64(u.lines))
		e.Confidence = max(e.Confidence, u.confidence)
		e.LastBin = max(e.LastBin, u.lastBin)
		e.Fields = append(e.Fields, r.fields)
	}
	var edges []Edge
	for _, e := range byEnds {
		if e.Weight < minWeight {
			continue
		}
		e.Fields = slices.Compact(slices.SortedFunc(slices.Values(e.Fields), func(a, b [2]string) int {
			return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1]))
		}))
		edges = append(edges, *e)
	}
	return edges
}

// callee is the service a column's value names as one end of a call: an address's, or a
// bare name's in a column whose name says which end it is, since a line naming a pod
// ("pod add data-warehouse-5") is about it, not a call to it.
func callee(column, v string, resolve func(string) string) string {
	if endpointRole(column) == 0 && !strings.ContainsAny(strings.Trim(v, `"'[]()<>`), ":.|") {
		return ""
	}
	return resolve(v)
}

// resolver names the service an endpoint value names, remembering each value's answer.
func (g *graph) resolver() func(string) string {
	known := func(name string) bool {
		_, node := g.nodes[name]
		return g.services[name] && node
	}
	seen := map[string]string{}
	return func(v string) string {
		service, ok := seen[v]
		if !ok {
			service = resolveEndpoint(v, known)
			seen[v] = service
		}
		return service
	}
}

// resolveEndpoint is the service known holds that an endpoint value names, empty for none:
// the value without quotes, a cluster's outbound|port|| prefix, a scheme, a path or a port,
// then its first DNS label, then the workload of a pod by that name.
func resolveEndpoint(v string, known func(string) bool) string {
	v = strings.Trim(v, `"'[]()<>`)
	if i := strings.LastIndexByte(v, '|'); i >= 0 {
		v = v[i+1:]
	}
	if _, rest, ok := strings.Cut(v, "://"); ok {
		v = rest
	}
	v, _, _ = strings.Cut(v, "/")
	if host, _, err := net.SplitHostPort(v); err == nil {
		v = host
	}
	if v == "" || net.ParseIP(v) != nil {
		return ""
	}
	label, _, _ := strings.Cut(v, ".")
	for _, name := range []string{v, label, identity.PodService(label, "")} {
		if known(name) {
			return name
		}
	}
	return ""
}
