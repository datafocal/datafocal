package tools

import (
	"cmp"
	"context"
	"maps"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/segment"
	"github.com/datafocal/datafocal/internal/selector"
)

// ResourceUsageRequest is the wire request for resource_usage.
type ResourceUsageRequest struct {
	Selector    selector.Selector `json:"selector" jsonschema:"the window, and predicates narrowing the samples such as service=checkout or pod=<name>"`
	GroupBy     string            `json:"group_by,omitempty" jsonschema:"service|pod|container; defaults to service"`
	Bins        string            `json:"bins,omitempty" jsonschema:"<n>s|<n>m|<n>h|<n>d of at least 15s, or auto, the default; rates need two bins, so not all"`
	SummaryOnly bool              `json:"summary_only,omitempty" jsonschema:"omit the per-bin series, keeping each group's summary"`
	Detail      string            `json:"detail,omitempty" jsonschema:"cpu|memory|network: add each use's parts of that resource: CPU's user and system time, memory's rss, cache, swap and mapped files, the network's errors each way"`
	Snapshot    string            `json:"snapshot,omitempty"`
}

// ResourceUsageResponse is each host's own use, from its root cgroup, and each group's, on
// bins starting at Start, Step ms wide.
type ResourceUsageResponse struct {
	Start  int64         `json:"start"`
	Step   int64         `json:"step"`
	Hosts  []ResourceUse `json:"hosts"`
	Groups []ResourceUse `json:"groups"`
	// Total is the groups summed, the selector's whole use apart from the hosts' own; with no
	// groups, as a host running no containers sampled alone, the hosts' own summed.
	Total ResourceUse `json:"total"`
	// Columns are the sample columns the window holds; a resourceStat whose column it lacks is null.
	Columns []string `json:"columns"`
	// Capped is set when more hosts sent samples than are read, the busiest first.
	Capped bool `json:"capped,omitempty"`
}

// ResourceUse is one host's or group's use per bin, null where it has no sample: CPU in
// cores, the working set and disk used in bytes, and the network in bytes a second.
type ResourceUse struct {
	Key           string          `json:"key"`
	Summary       ResourceSummary `json:"summary"`
	CPUCores      []*float64      `json:"cpu_cores,omitempty" tstype:"(number | null)[]"`
	MemoryBytes   []*float64      `json:"memory_bytes,omitempty" tstype:"(number | null)[]"`
	RxBytesPerSec []*float64      `json:"rx_bytes_per_sec,omitempty" tstype:"(number | null)[]"`
	TxBytesPerSec []*float64      `json:"tx_bytes_per_sec,omitempty" tstype:"(number | null)[]"`
	DiskBytes     []*float64      `json:"disk_bytes,omitempty" tstype:"(number | null)[]"`
	// Parts are the request's detail, one per part the samples carry, in detailParts' order.
	Parts []ResourcePart `json:"parts,omitempty"`
}

// ResourcePart is a part of a use the request's detail names: CPU in cores, memory in
// bytes, errors a second; its mean and peak over the window and, unless summary_only, its
// per-bin series.
type ResourcePart struct {
	Name   string     `json:"name"`
	Mean   *float64   `json:"mean" tstype:"number | null"`
	Peak   *float64   `json:"peak" tstype:"number | null"`
	Values []*float64 `json:"values,omitempty" tstype:"(number | null)[]"`
}

// ResourceSummary is a use over the window: the mean of its bins and its peak bin, and the
// last disk used; null where it has no sample.
type ResourceSummary struct {
	CPUCoresMean      *float64 `json:"cpu_cores_mean" tstype:"number | null"`
	CPUCoresPeak      *float64 `json:"cpu_cores_peak" tstype:"number | null"`
	MemoryBytesMean   *float64 `json:"memory_bytes_mean" tstype:"number | null"`
	MemoryBytesPeak   *float64 `json:"memory_bytes_peak" tstype:"number | null"`
	RxBytesPerSecMean *float64 `json:"rx_bytes_per_sec_mean" tstype:"number | null"`
	TxBytesPerSecMean *float64 `json:"tx_bytes_per_sec_mean" tstype:"number | null"`
	DiskBytesLast     *float64 `json:"disk_bytes_last" tstype:"number | null"`
}

const (
	// colContainerID names the container a sample measures, which identity names the pod of.
	colContainerID = "container.id"
	// hostCgroup is the root cgroup, whose samples are the whole host's.
	hostCgroup = "/"
	// maxResourceHosts bounds the reads one call makes, one per host.
	maxResourceHosts = 20
	// maxHostContainers bounds the containers one host's read returns.
	maxHostContainers = 10_000
	// measureCount counts a host's or a level's samples; binsAll is one bin over the window.
	measureCount = "count"
	binsAll      = "all"
	// totalKey keys the containers' use summed.
	totalKey = "total"
	// sandbox names a pod's sandbox, the one container of it carrying no name and, under
	// containerd, the pod's network.
	sandbox = "sandbox"
)

// resourceStat is one use resource_usage reads: the cAdvisor column, whether it is a running total
// turned into a rate, and the scale from the column's unit.
type resourceStat struct {
	column string
	total  bool
	scale  float64
}

// The resourceStats, in ResourceUse's order: cAdvisor counts CPU time in nanoseconds.
var resourceStats = []resourceStat{
	{column: "cpu_usage_total", total: true, scale: 1e-9},
	{column: "memory_working_set", scale: 1},
	{column: "rx_bytes", total: true, scale: 1},
	{column: "tx_bytes", total: true, scale: 1},
	{column: "fs_summary.fs_usage", scale: 1},
}

// resourcePart is a part of a use a detail adds: its name and the stat it reads.
type resourcePart struct {
	name string
	stat resourceStat
}

// detailParts are each detail's parts, as cAdvisor's stdout driver writes them.
var detailParts = map[string][]resourcePart{
	"cpu": {
		{name: "user", stat: resourceStat{column: "cpu_usage_user", total: true, scale: 1e-9}},
		{name: "system", stat: resourceStat{column: "cpu_usage_system", total: true, scale: 1e-9}},
	},
	"memory": {
		{name: "rss", stat: resourceStat{column: "memory_rss", scale: 1}},
		{name: "cache", stat: resourceStat{column: "memory_cache", scale: 1}},
		{name: "swap", stat: resourceStat{column: "memory_swap", scale: 1}},
		{name: "mapped_file", stat: resourceStat{column: "memory_mapped_file", scale: 1}},
	},
	"network": {
		{name: "rx_errors", stat: resourceStat{column: "rx_errors", total: true, scale: 1}},
		{name: "tx_errors", stat: resourceStat{column: "tx_errors", total: true, scale: 1}},
	},
}

// resourceLabels are the string columns naming a sample's group, read as each container's last.
var resourceLabels = []string{segment.ColService, segment.ColPod, segment.ColContainer}

// ResourceUsage reads the window's resource samples a host at a time, as a container name
// such as a systemd unit or the root cgroup is on every host, and sums each host's
// containers into the groups.
func (t toolSet) ResourceUsage(ctx context.Context, req ResourceUsageRequest) (query.Envelope[ResourceUsageResponse], error) {
	var zero query.Envelope[ResourceUsageResponse]
	if err := validateResourceUsage(req); err != nil {
		return zero, err
	}
	sampled := append(slices.Clip(req.Selector.Where), selector.Predicate{Field: colContainerID, Op: selector.OpExists})
	schema, err := t.DescribeSchema(ctx, FieldsRequest{Selector: selector.Selector{Time: req.Selector.Time, Where: sampled}, Snapshot: req.Snapshot})
	if err != nil {
		return zero, err
	}
	// The schema's distinct counts are sketched, but no number this answers reads them.
	meta := schema.Meta
	meta.Approx = false
	present := map[string]bool{}
	for _, c := range schema.Data.Columns {
		present[c.Name] = true
	}
	resp := ResourceUsageResponse{Hosts: []ResourceUse{}, Groups: []ResourceUse{}, Total: ResourceUse{Key: totalKey}, Columns: []string{}}
	if !present[colContainerID] {
		return usageEnvelope(resp, meta), nil
	}
	read := resourceRead{req: req, present: present}
	resp.Columns = read.columns()
	hosts, err := t.busiestHosts(ctx, &meta, selector.Selector{Time: req.Selector.Time, Where: sampled})
	if err != nil {
		return zero, err
	}
	resp.Capped = len(hosts) > maxResourceHosts
	var uses []containerUse
	for _, host := range hosts[:min(len(hosts), maxResourceHosts)] {
		host, err := t.hostUses(ctx, &meta, read, host)
		if err != nil {
			return zero, err
		}
		uses = append(uses, host...)
	}
	resp.Start, resp.Step = grid(uses)
	resp.Hosts, resp.Groups, resp.Total = rollUp(uses, resp.Start, resp.Step, read)
	return usageEnvelope(resp, meta), nil
}

func validateResourceUsage(req ResourceUsageRequest) error {
	if !slices.Contains([]string{"", segment.ColService, segment.ColPod, segment.ColContainer}, req.GroupBy) {
		return query.Invalidf("resource_usage: group_by is service, pod or container, got %q", req.GroupBy)
	}
	if _, ok := detailParts[req.Detail]; req.Detail != "" && !ok {
		return query.Invalidf("resource_usage: detail is cpu, memory or network, got %q", req.Detail)
	}
	if req.Bins == binsAll {
		return query.Invalidf("resource_usage: bins=all has one bin, and a rate needs two")
	}
	return nil
}

func usageEnvelope(resp ResourceUsageResponse, meta query.Meta) query.Envelope[ResourceUsageResponse] {
	meta.Tokens = query.EstimateTokens(resp)
	return query.Envelope[ResourceUsageResponse]{Data: resp, Meta: meta}
}

// columns are the label and stat columns the window holds, the detail's parts' after.
func (r resourceRead) columns() []string {
	cols := []string{}
	for _, l := range resourceLabels {
		if r.present[l] {
			cols = append(cols, l)
		}
	}
	for _, s := range r.stats() {
		if r.present[s.column] {
			cols = append(cols, s.column)
		}
	}
	return cols
}

// fold adds a child call's cost and coverage to meta, at the snapshot meta pins.
func foldMeta(meta *query.Meta, child query.Meta) {
	meta.Cost = addCost(meta.Cost, child.Cost)
	meta.Coverage = min(meta.Coverage, child.Coverage)
	meta.Approx = meta.Approx || child.Approx
}

// busiestHosts are the hosts sending the most samples, one more than are read so a cap shows.
func (t toolSet) busiestHosts(ctx context.Context, meta *query.Meta, sel selector.Selector) ([]string, error) {
	env, err := t.Aggregate(ctx, AggregateRequest{Selector: sel, Measure: measureCount, GroupBy: segment.ColHost,
		Bins: binsAll, Limit: maxResourceHosts + 1, Snapshot: meta.Snapshot})
	if err != nil {
		return nil, err
	}
	foldMeta(meta, env.Meta)
	var hosts []string
	for _, s := range env.Data.Series {
		if s.Key != "" {
			hosts = append(hosts, s.Key)
		}
	}
	return hosts, nil
}

// resourceRead is what each host's read asks: the request and the columns it may measure.
type resourceRead struct {
	req     ResourceUsageRequest
	present map[string]bool
}

// parts are the detail's parts the window's samples carry.
func (r resourceRead) parts() []resourcePart {
	var parts []resourcePart
	for _, p := range detailParts[r.req.Detail] {
		if r.present[p.stat.column] {
			parts = append(parts, p)
		}
	}
	return parts
}

// stats are what the read measures: the resourceStats, then the detail's parts.
func (r resourceRead) stats() []resourceStat {
	stats := slices.Clone(resourceStats)
	for _, p := range r.parts() {
		stats = append(stats, p.stat)
	}
	return stats
}

// measures are the read's label and resourceStat measures: a running total's last value and
// the time it was sampled at, for a rate between samples however they fall in the bins, and
// a level's sum and sample count, for a mean per bin. at gives each column's index, -1 when absent.
func (r resourceRead) measures() ([]selector.MeasureSpec, func(string) int) {
	var specs []selector.MeasureSpec
	index := map[string]int{}
	add := func(key string, spec selector.MeasureSpec) {
		index[key] = len(specs)
		specs = append(specs, spec)
	}
	for _, l := range resourceLabels {
		if r.present[l] {
			add(l, selector.MeasureSpec{Measure: "last:" + l})
		}
	}
	for _, s := range r.stats() {
		switch {
		case !r.present[s.column]:
		case s.total:
			add(s.column, selector.MeasureSpec{Measure: "last:" + s.column})
			add(s.column+"@", selector.MeasureSpec{Measure: "last:" + segment.ColTimestamp, Where: []selector.Predicate{{Field: s.column, Op: selector.OpExists}}})
			add(s.column+"<", selector.MeasureSpec{Measure: "first:" + s.column})
			add(s.column+"~", selector.MeasureSpec{Measure: "span:" + s.column})
		default:
			add(s.column, selector.MeasureSpec{Measure: "sum:" + s.column})
			add(s.column+"#", selector.MeasureSpec{Measure: measureCount, Where: []selector.Predicate{{Field: s.column, Op: selector.OpExists}}})
		}
	}
	return specs, func(key string) int {
		if i, ok := index[key]; ok {
			return i
		}
		return -1
	}
}

// containerUse is one container's samples on one host, per bin of its series.
type containerUse struct {
	host, id, service, pod, container string
	start, step                       int64
	values                            [][]*float64 // one per stat the read measures, nil when absent
}

func (t toolSet) hostUses(ctx context.Context, meta *query.Meta, r resourceRead, host string) ([]containerUse, error) {
	specs, at := r.measures()
	where := append([]selector.Predicate{{Field: segment.ColHost, Op: selector.OpEq, Value: host}}, r.req.Selector.Where...)
	env, err := t.Aggregate(ctx, AggregateRequest{
		Selector: selector.Selector{Time: r.req.Selector.Time, Where: where},
		GroupBy:  colContainerID, Bins: cmp.Or(r.req.Bins, "auto"), Limit: maxHostContainers,
		Measures: specs, Snapshot: meta.Snapshot,
	})
	if err != nil {
		return nil, err
	}
	foldMeta(meta, env.Meta)
	var uses []containerUse
	for _, s := range env.Data.Series {
		if s.Key != "" {
			uses = append(uses, containerOf(host, s, at, r.stats()))
		}
	}
	return uses, nil
}

func containerOf(host string, s Series, at func(string) int, stats []resourceStat) containerUse {
	label := func(name string) string {
		if i := at(name); i >= 0 && i < len(s.MeasureLabels) {
			return lastLabel(s.MeasureLabels[i])
		}
		return ""
	}
	u := containerUse{host: host, id: s.Key, service: label(segment.ColService), pod: label(segment.ColPod),
		container: label(segment.ColContainer), start: s.Start, step: s.Step, values: make([][]*float64, len(stats))}
	column := func(i int) []*float64 {
		if i < 0 || i >= len(s.MeasureValues) {
			return nil
		}
		return s.MeasureValues[i]
	}
	for i, st := range stats {
		switch {
		case st.total:
			u.values[i] = rates(totals{last: column(at(st.column)), at: column(at(st.column + "@")),
				first: column(at(st.column + "<")), span: column(at(st.column + "~"))}, st.scale)
		default:
			u.values[i] = heldLevels(means(column(at(st.column)), column(at(st.column+"#"))))
		}
	}
	return u
}

// lastLabel is a container's last label in the window, the one its latest bin names.
func lastLabel(bins []string) string {
	for i := len(bins) - 1; i >= 0; i-- {
		if bins[i] != "" {
			return bins[i]
		}
	}
	return ""
}

// totals are a running total's samples per bin: the last and the time it was taken at, in
// ms, and the first and the ms from it to the last.
type totals struct {
	last, at, first, span []*float64
}

// rates are a running total's increase a second, times scale, from each sample to the next
// by the times they were taken at, laid on every bin after the first up to the second: a
// sampler's interval need not match the bins, so some bins hold no sample. The first bin
// with samples, having none before it, rates from its first sample to its last, so a
// container living within one bin has a rate too; with one sample it has none. A fall is
// a restart and has none.
func rates(t totals, scale float64) []*float64 {
	if t.last == nil {
		return nil
	}
	out := make([]*float64, len(t.last))
	prev := -1
	for i, v := range t.last {
		if v == nil || i >= len(t.at) || t.at[i] == nil {
			continue
		}
		switch {
		case prev >= 0 && *v >= *t.last[prev] && *t.at[i] > *t.at[prev]:
			r := (*v - *t.last[prev]) / ((*t.at[i] - *t.at[prev]) / 1000) * scale
			for j := prev + 1; j <= i; j++ {
				out[j] = ptr(r)
			}
		case prev < 0:
			out[i] = withinBin(*v, at(t.first, i), at(t.span, i), scale)
		}
		prev = i
	}
	return out
}

// withinBin is the rate from a bin's first sample to its last, span ms apart; none without two.
func withinBin(last float64, first, span *float64, scale float64) *float64 {
	if first == nil || span == nil || *span <= 0 || last < *first {
		return nil
	}
	return ptr((last - *first) / (*span / 1000) * scale)
}

func at(bins []*float64, i int) *float64 {
	if i < len(bins) {
		return bins[i]
	}
	return nil
}

// heldLevels is levels with each bin between two samples holding the earlier one, so containers
// sampled in different bins still sum to the whole of their use in every bin.
func heldLevels(levels []*float64) []*float64 {
	prev := -1
	for i, v := range levels {
		if v == nil {
			continue
		}
		for j := prev + 1; prev >= 0 && j < i; j++ {
			levels[j] = levels[prev]
		}
		prev = i
	}
	return levels
}

// means are each bin's sum over its sample count.
func means(sums, counts []*float64) []*float64 {
	if sums == nil {
		return nil
	}
	out := make([]*float64, len(sums))
	for i, v := range sums {
		if v != nil && i < len(counts) && counts[i] != nil && *counts[i] > 0 {
			out[i] = ptr(*v / *counts[i])
		}
	}
	return out
}

func ptr(v float64) *float64 { return &v }

// grid is the bins every host's series lie on: the earliest start, at the first step.
func grid(uses []containerUse) (start, step int64) {
	for i, u := range uses {
		if i == 0 || u.start < start {
			start = u.start
		}
		step = cmp.Or(step, u.step)
	}
	return start, step
}

// groupOf is the group a container's use sums into, its id where it lacks the label:
// a pod's unnamed container is the pod's sandbox.
func groupOf(u containerUse, by string) string {
	switch by {
	case segment.ColPod:
		return cmp.Or(u.pod, u.id)
	case segment.ColContainer:
		if u.container == "" && u.pod != "" {
			return sandbox
		}
		return cmp.Or(u.container, u.id)
	default:
		return cmp.Or(u.service, u.id)
	}
}

// containerScope is a runtime's cgroup for a container, which a sampler names after the
// container rather than its path: what nests in it, as a kind node's own cgroups, is the
// container's, counted already by the container and by any sampler running inside it.
var containerScope = regexp.MustCompile(`(?:^|/)(?:docker-|cri-containerd-|libpod-|crio-)?[0-9a-f]{64}(?:\.scope)?/`)

// innerCgroups are each host's cgroups that hold others the window sampled, every systemd
// slice, which holds only units, and every cgroup inside a container's: their use is counted
// already. A runtime's name for a container, such as Docker's, is no path and holds nothing.
func innerCgroups(uses []containerUse) func(containerUse) bool {
	parents := map[[2]string]bool{}
	for _, u := range uses {
		if !strings.HasPrefix(u.id, "/") {
			continue
		}
		for p := path.Dir(u.id); p != hostCgroup; p = path.Dir(p) {
			parents[[2]string{u.host, p}] = true
		}
	}
	return func(u containerUse) bool {
		return u.id != hostCgroup && strings.HasPrefix(u.id, "/") &&
			(strings.HasSuffix(u.id, ".slice") || parents[[2]string{u.host, u.id}] || containerScope.MatchString(u.id))
	}
}

// rollUp sums the hosts' root cgroups by host and the rest by group, each in key order,
// and the rest again into their total, or with none, the hosts'. Two kinds of container are
// left out, as what they hold is counted already: one named after another host sampled in
// the window, as a kind node is a docker container of the machine running it, which its own
// samples count; and a cgroup holding others, as a systemd slice holds its units when a
// sampler watches every cgroup.
func rollUp(uses []containerUse, start, step int64, r resourceRead) (hosts, groups []ResourceUse, total ResourceUse) {
	req, parts := r.req, r.parts()
	byHost, byGroup := map[string]*ResourceUse{}, map[string]*ResourceUse{}
	all := map[string]*ResourceUse{totalKey: newUse(totalKey, parts)}
	hostsAll := newUse(totalKey, parts)
	sampled := map[string]bool{}
	for _, u := range uses {
		sampled[u.host] = true
	}
	inner := innerCgroups(uses)
	for _, u := range uses {
		if (u.id != u.host && sampled[u.id]) || inner(u) {
			continue
		}
		into, key, sum := byGroup, groupOf(u, cmp.Or(req.GroupBy, segment.ColService)), all[totalKey]
		if u.id == hostCgroup {
			into, key, sum = byHost, u.host, hostsAll
		}
		addUse(sum, u, start, step)
		if into[key] == nil {
			into[key] = newUse(key, parts)
		}
		addUse(into[key], u, start, step)
	}
	if len(byGroup) == 0 && len(byHost) > 0 {
		all[totalKey] = hostsAll
	}
	return finish(byHost, req.SummaryOnly), finish(byGroup, req.SummaryOnly), finish(all, req.SummaryOnly)[0]
}

// newUse is an empty use keyed key, with a part named for each of parts.
func newUse(key string, parts []resourcePart) *ResourceUse {
	u := &ResourceUse{Key: key}
	for _, p := range parts {
		u.Parts = append(u.Parts, ResourcePart{Name: p.name})
	}
	return u
}

// addUse adds u's bins to sum's, laid on the grid at start: a stat neither has stays nil.
func addUse(sum *ResourceUse, u containerUse, start, step int64) {
	offset := 0
	if step > 0 {
		offset = int((u.start - start) / step)
	}
	for i, field := range sum.fields() {
		*field = addBins(*field, u.values[i], offset)
	}
}

// fields are sum's series in the order the read measures them: resourceStats', then its parts'.
func (r *ResourceUse) fields() []*[]*float64 {
	fields := []*[]*float64{&r.CPUCores, &r.MemoryBytes, &r.RxBytesPerSec, &r.TxBytesPerSec, &r.DiskBytes}
	for i := range r.Parts {
		fields = append(fields, &r.Parts[i].Values)
	}
	return fields
}

func addBins(sum, add []*float64, offset int) []*float64 {
	if add == nil {
		return sum
	}
	if n := offset + len(add); len(sum) < n {
		sum = append(sum, make([]*float64, n-len(sum))...)
	}
	for i, v := range add {
		switch {
		case v == nil:
		case sum[offset+i] == nil:
			sum[offset+i] = ptr(*v)
		default:
			*sum[offset+i] += *v
		}
	}
	return sum
}

// finish summarizes each use, dropping its series for a summary only, in key order.
func finish(uses map[string]*ResourceUse, summaryOnly bool) []ResourceUse {
	out := make([]ResourceUse, 0, len(uses))
	for _, key := range slices.Sorted(maps.Keys(uses)) {
		u := *uses[key]
		u.Summary = ResourceSummary{
			CPUCoresMean: mean(u.CPUCores), CPUCoresPeak: peak(u.CPUCores),
			MemoryBytesMean: mean(u.MemoryBytes), MemoryBytesPeak: peak(u.MemoryBytes),
			RxBytesPerSecMean: mean(u.RxBytesPerSec), TxBytesPerSecMean: mean(u.TxBytesPerSec),
			DiskBytesLast: last(u.DiskBytes),
		}
		u.Parts = slices.Clone(u.Parts)
		for i, p := range u.Parts {
			u.Parts[i] = ResourcePart{Name: p.Name, Mean: mean(p.Values), Peak: peak(p.Values), Values: p.Values}
			if summaryOnly {
				u.Parts[i].Values = nil
			}
		}
		if summaryOnly {
			u = ResourceUse{Key: u.Key, Summary: u.Summary, Parts: u.Parts}
		}
		out = append(out, u)
	}
	return out
}

func mean(bins []*float64) *float64 {
	var sum float64
	n := 0
	for _, v := range bins {
		if v != nil {
			sum += *v
			n++
		}
	}
	if n == 0 {
		return nil
	}
	return ptr(sum / float64(n))
}

func peak(bins []*float64) *float64 {
	var top *float64
	for _, v := range bins {
		if v != nil && (top == nil || *v > *top) {
			top = v
		}
	}
	return top
}

func last(bins []*float64) *float64 {
	for i := len(bins) - 1; i >= 0; i-- {
		if bins[i] != nil {
			return bins[i]
		}
	}
	return nil
}
