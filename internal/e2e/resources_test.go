package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/datafocal/datafocal/internal/catalog"
	"github.com/datafocal/datafocal/internal/collector"
	"github.com/datafocal/datafocal/internal/ingest"
	"github.com/datafocal/datafocal/internal/logformat"
	"github.com/datafocal/datafocal/internal/querier"
	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
	"github.com/datafocal/datafocal/internal/selector"
	"github.com/datafocal/datafocal/internal/storage"
)

// sampled is a container cAdvisor samples and the use it is given: cores, a working set in
// MiB that from spikeAt on rises by spikeMiB, network bytes a second each way, and disk MiB.
type sampled struct {
	host, name               string
	cores, mib, spikeMiB, in float64
	out, diskMiB             float64
	spikeAt                  int
}

// resourceContainers run on two nodes, kube-proxy on both, so a container's samples are only
// a series per host; checkout's working set grows by 900 MiB from 14:05 of the incident.
// node-2 is a container of node-1, as a kind node is of the machine running it, which
// node-2's own samples already count. Two edge devices run no containers: device-1's
// sampler watches every cgroup, so its slices and user@1000.service hold the units sampled
// under them, and a cgroup inside a container's, as a kind node's pods, which the container
// counts; device-2's watches containers only, so it sends its root alone.
var resourceContainers = []sampled{
	{host: "node-1", name: "/", cores: 3.5, mib: 6144, in: 9e5, out: 6e5, diskMiB: 40960},
	{host: "node-1", name: "gateway", cores: 0.5, mib: 256, in: 4e5, out: 3e5, diskMiB: 120},
	{host: "node-1", name: "checkout", cores: 1.25, mib: 512, spikeMiB: 900, spikeAt: 15, in: 2e5, out: 1e5, diskMiB: 80},
	{host: "node-1", name: "kube-proxy", cores: 0.05, mib: 32, in: 1e3, out: 1e3, diskMiB: 10},
	{host: "node-1", name: "node-2", cores: 3.1, mib: 6200, in: 8e5, out: 6e5, diskMiB: 24600},
	{host: "node-2", name: "/", cores: 2, mib: 4096, in: 5e5, out: 4e5, diskMiB: 20480},
	{host: "node-2", name: "db", cores: 1, mib: 2048, in: 3e5, out: 2e5, diskMiB: 4096},
	{host: "node-2", name: "kube-proxy", cores: 0.05, mib: 32, in: 1e3, out: 1e3, diskMiB: 10},
	{host: "device-1", name: "/", cores: 1, mib: 2048, in: 2e3, out: 1e3, diskMiB: 8192},
	{host: "device-1", name: "/system.slice", cores: 0.2, mib: 200},
	{host: "device-1", name: "/system.slice/sshd.service", cores: 0.2, mib: 200},
	{host: "device-1", name: "/user.slice", cores: 0.6, mib: 1024},
	{host: "device-1", name: "/user.slice/user-1000.slice/user@1000.service", cores: 0.6, mib: 1024},
	{host: "device-1", name: "/user.slice/user-1000.slice/user@1000.service/app.slice/app-gnome-firefox-4211.scope", cores: 0.6, mib: 1024},
	{host: "device-1", name: "/system.slice/docker-" + strings.Repeat("8c", 32) + ".scope/kubelet.slice/kubelet-kubepods.slice/cri-containerd-" + strings.Repeat("9d", 32) + ".scope", cores: 5, mib: 4096},
	{host: "device-2", name: "/", cores: 0.25, mib: 512, in: 1e3, out: 1e3, diskMiB: 1024},
}

// cadvisorLines are host's containers sampled every 20s over the incident's ten minutes, as
// cAdvisor's stdout driver writes them: housekeeping takes longer than its 15s interval, so
// a 15s bin can hold no sample.
// Each spends four fifths of its CPU in user time, and three quarters of its working set is rss.
func cadvisorLines(host string) string {
	start := time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC)
	var b strings.Builder
	for i := range 31 {
		at := start.Add(time.Duration(i) * 20 * time.Second)
		for _, c := range resourceContainers {
			if c.host != host {
				continue
			}
			mib := c.mib
			if c.spikeMiB > 0 && i >= c.spikeAt {
				mib += c.spikeMiB
			}
			secs := float64(i * 20)
			fmt.Fprintf(&b, "cName=%s host=localhost:8086 timestamp=%d cpu_usage_total=%.0f cpu_usage_user=%.0f cpu_usage_system=%.0f memory_working_set=%.0f memory_rss=%.0f memory_cache=%.0f rx_bytes=%.0f tx_bytes=%.0f fs_summary.fs_usage=%.0f\n",
				c.name, at.UnixNano(), c.cores*secs*1e9, c.cores*secs*0.8e9, c.cores*secs*0.2e9, mib*(1<<20), mib*0.75*(1<<20), mib*0.25*(1<<20),
				c.in*secs, c.out*secs, c.diskMiB*(1<<20))
		}
	}
	return b.String()
}

// ingestResourceFixtures collects each node's cAdvisor samples through its own collector, as
// the DaemonSets do, into one store.
func ingestResourceFixtures(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewFS(dir)
	for _, host := range []string{"node-1", "node-2", "device-1", "device-2"} {
		logs := filepath.Join(dir, "logs", host, "cadvisor.log")
		require.NoError(t, os.MkdirAll(filepath.Dir(logs), 0o755))
		require.NoError(t, os.WriteFile(logs, []byte(cadvisorLines(host)), 0o644))
		cfg := collector.DefaultConfig()
		cfg.Tenant = tenant
		c, err := collector.New(cfg, collector.Dependencies{Store: store, CatalogWriter: catalog.New(store, tenant), Clock: fixedClock{}})
		require.NoError(t, err)
		in, err := ingest.New(ingest.Config{
			StateDir: filepath.Join(dir, "state", host), Host: host, FlushInterval: time.Hour,
			Sources: []ingest.Source{{Path: logs, Parse: logformat.Config{Format: "cadvisor"}}},
		}, c)
		require.NoError(t, err)
		require.NoError(t, in.Drain(context.Background()))
	}
	return dir
}

// assertResourceUsage: the seeded corpus logs no resource samples, which is an empty
// answer naming no column, not an error.
func assertResourceUsage(t *testing.T, h *toolHarness) any {
	t.Helper()
	env := postTool[tools.ResourceUsageResponse](t, h, "resource_usage", tools.ResourceUsageRequest{Selector: incidentSelector()})
	assert.Equal(t, tools.ResourceUsageResponse{Hosts: []tools.ResourceUse{}, Groups: []tools.ResourceUse{},
		Total: tools.ResourceUse{Key: "total"}, Columns: []string{}}, env.Data)
	return env.Data
}

// rounded is a summary to six significant digits, as a rate's division leaves float noise.
func rounded(s tools.ResourceSummary) tools.ResourceSummary {
	r := func(v *float64) *float64 {
		if v == nil || *v == 0 {
			return v
		}
		scale := math.Pow(10, 5-math.Floor(math.Log10(math.Abs(*v))))
		x := math.Round(*v*scale) / scale
		return &x
	}
	return tools.ResourceSummary{CPUCoresMean: r(s.CPUCoresMean), CPUCoresPeak: r(s.CPUCoresPeak),
		MemoryBytesMean: r(s.MemoryBytesMean), MemoryBytesPeak: r(s.MemoryBytesPeak),
		RxBytesPerSecMean: r(s.RxBytesPerSecMean), TxBytesPerSecMean: r(s.TxBytesPerSecMean), DiskBytesLast: r(s.DiskBytesLast)}
}

func f(v float64) *float64 { return &v }

// roundedParts are parts' means and peaks rounded as rounded does.
func roundedParts(parts []tools.ResourcePart) []tools.ResourcePart {
	var out []tools.ResourcePart
	for _, p := range parts {
		s := rounded(tools.ResourceSummary{CPUCoresMean: p.Mean, CPUCoresPeak: p.Peak})
		out = append(out, tools.ResourcePart{Name: p.Name, Mean: s.CPUCoresMean, Peak: s.CPUCoresPeak, Values: p.Values})
	}
	return out
}

const mib = 1 << 20

// steady is a use at a fixed rate the whole window, as resourceContainers gives it.
func steady(cores, meanMiB, peakMiB, in, out, diskMiB float64) tools.ResourceSummary {
	return rounded(tools.ResourceSummary{CPUCoresMean: f(cores), CPUCoresPeak: f(cores), MemoryBytesMean: f(meanMiB * mib),
		MemoryBytesPeak: f(peakMiB * mib), RxBytesPerSecMean: f(in), TxBytesPerSecMean: f(out), DiskBytesLast: f(diskMiB * mib)})
}

// TestResourceUsage reads the hosts' samples back as the rates and levels
// resourceContainers wrote them at, in the bins between samples too, node-2's container
// on node-1 counted only as node-2 and device-1's cgroups only as the units holding no other:
// kube-proxy is both nodes' summed, and checkout's working set is 512 MiB for 20 bins and
// 1412 for the 19 up to its last sample, the last bin holding none.
func TestResourceUsage(t *testing.T) {
	store := storage.NewFS(ingestResourceFixtures(t))
	ts := httptest.NewServer(querier.Open(querier.Options{Catalog: catalog.New(store, tenant), Store: store}).Handler())
	t.Cleanup(ts.Close)
	window := selector.TimeSpec{From: "2026-07-18T14:00:00Z", To: "2026-07-18T14:10:00Z"}
	call := func(req tools.ResourceUsageRequest) (int, query.Envelope[tools.ResourceUsageResponse]) {
		body, err := json.Marshal(req)
		require.NoError(t, err)
		resp, err := http.Post(ts.URL+"/v1/resource_usage", "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer resp.Body.Close()
		var env query.Envelope[tools.ResourceUsageResponse]
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
		return resp.StatusCode, env
	}
	summaries := func(uses []tools.ResourceUse) map[string]tools.ResourceSummary {
		out := map[string]tools.ResourceSummary{}
		for _, u := range uses {
			out[u.Key] = rounded(u.Summary)
		}
		return out
	}

	const checkoutMiB = (20*512 + 19*1412) / 39.0
	status, env := call(tools.ResourceUsageRequest{Selector: selector.Selector{Time: window}})
	require.Equal(t, http.StatusOK, status)
	assert.Equal(t, map[string]tools.ResourceSummary{
		"checkout":      steady(1.25, checkoutMiB, 1412, 2e5, 1e5, 80),
		"db":            steady(1, 2048, 2048, 3e5, 2e5, 4096),
		"gateway":       steady(0.5, 256, 256, 4e5, 3e5, 120),
		"kube-proxy":    steady(0.1, 64, 64, 2e3, 2e3, 20),
		"sshd":          steady(0.2, 200, 200, 0, 0, 0),
		"gnome-firefox": steady(0.6, 1024, 1024, 0, 0, 0),
	}, summaries(env.Data.Groups))
	assert.Equal(t, map[string]tools.ResourceSummary{
		"node-1":   steady(3.5, 6144, 6144, 9e5, 6e5, 40960),
		"node-2":   steady(2, 4096, 4096, 5e5, 4e5, 20480),
		"device-1": steady(1, 2048, 2048, 2e3, 1e3, 8192),
		"device-2": steady(0.25, 512, 512, 1e3, 1e3, 1024),
	}, summaries(env.Data.Hosts))
	assert.Equal(t, steady(3.65, 3330-962+1224+checkoutMiB, 5004, 902e3, 602e3, 4316), rounded(env.Data.Total.Summary), "the containers summed, apart from the hosts")
	assert.Equal(t, []string{"service", "cpu_usage_total", "memory_working_set", "rx_bytes", "tx_bytes", "fs_summary.fs_usage"}, env.Data.Columns)
	first := env.Data.Groups[0]
	assert.Equal(t, [3]any{int64(1784383200000), int64(15000), 40}, [3]any{env.Data.Start, env.Data.Step, len(first.CPUCores)})
	assert.Nil(t, first.CPUCores[0], "the first bin has no bin before it to rate against")

	_, env = call(tools.ResourceUsageRequest{Selector: selector.Selector{Time: window, Where: []selector.Predicate{
		{Field: "service", Op: selector.OpEq, Value: "checkout"}}}, GroupBy: "container", SummaryOnly: true})
	assert.Equal(t, []tools.ResourceUse{{Key: "checkout", Summary: env.Data.Groups[0].Summary}}, env.Data.Groups)
	assert.Equal(t, []tools.ResourceUse{}, env.Data.Hosts)

	_, env = call(tools.ResourceUsageRequest{Selector: selector.Selector{Time: window, Where: []selector.Predicate{
		{Field: "host", Op: selector.OpEq, Value: "device-2"}}}, SummaryOnly: true})
	assert.Equal(t, []tools.ResourceUse{}, env.Data.Groups, "a host running no containers has its root alone")
	assert.Equal(t, steady(0.25, 512, 512, 1e3, 1e3, 1024), rounded(env.Data.Total.Summary), "with no groups, the total is the hosts'")

	_, env = call(tools.ResourceUsageRequest{Selector: selector.Selector{Time: window, Where: []selector.Predicate{
		{Field: "service", Op: selector.OpEq, Value: "db"}}}, Bins: "1h", SummaryOnly: true})
	assert.Equal(t, f(1), rounded(env.Data.Groups[0].Summary).CPUCoresMean, "a bin with no sample before it rates from its first sample to its last")

	db := []selector.Predicate{{Field: "service", Op: selector.OpEq, Value: "db"}}
	for detail, want := range map[string][]tools.ResourcePart{
		"cpu":     {{Name: "user", Mean: f(0.8), Peak: f(0.8)}, {Name: "system", Mean: f(0.2), Peak: f(0.2)}},
		"memory":  {{Name: "rss", Mean: f(1536 * mib), Peak: f(1536 * mib)}, {Name: "cache", Mean: f(512 * mib), Peak: f(512 * mib)}},
		"network": nil, // the samples carry no errors
	} {
		_, env = call(tools.ResourceUsageRequest{Selector: selector.Selector{Time: window, Where: db}, Detail: detail, SummaryOnly: true})
		assert.Equal(t, roundedParts(want), roundedParts(env.Data.Groups[0].Parts), "%s: the parts the samples carry, in the detail's order", detail)
	}

	for _, req := range []tools.ResourceUsageRequest{
		{Selector: selector.Selector{Time: window}, GroupBy: "namespace"},
		{Selector: selector.Selector{Time: window}, Bins: "all"},
		{Selector: selector.Selector{Time: window}, Detail: "disk"},
	} {
		status, _ := call(req)
		assert.Equal(t, http.StatusBadRequest, status, "%+v", req)
	}
}
