package seedcorpus

import "time"

// This file is the seeded corpus's ground-truth contract.
// Tests assert against these values rather than numbers copied out of the
// generator's internals, and the generator reads the same values where it can,
// so a change to the fixture is a change in one place.

// Tenant is the tenant every seeded stream is written under.
const Tenant = "acme"

// Window anchors for the two scenarios.
func IncidentStart() time.Time { return time.Date(2026, 7, 18, 14, 0, 0, 0, time.UTC) }
func HealthyStart() time.Time  { return time.Date(2026, 7, 18, 14, 48, 0, 0, time.UTC) }

const (
	IncidentDur = 600 // seconds: 14:00–14:10Z
	HealthyDur  = 720 // seconds: 14:48–15:00Z ("now")
)

// IncidentWindow is the time range of the incident scenario.
func IncidentWindow() (start, end time.Time) {
	start = IncidentStart()
	return start, start.Add(time.Duration(IncidentDur) * time.Second)
}

// HealthyWindow is the time range of the healthy scenario.
func HealthyWindow() (start, end time.Time) {
	start = HealthyStart()
	return start, start.Add(time.Duration(HealthyDur) * time.Second)
}

// Ramp is one of the incident's piecewise value ramps: a quantity moving from
// From to To across the seconds [StartSec, EndSec] measured from IncidentStart.
//
// The ramps are piecewise, not window-wide, and an assertion that treats one as
// spanning the whole window picks the wrong bins — DiskAwaitRamp is already
// pinned at its 180ms ceiling for the last 400 of the window's 600 seconds.
type Ramp struct {
	StartSec, EndSec int
	From, To         float64
}

// BinBefore is the index of the last bin that ends at or before the ramp starts:
// the newest bin still holding the pre-ramp baseline on a grid of this width.
func (r Ramp) BinBefore(width time.Duration) int {
	return int(time.Duration(r.StartSec)*time.Second/width) - 1
}

// BinAfter is the index of the first bin that begins at or after the ramp ends:
// the oldest bin holding the settled post-ramp value on a grid of this width.
func (r Ramp) BinAfter(width time.Duration) int {
	end := time.Duration(r.EndSec) * time.Second
	n := int(end / width)
	if end%width != 0 {
		n++
	}
	return n
}

// The incident's five ramps. Each names the generator function that emits it so
// the two stay in step.
var (
	// DiskAwaitRamp is host-7's disk await in ms (genHost).
	DiskAwaitRamp = Ramp{StartSec: 120, EndSec: 186, From: 2, To: 180}
	// DBWriteP95Ramp is db-3's write p95 in ms (genDB). It starts
	// RampOffsetDiskToDB after DiskAwaitRamp.
	DBWriteP95Ramp = Ramp{StartSec: 126, EndSec: 186, From: 40, To: 1200}
	// ErrorRateRamp is the user-create 5xx rate as a fraction (genEdge).
	ErrorRateRamp = Ramp{StartSec: 180, EndSec: 300, From: 0.001, To: 0.07}
	// QueueDepthRamp is the jobs queue depth (genJobs).
	QueueDepthRamp = Ramp{StartSec: 120, EndSec: 300, From: 0, To: 4200}
	// RowsPerFlushRamp is analytics-export's rows per flush (genAnalyticsExport).
	RowsPerFlushRamp = Ramp{StartSec: 120, EndSec: 180, From: 5000, To: 80000}
)

// RampOffsetDiskToDB is the lead from the disk-await ramp to the db-p95 ramp.
//
// It is deliberately not an expected correlate lag. Both patterns' count series
// are slow monotonic ramps, and two co-ramps offset by 6s are ~0.998 correlated
// at lag zero, so the cross-correlation peak sits at lag 0 on every supported
// grid — including grids finer than the 15s minimum ParseBins accepts. The
// corpus proves co-movement, not ordering. Lag recovery is proven where the
// signal actually localizes it, against a shifted pulse, in
// stats.TestCrossCorrelateRecoversTheGeneratingShift.
const RampOffsetDiskToDB = 6 * time.Second

// What correlate provably returns for the writes-vs-slow-query pair over the
// incident window: near-perfect co-movement with the peak at lag zero. Asserting
// the quantised truth keeps the assertion falsifiable; asserting "≈6s within
// ±30s" did not.
const (
	CorrelateRhoFloor       = 0.9
	CorrelateExpectedLag    = time.Duration(0)
	CorrelateExpectedDirect = "none"
)

// SketchRelTolerance is how far a sketch-derived quantile may sit from the
// generator's target value. DDSketch's guarantee is relative, and db-3's p95
// recovers DBWriteP95Ramp's 40ms floor as ~40.0ms and its 1200ms ceiling as
// ~1224ms once merged across the bin's segments.
const SketchRelTolerance = 0.15

// QuantileSampleRelTolerance is the slack a quantile needs on top of the sketch's own
// error when the bin holds few rows — a different quantity from SketchRelTolerance,
// hidden inside it while segments spanned whole minutes and each bin merged a minute of
// rows. db-1 emits 12 lines a second, so a 15s bin is ~180 rows whose 95th order
// statistic spreads ~18% around the 40ms target, still far tighter than the 30x ramp.
const QuantileSampleRelTolerance = 0.30

// The saturated host and the db instance sharing it.
const (
	SaturatedHost  = "host-7"
	SaturatedDB    = "db-3"   // db's instance label, not its service
	SaturatedDBPod = "db-3-0" // the same workload's pod, as the scheduler names it
	// HealthyDB is db's other instance. genDB pins it at DBWriteP95Ramp.From for
	// the whole window, so grouping p95 by instance must separate it from
	// SaturatedDB's ramp instead of averaging the two into one service series.
	HealthyDB          = "db-1"
	AnalyticsExportPod = "analytics-export-0"
)

// Placement is one pod→host scheduling record.
type Placement struct{ Pod, Host string }

// Placements is the scheduler stream's pod→host records in emit order.
// genScheduler emits exactly these, and it is the only place the corpus states
// them — no trace_id spans layers, so this list is the sole path from a workload
// to the host it saturates.
var Placements = []Placement{
	{"analytics-export-0", "host-7"},
	{"db-3-0", "host-7"},
	{"db-1-0", "host-2"},
	{"gateway-0", "host-4"},
	{"checkout-0", "host-5"},
	{"jobs-0", "host-6"},
	{"recommendations-0", "host-8"},
}

// PodsOn returns the pods scheduled onto host, in Placements order.
func PodsOn(host string) []string {
	var pods []string
	for _, p := range Placements {
		if p.Host == host {
			pods = append(pods, p.Pod)
		}
	}
	return pods
}

// HostSevenPodLines is the exact log-line count each pod on host-7 contributes
// over the incident window — what attribute_writes totals at group_by=pod.
//
// These are line counts, not write volume: db-3-0 out-logs analytics-export-0
// even though analytics-export drives the writes, so a line-count attribution
// ranks db-3-0 first.
//
// Each count runs a few lines above the workload's own output because the
// scheduler's placement records carry the same pod attribute — one for
// analytics-export-0, two for db-3-0, which is rescheduled mid-window. They are
// genuinely tagged with the pod, so excluding them would mean special-casing the
// placement stream out of a dimension it legitimately carries.
var HostSevenPodLines = map[string]int64{
	"analytics-export-0": 8543,  // 8542 own lines + 1 placement record
	"db-3-0":             12002, // 12000 own lines + 2 placement records
}

// HostSevenServiceLines is what the same attribution returns at the default
// group_by=service, and it is recorded here because it is a known limitation
// rather than the answer.
//
// Both co-tenants appear, resolved through stream identity rather than through the
// spelling of their pod names (tools.observeSubjectGroups): db-3-0's group is the
// service "db", which no amount of surgery on the pod name produces.
//
// The counts run above HostSevenPodLines' and deliberately so, because the group is
// coarser than the placement. "db" is a service with two instances and only db-3 sits
// on host-7, so db's total here includes db-1's lines, which host-7 never served. Read
// this as "which services are implicated on host-7, and how much those services emit"
// — the question group_by=service can answer off counts the summary already keys on,
// at 3 GETs. HostSevenPodLines answers "how much of host-7 each workload drives", and
// it costs the scan.
//
// It stays asserted alongside the pod attribution because it is the only grouping the
// summary can key counts on, and so the suite's only cross-path comparison that
// exercises the summary rather than scanning twice.
var HostSevenServiceLines = map[string]int64{
	"analytics-export": 8542,
	"db":               19200,
}

// GatewayTrafficStep doubles the gateway's request rate after ErrorRateRamp has
// settled, so the failure fraction is flat across it while the failure count doubles
// with the traffic — which is what makes the error-rate ratio provably better than an
// error count on this corpus. A step and not a ramp because countFor rounds
// deterministically only at an integral rate, and it fires at 360s so bins 0-5 and every
// ErrorRateRamp assertion keep the values they had before it existed.
var GatewayTrafficStep = TrafficStep{AtSec: 360, From: 30, To: 60}

// TrafficStep is a request rate that doubles at an instant rather than ramping.
type TrafficStep struct {
	AtSec    int
	From, To float64
}

func (s TrafficStep) rateAt(t float64) float64 {
	if t >= float64(s.AtSec) {
		return s.To
	}
	return s.From
}

// Bin is the index of the first bin emitting at the stepped-up rate, on a grid of
// this width. The bin before it is the last one at the old rate, so the pair
// brackets the step the way Ramp.BinBefore/BinAfter bracket a ramp.
func (s TrafficStep) Bin(width time.Duration) int {
	return int(time.Duration(s.AtSec) * time.Second / width)
}

// GatewayLinesPerMinute is how many lines the gateway stream emits in each
// one-minute bin of the incident window, exact rather than averaged because
// GatewayTrafficStep keeps the rate integral throughout. It is the denominator of
// the gateway's error-rate ratio, which is what lets a test multiply a rate back
// out into Gateway5xxPerMinute.
var GatewayLinesPerMinute = []float64{1800, 1800, 1800, 1800, 1800, 1800, 3600, 3600, 3600, 3600}

// Gateway5xxPerMinute is the gateway's 5xx count in each of the incident
// window's ten one-minute bins, summed across status 500 and 503.
//
// It is the assertable form of ErrorRateRamp: the ramp's own From and To are the
// error rate for the user-create route alone, not for all gateway traffic, so
// the rate does not reproduce against a whole-service count — but the per-bin
// step does, and crisply. Bins 0-2 sit before the ramp starts and bins 5 on sit
// after it settles (ErrorRateRamp.BinBefore / BinAfter at one minute).
//
// The jump at bin 6 is GatewayTrafficStep, not a worsening incident: the failure
// fraction is flat from bin 5 on and the count doubles only because the traffic under it
// does. That gap between this series and the error-rate ratio is why the measure exists.
var Gateway5xxPerMinute = []float64{1, 0, 1, 16, 64, 68, 127, 132, 137, 118}

// RCAExpectedChain holds only analytics-export's "wrote" patterns, one per table since
// the table name carries no digit, admitted by attribution against host-7 rather than
// topology because the fixture keeps trace_id per-service so the incident has no id
// shortcut across layers.
var RCAExpectedChain = []string{
	"wrote <*> rows to sessions",
	"wrote <*> rows to events",
	"wrote <*> rows to pageviews",
	"wrote <*> rows to clickstream",
}

// RCAExpectedRejectionReason is what rca_walk reports for the window's flat-rate
// patterns, which do not track the ramp and so are dropped before either gate runs.
const RCAExpectedRejectionReason = "below_rho"

// RCACoincidence ramps in lockstep with the slow queries yet owns none of host-7 and
// shares no topology edge with db, so a correlation-only walk that admits it fails.
const (
	RCACoincidence           = "job queued depth "
	RCACoincidenceRejectedBy = "no_attribution"
)

// HealthyPatterns must be spelled exactly as masking renders them, with "<*>" for
// each digit-bearing token, because a mismatch matches nothing and fails silently.
var HealthyPatterns = []string{
	"served <*> recommendations in <*> ms",
	"scored <*> candidates in <*> ms model ranker",
	"scored <*> candidates in <*> ms model blend",
	"scored <*> candidates in <*> ms model popularity",
	"cache lookup <*> ms result hit",
	"cache lookup <*> ms result miss",
}
