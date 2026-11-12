package seedcorpus

import (
	"fmt"
	"hash/fnv"
	"maps"
	"math/rand"
	"slices"
	"time"

	"github.com/datafocal/datafocal/internal/segment"
)

// This file generates the deterministic event corpus for the seeded fixture.
// It emits raw segment.Records only; the collector turns them into patterns,
// sketches, filters, and sealed segments. Everything is reproducible: each stream
// draws from a math/rand source seeded from its (service, labels) identity, so
// re-running the seeder byte-for-byte reproduces the corpus.
//
// Every workload stream stamps a "pod" attribute, as real agent-collected
// telemetry does. It is what makes attribution resolvable: the scheduler records
// placements by pod, so attribute_writes can only tie a host's activity back to a
// workload when the activity rows name that same pod. Without it host-7's
// co-tenant db-3 is unmatchable — its service is "db", and "db-3" is only its
// instance label — and the attribution credits one workload with the whole host.
//
// It is an attribute rather than a stream label because labels feed rngFor, so
// promoting it would reseed every stream. Host streams carry no pod.

// stream is one (service, labels) log stream and its generated events. A stream
// maps 1:1 to a fingerprinted segment key prefix, so each becomes its own run of
// time-partitioned segments.
type stream struct {
	service string
	labels  map[string]string
	events  []segment.Record
	// segmentWidth is the span its segments are cut at, the summary's bin width when zero.
	segmentWidth time.Duration
}

func (s *stream) emit(ts time.Time, level, msg string, attrs map[string]string) {
	s.events = append(s.events, segment.LogRecord(ts, level, msg, attrs))
}

// rngFor returns a deterministic rand source for a stream identity.
func rngFor(service string, labels map[string]string) *rand.Rand {
	h := fnv.New64a()
	fmt.Fprint(h, service)
	keys := slices.Sorted(maps.Keys(labels))
	for _, k := range keys {
		fmt.Fprintf(h, "\x00%s=%s", k, labels[k])
	}
	return rand.New(rand.NewSource(int64(h.Sum64())))
}

// --- small generation helpers ---

// countFor rounds a fractional per-second rate to an integer count, carrying the
// fraction as a probability so the long-run average matches rate exactly.
func countFor(rate float64, r *rand.Rand) int {
	n := int(rate)
	if r.Float64() < rate-float64(n) {
		n++
	}
	return n
}

// lerp is a clamped linear ramp from v0 (at t0) to v1 (at t1).
func lerp(t, t0, t1, v0, v1 float64) float64 {
	switch {
	case t <= t0:
		return v0
	case t >= t1:
		return v1
	default:
		return v0 + (v1-v0)*(t-t0)/(t1-t0)
	}
}

// tsWithin spreads the i-th of k events evenly across the given second.
func tsWithin(start time.Time, sec, i, k int) time.Time {
	off := time.Duration(sec)*time.Second + time.Duration(int64(i)*int64(time.Second)/int64(k))
	return start.Add(off)
}

// sampleLatency draws a latency (ms) whose median ≈ p50 and 95th percentile ≈
// p95, with a modest tail beyond p95. Feeding these to the param DDSketch makes
// Quantile(0.95) recover the target within the sketch's relative accuracy.
func sampleLatency(p50, p95 float64, r *rand.Rand) float64 {
	u := r.Float64()
	switch {
	case u < 0.5:
		return p50 * (0.4 + 1.2*u) // 0.4·p50 … 1.0·p50
	case u < 0.95:
		return p50 + (p95-p50)*((u-0.5)/0.45)
	default:
		return p95 * (1 + 0.6*(u-0.95)/0.05)
	}
}

func hexID(r *rand.Rand, prefix string) string {
	return fmt.Sprintf("%s-%08x%08x", prefix, r.Uint32(), r.Uint32())
}

func userID(r *rand.Rand) string { return fmt.Sprintf("user-%05d", r.Intn(3000)) }

// ============================ WIDE ============================

// WideStart is when the wide scenario's first segment starts.
func WideStart() time.Time { return time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC) }

const (
	// WideSegments is the wide scenario's segments, 10s each: 360 an hour.
	WideSegments = 1100
	wideRows     = 16
	// wideRowKeys is the keys a row takes from its segment's window of wideWindow.
	wideRowKeys = 12
	wideWindow  = 45
	// wideFamilyKeys is each key family's keys, about 800 in all.
	wideFamilyKeys = 270
)

// genWide is an agent transcript stream with the shape measured on a cluster: each
// segment's rows draw from about 50 of 800 flattened keys, and one in a hundred segments
// spans more than 700, as a tool result with every attachment does.
func genWide() stream {
	s := stream{service: "agent", segmentWidth: 10 * time.Second}
	r := rngFor(s.service, nil)
	families := []string{"attachment", "payload", "toolUseResult"}
	key := func(i int) string {
		i %= len(families) * wideFamilyKeys
		return fmt.Sprintf("%s.f%03d", families[i/wideFamilyKeys], i%wideFamilyKeys)
	}
	session := hexID(r, "session")
	for seg := range WideSegments {
		window := r.Intn(len(families) * wideFamilyKeys)
		tail := seg%100 == 99
		for row := range wideRows {
			ts := WideStart().Add(time.Duration(seg)*10*time.Second + time.Duration(row)*500*time.Millisecond)
			attrs := map[string]string{transcriptKeys.Session: session, transcriptKeys.Request: hexID(r, "req"),
				transcriptKeys.Prompt: fmt.Sprintf("prompt-%04d", seg/6)}
			for k := range wideWindow {
				switch {
				case tail: // each row its own 45 keys, 720 in all
					attrs[key(window+row*wideWindow+k)] = fmt.Sprintf("v%d", r.Intn(8))
				case r.Intn(wideWindow) < wideRowKeys:
					attrs[key(window+k)] = fmt.Sprintf("v%d", r.Intn(8))
				}
			}
			s.emit(ts, "info", fmt.Sprintf("turn %d step %d", seg, row), attrs)
		}
	}
	return s
}

// ============================ INCIDENT ============================

// incidentStreams returns the cross-layer incident corpus. The chain — with no
// shared trace_id across layers, forcing the temporal+topology path — is:
// analytics-export write spike → host-7 disk saturation → db-3 write latency
// (lagging disk ~6s) → jobs queue backup → gateway/checkout user-create 5xx.
func incidentStreams() []func() stream {
	return []func() stream{
		genScheduler,
		genAnalyticsExport,
		func() stream { return genHost("host-7", true) },
		func() stream { return genHost("host-1", false) },
		func() stream { return genDB("db-3", true) },
		func() stream { return genDB("db-1", false) },
		genJobs,
		genGateway,
		genCheckout,
	}
}

// genScheduler emits the pod→host scheduler log. It carries the ONLY link from
// db-3 and analytics-export to host-7 (both pods land on host-7), so the topology
// db-3→host-7 is derivable even though no trace_id is shared.
func genScheduler() stream {
	labels := map[string]string{"instance": "scheduler-0", "region": "us-east-1"}
	s := &stream{service: "scheduler", labels: labels}
	start := IncidentStart()

	for i, p := range Placements {
		ts := start.Add(time.Duration(i) * 400 * time.Millisecond)
		s.emit(ts, "info", fmt.Sprintf("scheduled pod %s host %s", p.Pod, p.Host),
			map[string]string{"pod": p.Pod, "host": p.Host})
	}
	// A mid-window reschedule keeps the stream from being a single tiny segment
	// and reaffirms the db-3→host-7 placement after the incident starts.
	s.emit(start.Add(300*time.Second), "info", "scheduled pod db-3-0 host host-7",
		map[string]string{"pod": "db-3-0", "host": "host-7"})
	return *s
}

// genAnalyticsExport emits the write-volume spike that saturates host-7's disk:
// rows-written per flush climbs from ~5k to ~80k and the flush rate rises.
func genAnalyticsExport() stream {
	labels := map[string]string{"instance": "analytics-export-0", "region": "us-east-1"}
	s := &stream{service: "analytics-export", labels: labels}
	r := rngFor(s.service, labels)
	start := IncidentStart()
	tables := []string{"events", "sessions", "pageviews", "clickstream"}

	for sec := range IncidentDur {
		t := float64(sec)
		rate := lerp(t, 120, 180, 3, 18)
		k := countFor(rate, r)
		rowsBase := lerp(t, 120, 180, 5000, 80000)
		for i := range k {
			ts := tsWithin(start, sec, i, k)
			n := int(rowsBase*(0.85+0.3*r.Float64()) + 0.5)
			table := tables[r.Intn(len(tables))]
			s.emit(ts, "info", fmt.Sprintf("wrote %d rows to %s", n, table),
				map[string]string{"instance": "analytics-export-0", "region": "us-east-1",
					"pod": AnalyticsExportPod})
		}
	}
	return *s
}

// genHost emits per-second disk metrics for one host. host-7 saturates
// (util 24→100%, await 2→180ms); other hosts stay healthy.
func genHost(host string, incident bool) stream {
	labels := map[string]string{"host": host, "dc": "us-east-1a"}
	s := &stream{service: "host", labels: labels}
	r := rngFor(s.service, labels)
	start := IncidentStart()

	for sec := range IncidentDur {
		t := float64(sec)
		var util, await float64
		if incident {
			util = lerp(t, 120, 180, 24, 100) + 2*(r.Float64()-0.5)
			await = lerp(t, 120, 186, 2, 180) * (0.9 + 0.2*r.Float64())
		} else {
			util = 22 + 8*r.Float64()
			await = 2 + 2*r.Float64()
		}
		if util > 100 {
			util = 100
		}
		level := "info"
		if util >= 90 {
			level = "warn"
		}
		ts := tsWithin(start, sec, 0, 1)
		s.emit(ts, level, fmt.Sprintf("disk util %d pct await %d ms", int(util+0.5), int(await+0.5)),
			map[string]string{"host": host, "dc": "us-east-1a"})

		if sec%5 == 0 {
			mem := 40 + 20*r.Float64()
			if incident {
				mem = lerp(t, 120, 240, 45, 88)
			}
			s.emit(ts.Add(100*time.Millisecond), "info",
				fmt.Sprintf("memory used %d pct", int(mem+0.5)),
				map[string]string{"host": host, "dc": "us-east-1a"})
		}
	}
	return *s
}

// genDB emits query logs for one db instance. db-3's write p95 climbs 40ms→1.2s,
// lagging the disk ramp by ~6s; slow queries switch to a distinct pattern and
// escalate the level.
func genDB(instance string, incident bool) stream {
	labels := map[string]string{"instance": instance, "cluster": "primary"}
	s := &stream{service: "db", labels: labels}
	r := rngFor(s.service, labels)
	start := IncidentStart()
	rate := 20.0
	if !incident {
		rate = 12.0
	}

	for sec := range IncidentDur {
		t := float64(sec)
		var p50, p95 float64
		if incident {
			// Lags the disk ramp (t=120) by ~6s.
			p50 = lerp(t, 126, 186, 5, 60)
			p95 = lerp(t, 126, 186, 40, 1200)
		} else {
			p50, p95 = 5, 40
		}
		k := countFor(rate, r)
		for i := range k {
			ts := tsWithin(start, sec, i, k)
			ms := int(sampleLatency(p50, p95, r) + 0.5)
			attrs := map[string]string{
				"instance": instance,
				"cluster":  "primary",
				// db is the one service whose instance label is not already its pod
				// name, so the pod carries the replica suffix the scheduler used.
				"pod":        instance + "-0",
				"op":         pick(r, "read", "write"),
				"trace_id":   hexID(r, "db"),
				"request_id": hexID(r, "req"),
			}
			if ms >= 200 {
				s.emit(ts, "warn", fmt.Sprintf("slow query %d ms", ms), attrs)
			} else {
				s.emit(ts, "info", fmt.Sprintf("query ok %d ms", ms), attrs)
			}
		}
	}
	return *s
}

// genJobs emits the job queue log. Enqueue rate rises and the reported queue
// depth climbs 0→4200 as downstream db writes stall.
func genJobs() stream {
	labels := map[string]string{"instance": "jobs-0", "region": "us-east-1"}
	s := &stream{service: "jobs", labels: labels}
	r := rngFor(s.service, labels)
	start := IncidentStart()
	types := []string{"export", "email", "index", "reindex"}

	for sec := range IncidentDur {
		t := float64(sec)
		rate := lerp(t, 120, 200, 8, 20)
		depth := int(lerp(t, 120, 300, 0, 4200) + 0.5)
		k := countFor(rate, r)
		for i := range k {
			ts := tsWithin(start, sec, i, k)
			level := "info"
			if depth > 2000 {
				level = "warn"
			}
			// depth/type are placed after the depth-4 routing prefix so the type
			// generalizes to a param rather than splitting into a pattern per type.
			s.emit(ts, level,
				fmt.Sprintf("job queued depth %d type %s", depth, types[r.Intn(len(types))]),
				map[string]string{"instance": "jobs-0", "region": "us-east-1", "pod": "jobs-0"})
		}
	}
	return *s
}

// genGateway emits the ingress user-create traffic whose 5xx rate climbs
// 0.1%→7% as db-3 saturates. Each request carries its own trace_id (gateway
// namespace — never shared with db), plus request_id and a pooled user_id.
func genGateway() stream {
	return genEdge("gateway", "gateway-0", GatewayTrafficStep.rateAt, true, map[string]string{
		"/users":  "POST",
		"/health": "GET",
	})
}

// genCheckout emits the checkout service traffic; user-create there fails in
// lockstep with gateway as the shared db dependency degrades.
func genCheckout() stream {
	return genEdge("checkout", "checkout-0", constantRate(25.0), false, map[string]string{
		"/users":    "POST",
		"/checkout": "POST",
	})
}

// constantRate is the flat request rate every edge service but gateway runs at.
func constantRate(perSecond float64) func(float64) float64 {
	return func(float64) float64 { return perSecond }
}

// correlatedSlots is how many of each second's user-create requests gateway and
// checkout emit as a correlated pair: gateway serves the ingress request, calls
// checkout, and both lines carry the same request_id and user_id. Their windows
// nest, so get_trace reconstructs a real two-span waterfall from plain log lines
// — the corpus contains no span_id anywhere, which is the point.
//
// The sharing stays inside the app layer. db and host still share no id with
// anything, so resolving the incident across layers remains the
// temporal+topology+attribution path with no id shortcut.
const correlatedSlots = 4

// correlatedCall is one ingress request and the downstream call it made. A log
// line is written when its work finishes, so each window is [end-ms, end]; the
// callee's window falls strictly inside the caller's, which is what lets
// get_trace infer the nesting without a parent_span_id.
type correlatedCall struct {
	requestID string
	user      string
	status    int
	level     string
	callerEnd time.Time
	callerMs  int
	calleeEnd time.Time
	calleeMs  int
}

// correlate builds the slot-th correlated call of second sec. It is a pure
// function of its arguments, seeded from (sec, slot) alone: gateway and checkout
// each call it and see identical values, so the pair needs no state crossing
// between the two streams. Latency and error rolls use the same distributions
// the uncorrelated requests draw from, so the fixture's p95 and 5xx ramps hold.
func correlate(start time.Time, sec, slot int, errRate float64) correlatedCall {
	h := fnv.New64a()
	fmt.Fprintf(h, "call\x00%d\x00%d", sec, slot)
	r := rand.New(rand.NewSource(int64(h.Sum64())))

	c := correlatedCall{
		requestID: fmt.Sprintf("req-%03d-%02d", sec, slot),
		user:      userID(r),
		status:    201,
		level:     "info",
	}
	c.callerMs = int(sampleLatency(8, 25, r) + 0.5)
	if r.Float64() < errRate {
		c.status, c.level = pickInt(r, 500, 503), "error"
		c.callerMs = int(sampleLatency(200, 1200, r) + 0.5)
	}
	// The downstream call sits inside the request: the caller does a sixth of the
	// work before the call and the same after it, so the callee's window nests in
	// the caller's. Both offsets floor at a millisecond, which is what keeps the
	// nesting derivable from containment for even the fastest request.
	head := max(1, c.callerMs/6)
	c.calleeMs = max(1, c.callerMs-2*head)

	// The callee ends before the caller does, so a pair landing on the window's first
	// instant puts its callee outside the window entirely — where pruning drops it and
	// a needle lookup returns half a trace. Hold the pair far enough in to fit.
	c.callerEnd = tsWithin(start, sec, slot, correlatedSlots)
	if floor := start.Add(time.Duration(head) * time.Millisecond); c.callerEnd.Before(floor) {
		c.callerEnd = floor
	}
	c.calleeEnd = c.callerEnd.Add(-time.Duration(head) * time.Millisecond)
	return c
}

// genEdge is the shared generator for request-serving edge services. ingress
// marks the service that fronts the correlated user-create call (gateway); the
// other end (checkout) emits the downstream half of the same pair.
func genEdge(service, instance string, rateAt func(t float64) float64, ingress bool, routes map[string]string) stream {
	labels := map[string]string{"instance": instance, "region": "us-east-1"}
	s := &stream{service: service, labels: labels}
	r := rngFor(service, labels)
	start := IncidentStart()

	paths := slices.Sorted(maps.Keys(routes))

	for sec := range IncidentDur {
		t := float64(sec)
		errRate := lerp(t, 180, 300, 0.001, 0.07)
		k := countFor(rateAt(t), r)

		// The second's first few user-create requests are the correlated
		// gateway→checkout pair. They come out of k, so the per-second rate is
		// unchanged and only which requests are correlated differs.
		pairs := min(k, correlatedSlots)
		for slot := range pairs {
			c := correlate(start, sec, slot, errRate)
			end, ms := c.callerEnd, c.callerMs
			if !ingress {
				end, ms = c.calleeEnd, c.calleeMs
			}
			s.emit(end, c.level, fmt.Sprintf("POST /users %d %d ms", c.status, ms),
				map[string]string{
					"instance": instance,
					"pod":      instance,
					"region":   "us-east-1",
					"endpoint": "/users",
					"status":   fmt.Sprintf("%d", c.status),
					// trace_id stays per-service: only request_id links the hops.
					"trace_id":   hexID(r, service),
					"request_id": c.requestID,
					"user_id":    c.user,
				})
		}
		k -= pairs

		for i := range k {
			ts := tsWithin(start, sec, i, k)
			path := paths[r.Intn(len(paths))]
			method := routes[path]
			// Only the user-create write path degrades; other routes stay healthy.
			degrades := path == "/users" || path == "/checkout"

			status := 201
			if method == "GET" {
				status = 200
			}
			level := "info"
			ms := int(sampleLatency(8, 25, r) + 0.5)
			if degrades && r.Float64() < errRate {
				status = pickInt(r, 500, 503)
				level = "error"
				ms = int(sampleLatency(200, 1200, r) + 0.5)
			}
			s.emit(ts, level, fmt.Sprintf("%s %s %d %d ms", method, path, status, ms),
				map[string]string{
					"instance":   instance,
					"pod":        instance,
					"region":     "us-east-1",
					"endpoint":   path,
					"status":     fmt.Sprintf("%d", status),
					"trace_id":   hexID(r, service),
					"request_id": hexID(r, "req"),
					"user_id":    userID(r),
				})
		}
	}
	return *s
}

// ============================ ACCESS ============================

// AccessStart is when the access scenario's one access-log pattern starts logging, 20 lines a
// second for AccessDur seconds, into the next hour so the compactor merges the first.
func AccessStart() time.Time { return time.Date(2026, 7, 18, 15, 50, 0, 0, time.UTC) }

// The access scenario's faults, in seconds from AccessStart: its 5xx share rises from 1% to 30%,
// its p99 latency rises tenfold, and status 429 is first seen, each at a steady line rate.
const (
	AccessDur         = 1200
	AccessErrorsAt    = 400
	AccessSlowAt      = 800
	AccessThrottledAt = 1000 //nolint:dupdefault // a fault's second, unrelated to logformat's line cap
)

func genAccess() stream {
	s := &stream{service: "storefront", labels: map[string]string{"instance": "storefront-0"}}
	r := rngFor(s.service, s.labels)
	for sec := range AccessDur {
		errRate := 0.01
		if sec >= AccessErrorsAt {
			errRate = 0.3
		}
		for i := range 20 {
			status := "200"
			switch u := r.Float64(); {
			case sec >= AccessThrottledAt && u < 0.05:
				status = "429"
			case u < errRate:
				status = pick(r, "500", "503")
			}
			ms := sampleLatency(8, 25, r)
			if sec >= AccessSlowAt && r.Float64() < 0.03 {
				ms *= 10
			}
			s.emit(tsWithin(AccessStart(), sec, i, 20), "info", "GET /products",
				map[string]string{"status": status, "latency_ms": fmt.Sprintf("%.0f", ms)})
		}
	}
	return *s
}

// ============================ HEALTHY ============================

// healthyStreams returns the freshly-rolled-out recommendations service: online
// twelve minutes before "now", ~210 req/s, p95 ~41ms, zero errors, exactly three
// patterns.
func healthyStreams() []func() stream {
	return []func() stream{genRecommendations}
}

func genRecommendations() stream {
	labels := map[string]string{"instance": "recommendations-0", "version": "v1", "region": "us-east-1"}
	s := &stream{service: "recommendations", labels: labels}
	r := rngFor(s.service, labels)
	start := HealthyStart()
	models := []string{"ranker", "blend", "popularity"}

	for sec := range HealthyDur {
		k := countFor(210.0, r)
		for i := range k {
			ts := tsWithin(start, sec, i, k)
			ms := int(sampleLatency(18, 41, r) + 0.5)
			attrs := map[string]string{
				"instance":   "recommendations-0",
				"pod":        "recommendations-0",
				"version":    "v1",
				"region":     "us-east-1",
				"request_id": hexID(r, "req"),
				"user_id":    userID(r),
			}
			// The categorical tokens sit after the depth-4 routing prefix so they
			// generalize to params instead of splitting the three patterns.
			switch d := r.Float64(); {
			case d < 0.6:
				s.emit(ts, "info",
					fmt.Sprintf("served %d recommendations in %d ms", 3+r.Intn(18), ms), attrs)
			case d < 0.85:
				s.emit(ts, "info",
					fmt.Sprintf("scored %d candidates in %d ms model %s",
						50+r.Intn(400), ms, models[r.Intn(len(models))]), attrs)
			default:
				s.emit(ts, "info",
					fmt.Sprintf("cache lookup %d ms result %s", ms, pick(r, "hit", "miss")), attrs)
			}
		}
	}
	return *s
}

// --- tiny pickers ---

func pick(r *rand.Rand, opts ...string) string { return opts[r.Intn(len(opts))] }
func pickInt(r *rand.Rand, opts ...int) int    { return opts[r.Intn(len(opts))] }
