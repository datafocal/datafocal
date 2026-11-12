// Package api is the HTTP tool surface: one POST /v1/<tool>
// per tool, JSON in and a query.Envelope out, plus GET /healthz and GET
// /v1/describe. Server delegates routing, dispatch, and error classification to
// the shared Registry so every tool goes through one seam.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/datafocal/datafocal/internal/query"
	"github.com/datafocal/datafocal/internal/query/tools"
)

// Server binds the tools and owns the Registry that fans tool definitions out to
// HTTP and MCP surfaces.
type Server struct {
	tools    tools.Tools
	registry *Registry
}

// NewServer registers t's tools once for HTTP and MCP, each request served under l.
func NewServer(t tools.Tools, l Limits) *Server {
	s := &Server{tools: t, registry: NewRegistry(t.Snapshot)}
	s.registry.requests = newRequests(l)
	s.registerTools()
	return s
}

// Registry exposes the underlying Registry so callers (main.go) can bridge the
// same tool set to the MCP transport.
func (s *Server) Registry() *Registry { return s.registry }

// Handler returns the HTTP handler for the tool surface. It delegates routing,
// middleware, and describe/healthz to the shared Registry seam, then mounts the
// SSE follow and batch endpoints as extra routes on the same mux, past the request
// timeout that would cut every stream at 30s.
func (s *Server) Handler() http.Handler {
	return s.registry.HTTPHandlerWith(s.handleHealthz, func(r chi.Router) {
		r.Post("/v1/search_logs/follow", s.handleSearchFollow)
		r.Post("/v1/batch/stream", s.handleBatchStream)
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleSearchFollow handles POST /v1/search_logs/follow, streaming the live tail as SSE
// for as long as the client stays connected.
func (s *Server) handleSearchFollow(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	ctx := r.Context()

	raw, err := readBody(w, r)
	if err != nil {
		return // readBody already wrote the 400
	}

	var req tools.SearchRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeRegistryError(w, r, http.StatusBadRequest,
			query.NewAPIError(query.ErrCodeInvalidRequest, "unmarshal request: "+err.Error()))
		return
	}

	flusher, ok := startStream(w)
	if !ok {
		return
	}

	// Drive FollowSearch with an emit callback that writes SSE events
	err = s.tools.FollowSearch(ctx, req, func(env query.Envelope[tools.SearchResponse]) error {
		data, marshalErr := json.Marshal(env)
		if marshalErr != nil {
			return marshalErr
		}
		_, writeErr := fmt.Fprintf(w, "data: %s\n\n", string(data))
		if writeErr != nil {
			return writeErr
		}
		flusher.Flush()
		return nil
	})

	if err != nil {
		// The failure arrives after the 200 the stream already committed to, so the
		// body is all the caller has to classify it by. It is the same APIError the
		// non-streaming paths return, for the same reason: one shape to parse.
		apiErr, _ := classifyError(err)
		outcomeOf(ctx).failure = apiErr
		errJSON, _ := json.Marshal(apiErr)
		fmt.Fprintf(w, "data: %s\n\n", string(errJSON))
		flusher.Flush()
	}
}

// startStream sets w up for SSE, each event flushed as it is written, or fails the request
// when w cannot stream.
func startStream(w http.ResponseWriter) (http.Flusher, bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// A proxy such as nginx would otherwise hold events back until its buffer fills.
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return nil, false
	}
	// The server's write timeout would cut the stream.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
	return flusher, true
}

// registerTools binds every built-in tool to the Registry. Adding a new tool means
// adding one row here; the HTTP route, describe entry, and MCP registration all follow.
func (s *Server) registerTools() {
	for _, td := range []ToolDef{
		{"overview", overviewDoc, overviewSchema, bind(s.tools.Overview), CacheBySnapshot},
		{"aggregate", aggregateDoc, aggregateSchema, bind(s.tools.Aggregate), CacheBySnapshot},
		{"attribute_writes", attributeWritesDoc, attributeWritesSchema, bind(s.tools.AttributeWrites), CacheBySnapshot},
		{"co_occurrence", coOccurrenceDoc, coOccurrenceSchema, bind(s.tools.CoOccurrence), CacheBySnapshot},
		{"correlate", correlateDoc, correlateSchema, bind(s.tools.Correlate), CacheBySnapshot},
		{"explain_spike", explainSpikeDoc, explainSpikeSchema, bind(s.tools.ExplainSpike), CacheBySnapshot},
		{"find_anomalies", findAnomaliesDoc, findAnomaliesSchema, bind(s.tools.FindAnomalies), CacheBySnapshot},
		{"focus", focusDoc, focusSchema, bind(s.tools.Focus), CacheBySnapshot},
		{"funnel", funnelDoc, funnelSchema, bind(s.tools.Funnel), CacheBySnapshot},
		{"search_logs", searchDoc, searchSchema, bind(s.tools.SearchLogs), CacheBySnapshot},
		{"rank_fields", rankFieldsDoc, rankFieldsSchema, bind(s.tools.RankFields), CacheBySnapshot},
		{"describe_schema", describeSchemaDoc, rankFieldsSchema, bind(s.tools.DescribeSchema), CacheBySnapshot},
		{"query", queryDoc, querySchema, bind(s.tools.Query), CacheBySnapshot},
		{"list_patterns", listPatternsDoc, listPatternsSchema, bind(s.tools.Patterns), CacheBySnapshot},
		{"get_trace", traceDoc, traceSchema, bind(s.tools.GetTrace), CacheBySnapshot},
		{"trace_shape", traceShapeDoc, traceShapeSchema, bind(s.tools.TraceShape), CacheBySnapshot},
		{"service_graph", serviceGraphDoc, serviceGraphSchema, bind(s.tools.ServiceGraph), CacheBySnapshot},
		{"rca_walk", rcaWalkDoc, rcaWalkSchema, bind(s.tools.RCAWalk), CacheBySnapshot},
		{"resource_usage", resourceUsageDoc, resourceUsageSchema, bind(s.tools.ResourceUsage), CacheBySnapshot},
		{"list_slos", listSLOsDoc, listSLOsSchema, strict(s.tools.ListSLOs), NoCache},
		{"slo_status", sloStatusDoc, sloStatusSchema, strict(s.tools.SLOStatus), NoCache},
		{"import_definitions", importDefinitionsDoc, importDefinitionsSchema, strict(s.tools.ImportDefinitions), NoCache},
		{"export_definitions", exportDefinitionsDoc, exportDefinitionsSchema, strict(s.tools.ExportDefinitions), NoCache},
		// Both read the definitions, not the snapshot.
		{"list_views", listViewsDoc, listViewsSchema, strict(s.tools.ListViews), NoCache},
		{"get_view", getViewDoc, getViewSchema, strict(s.tools.GetView), NoCache},
		{Name: batchTool, Doc: batchDoc, Schema: batchSchema, Handler: s.batch, Caching: NoCache},
	} {
		s.registry.Register(td)
	}
}

// bind adapts a tool method to the Registry's handler: it decodes the request and
// reports a body that does not fit the request type as invalid_request, since it is
// the caller's to fix.
func bind[Req, Resp any](run func(context.Context, Req) (query.Envelope[Resp], error)) func(context.Context, json.RawMessage) (any, error) {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		var req Req
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, query.Invalidf("decode request: %v", err)
		}
		env, err := run(ctx, req)
		if err != nil {
			return nil, err
		}
		outcomeOf(ctx).cost = env.Meta.Cost
		return env, nil
	}
}

// strict is bind refusing a field the request does not have, for the tools that take no
// selector, so one sent to them is refused rather than ignored.
func strict[Req, Resp any](run func(context.Context, Req) (query.Envelope[Resp], error)) func(context.Context, json.RawMessage) (any, error) {
	handle := bind(run)
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(new(Req)); err != nil {
			return nil, query.Invalidf("decode request: %v", err)
		}
		return handle(ctx, raw)
	}
}

const overviewDoc = "Show what is running and what is unhealthy in one call: one row per (namespace, service) over a window, the last hour by default, with its lines, error lines, first and last seen bin, and pods seen, and from cluster lifecycle lines its terminations (lines with an exit_code) and restarts (the highest restart_count a pod reported). Sorted by error lines, then lines. Answered from the summary and segment footers when the summary covers the window, reading rows only of the segments holding lifecycle lines."

var overviewSchema = inferSchema[tools.OverviewRequest]()

const aggregateDoc = "Aggregate a measure (count|rate|p50|p95|p99|distinct:<field>|sum:<field>|first:<field>|last:<field>|min:<field>|max:<field>|max_run:<field>|changes:<field>, where a sum, quantile, min or max may take len(<field>) or bytes(<field>) as its field) over a time range, grouped by service, pattern_id, or a field such as a session or user id, with bins=all for one bin over the whole window. Returns a page of series, one per group, in a query.Envelope. Each series is columns on the bins: start (the first bin's start in unix ms, or with ordinal_of its first position), step (the bin width), and one entry per bin in values, baselines (the EWMA baseline to 4 significant digits) and labels; with measures, measure_values and measure_labels hold one array per measure in request order. Bin i starts at start + i*step; null marks a bin with no value, such as a ratio's 0/0."

// aggregateSchema is the JSON Schema describing AggregateRequest. It is served
// verbatim by /v1/describe and used as the MCP tool's InputSchema. It is
// generated from the Go struct (see schema.go) so it cannot drift from the type
// its handler unmarshals into.
var aggregateSchema = inferSchema[tools.AggregateRequest]()

const searchDoc = "Search logs for the lines a selector matches, newest first, with every column each line has. An exact equality on an id such as trace_id, request_id or user_id needs no time range; any other selector needs one, and refuses with scan_too_large past max_scan_segments. around returns the lines of a line's producer (its service, namespace, pod and container) either side of it, in time order. POST /v1/search_logs/follow streams every log line as it lands, in time order, as SSE; it ignores the selector."

// searchSchema is generated from SearchRequest (see schema.go). It is served
// verbatim by /v1/describe and used as the MCP tool's InputSchema. It cannot
// drift from the type its handler unmarshals into.
var searchSchema = inferSchema[tools.SearchRequest]()

const rankFieldsDoc = "Rank the fields of the lines a selector matches by signal: its predicates filter rows, and a row scan past the scan bound refuses with scan_too_large. Returns per-field {name, type, cardinality, present, signal, high_signal} sorted by signal descending. High-signal fields are the best candidates for group-by and filter predicates."

var rankFieldsSchema = inferSchema[tools.FieldsRequest]()

const describeSchemaDoc = "Describe the schema of a scope: every column the segments a selector prunes to hold, with its stored type, its kind (str, num, enum or id, as rank_fields types it) and its distinct count, sorted by name, and the pattern profiles that produced those segments, one per profile version, with the highest degradation (none, column_names or catch_all) any of its segments reached and how many segments it wrote. Reads segment footers only, so it costs no data reads; its predicates prune segments rather than filter rows."

const listPatternsDoc = "List deduplicated log patterns over a time range, which is required, ranked by count or rate. The selector's predicates filter the lines counted, and a row scan past the scan bound refuses with scan_too_large. Returns per-pattern aggregates (pattern_id, pattern text, dominant service, count, rate) in a query.Envelope with opaque-cursor pagination. Sort defaults to \"count\"."

// listPatternsSchema is generated from PatternsRequest (see schema.go). The
// sort enum is pinned from tools.PatternSortModes — the same list
// validatePatternsRequest checks against — so schema and validation stay in
// lockstep.
var listPatternsSchema = inferSchema[tools.PatternsRequest](
	setEnum("sort", tools.PatternSortModes),
)

const queryDoc = "Run a LogQL query, with vars giving the value of each \"$name\" an = or != predicate's value names, and return the tool call it made: a log query searches, `| patterns` lists patterns, `| explain` explains the filtered rows against the rest of the window, `| trace` reconstructs the trace an id selects, and count_over_time, rate, sum_over_time or quantile_over_time aggregate, ranked by topk. `| json`, `| logfmt` and `| unpack` do nothing, since every format is parsed at ingest. Returns the canonical query, the call, and the called tool's data and meta; line_format, label_format, more than one grouping label and binary operations are refused with the offset of the construct."

var querySchema = inferSchema[tools.QueryRequest]()

const traceDoc = "Reconstruct a span waterfall from the log events sharing a correlation id. Group by any _id column the logs carry, defaulting to trace_id; call rank_fields to see which ids this tenant emits (they report type \"id\"). Any id with many values per segment prunes via its membership filter, and a time range is optional for an id; meta.cost reports what was read. Spans are read from span_id/parent_span_id when the events carry them and derived from the emitting service and event timings when they do not, so uninstrumented services still appear; meta.approx reports whether any span was derived. Returns parent-linked, start-ordered spans (name, service, start_ms, dur_ms, error, parent) in a query.Envelope."

// traceSchema is generated from TraceRequest (see schema.go). It is served
// verbatim by /v1/describe and used as the MCP tool's InputSchema. It cannot
// drift from the type its handler unmarshals into.
//
// field carries no enum on purpose. These are unstructured logs: any _id a
// producer emits can correlate events, so a compile-time list would describe our
// source code rather than the tenant's data. Discovery is rank_fields' job and a
// bad guess comes back as a re-plannable error with suggested_fields, which is the
// pairing that makes an open field safe.
var traceSchema = inferSchema[tools.TraceRequest]()

const traceShapeDoc = "Merge a sample of traces starting the same way into their typical tree, to tell whether one trace is normal. Picks the earliest traces whose events match root in the window, grouped by field (trace_id by default), reads each as get_trace does, and merges the spans by their path of (service, name) from the root. Each node reports the share of traces holding it, its p50 and p95 duration, its error share and its span count, parent before child. With compare set to one correlation value, that trace's nodes are marked extra when absent from the shape and slower when above the node's p95, and the shape's nodes held by at least 90% of traces but absent from it are marked missing. meta.cost is the sum of the aggregate and get_trace calls made."

var traceShapeSchema = inferSchema[tools.TraceShapeRequest]()

const resourceUsageDoc = "Report CPU, memory, network and disk use per service, pod or container over a window, from the resource samples a sampler such as cAdvisor logs, whose container.id ingest names the pod and service of. Reads the window's 20 busiest hosts, one aggregate call each, and turns running totals into rates between consecutive bins: CPU in cores, network in bytes a second; memory is the mean working set in a bin and disk the bytes used. Returns each host's own use from its root cgroup, then each group's, each with a summary of its mean, peak and last values and, unless summary_only, its per-bin series on bins starting at start, step ms wide. A stat whose column the samples lack is null, and columns lists those present. Detail cpu, memory or network adds each use's parts of that resource: user and system CPU; memory's rss, cache, swap and mapped files; or the network's errors a second each way. Narrow with selector predicates such as service=checkout; group_by container names a pod's unnamed container sandbox, which carries the pod's network under containerd."

var resourceUsageSchema = inferSchema[tools.ResourceUsageRequest]()

const serviceGraphDoc = "Return the service topology graph: nodes (services, plus the workloads and resources underneath them) and weighted directed edges. A co-occurrence edge joins two services by the trace_ids they share, weighted by the estimated number of shared traces in the window; a topology edge places a workload on the resource it runs on — a pod on a host, an allocation on a client, a job on a worker pool — weighted by the bins the placement was seen in. Filter by time window and optional service predicate, which expands to the workloads those services' streams declared. Heaviest edges come first, so limit keeps the strongest relationships and reports the rest via coverage. Answers with or without a summary index: a warm one is read directly, a cold one is folded from the window's footers for the price of the reads. Costs a bounded scan of two columns of the placement stream, whose service and field names default to the scheduler's pod and host and are overridable per request."

var serviceGraphSchema = inferSchema[tools.ServiceGraphRequest]()

const coOccurrenceDoc = "Return the pairs of entities that carried the same value of another column in overlapping time: sessions that edited the same file, jobs that wrote the same key, users that shared a device. For each shared value, an entity's interval runs from the first to the last time it carried the value, and two entities pair when their intervals overlap or come within max_gap_seconds; a list column contributes each element. Requires a bounded time window and scans the entity and shared columns. Pairs come longest overlap first, A before B by name."

var coOccurrenceSchema = inferSchema[tools.CoOccurrenceRequest]()

const funnelDoc = "Return how many entities reach each step of an ordered sequence: sessions going from a prompt to an edit to a passing test, checkouts, job pipelines. Groups the window's rows by the entity column, orders each entity's rows by time and matches the steps greedily in order, each step to the first later row satisfying all its predicates; with within_seconds set, the whole sequence must fit in that time. Each step reports the entities that reached it, the median milliseconds from the previous step, and up to three entities, by name, that stopped there. Requires a bounded time window and scans the entity and step columns."

var funnelSchema = inferSchema[tools.FunnelRequest]()

const correlateDoc = "Cross-correlate two pattern count series to detect temporal relationships with lag detection. Returns the peak correlation coefficient (rho in [-1,1]), the lag in seconds, and the direction (a_leads_b, b_leads_a, or none). Requires a bounded time window; bins set the lag resolution. Served from the summary index at zero data GETs when it covers the window, and from a footer merge otherwise."

var correlateSchema = inferSchema[tools.CorrelateRequest]()

const explainSpikeDoc = "Explain a slice of a scope, such as its error rows: rank every low-cardinality field value by how much more common it is in the slice than in the rest of the scope. Fields are ranked by the KL divergence of the slice's mix from the rest's, values within a field by the rise in share; a field whose mix matches is left out, as are id fields and the fields the slice is defined on. Requires a bounded time window and at least one slice predicate."

var explainSpikeSchema = inferSchema[tools.ExplainSpikeRequest]()

const findAnomaliesDoc = "Find the pattern-count spikes and newly appearing patterns in a scope, with each pattern's text. Each pattern's count series is scored against its EWMA baseline, and its strongest spike is reported as kind \"spike\" with the bin it peaked in, the observed count and the expected one; a pattern with no record before the window is also reported as kind \"new_pattern\" at its first bin. Ranked by standard deviations above the baseline, new patterns after spikes. Requires a bounded time window."

var findAnomaliesSchema = inferSchema[tools.FindAnomaliesRequest]()

const focusDoc = "Read one slice of a scope in one call, at one snapshot: its count trend over auto bins (aggregate), the field values it over-represents against the rest of the scope (explain_spike), its top 10 patterns with their text (list_patterns) and 5 example lines (search_logs). A section whose tool fails is left out and its error is reported under errors by section name; the rest still answer. meta.cost is the sum of the sections' costs and meta.coverage the lowest of theirs."

var focusSchema = inferSchema[tools.FocusRequest]()

const attributeWritesDoc = "Attribute activity on an entity (a host, node, broker, or volume) to the groups responsible for it. Returns a breakdown of key/lines/pct with anomaly detection (KL divergence, MI, and an anomalous flag) comparing the selector window to the window immediately before it. Groups are resolved from a placement stream (scheduler pod-to-host by default, overridable); narrow the attributed activity with selector.where."

var attributeWritesSchema = inferSchema[tools.AttributeWritesRequest]()

const rcaWalkDoc = "Investigate a symptom using candidate time-series correlations and supporting topology, entity ownership or attribution. Chain contains admitted direct candidates; Path is a transitive route assembled from supported candidate relationships. Rejected reports candidates excluded by the evidence checks. Patterns maps every named pattern_id to its text. Temporal association is not proof of causality. Requires a bounded time window; limits and coverage describe the selected work."

var rcaWalkSchema = inferSchema[tools.RCAWalkRequest]()

const listSLOsDoc = "List every SLO defined in the definitions directory with its state (ok, firing, no_data, unbound, or partial while its window is still being counted), since when, its SLI, budget remaining, burn rate over 1h and 6h and coverage, firing first, then pending, then by budget remaining. A grouped SLO lists each group's own. state: firing answers which SLOs are firing in one call. Status is computed at the end of the minute of the snapshot's newest line, and meta.definitions names the definitions it read."

var listSLOsSchema = inferSchema[tools.ListSLOsRequest](setEnum("state", tools.SLOStates))

const sloStatusDoc = "Return one SLO's page, or one of its groups': its status, each alert condition's state, burn rates and since, its groups, the SLI, budget remaining and each condition's burn rate at every minute of time_range (every hour past the fine horizon), its window to the snapshot's newest line by default, and bad_events, the selector of its bad events to open in search_logs."

var sloStatusSchema = inferSchema[tools.SLOStatusRequest]()

const importDefinitionsDoc = "Import OpenSLO v1 documents as YAML or JSON: each is normalized, a Loki-sourced SLI (type grafanaLoki or loki with spec.logql) converted to a DataFocal one with its rewrites noted, and classified as new, changed, unchanged, unbound (any other source, kept verbatim and never evaluated) or invalid with the field at fault, with its status over the last 24 hours. With dry_run false and a writable definitions directory it saves them as well; content returns the normalized file to commit otherwise."

var importDefinitionsSchema = inferSchema[tools.ImportDefinitionsRequest]()

const exportDefinitionsDoc = "Export the definitions named as kind/name, such as View/checkout, or by a name of any kind, or every one, normalized, as one multi-document YAML file or a JSON array. Importing the export classifies every document unchanged."

var exportDefinitionsSchema = inferSchema[tools.ExportDefinitionsRequest]()

const listViewsDoc = "List the saved views in the definitions directory by name, each with its title and panel count, and writable, whether import_definitions can save into the directory. query keeps the views whose name or title contains it."

var listViewsSchema = inferSchema[tools.ListViewsRequest]()

const getViewDoc = "Return one saved view: its normalized document, and the view compiled with its variables and each panel's kind, grid cell and canonical query. Calling query with a panel's query and the view's variables in vars reproduces that panel."

var getViewSchema = inferSchema[tools.GetViewRequest]()
