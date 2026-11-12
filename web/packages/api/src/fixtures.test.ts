import { expect, test } from "vitest";
import aggregate from "../fixtures/aggregate.golden.json";
import attributeWrites from "../fixtures/attribute_writes.golden.json";
import batch from "../fixtures/batch.golden.json";
import coOccurrence from "../fixtures/co_occurrence.golden.json";
import correlate from "../fixtures/correlate.golden.json";
import explainSpike from "../fixtures/explain_spike.golden.json";
import findAnomalies from "../fixtures/find_anomalies.golden.json";
import focus from "../fixtures/focus.golden.json";
import funnel from "../fixtures/funnel.golden.json";
import searchLogs from "../fixtures/search_logs.golden.json";
import rankFields from "../fixtures/rank_fields.golden.json";
import query from "../fixtures/query.golden.json";
import listPatterns from "../fixtures/list_patterns.golden.json";
import getTrace from "../fixtures/get_trace.golden.json";
import traceShape from "../fixtures/trace_shape.golden.json";
import serviceGraph from "../fixtures/service_graph.golden.json";
import rcaWalk from "../fixtures/rca_walk.golden.json";
import resourceUsage from "../fixtures/resource_usage.golden.json";
import listSLOs from "../fixtures/list_slos.golden.json";
import sloStatus from "../fixtures/slo_status.golden.json";
import importDefinitions from "../fixtures/import_definitions.golden.json";
import exportDefinitions from "../fixtures/export_definitions.golden.json";
import listViews from "../fixtures/list_views.golden.json";
import getView from "../fixtures/get_view.golden.json";
import type { Responses, ToolName } from "./client";
import type { BatchResponse } from "./generated/batch";
import type { Envelope } from "./generated/query";

// Each golden must type-check as its tool's generated response, so a Go-side rename fails here.
const fixtures: { [T in ToolName]: Envelope<Responses[T]> } = {
  aggregate,
  attribute_writes: attributeWrites,
  co_occurrence: coOccurrence,
  correlate,
  explain_spike: explainSpike,
  find_anomalies: findAnomalies,
  focus,
  funnel,
  search_logs: searchLogs,
  rank_fields: rankFields,
  query,
  list_patterns: listPatterns,
  get_trace: getTrace,
  trace_shape: traceShape,
  service_graph: serviceGraph,
  rca_walk: rcaWalk,
  resource_usage: resourceUsage,
  list_slos: listSLOs,
  slo_status: sloStatus,
  import_definitions: importDefinitions,
  export_definitions: exportDefinitions,
  list_views: listViews,
  get_view: getView,
};

// batch answers each call's envelope or error rather than an envelope of its own.
const batched: BatchResponse = batch;

test("every tool's golden is a complete answer", () => {
  for (const [tool, env] of Object.entries(fixtures)) {
    expect(env.meta.coverage, tool).toBe(1);
  }
  expect(batched.results.map((r) => r.index)).toEqual([0, 1, 2]);
});
