import type {
  GraphEdge,
  LogLine,
  SearchResponse,
  ServiceGraphResponse,
  Span,
  TraceResponse,
} from "../packages/api/src";

// The scale datasets the performance budgets run on, generated at test time from a seed:
// the same seed gives the same dataset. Each is typed as the response it stands in for.

const t0 = Date.UTC(2026, 0, 1);
const levels = ["debug", "info", "warn", "error"];

// rng is mulberry32: a deterministic source of numbers in [0, 1).
function rng(seed: number): () => number {
  let s = seed;
  return () => {
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
  };
}

// logs returns n lines, one a millisecond, from 50 services; logs(1e6, seed) is the 1M-row table.
export function logs(n: number, seed: number): SearchResponse {
  const r = rng(seed);
  const lines = Array.from({ length: n }, (_, row): LogLine => {
    const ms = Math.floor(r() * 1000);
    return {
      timestamp: new Date(t0 + row).toISOString(),
      level: levels[Math.floor(r() * levels.length)] ?? "info",
      message: `GET /items/${row} 200 ${ms} ms`,
      pattern_id: `pattern-${ms % 100}`,
      service: `service-${Math.floor(r() * 50)}`,
      row,
    };
  });
  return { mode: "lines", lines };
}

function span(i: number, parent: number, service: string, start: number, dur: number): Span {
  return {
    name: `step ${i}`,
    service,
    start_ms: t0 + start,
    dur_ms: dur,
    error: false,
    parent,
    id: `span-${i}`,
    parent_method: parent < 0 ? "" : "contained",
    duration_method: "reported",
    lines: 1,
    where: [],
    self_ms: dur,
    critical: false,
    critical_inferred: false,
  };
}

// trace returns a trace of n spans across 20 services, each inside a random earlier span:
// trace(1e4, seed) and trace(1e6, seed) are the 10k- and 1M-span traces.
export function trace(n: number, seed: number): TraceResponse {
  const r = rng(seed);
  const root = span(0, -1, "service-0", 0, 60_000);
  const spans = [root];
  for (let i = 1; i < n; i++) {
    const parent = Math.floor(r() * i);
    const p = spans[parent] ?? root;
    const start = p.start_ms - t0 + Math.floor(r() * p.dur_ms);
    const dur = Math.floor(r() * (p.start_ms - t0 + p.dur_ms - start));
    spans.push(span(i, parent, `service-${Math.floor(r() * 20)}`, start, dur));
  }
  return { field: "request_id", value: `req-${seed}`, spans, total: n };
}

// run returns steps back-to-back steps spread over lanes, each lane a service: run(5000, 4,
// seed) is the 5k-step, 4-lane run. No generated type describes a run, so it is a trace.
export function run(steps: number, lanes: number, seed: number): TraceResponse {
  const r = rng(seed);
  const ends = new Array<number>(lanes).fill(0);
  const spans = Array.from({ length: steps }, (_, i) => {
    const lane = Math.floor(r() * lanes);
    const start = ends[lane] ?? 0;
    const dur = 1 + Math.floor(r() * 2000);
    ends[lane] = start + dur;
    return span(i, -1, `lane-${lane}`, start, dur);
  });
  return { field: "run_id", value: `run-${seed}`, spans, total: steps };
}

// graph returns n services each calling up to three random others; graph(1e5, seed) is the
// 100k-service graph.
export function graph(n: number, seed: number): ServiceGraphResponse {
  const r = rng(seed);
  const nodes = Array.from({ length: n }, (_, i) => ({ id: `service-${i}`, kind: "workload" }));
  const edges: GraphEdge[] = [];
  for (let i = 0; i < n; i++) {
    for (let k = Math.floor(r() * 4); k > 0; k--) {
      const to = `service-${Math.floor(r() * n)}`;
      edges.push({ from: `service-${i}`, to, method: "topology", weight: r(), confidence: 1 });
    }
  }
  return { nodes, edges };
}
