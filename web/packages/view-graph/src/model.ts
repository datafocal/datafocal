import type { GraphEdge, RCAHop, ServiceGraphResponse } from "@datafocal/api";
import {
  type EdgeLabel,
  Graph,
  type GraphLabel,
  layout as dagre,
  type NodeLabel,
  type Point,
} from "@dagrejs/dagre";
import type { GraphModel } from "@datafocal/render-graph";

// Laid is a service graph laid out for render-graph, with each model node's ID, and each
// edge's route and the centre of its label.
export interface Laid {
  readonly model: GraphModel;
  readonly ids: readonly string[];
  readonly routes: readonly (readonly Point[])[];
  readonly marks: readonly Point[];
}

const methods = ["co-occurrence", "correlation", "topology"];
// A node's box holds its disc and the two lines of label under it, as wide as the wider at
// most each character's width, so the layout keeps edges and other nodes off its label.
const [boxWidth, nameChar, statChar, labelHeight, rowHeight] = [100, 7.5, 6.5, 36, 80];
// edgeLabel is the room a call edge's two lines of label are given, and labelLine a line's.
const edgeLabel = { width: 130, height: 28, labelpos: "c" as const };
export const labelLine = 13;
// A node's radius grows with the square root of its lines, from least to most.
const [least, most] = [9, 22];

// fits reports whether an edge label's lines fit the room the layout gave it.
export const fits = (lines: readonly string[]): boolean =>
  lines.every((l) => l.length * statChar <= edgeLabel.width);

const short = (n: number) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}k` : String(n);

// stat is what a node logged in the window: its lines and, when any, its errors' share.
export function stat(lines: number, errors = 0): string {
  const share = errors >= 0.001 ? ` · ${(errors * 100).toFixed(errors < 0.1 ? 1 : 0)}% errors` : "";
  return `${short(lines)} ${lines === 1 ? "line" : "lines"}${share}`;
}

// linked is the nodes an edge touches, and the focus.
function linked(g: ServiceGraphResponse, focus: string | undefined): Set<string> {
  return new Set([...g.edges.flatMap((e) => [e.from, e.to]), ...(focus ? [focus] : [])]);
}

// route runs an edge from disc a out of its box level with the disc, so above its label, then
// along the layout's points and into disc b's box the same way; aHalf and bHalf are the
// boxes' half widths.
function route(points: readonly Point[], [a, aHalf]: [Point, number], [b, bHalf]: [Point, number]) {
  const inner = points.slice(1, -1);
  const port = (disc: Point, half: number, toward: Point) => ({
    x: disc.x + Math.sign(toward.x - disc.x) * half,
    y: disc.y,
  });
  return [a, port(a, aHalf, inner[0] ?? b), ...inner, port(b, bHalf, inner.at(-1) ?? a), b];
}

// place lays the linked nodes out in layers left to right, callers before callees, each in a
// box holding its label, and the rest in a grid under them, busiest first.
function place(
  g: ServiceGraphResponse,
  ids: readonly string[],
  focus: string | undefined,
  { radius, lines }: Record<"radius" | "lines", (id: string) => number>,
) {
  const links = linked(g, focus);
  const d = new Graph<GraphLabel, NodeLabel, EdgeLabel>({ multigraph: true });
  d.setGraph({ rankdir: "LR", nodesep: 20, ranksep: 40 });
  const byId = new Map(g.nodes.map((n) => [n.id, n]));
  const width = (id: string) => {
    const n = byId.get(id);
    const name = `${n?.group ?? id}${(n?.members ?? 1) > 1 ? ` (${n?.members})` : ""}`;
    return Math.max(
      boxWidth,
      name.length * nameChar,
      stat(lines(id), n?.error_ratio).length * statChar,
    );
  };
  for (const id of ids.filter((i) => links.has(i))) {
    d.setNode(id, { width: width(id), height: 2 * radius(id) + labelHeight });
  }
  g.edges.forEach((e, i) =>
    d.setEdge(e.from, e.to, e.method === "call" ? { ...edgeLabel } : {}, `${i}`),
  );
  dagre(d);
  const at = new Map<string, Point>();
  for (const id of d.nodes()) {
    const n = d.node(id);
    at.set(id, { x: n.x ?? 0, y: (n.y ?? 0) - labelHeight / 2 });
  }
  const end = (id: string): [Point, number] => [at.get(id) ?? { x: 0, y: 0 }, width(id) / 2];
  const routes = g.edges.map((e, i) =>
    route(d.edge(e.from, e.to, `${i}`)?.points ?? [], end(e.from), end(e.to)),
  );
  const marks = g.edges.map((e, i) => {
    const { x = 0, y = 0 } = d.edge(e.from, e.to, `${i}`) ?? {};
    return { x, y };
  });
  const alone = ids.filter((i) => !links.has(i)).sort((a, b) => lines(b) - lines(a));
  const across = Math.max(1, Math.ceil(Math.sqrt(alone.length)));
  const top = (d.graph().height ?? 0) + rowHeight;
  alone.forEach((id, k) => {
    at.set(id, { x: (k % across) * boxWidth, y: top + Math.floor(k / across) * rowHeight });
  });
  return { at, routes, marks };
}

// layout lays g out in layers left to right, callers before callees, so the same graph is
// always drawn the same, with nodes no edge links in a grid under them. Every node is drawn
// at level, so render-graph draws them all, as big as its share of lines says.
export function layout(
  g: ServiceGraphResponse,
  level: number,
  focus?: string,
  lines: (id: string) => number = () => 0,
): Laid {
  const ids = g.nodes.map((n) => n.id).sort();
  const busiest = Math.max(...ids.map(lines), 1);
  const radius = (id: string) => least + (most - least) * Math.sqrt(lines(id) / busiest);
  const { at, routes, marks } = place(g, ids, focus, { radius, lines });
  const pos = ids.map((id) => at.get(id) ?? { x: 0, y: 0 });
  const byId = new Map(g.nodes.map((n) => [n.id, n]));
  const index = new Map(ids.map((id, i) => [id, i]));
  const heaviest = Math.max(...g.edges.map((e) => e.weight), 1e-9);
  const model: GraphModel = {
    nodes: {
      x: Float32Array.from(pos, (p) => p.x),
      y: Float32Array.from(pos, (p) => p.y),
      r: Float32Array.from(ids, radius),
      kind: new Uint8Array(ids.length).fill(level),
      health: Uint8Array.from(ids, (id) => health(byId.get(id)?.error_ratio)),
      members: Uint32Array.from(ids, (id) => byId.get(id)?.members ?? 1),
    },
    edges: {
      from: Uint32Array.from(g.edges, (e) => index.get(e.from) ?? 0),
      to: Uint32Array.from(g.edges, (e) => index.get(e.to) ?? 0),
      weight: Float32Array.from(g.edges, (e) => e.weight / heaviest),
      // A bundled edge's methods are joined sorted; it is drawn as the first.
      method: Uint8Array.from(g.edges, (e) =>
        Math.max(0, methods.indexOf(e.method.split(",")[0] ?? "")),
      ),
      confidence: Float32Array.from(g.edges, (e) => e.confidence),
      flags: new Uint8Array(g.edges.length),
    },
    labels: ids.map((id) => byId.get(id)?.group ?? id),
  };
  return { model, ids, routes, marks };
}

// health is the level an error ratio warns at: 0 none, 1 above 1%, 2 above 5%.
export const health = (ratio = 0): number => (ratio > 0.05 ? 2 : ratio > 0.01 ? 1 : 0);

// search returns the first ten names containing q, ignoring case.
export function search(names: readonly string[], q: string): string[] {
  const want = q.toLowerCase();
  return want === "" ? [] : names.filter((n) => n.toLowerCase().includes(want)).slice(0, 10);
}

const key = (e: { from: string; to: string }) => `${e.from} → ${e.to}`;

// Changes are the edges a window has and its baseline lacks, and the reverse.
export interface Changes {
  readonly added: readonly GraphEdge[];
  readonly removed: readonly GraphEdge[];
}

export function diff(now: ServiceGraphResponse, base: ServiceGraphResponse): Changes {
  const has = (g: ServiceGraphResponse) => new Set(g.edges.map(key));
  const [inNow, inBase] = [has(now), has(base)];
  return {
    added: now.edges.filter((e) => !inBase.has(key(e))),
    removed: base.edges.filter((e) => !inNow.has(key(e))),
  };
}

// causeOf is hop's edge from its cause to the node it explains.
const causeOf = (h: RCAHop) => ({ from: h.to, to: h.from });

// withAll is g plus the removed edges and an edge for each hop between nodes g does not
// link, with any node those edges need, so the chain and the diff can both be drawn.
export function withAll(
  g: ServiceGraphResponse,
  hops: readonly RCAHop[],
  removed: readonly GraphEdge[],
): ServiceGraphResponse {
  const edges = [...g.edges, ...removed];
  const linked = new Set(edges.flatMap((e) => [key(e), key({ from: e.to, to: e.from })]));
  for (const h of hops) {
    if (h.from !== h.to && !linked.has(key(h))) {
      linked.add(key(h)).add(key(causeOf(h)));
      edges.push({ ...causeOf(h), method: "correlation", weight: 1, confidence: Math.abs(h.rho) });
    }
  }
  const ids = new Set(g.nodes.map((n) => n.id));
  const missing = new Set(edges.flatMap((e) => [e.from, e.to]).filter((id) => !ids.has(id)));
  return { nodes: [...g.nodes, ...[...missing].map((id) => ({ id, kind: "workload" }))], edges };
}

// Shown is a service graph and its layout.
export interface Shown {
  readonly graph: ServiceGraphResponse;
  readonly laid: Laid;
}

// ghost is render-graph's ghostFlag, and dim the health it draws faded.
const [ghost, dim] = [1, 3];

// overlay draws the lit hops of the chain over shown: their causes numbered in hop order
// and everything off the chain dimmed. Removed edges are ghosts, and a node an edge was
// added to or removed from is marked Δ.
export function overlay(
  { graph, laid }: Shown,
  hops: readonly RCAHop[],
  lit: (h: RCAHop) => boolean,
  changes: Changes,
): Shown {
  const { model, ids } = laid;
  const on = hops.filter(lit);
  const nodes = new Set(on.flatMap((h) => [h.from, h.to]));
  const chain = new Set(on.flatMap((h) => [key(h), key(causeOf(h))]));
  const gone = new Set(changes.removed.map(key));
  const changed = new Set([...changes.added, ...changes.removed].flatMap((e) => [e.from, e.to]));
  const off = (inChain: boolean) => hops.length > 0 && !inChain;
  const numbers = (id: string) =>
    hops.flatMap((h, i) => (lit(h) && h.to === id ? [i + 1] : [])).join(",");
  return {
    graph,
    laid: {
      ...laid,
      model: {
        ...model,
        nodes: {
          ...model.nodes,
          health: model.nodes.health.map((h, i) => (off(nodes.has(ids[i] ?? "")) ? dim : h)),
        },
        edges: {
          ...model.edges,
          flags: Uint8Array.from(graph.edges, (e) =>
            gone.has(key(e)) || off(chain.has(key(e))) ? ghost : 0,
          ),
        },
        labels: model.labels.map((l, i) => {
          const id = ids[i] ?? "";
          const n = numbers(id);
          return `${n && `${n}. `}${l}${changed.has(id) ? " Δ" : ""}`;
        }),
      },
    },
  };
}
