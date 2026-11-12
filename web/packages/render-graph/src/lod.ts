import type { Viewport } from "@datafocal/render-core";

// GraphModel is a laid-out service graph. A node's kind is its level, 0 domain, 1 group,
// 2 service, 3 workload or 4 host; members is how many nodes it collapses.
export interface GraphModel {
  nodes: {
    x: Float32Array;
    y: Float32Array;
    r: Float32Array;
    kind: Uint8Array;
    health: Uint8Array;
    members: Uint32Array;
  };
  // method is 0 trace co-occurrence, 1 temporal correlation, 2 placement or topology, 3 write
  // or resource attribution; flags are ghostFlag and asyncFlag bits.
  edges: {
    from: Uint32Array;
    to: Uint32Array;
    weight: Float32Array;
    method: Uint8Array;
    confidence: Float32Array;
    flags: Uint8Array;
  };
  labels: readonly string[];
}

export const ghostFlag = 1;
export const asyncFlag = 2;

// maxNodes is the most nodes a frame draws, the graph pan-and-zoom budget.
export const maxNodes = 420;
export const nodeStride = 5;
export const edgeStride = 8;
const levels = 5;
const labelRadius = 16;
// fewLabels is how many nodes a frame can label every one of and stay readable.
const fewLabels = 60;
const minimapSize = 128;
const host = 4;
const panel = 3;

// DrawList is what a frame draws, in CSS pixels. Nodes are x, y, r, health and kind, edges
// x0, y0, x1, y1, width, alpha, method and flags, and labels the slots of labelled nodes.
export interface DrawList {
  nodes: Float32Array;
  index: Uint32Array;
  edges: Float32Array;
  labels: Uint32Array;
  minimap: { nodes: Float32Array; edges: Float32Array };
}

const get = (a: ArrayLike<number>, i: number) => a[i] ?? 0;

// Projection places model coordinates on the canvas, in CSS pixels.
interface Projection {
  x(x: number): number;
  y(y: number): number;
  r(r: number): number;
  inView(m: GraphModel, i: number): boolean;
}

function project(v: Viewport): Projection {
  const sx = v.width / (v.x1 - v.x0);
  const sy = v.height / (v.y1 - v.y0);
  const p: Projection = {
    x: (x) => (x - v.x0) * sx,
    y: (y) => (y - v.y0) * sy,
    r: (r) => r * sx,
    inView: (m, i) => {
      const [x, y, r] = [p.x(get(m.nodes.x, i)), p.y(get(m.nodes.y, i)), p.r(get(m.nodes.r, i))];
      return x + r >= 0 && x - r <= v.width && y + r >= 0 && y - r <= v.height;
    },
  };
  return p;
}

// level is the deepest level with nodes in view but no more than maxNodes of them, so
// zooming in steps from domains down to hosts.
function level(m: GraphModel, p: Projection): number {
  const counts = new Uint32Array(levels);
  for (let i = 0; i < m.nodes.kind.length; i++) {
    if (p.inView(m, i)) {
      const k = get(m.nodes.kind, i);
      counts[k] = get(counts, k) + 1;
    }
  }
  let l = levels - 1;
  while (l > 0 && (get(counts, l) === 0 || get(counts, l) > maxNodes)) {
    l--;
  }
  return l;
}

function collectEdges(m: GraphModel, p: Projection, slots: Map<number, number>, l: number) {
  const { from, to, weight, method, confidence, flags } = m.edges;
  const { x, y, kind } = m.nodes;
  const out: number[] = [];
  for (let e = 0; e < from.length; e++) {
    const [a, b] = [get(from, e), get(to, e)];
    if (kind[a] === l && kind[b] === l && (slots.has(a) || slots.has(b))) {
      out.push(p.x(get(x, a)), p.y(get(y, a)), p.x(get(x, b)), p.y(get(y, b)));
      out.push(1 + 3 * Math.min(get(weight, e), 1), get(confidence, e), get(method, e));
      out.push(get(flags, e));
    }
  }
  return out;
}

// neighbours is the focus and the nodes it shares an edge with.
function neighbours(m: GraphModel, focus: number | null): Set<number> {
  const out = new Set<number>(focus === null ? [] : [focus]);
  const { from, to } = m.edges;
  for (let e = 0; focus !== null && e < from.length; e++) {
    if (from[e] === focus || to[e] === focus) {
      out.add(from[e] === focus ? get(to, e) : get(from, e));
    }
  }
  return out;
}

// minimap draws the domains and the view into a square in the bottom-right corner.
function minimap(m: GraphModel, v: Viewport): DrawList["minimap"] {
  const { x, y, r, kind, health } = m.nodes;
  const domains: number[] = [];
  kind.forEach((k, i) => {
    if (k === 0) {
      domains.push(i);
    }
  });
  if (domains.length === 0) {
    return { nodes: new Float32Array(), edges: new Float32Array() };
  }
  const x0 = Math.min(...domains.map((i) => get(x, i) - get(r, i)));
  const y0 = Math.min(...domains.map((i) => get(y, i) - get(r, i)));
  const x1 = Math.max(...domains.map((i) => get(x, i) + get(r, i)));
  const y1 = Math.max(...domains.map((i) => get(y, i) + get(r, i)));
  const k = minimapSize / Math.max(x1 - x0, y1 - y0);
  const [left, top] = [v.width - minimapSize - 8, v.height - minimapSize - 8];
  const [mx, my] = [(px: number) => left + (px - x0) * k, (py: number) => top + (py - y0) * k];
  const nodes = [left + minimapSize / 2, top + minimapSize / 2, minimapSize / 2, panel, host];
  for (const i of domains) {
    nodes.push(mx(get(x, i)), my(get(y, i)), Math.max(get(r, i) * k, 1.5), get(health, i), 0);
  }
  const [a, b, c, d] = [mx(v.x0), my(v.y0), mx(v.x1), my(v.y1)];
  const edges = [a, b, c, b, 1, 1, 0, 0, c, b, c, d, 1, 1, 0, 0];
  edges.push(c, d, a, d, 1, 1, 0, 0, a, d, a, b, 1, 1, 0, 0);
  return { nodes: Float32Array.from(nodes), edges: Float32Array.from(edges) };
}

// drawList is what to draw of m in v. Edges hide while panning, and labels show on every
// node when there are few, else only on nodes drawn large enough to read or on the focus
// and its neighbours.
export function drawList(
  m: GraphModel,
  v: Viewport,
  focus: number | null,
  panning: boolean,
): DrawList {
  const p = project(v);
  const l = level(m, p);
  const nodes: number[] = [];
  const slots = new Map<number, number>();
  for (let i = 0; i < m.nodes.kind.length && slots.size < maxNodes; i++) {
    if (m.nodes.kind[i] === l && p.inView(m, i)) {
      slots.set(i, slots.size);
      const { x, y, r, health } = m.nodes;
      nodes.push(p.x(get(x, i)), p.y(get(y, i)), p.r(get(r, i)), get(health, i), l);
    }
  }
  const edges = panning ? [] : collectEdges(m, p, slots, l);
  const near = neighbours(m, focus);
  const labels = [...slots].filter(
    ([i, s]) =>
      slots.size <= fewLabels || near.has(i) || get(nodes, s * nodeStride + 2) >= labelRadius,
  );
  return {
    nodes: Float32Array.from(nodes),
    index: Uint32Array.from(slots.keys()),
    edges: Float32Array.from(edges),
    labels: Uint32Array.from(labels, ([, s]) => s),
    minimap: minimap(m, v),
  };
}

const cell = 32;

// hitGrid indexes list's model nodes on a uniform grid of the width by height canvas. Its
// result finds the topmost such node whose disc holds a point, as a model index, or -1.
export function hitGrid(
  list: DrawList,
  width: number,
  height: number,
): (x: number, y: number) => number {
  const cols = Math.ceil(width / cell);
  const cells = Array.from({ length: cols * Math.ceil(height / cell) }, (): number[] => []);
  const at = (s: number, k: number) => get(list.nodes, s * nodeStride + k);
  const first = (c: number) => Math.max(0, Math.floor(c / cell));
  for (let s = 0; s < list.index.length; s++) {
    const [x, y, r] = [at(s, 0), at(s, 1), at(s, 2)];
    const [cx1, cy1] = [Math.min(cols - 1, Math.floor((x + r) / cell)), Math.floor((y + r) / cell)];
    for (let cy = first(y - r); cy <= cy1; cy++) {
      for (let cx = first(x - r); cx <= cx1; cx++) {
        cells[cy * cols + cx]?.push(s);
      }
    }
  }
  return (x, y) => {
    const inside = x >= 0 && x < width;
    const found = (inside && cells[Math.floor(y / cell) * cols + Math.floor(x / cell)]) || [];
    let hit = -1;
    for (const s of found) {
      hit = Math.hypot(x - at(s, 0), y - at(s, 1)) <= at(s, 2) ? s : hit;
    }
    return hit < 0 ? -1 : get(list.index, hit);
  };
}
