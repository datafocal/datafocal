import { expect, test } from "vitest";
import {
  asyncFlag,
  drawList,
  edgeStride,
  type GraphModel,
  ghostFlag,
  hitGrid,
  maxNodes,
  nodeStride,
} from "./lod";

// model has n nodes of the given kinds placed by at, and the given edges.
function model(
  kinds: number[],
  at: (i: number) => [number, number, number],
  edges: number[][] = [],
): GraphModel {
  const n = kinds.length;
  const col = (k: number) => edges.map((e) => e[k] ?? 0);
  return {
    nodes: {
      x: Float32Array.from({ length: n }, (_, i) => at(i)[0]),
      y: Float32Array.from({ length: n }, (_, i) => at(i)[1]),
      r: Float32Array.from({ length: n }, (_, i) => at(i)[2]),
      kind: Uint8Array.from(kinds),
      health: Uint8Array.from(kinds, (_, i) => i % 3),
      members: Uint32Array.from(kinds, () => 1),
    },
    edges: {
      from: Uint32Array.from(col(0)),
      to: Uint32Array.from(col(1)),
      weight: Float32Array.from(col(2)),
      method: Uint8Array.from(col(3)),
      confidence: Float32Array.from(col(4)),
      flags: Uint8Array.from(col(5)),
    },
    labels: kinds.map((_, i) => `node-${i}`),
  };
}

const view = { x0: 0, x1: 100, y0: 0, y1: 100, width: 400, height: 400, dpr: 1 };
const rows = (a: Float32Array | Uint32Array, stride: number) =>
  Array.from({ length: a.length / stride }, (_, i) =>
    [...a.subarray(i * stride, (i + 1) * stride)].join(" "),
  );

test("each edge method and flag draws as its golden", async () => {
  const edges = [0, 1, 2, 3].flatMap((method) =>
    [0, ghostFlag, asyncFlag, ghostFlag | asyncFlag].map((flags) => [
      method + 1,
      5 + flags,
      0.5,
      method,
      0.8,
      flags,
    ]),
  );
  const m = model(
    [0, 2, 2, 2, 2, 2, 2, 2, 2],
    (i) => (i === 0 ? [50, 50, 50] : [i * 10, 20 + (i % 3) * 30, 3]),
    edges,
  );
  const list = drawList(m, view, 1, false);
  const golden = [
    `index ${[...list.index].join(" ")}`,
    `labels ${[...list.labels].join(" ")}`,
    "nodes",
  ];
  golden.push(...rows(list.nodes, nodeStride), "edges", ...rows(list.edges, edgeStride));
  golden.push(
    "minimap",
    ...rows(list.minimap.nodes, nodeStride),
    ...rows(list.minimap.edges, edgeStride),
    "",
  );
  await expect(golden.join("\n")).toMatchFileSnapshot("testdata/draw.golden");
});

test("edges hide while panning", () => {
  const m = model([0, 2, 2], (i) => [i * 40, 50, 5], [[1, 2, 1, 0, 1, 0]]);
  expect(drawList(m, view, null, false).edges.length).toBe(edgeStride);
  expect(drawList(m, view, null, true).edges.length).toBe(0);
});

test("zooming in steps down a level while it stays under maxNodes", () => {
  // 25 domains over 10,000 services on a 100 by 100 grid.
  const kinds = [...Array<number>(25).fill(0), ...Array<number>(10_000).fill(2)];
  const m = model(kinds, (i) =>
    i < 25
      ? [(i % 5) * 20 + 10, Math.floor(i / 5) * 20 + 10, 10]
      : [(i - 25) % 100, Math.floor((i - 25) / 100), 0.4],
  );
  const drawn = (span: number) => drawList(m, { ...view, x1: span, y1: span }, null, false);
  expect(drawn(100).nodes[4]).toBe(0);
  expect(drawn(100).index.length).toBe(25);
  expect(drawn(15).nodes[4]).toBe(2);
  expect(drawn(15).index.length).toBeLessThanOrEqual(maxNodes);
});

test("hit-testing matches brute force over 10,000 random points", () => {
  let seed = 1;
  const random = () => {
    seed = (seed * 16807) % 2147483647;
    return seed / 2147483647;
  };
  const m = model(Array<number>(maxNodes).fill(2), () => [
    random() * 100,
    random() * 100,
    random() * 3,
  ]);
  const list = drawList(m, view, null, false);
  const hit = hitGrid(list, view.width, view.height);
  const points = Array.from({ length: 10_000 }, () => [random() * 400, random() * 400] as const);
  const brute = ([x, y]: readonly [number, number]) => {
    let found = -1;
    for (let s = 0; s < list.index.length; s++) {
      const [nx, ny, r] = list.nodes.subarray(s * nodeStride, s * nodeStride + 3);
      found = Math.hypot(x - (nx ?? 0), y - (ny ?? 0)) <= (r ?? 0) ? (list.index[s] ?? -1) : found;
    }
    return found;
  };
  expect(points.map(([x, y]) => hit(x, y))).toEqual(points.map(brute));
});
