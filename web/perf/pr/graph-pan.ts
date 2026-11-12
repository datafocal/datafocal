import { readFile } from "node:fs/promises";
import { stripTypeScriptTypes } from "node:module";
import { expect, type Page } from "@playwright/test";
import { graph } from "../gen";
import type { Scenario } from "../harness";

const src = new URL("../../packages/render-graph/src/", import.meta.url);

// serveRenderer answers render-graph/<module> with that module's source, its types stripped,
// so the page can run the renderer before any view mounts it.
export async function serveRenderer({ page }: { page: Page }): Promise<void> {
  await page.route("**/render-graph/*", async (route) => {
    const name = new URL(route.request().url()).pathname.split("/").pop() ?? "";
    const ts = await readFile(new URL(`${name}.ts`, src), "utf8");
    await route.fulfill({ contentType: "text/javascript", body: stripTypeScriptTypes(ts) });
  });
}

// fixture is the 100k-service graph on a 400 by 250 grid as x, y, r and level, with a group
// over each 10 by 10 block and a domain over each 80 by 50; groups' edges bundle services'.
function fixture() {
  const g = graph(1e5, 1);
  const block = (i: number, cols: number, w: number, h: number, r: number, level: number) => [
    (i % cols) * w + (w - 1) / 2,
    Math.floor(i / cols) * h + (h - 1) / 2,
    r,
    level,
  ];
  const nodes = [
    ...g.nodes.map((_, i) => block(i, 400, 1, 1, 0.4, 2)),
    ...Array.from({ length: 1000 }, (_, i) => block(i, 40, 10, 10, 4, 1)),
    ...Array.from({ length: 25 }, (_, i) => block(i, 5, 80, 50, 20, 0)),
  ];
  const group = (i: number) => 1e5 + Math.floor(i / 4000) * 40 + Math.floor((i % 400) / 10);
  const edges = g.edges.map((e) => [Number(e.from.slice(8)), Number(e.to.slice(8))]);
  const bundles = new Set(edges.map(([a = 0, b = 0]) => `${group(a)} ${group(b)}`));
  return { nodes, edges: [...edges, ...[...bundles].map((p) => p.split(" ").map(Number))] };
}

// pan draws the fixture for ten seconds while panning across it and zooming from all of it
// down to a few services and back every five, and checks every frame's counts.
export const pan: Omit<Scenario, "name"> = {
  path: "./",
  run: async (page) => {
    const most = await page.evaluate(async (g) => {
      const module = "./render-graph/index";
      const { createGraphRenderer } = await import(module);
      const canvas = document.body.appendChild(document.createElement("canvas"));
      canvas.setAttribute("style", "position: fixed; inset: 0; width: 100%; height: 100%");
      const renderer = createGraphRenderer(canvas.transferControlToOffscreen());
      const [width, height] = [innerWidth, innerHeight];
      renderer.resize(width, height, devicePixelRatio);
      const [n, e] = [g.nodes.length, g.edges.length];
      const node = (k: number) => g.nodes.map((v) => v[k] ?? 0);
      const edge = (k: number) => g.edges.map((v) => v[k] ?? 0);
      const model = {
        nodes: {
          x: Float32Array.from(node(0)),
          y: Float32Array.from(node(1)),
          r: Float32Array.from(node(2)),
          kind: Uint8Array.from(node(3)),
          health: Uint8Array.from({ length: n }, (_, i) => i % 3),
          members: Uint32Array.from(node(3), (k) => [4000, 100, 1][k] ?? 1),
        },
        edges: {
          from: Uint32Array.from(edge(0)),
          to: Uint32Array.from(edge(1)),
          weight: Float32Array.from({ length: e }, (_, i) => (i % 10) / 10),
          method: Uint8Array.from({ length: e }, (_, i) => i % 4),
          confidence: Float32Array.from({ length: e }, (_, i) => 0.3 + (i % 8) / 10),
          flags: Uint8Array.from({ length: e }, (_, i) => i % 4),
        },
        labels: Array.from({ length: n }, (_, i) => `node-${i}`),
      };
      const most = { drawCalls: 0, primitives: 0 };
      const start = performance.now();
      await new Promise<void>((done) => {
        const frame = (t: number) => {
          const s = (t - start) / 1000;
          const span = 400 * 80 ** -((1 - Math.cos((2 * Math.PI * s) / 5)) / 2);
          const [cx, cy] = [200 + 150 * Math.sin(s / 2), 125 + 100 * Math.sin(s / 3)];
          const [hx, hy] = [span / 2, (span * height) / width / 2];
          const view = {
            x0: cx - hx,
            x1: cx + hx,
            y0: cy - hy,
            y1: cy + hy,
            width,
            height,
            dpr: devicePixelRatio,
          };
          const stats = renderer.render(model, view);
          performance.mark("draw", { detail: stats });
          most.drawCalls = Math.max(most.drawCalls, stats.drawCalls);
          most.primitives = Math.max(most.primitives, stats.primitives);
          s < 10 ? requestAnimationFrame(frame) : done();
        };
        requestAnimationFrame(frame);
      });
      return most;
    }, fixture());
    // The graph's edges, nodes and labels are a draw call each, as are the minimap's nodes
    // and edges; nodes drawn are held to maxNodes.
    expect(most.drawCalls).toBeLessThanOrEqual(5);
    expect(most.primitives).toBeLessThanOrEqual(420);
  },
};
