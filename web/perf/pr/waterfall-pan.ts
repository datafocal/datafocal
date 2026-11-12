import { expect, type Page } from "@playwright/test";
import { trace } from "../gen";
import { budget, type Scenario } from "../harness";
import { pan, serve } from "./timeline-pan";

type Waterfall = typeof import("../../packages/render-waterfall/src");

// spans is the 10k-span trace: each span's start,
// duration, parent and service.
const spans = trace(1e4, 1).spans.map((s) => [
  s.start_ms,
  s.dur_ms,
  s.parent,
  Number(s.service.slice(8)),
]);

// setup runs in the page as it loads: it builds the trace's model and a renderer on a new
// canvas, draws the window at x0 = 0, sets window.firstPaint when that frame is painted,
// and sets window.draw to draw the window at x0 and window.end to the last x0.
function setup(cols: number[][]): void {
  const waterfall: Waterfall = Reflect.get(window, "waterfall");
  const canvas = document.body.appendChild(document.createElement("canvas"));
  const r = waterfall.createWaterfallRenderer(canvas.transferControlToOffscreen());
  r.resize(1280, 720, 1);
  const t0 = cols[0]?.[0] ?? 0;
  const parent = new Int32Array(cols.map((c) => c[2] ?? -1));
  const depth = new Uint16Array(cols.length);
  parent.forEach((p, i) => {
    depth[i] = p < 0 ? 0 : (depth[p] ?? 0) + 1;
  });
  const model = {
    spans: {
      start: new Float64Array(cols.map((c) => (c[0] ?? 0) - t0)),
      dur: new Float32Array(cols.map((c) => c[1] ?? 0)),
      self: new Float32Array(cols.length),
      depth,
      parent,
      flags: new Uint8Array(cols.length),
      group: Uint32Array.from(cols, (_, i) => i),
    },
    names: cols.map((_, i) => `step ${i}`),
    services: new Uint16Array(cols.map((c) => c[3] ?? 0)),
    ticks: { span: new Uint32Array(), t: new Float64Array(), error: new Uint8Array() },
    gaps: [],
    facts: [],
  };
  const draw = (x0: number) =>
    r.render(model, { x0, x1: x0 + 10_000, y0: 0, y1: 36, width: 1280, height: 720, dpr: 1 });
  draw(0);
  requestAnimationFrame(() => Reflect.set(window, "firstPaint", performance.now()));
  Reflect.set(window, "draw", draw);
  Reflect.set(window, "end", 50_000);
}

// waterfallPan registers the budget max for panning a ten-second window across the trace,
// and holds the time from opening the page to its first paint under firstPaint if given.
// The app has no trace waterfall yet, so the page is served here.
export function waterfallPan(max: NonNullable<Scenario["max"]>, firstPaint?: number): void {
  serve("waterfall", `(${setup})(${JSON.stringify(spans)})`);
  const run = async (page: Page) => {
    await page.waitForFunction(() => Reflect.has(window, "firstPaint"));
    if (firstPaint !== undefined) {
      const at: number = await page.evaluate(() => Reflect.get(window, "firstPaint"));
      expect.soft(at, "first paint").toBeLessThanOrEqual(firstPaint);
    }
    await pan(page);
  };
  budget({ name: "trace waterfall pan", path: "./waterfall.html", run, max });
}
