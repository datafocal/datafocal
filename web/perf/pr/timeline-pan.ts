import { type Page, test } from "@playwright/test";
import { build } from "vite";
import type { DrawStats } from "../../packages/render-core/src";
import { run } from "../gen";
import { budget, type Scenario } from "../harness";

type Timeline = typeof import("../../packages/render-timeline/src");

// steps is the 5k-step, 4-lane run: each step's
// start, duration and lane.
const steps = run(5000, 4, 1).spans.map((s) => [s.start_ms, s.dur_ms, Number(s.service.slice(5))]);

// setup runs in the page as it loads: it builds the run's model and a renderer on a new
// canvas and, a frame later, draws the window at x0 = 0 and sets window.draw to draw the
// window at x0.
function setup(cols: number[][]): void {
  const timeline: Timeline = Reflect.get(window, "timeline");
  const canvas = document.body.appendChild(document.createElement("canvas"));
  const r = timeline.createTimelineRenderer(canvas.transferControlToOffscreen());
  r.resize(1280, 160, 1);
  const t0 = cols[0]?.[0] ?? 0;
  const model = {
    lanes: [0, 1, 2, 3].map((i) => ({ id: `${i}`, label: `lane ${i}`, depth: 0, parent: -1 })),
    kinds: ["tool", "model", "think"],
    steps: {
      start: new Float64Array(cols.map((c) => (c[0] ?? 0) - t0)),
      dur: new Float32Array(cols.map((c) => c[1] ?? 0)),
      lane: new Uint16Array(cols.map((c) => c[2] ?? 0)),
      kind: new Uint8Array(cols.map((_, i) => i % 3)),
      group: new Uint32Array(cols.length).fill(1),
    },
    markers: [],
  };
  const draw = (x0: number) =>
    r.render(model, { x0, x1: x0 + 120_000, y0: 0, y1: 4, width: 1280, height: 160, dpr: 1 });
  // The first draw packs the hit index, so it gets a frame of its own: the one after the
  // frame this script runs in.
  requestAnimationFrame(() =>
    requestAnimationFrame(() => {
      draw(0);
      Reflect.set(window, "draw", draw);
    }),
  );
  Reflect.set(window, "end", Math.max(...model.steps.start) - 120_000);
}

// timelinePan registers the budget max for panning a two-minute window across the run for
// ten seconds, drawing each animation frame and marking its DrawStats. The app has no run
// timeline yet, so the page is served here: render-timeline built as a script that sets
// the global timeline, then setup.
export function timelinePan(max: NonNullable<Scenario["max"]>): void {
  serve("timeline", `(${setup})(${JSON.stringify(steps)})`);
  budget({ name: "run timeline pan", path: "./timeline.html", run: pan, max });
}

// serve serves each test's page name.html: render-name built as a script that sets the
// global name, then script.
export function serve(name: string, script: string): void {
  test.beforeEach(async ({ page }) => {
    const out = await build({
      configFile: false,
      logLevel: "silent",
      build: {
        write: false,
        lib: { entry: `packages/render-${name}/src/index.ts`, formats: ["iife"], name },
      },
    });
    const lib = Array.isArray(out) ? out[0]?.output[0].code : "";
    await page.route(`**/${name}.html`, (route) =>
      route.fulfill({
        contentType: "text/html",
        body: `<body><script>${lib}</script><script>${script}</script>`,
      }),
    );
  });
}

// pan draws, each animation frame for ten seconds, the window at x0 from 0 to window.end.
export async function pan(page: Page): Promise<void> {
  await page.waitForFunction(() => Reflect.has(window, "draw"));
  await page.evaluate(() => {
    const draw: (x0: number) => DrawStats = Reflect.get(window, "draw");
    const end: number = Reflect.get(window, "end");
    const began = performance.now();
    return new Promise<void>((done) => {
      const frame = (now: number) => {
        performance.mark("draw", { detail: draw((end * (now - began)) / 10_000) });
        if (now - began < 10_000) {
          requestAnimationFrame(frame);
        } else {
          done();
        }
      };
      requestAnimationFrame(frame);
    });
  });
}
