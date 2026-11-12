import { writeFile } from "node:fs/promises";
import type { DrawStats } from "../packages/render-core/src";
import { expect, type Page, test } from "@playwright/test";
import { ab } from "./ab";

// Metrics are what one run of a scenario measured: times in
// milliseconds, heap in bytes, and the DrawStats renderers reported as performance marks
// named "draw" with the stats as their detail; a scenario's run may measure the rest.
export interface Metrics extends DrawStats {
  domRows: number;
  longFrames: number;
  frameP50: number;
  frameP95: number;
  frameP99: number;
  heapBytes: number;
  // firstPartMs and allPartsMs are when the page drew its first part and its last, from its
  // navigation.
  firstPartMs: number;
  allPartsMs: number;
}

type Budget = Partial<Record<keyof Metrics, number>>;

export interface Scenario {
  name: string;
  // path is opened relative to the app's URL.
  path: string;
  // setup prepares each page the scenario opens before it navigates, as routing its answers.
  setup?: (page: Page) => Promise<void>;
  // run drives the page while it is measured, and returns what it measured itself.
  run(page: Page): Promise<Partial<Metrics> | void>;
  // max is each metric's ceiling.
  max?: Budget;
  // delta is each metric's largest change against main, relative to main: 0.1 is +10%.
  // It is checked only when the PR changes web/ outside web/packages/ or one of packages.
  delta?: Budget;
  packages?: readonly string[];
}

// prRunMs caps a per-PR run, page load included, to bound the tier's Actions time.
const prRunMs = 5000;

// changed reports whether PERF_CHANGED_FILES, if set, has web/ outside web/packages/ or packages.
function changed(packages: readonly string[]): boolean {
  const files = process.env["PERF_CHANGED_FILES"];
  if (files === undefined) {
    return true;
  }
  return files.split("\n").some((f) => {
    const pkg = /^web\/packages\/([^/]+)\//.exec(f)?.[1];
    return pkg ? packages.includes(pkg) : f.startsWith("web/");
  });
}

// measure opens s.path under base at 4× CPU throttle, runs s, and returns its metrics.
async function measure(page: Page, s: Scenario, base: string): Promise<Metrics> {
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Emulation.setCPUThrottlingRate", { rate: 4 });
  await s.setup?.(page);
  await page.goto(new URL(s.path, base).href, { waitUntil: "load" });
  // The app renders once its fixtures load, which can be after the load event.
  await page.locator("#root > *").first().waitFor();
  const recording = await page.evaluateHandle(() => {
    const r = { frames: [] as number[], longFrames: 0, drawCalls: 0, primitives: 0, labels: 0 };
    const start = performance.now();
    let last = start;
    const frame = (t: number) => {
      r.frames.push(t - last);
      last = t;
      requestAnimationFrame(frame);
    };
    requestAnimationFrame(frame);
    // A frame that began before the run can still end after the observer starts.
    new PerformanceObserver((l) => {
      r.longFrames += l.getEntries().filter((e) => e.startTime >= start).length;
    }).observe({ type: "long-animation-frame" });
    new PerformanceObserver((l) => {
      for (const e of l.getEntriesByName("draw")) {
        const d: DrawStats = (e as PerformanceMark).detail;
        r.drawCalls += d.drawCalls;
        r.primitives += d.primitives;
        r.labels += d.labels;
      }
    }).observe({ type: "mark" });
    return r;
  });
  const own = await s.run(page);
  const { frames, ...counts } = await recording.jsonValue();
  await cdp.send("HeapProfiler.collectGarbage");
  await cdp.send("Performance.enable");
  const { metrics } = await cdp.send("Performance.getMetrics");
  frames.sort((a, b) => a - b);
  const at = (p: number) => frames[Math.floor(p * frames.length)] ?? 0;
  return {
    ...counts,
    domRows: await page.locator('[role="row"]').count(),
    frameP50: at(0.5),
    frameP95: at(0.95),
    frameP99: at(0.99),
    heapBytes: metrics.find((m) => m.name === "JSHeapUsedSize")?.value ?? 0,
    firstPartMs: 0,
    allPartsMs: 0,
    ...own,
  };
}

// budget registers s as a test that writes its metrics to metrics.json and fails when one
// exceeds its max or, given main's build at PERF_MAIN_URL, its delta.
export function budget(s: Scenario): void {
  test(s.name, async ({ page, browser, baseURL }, info) => {
    const base = baseURL ?? "";
    const started = Date.now();
    const m = await measure(page, s, base);
    if (info.titlePath[0]?.startsWith("perf/pr/")) {
      expect
        .soft(Date.now() - started, "run time in ms, page load included")
        .toBeLessThanOrEqual(prRunMs);
    }
    await writeFile(info.outputPath("metrics.json"), JSON.stringify(m));
    for (const [k, max] of Object.entries(s.max ?? {})) {
      expect.soft(m[k as keyof Metrics], k).toBeLessThanOrEqual(max);
    }
    const main = process.env["PERF_MAIN_URL"];
    if (!s.delta || !main || !changed(s.packages ?? [])) {
      return;
    }
    const run = async (url: string) => {
      const p = await browser.newPage();
      const runMetrics = await measure(p, s, url);
      await p.close();
      return runMetrics;
    };
    const intervals = await ab(run, Object.keys(s.delta) as (keyof Metrics)[], main, base);
    for (const [k, max] of Object.entries(s.delta)) {
      const [lo, hi] = intervals[k as keyof Metrics] ?? [0, 0];
      expect
        .soft(lo, `${k} change against main, 95% interval [${lo}, ${hi}]`)
        .toBeLessThanOrEqual(max);
    }
  });
}
