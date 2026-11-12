import type { AggregateResponse, Envelope } from "../../packages/api/src";
import { expect, test } from "@playwright/test";
import { run } from "../gen";
import { budget } from "../harness";

// runs stands in for the aggregate golden: 10,000 runs, each one bin of the Runs tab's
// measures (steps, spend, input, peak, outcome).
const runs: Envelope<AggregateResponse> = {
  data: {
    series: Array.from({ length: 10_000 }, (_, i) => ({
      key: `run-${i}`,
      start: 0,
      step: 1,
      measure_values: [[i % 90], [i / 100], [i * 7], [i * 3], [0]],
      measure_labels: [[""], [""], [""], [""], [`pattern-${i % 7}`]],
    })),
  },
  meta: {
    snapshot: "",
    coverage: 1,
    approx: false,
    cost: { gets: 0, bytes_read: 0 },
    token_estimate: 0,
  },
};

// The fixtures build answers every aggregate call with runs, and opens any run on the
// 5k-step, 4-lane run with no lines.
const goldens = {
  aggregate: runs,
  get_trace: { ...runs, data: run(5000, 4, 1) },
  search_logs: { ...runs, data: { mode: "needle", lines: [] } },
};
test.beforeEach(({ page }) =>
  Promise.all(
    Object.entries(goldens).map(([tool, golden]) =>
      page.route(`**/${tool}.golden-*.js`, (route) =>
        route.fulfill({
          contentType: "text/javascript",
          body: `export default ${JSON.stringify(golden)};`,
        }),
      ),
    ),
  ),
);

// The Runs table at 10,000 runs, flung for a second. Its 60vh body at the 720 px
// viewport shows 16 rows of 28 px, partly 17, plus 10 overscan either side and the header.
budget({
  name: "ai runs scroll",
  path: "./",
  run: async (page) => {
    await page.getByRole("tab", { name: "Runs" }).click();
    await page.locator(".ai-runs-b [role=row]").first().waitFor();
    await page.locator(".ai-runs-b").hover();
    for (let i = 0; i < 10; i++) {
      await page.mouse.wheel(0, 4000);
      await page.waitForTimeout(100);
    }
  },
  max: { domRows: 17 + 2 * 10 + 1 },
});

// Ten steps picked across the run's timeline. Each click's event duration, from input to
// the next paint, stays within INP's 200 ms "good" threshold, and none redraws the timeline:
// its one draw is a call per kind (15 named and other) and its labels.
budget({
  name: "ai run step select",
  path: "./",
  run: async (page) => {
    await page.getByRole("tab", { name: "Runs" }).click();
    await page.locator(".ai-runs-b [role=row]").first().click();
    const timeline = page.locator(".ai-timeline");
    const width = (await timeline.boundingBox())?.width ?? 0;
    await page.evaluate(() => {
      // durationThreshold, 104 ms by default, is missing from TypeScript's DOM types.
      const init = { type: "event", durationThreshold: 16 };
      new PerformanceObserver((l) => {
        for (const e of l.getEntriesByName("click")) {
          performance.measure("step select", { start: e.startTime, duration: e.duration });
        }
      }).observe(init);
    });
    for (let i = 0; i < 10; i++) {
      await timeline.click({ position: { x: ((i + 0.5) * width) / 10, y: 12 } });
      await page.waitForTimeout(100);
    }
    const clicks = await page.evaluate(() =>
      performance.getEntriesByName("step select").map((e) => e.duration),
    );
    expect(Math.max(0, ...clicks), "slowest step select, ms").toBeLessThanOrEqual(200);
  },
  max: { drawCalls: 25 },
});
