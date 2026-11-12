import type { Envelope } from "../../packages/api/src";
import { expect, test } from "@playwright/test";
import { trace } from "../gen";
import { budget } from "../harness";

const meta: Envelope<unknown>["meta"] = {
  snapshot: "",
  coverage: 1,
  approx: false,
  cost: { gets: 0, bytes_read: 0 },
  token_estimate: 0,
};

// The fixtures build lists one trace and opens it as the 10k-span trace with no lines.
const goldens = {
  aggregate: {
    data: { series: [{ key: "req-1", start: 0, step: 1, measure_values: [[0], [60_000], [0]] }] },
    meta,
  },
  get_trace: { data: trace(1e4, 1), meta },
  search_logs: { data: { mode: "lines", lines: [] }, meta },
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

// The root span clicked, then ten spans stepped through with j. Each input's event duration,
// from input to the next paint, stays within INP's 200 ms "good" threshold, and none redraws
// the waterfall: selecting moves the row band over the canvas.
budget({
  name: "trace span select",
  path: "./",
  run: async (page) => {
    await page.getByRole("button", { name: "Trace", exact: true }).click();
    await page.getByRole("button", { name: /^req-1/ }).click();
    const canvas = page.getByRole("img", { name: "Trace" });
    await page.waitForFunction(() => performance.getEntriesByName("draw").length > 0);
    await page.evaluate(() => {
      // durationThreshold, 104 ms by default, is missing from TypeScript's DOM types.
      const init = { type: "event", durationThreshold: 16 };
      new PerformanceObserver((l) => {
        for (const e of l.getEntries().filter((e) => ["click", "keydown"].includes(e.name))) {
          performance.measure("span select", { start: e.startTime, duration: e.duration });
        }
      }).observe(init);
    });
    const draws = () => page.evaluate(() => performance.getEntriesByName("draw").length);
    const before = await draws();
    await canvas.click({ position: { x: 4, y: 10 } });
    for (let i = 0; i < 10; i++) {
      await page.keyboard.press("j");
      await page.waitForTimeout(100);
    }
    const inputs = await page.evaluate(() =>
      performance.getEntriesByName("span select").map((e) => e.duration),
    );
    expect(Math.max(0, ...inputs), "slowest span select, ms").toBeLessThanOrEqual(200);
    expect(await draws(), "draws while selecting").toBe(before);
  },
});
