import type { Page } from "@playwright/test";
import type { Scenario } from "../harness";

// The stream answers search_logs in place of its golden: each read of its lines is the
// lines made since the last, two a millisecond.
const stream = `
let last = Date.now();
let row = 0;
const line = (t) => ({
  timestamp: new Date(t).toISOString(),
  level: "info",
  message: "GET /items/" + row + " 200",
  pattern_id: "pattern-" + (row % 100),
  service: "db",
  attributes: { trace_id: "trace-" + row },
  segment: "tail",
  row: row++,
});
export default {
  data: {
    mode: "needle",
    get lines() {
      const n = Math.floor((Date.now() - last) * 2);
      const lines = Array.from({ length: n }, (_, i) => line(last + i / 2));
      last += n / 2;
      return lines;
    },
  },
  meta: { snapshot: "perf", coverage: 1, approx: false, cost: {}, token_estimate: 0 },
};`;

export async function serveStream({ page }: { page: Page }): Promise<void> {
  await page.route("**/assets/search_logs.golden-*.js", (route) =>
    route.fulfill({ contentType: "text/javascript", body: stream }),
  );
}

// tail opens the Logs view, whose lines follow new ones while the range runs to now, and
// follows 2,000 lines a second for ms.
export const tail = (ms: number): Omit<Scenario, "name"> => ({
  path: "./",
  run: async (page) => {
    await page.getByRole("button", { name: "Logs" }).click();
    await page.locator('[role="row"]').first().waitFor();
    await page.waitForTimeout(ms);
  },
});
