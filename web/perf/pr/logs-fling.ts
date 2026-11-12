import type { Page } from "@playwright/test";
import type { Scenario } from "../harness";

// The query tool answers in place of its golden as the engine pages: each read is the 500
// lines before the last read's, newest first, of 50,000 a millisecond apart. Lines are made
// as they are read, since parsing them as JSON would take longer than the fling runs.
const rows = `
const n = 5e4;
const page = 500;
let next = n;
const pad = (v, w) => String(v).padStart(w, "0");
const line = (row) => ({
  timestamp: "2026-01-01T00:00:" + pad(Math.floor(row / 1000), 2) + "." + pad(row % 1000, 3) + "Z",
  level: "info",
  message: "GET /items/" + row + " 200",
  pattern_id: "pattern-" + (row % 100),
  service: "service-" + (row % 50),
  attributes: { trace_id: "trace-" + row },
  segment: "perf",
  row,
});
export default {
  data: {
    canonical: "",
    call: { tool: "search_logs", args: {} },
    result: {
      mode: "scan",
      get lines() {
        const to = next;
        next = Math.max(0, next - page);
        return Array.from({ length: to - next }, (_, i) => line(to - 1 - i));
      },
    },
  },
  meta: { snapshot: "perf", coverage: 1, approx: false, cost: {}, token_estimate: 0 },
};`;

export async function serveRows({ page }: { page: Page }): Promise<void> {
  await page.route("**/assets/query.golden-*.js", (route) =>
    route.fulfill({ contentType: "text/javascript", body: rows }),
  );
}

// fling opens the Logs view, which shows the newest line, and flings its table back
// through the older ones for ms milliseconds, reading their pages as it goes.
export const fling = (ms: number): Omit<Scenario, "name"> => ({
  path: "./",
  run: async (page) => {
    await page.getByRole("button", { name: "Logs" }).click();
    await page.getByRole("button", { name: "Lines", exact: true }).click();
    await page.locator('[role="row"]').first().waitFor();
    await page.mouse.move(640, 400);
    for (const end = Date.now() + ms; Date.now() < end; ) {
      await page.mouse.wheel(0, -4000);
      await page.waitForTimeout(50);
    }
  },
});
