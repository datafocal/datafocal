import { readFileSync } from "node:fs";
import { expect, type Page } from "@playwright/test";
import { budget } from "../harness";

// Each tool's fixture answer takes about what the querier takes: a footer-answered one 20 ms,
// one that scans rows 300 ms.
const scanning = [
  "search_logs",
  "explain_spike",
  "slo_status",
  "query",
  "focus",
  "get_trace",
  "trace_shape",
  "rca_walk",
  "funnel",
  "co_occurrence",
  "attribute_writes",
  "resource_usage",
];
const footers = [
  "list_slos",
  "aggregate",
  "rank_fields",
  "list_patterns",
  "describe_schema",
  "overview",
  "find_anomalies",
  "correlate",
  "service_graph",
  "export_definitions",
];
const delays = [...footers.map((t) => `${t}:20`), ...scanning.map((t) => `${t}:300`)].join(",");

declare global {
  interface Window {
    parts: { first: number; all: number };
  }
}

// parts opens the view ?view= names, once the fixtures build's assets are found by the root's
// URL, and records when the page first draws a part and when it last stops showing a part
// loading, in window.parts, from the first frame.
function parts(): void {
  document.addEventListener("readystatechange", () => {
    const view = new URLSearchParams(location.search).get("view");
    if (document.readyState === "interactive" && view) {
      history.replaceState(null, "", `/ui/${view}${location.search}`);
    }
  });
  const times = { first: 0, all: 0 };
  window.parts = times;
  const loading = () =>
    document.querySelector("progress.busy") !== null ||
    [...document.querySelectorAll(".kpi strong, .badge, main p")].some((e) =>
      /^…|Loading…/.test(e.textContent ?? ""),
    );
  const drawn = () => document.querySelector('[aria-label="SLOs"] article, .kpi strong') !== null;
  const frame = () => {
    const now = performance.now();
    times.first ||= drawn() ? now : 0;
    times.all = !times.first || loading() ? 0 : times.all || now;
    requestAnimationFrame(frame);
  };
  requestAnimationFrame(frame);
}

// settled waits until every part has been drawn for half a second, and returns when the
// first and the last were, and how many calls the page sent.
async function settled(page: Page, calls: number) {
  await page.waitForFunction(
    () => window.parts.all > 0 && performance.now() - window.parts.all > 500,
  );
  const m = await page.evaluate(() => ({
    ...window.parts,
    calls: performance.getEntriesByName("call").length,
  }));
  expect(m.calls, "calls the page sent").toBe(calls);
  return { firstPartMs: m.first, allPartsMs: m.all };
}

const packages = ["app", "shell", "ui", "api", "view-slos", "view-ai"];

// agents answers the AI view as the factory corpus does: its fields, and its spend by model
// for every aggregate.
const engine: readonly { tool: string; answer: unknown }[] = JSON.parse(
  readFileSync(new URL("../../packages/view-ai/fixtures/engine.json", import.meta.url), "utf8"),
);
const agents = Object.fromEntries(
  ["rank_fields", "aggregate"].map((tool) => [tool, engine.find((a) => a.tool === tool)?.answer]),
);
const setup = (goldens: Record<string, unknown>) => async (page: Page) => {
  await page.addInitScript(parts);
  for (const [tool, golden] of Object.entries(goldens)) {
    await page.route(`**/${tool}.golden-*.js`, (route) =>
      route.fulfill({
        contentType: "text/javascript",
        body: `export default ${JSON.stringify(golden)};`,
      }),
    );
  }
};

// The SLOs list, its cards and the firing badge, and the AI view's overview, loaded on answers
// as slow as the querier's: neither draws its first part or its last later than main does.
budget({
  name: "slos page load",
  path: `./?view=slos&delays=${delays}`,
  setup: setup({}),
  run: (page) => settled(page, 2),
  delta: { firstPartMs: 0, allPartsMs: 0 },
  packages,
});

budget({
  name: "ai overview load",
  path: `./?view=ai&delays=${delays}`,
  setup: setup(agents),
  run: (page) => settled(page, 12),
  delta: { firstPartMs: 0, allPartsMs: 0 },
  packages,
});
