import {
  countingClient,
  createFakeClient,
  fixtureClient,
  type DataFocalClient,
  type LogLine,
  type SearchRequest,
  ToolError,
} from "@datafocal/api";
import { navigate, Shell } from "@datafocal/shell";
import axe from "axe-core";
import { createElement } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, expect, test, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import engine from "../fixtures/engine.json";
import { LogsView } from "./index";

const line = (row: number, service: string): LogLine => ({
  timestamp: new Date(Date.UTC(2026, 9, 20, 14, 0, row)).toISOString(),
  level: "error",
  message: `pool timeout ${row}`,
  pattern_id: "p10",
  service,
  attributes: { trace_id: `t${row}`, pool: "users_rw", "message.role": "user" },
  segment: "s1",
  row,
});
const lines = Array.from({ length: 60 }, (_, i) => line(i, i % 2 ? "db-3" : "checkout"));
const meta = { snapshot: "s", coverage: 1, approx: false, cost: { gets: 0, bytes_read: 0 } };
const answer = (data: unknown) => ({ data, meta: { ...meta, token_estimate: 0 } });
// patterns are list_patterns' first page by count, the second without text.
const patterns = Array.from({ length: 50 }, (_, i) => ({
  pattern_id: `p${i + 10}`,
  pattern: ["pool timeout <*>", ""][i] ?? `pattern ${i}`,
  service: "db-3",
  count: 1000 - i,
  rate: 1,
}));
const burst = {
  kind: "new_pattern",
  pattern_id: "p10",
  pattern: "pool timeout <*>",
  service: "db-3",
};
const anomaly = { ...burst, at: "2026-10-20T14:00:00Z", observed: 40, expected: 0, score: 20 };
const fixtures = fixtureClient([
  {
    tool: "find_anomalies",
    answer: answer({ anomalies: [{ ...anomaly, level: "error", members: [anomaly] }] }),
  },
  {
    tool: "aggregate",
    answer: answer({ series: [{ key: "error", start: 0, step: 60_000, values: [1, 2] }] }),
  },
  {
    tool: "query",
    answer: answer({
      call: { tool: "search_logs", args: {} },
      result: { mode: "scan", lines: [...lines].reverse() },
    }),
  },
  { tool: "list_patterns", answer: answer({ patterns, total: 1007 }) },
  {
    tool: "rank_fields",
    answer: answer({
      fields: [
        { name: "trace_id", signal: 0.9, high_signal: true, type: "id", cardinality: 60 },
        { name: "duration_ms", signal: 0.5, high_signal: false, type: "num", cardinality: 900 },
      ],
    }),
  },
  {
    tool: "search_logs",
    answer: answer({ mode: "around", lines: lines.filter((l) => l.service === "db-3") }),
  },
]);
// onEngine answers what the view asks of the engine's corpus, about user-00042, as it did.
const onEngine = fixtureClient(engine);
const engineQuery = '{user_id="user-00042"}';
// slow are the signals of queries for "slow", which never answer.
const slow: AbortSignal[] = [];
const searches: unknown[] = [];
const follows: AbortSignal[] = [];
const followed: SearchRequest[] = [];
let refusal: ToolError | undefined;
// holdFields leaves rank_fields unanswered while set.
let holdFields = false;
const spread = answer({
  series: [{ key: "db-3", start: 0, step: 0, measure_values: [[1], [120], [480], [2400]] }],
});
const malformed = '{a="x"} | line_format "x"';
const tooLarge = '{service="all"}';
const narrow =
  "this request read more than it may hold in memory; narrow the time range or the selector";
// refused are the queries the querier refuses, by query.
const refused = new Map([
  [
    malformed,
    new ToolError({
      code: "invalid_request",
      message: "query: logql: at offset 10: the line_format stage is not supported",
      retryable: false,
    }),
  ],
  [tooLarge, new ToolError({ code: "too_large", message: narrow, retryable: false })],
]);
// many is 1,200 lines a second apart, which the query "many" pages through 500 at a time,
// newest first, up to its time's end.
const many = Array.from({ length: 1200 }, (_, i) => line(i, "db-3"));
function manyPage(to: string | undefined) {
  const end = to === undefined || to === "now" ? Infinity : Date.parse(to);
  const page = many
    .filter((l) => Date.parse(l.timestamp) <= end)
    .reverse()
    .slice(0, 500);
  return answer({ call: { tool: "search_logs", args: {} }, result: { mode: "scan", lines: page } });
}
// otherCall answers a call but a handmade query's, recording each search.
const otherCall: DataFocalClient["call"] = (tool, req, opts) => {
  if (tool === "search_logs") {
    searches.push(req);
  }
  if (tool === "rank_fields" && holdFields) {
    return new Promise(() => {});
  }
  if (tool === "aggregate" && "measures" in req && req.measures) {
    return fixtureClient([{ tool, answer: spread }]).call(tool, req, opts);
  }
  return fixtures.call(tool, req, opts);
};
const handmade = createFakeClient({
  callFn: (tool, req, opts) => {
    if (tool === "query" && "q" in req && refused.has(req.q)) {
      return Promise.reject(refused.get(req.q));
    }
    if (tool === "query" && "q" in req && req.q.includes("many")) {
      return fixtureClient([{ tool: "query", answer: manyPage(req.time?.to) }]).call(
        tool,
        req,
        opts,
      );
    }
    return otherCall(tool, req, opts);
  },
  followFn: (req, onBatch, signal) => {
    follows.push(signal);
    followed.push(req);
    if (refusal) {
      return Promise.reject(refusal);
    }
    const db3 = lines.filter((l) => l.service === "db-3");
    onBatch({ data: { mode: "scan", lines: db3 }, meta: { ...meta, token_estimate: 0 } });
    return new Promise((resolve) => signal.addEventListener("abort", () => resolve()));
  },
});
const client = createFakeClient({
  callFn: (tool, req, opts) => {
    if (JSON.stringify(req).includes("user-00042")) {
      return onEngine.call(tool, req, opts);
    }
    if (tool === "query" && "q" in req && req.q.includes("slow")) {
      slow.push(opts?.signal ?? new AbortController().signal);
      return new Promise(() => {});
    }
    return handmade.call(tool, req, opts);
  },
  followFn: (req, onBatch, signal) => handmade.follow(req, onBatch, signal),
});

const counted = countingClient(client);
history.replaceState(null, "", "/ui/logs");
const el = document.body.appendChild(document.createElement("div"));
el.style.height = "600px";
createRoot(el).render(
  createElement(Shell, {
    routes: [
      { path: "/ui/logs", view: LogsView, keep: ["by"] },
      { path: "/ui/resources", view: () => createElement("p", null, "Resources") },
    ],
    client: counted,
  }),
);
// Every call reaches the fixtures through a loader, which sends each once, a frame's at once.
afterEach(async () => {
  expect(await counted.faults(), "each call sent once, a frame's at once").toEqual([]);
});

const button = (name: string) =>
  [...document.querySelectorAll("button")].find((b) => b.textContent?.startsWith(name));

// Drill-down queries, in LogQL.
const drillDowns = [
  '{service="checkout"} | trace_id="4bf92f3577b34da6a3ce929d0e0e4736" | span_id="3f1"',
  '{service=~".+"} | trace_id="4bf92f3577b34da6a3ce929d0e0e4736"',
  '{service="db-3"} | trace_id="4bf92f3577b34da6a3ce929d0e0e4736" | span_name="acquire"',
  '{service=~".+"}',
  '{workload="Fix issue"} | failure_class="tool_error"',
  '{sessionId=~"s-1|s-2"}',
  '{service_group="payments"} or {domain="api.stripe.com"}',
];

test.each(drillDowns)("%s lands on Lines with its focus line picked out", async (query) => {
  const focus = lines[41] ?? line(0, "");
  navigate({ view: "logs", query, mode: "lines", focus: { segment: "s1", row: 41 }, title: query });
  await expect.poll(() => el.querySelector(".lrow.sel")?.textContent).toContain(focus.message);
  await expect.poll(() => el.querySelector(".lrow.sel")?.textContent).toContain("trace_id=t41");
  expect(el.querySelector(".logs-ctx")?.textContent).toBe(`${query}Back`);
});

test("A line opens its fields under it, and View in context opens its producer around it", async () => {
  expect((await axe.run(el)).violations).toEqual([]);
  await userEvent.click(el.querySelector(".lrow.sel") ?? el);
  await expect.poll(() => el.querySelectorAll(".ldet.first").length).toBeGreaterThan(3);
  expect([...el.querySelectorAll(".ldet.first .dk")].map((d) => d.textContent)).toContain("pool");
  expect((await axe.run(el)).violations).toEqual([]);
  const context = [...el.querySelectorAll(".ldet.acts button")].find(
    (b) => b.textContent === "View in context",
  );
  await userEvent.click(context ?? el);
  const path = decodeURIComponent(location.pathname);
  expect(path).toContain('query={service="db-3"}');
  expect(path).toContain('focus={"segment":"s1","row":41}');
  expect(path).toContain("time=2026-10-20T13:50:41.000Z,2026-10-20T14:02:41.000Z");
});

test("A service's lines, and each line of it, open the service's resources", async () => {
  const query = '{service="db-3"}';
  navigate({ view: "logs", query, mode: "lines", focus: { segment: "s1", row: 41 } });
  await expect.poll(() => el.querySelector(".lrow.sel")).toBeTruthy();
  const act = () =>
    [...el.querySelectorAll<HTMLButtonElement>(".ldet.acts button")].find(
      (b) => b.textContent === "Resources",
    );
  // The line stays open from the test before.
  if (!act()) {
    await userEvent.click(el.querySelector(".lrow.sel") ?? el);
  }
  await expect.poll(act).toBeTruthy();
  // The page is narrower than the line's actions, so the last of them is clicked unseen.
  act()?.click();
  expect(decodeURIComponent(location.pathname)).toBe(
    "/ui/resources/service=db-3/time=2026-10-20T13:55:41.000Z,2026-10-20T14:05:41.000Z",
  );
  navigate({ view: "logs", query, mode: "lines" });
  await expect.poll(() => button("Resources")).toBeTruthy();
  await userEvent.click(button("Resources") ?? el);
  expect(decodeURIComponent(location.pathname)).toMatch(/^\/ui\/resources\/service=db-3\//);
});

test("A plain search runs as LogQL and marks what it looks for", async () => {
  history.replaceState(null, "", "/ui/logs");
  navigate({ view: "logs", query: 'timeout -healthz service:db-3 "pool t"', mode: "lines" });
  await expect
    .poll(() => el.querySelector(".q-call")?.textContent)
    .toBe(
      'LogQL {service=~".+"} |~ "(?i)timeout" !~ "(?i)healthz" | service="db-3" |~ "(?i)pool t"',
    );
  await expect
    .poll(() => el.querySelector(".lrow mark")?.textContent?.toLowerCase())
    .toBe("pool t");
  expect(el.querySelector(".q-in .q-not")?.textContent).toBe("-healthz");
});

test("Scrolling to the top reads the page of older lines", async () => {
  // A range in the past, so no tail appends while it reads back.
  history.replaceState(null, "", "/ui/logs/time=2026-10-20T13:00:00Z,2026-10-20T15:00:00Z");
  navigate({ view: "logs", query: "many", mode: "lines" });
  await expect.poll(() => el.querySelector(".live-held")?.textContent).toContain("500 lines");
  const table = el.querySelector("[role=table]");
  table?.scrollTo({ top: 0 });
  await expect.poll(() => el.querySelector(".live-held")?.textContent).toContain("999 lines");
  // The page went in above, keeping the view where it was; reading on takes scrolling on.
  expect(table?.scrollTop).toBeGreaterThan(400 * 24);
  table?.scrollTo({ top: 0 });
  await expect
    .poll(() => el.querySelector(".live-held")?.textContent)
    .toBe("1,200 lines · start of range");
});

test("A malformed query underlines the offset the querier returns", async () => {
  navigate({ view: "logs", query: malformed });
  await expect.poll(() => el.querySelector(".q-fault")?.textContent).toBe("line_format");
  expect(el.querySelector(".qbar [role=alert]")?.textContent).toContain("at offset 10");
});

test("A query that reads too much says to narrow it", async () => {
  navigate({ view: "logs", query: tooLarge });
  await expect
    .poll(() => el.querySelector(".qbar [role=alert] p")?.textContent)
    .toBe(`Failed: ${narrow}`);
});

test("Brushing the histogram sets the time range in the URL", async () => {
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  await expect.poll(() => el.querySelector(".hist [role=img]")).toBeTruthy();
  const hist = el.querySelector(".hist") ?? el;
  const { left, right } = hist.getBoundingClientRect();
  hist.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, clientX: left - 10 }));
  dispatchEvent(new PointerEvent("pointerup", { clientX: right + 10 }));
  expect(decodeURIComponent(location.pathname)).toContain(
    "/time=1970-01-01T00:00:00.000Z,1970-01-01T00:02:00.000Z",
  );
});

test("A field's heading stacks the volume by its values, kept across queries, and its chip stacks by level again", async () => {
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  const label = () => el.querySelector(".hist [role=img]")?.getAttribute("aria-label");
  await expect.poll(label).toMatch(/by level$/);
  const heading = () =>
    el.querySelector<HTMLElement>(".fac h3 button[title='Stack the volume by service']");
  await expect.poll(heading).toBeTruthy();
  // The test page is narrower than the rail shows at, so the heading is clicked unseen.
  heading()?.click();
  await expect.poll(label).toMatch(/by service$/);
  expect(decodeURIComponent(location.pathname)).toContain("/by=service");
  await expect.poll(() => el.querySelector(".stale")).toBeNull();
  expect((await axe.run(el)).violations).toEqual([]);
  navigate({ view: "logs", query: '{service="db-3"} |= "pool"', mode: "lines" });
  await expect.poll(label).toMatch(/by service$/);
  await userEvent.click(button("by service") ?? el);
  await expect.poll(label).toMatch(/by level$/);
  expect(location.pathname).not.toContain("by=");
});

test("A field past the listed ones is a click away from listing its values", async () => {
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  const more = () => el.querySelector<HTMLDetailsElement>(".fac-more");
  await expect.poll(() => more()?.querySelector("summary")?.textContent).toBe("1 more field");
  more()?.setAttribute("open", "");
  more()?.querySelector<HTMLElement>('button[title="List trace_id\'s values"]')?.click();
  await expect
    .poll(() => el.querySelector(".fac h3 button[title='Stack the volume by trace_id']"))
    .toBeTruthy();
  expect(more()).toBeNull();
  expect((await axe.run(el)).violations).toEqual([]);
});

test("Lines tail the query while the range runs to now, until the view leaves them", async () => {
  history.replaceState(null, "", "/ui/logs");
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  await expect.poll(() => el.querySelector(".live")?.textContent).toContain("pool timeout 59");
  await expect.poll(() => el.querySelector(".live-rate")?.textContent).toMatch(/^\d+ lines\/s$/);
  await expect.poll(() => el.querySelector(".busy")).toBeNull();
  expect(followed.every((r) => r.selector.time?.to === "now")).toBe(true);
  expect((await axe.run(el)).violations).toEqual([]);
  navigate({ view: "logs", query: '{service="db-3"}', mode: "patterns" });
  await expect.poll(() => follows.at(-1)?.aborted).toBe(true);
});

test("A refused follow shows in the status line", async () => {
  const message = "search_logs: this search reads every row";
  refusal = new ToolError({ code: "unbounded_scan", message, retryable: false });
  history.replaceState(null, "", "/ui/logs");
  navigate({ view: "logs", query: '{service=~".+"}', mode: "lines" });
  await expect
    .poll(() => el.querySelector(".live [role=alert] p")?.textContent)
    .toBe(`Failed: unbounded_scan: ${message}`);
  refusal = undefined;
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
});

test("Patterns lists every pattern by count, and a line's field filters in", async () => {
  history.replaceState(null, "", "/ui/logs");
  navigate({ view: "logs", query: '{service="db-3"}', mode: "patterns" });
  await expect.poll(() => el.querySelector(".pats > p")?.textContent).toBe("50 of 1,007 patterns");
  const shown = [...el.querySelectorAll(".pats li > button[title]")].map((b) =>
    b.getAttribute("title"),
  );
  expect(shown).toEqual(patterns.map((p) => p.pattern_id));
  expect(el.querySelector('.pats button[title="p10"]')?.textContent).toBe("pool timeout <*>");
  expect(el.querySelector('.pats button[title="p11"]')?.textContent).toBe("p11");
  expect(el.querySelector(".pats > button")).toBeNull();
  await userEvent.click(el.querySelector('.pats button[title="p10"]') ?? el);
  await userEvent.click(button("Open lines") ?? el);
  await expect.poll(() => decodeURIComponent(location.pathname)).toContain('pattern_id="p10"');

  // The pattern's lines, not the last query's, kept dimmed while they load. Their table
  // remounts once after they land, with a window a row later, so the newest line, in both,
  // is clicked by a locator that finds it again rather than the first row found.
  await expect.poll(() => el.querySelector(".logs-b:not(.stale) .lrow")).toBeTruthy();
  await userEvent.click(page.getByRole("button", { name: "14:00:59.000 error db-3" }));
  const role = () =>
    [...el.querySelectorAll(".ldet.first")].find(
      (d) => d.querySelector(".dk")?.textContent === "message.role",
    );
  await expect.poll(role).toBeTruthy();
  await userEvent.click(role()?.querySelector<HTMLElement>(".dt button") ?? el);
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toContain('{service="db-3"} | pattern_id="p10" | message.role="user"');
});

test("With no query, Logs tails every service's lines, and Insights lists the anomalies", async () => {
  history.replaceState(null, "", "/ui/logs");
  navigate({ view: "logs", query: "" });
  await expect.poll(() => el.querySelector(".live")).toBeTruthy();
  expect(button("Lines")?.getAttribute("aria-pressed")).toBe("true");
  await userEvent.click(button("Insights") ?? el);
  expect(decodeURIComponent(location.pathname)).toContain("/mode=insights");
  const row = () => el.querySelector(".pats li");
  await expect
    .poll(() => row()?.textContent)
    .toBe("pool timeout <*> +1 new patternsdb-340 vs 0.0error");
  expect((await axe.run(el)).violations).toEqual([]);
  await userEvent.click(row()?.querySelector("button") ?? el);
  await expect
    .poll(() => el.ownerDocument.querySelector("[aria-label=Focus] .focus-why")?.textContent)
    .toMatch(/^Flagged as new: db-3 first logged this pattern at \d\d:\d\d UTC, 40 lines since\.$/);
  await userEvent.click(button("Open lines") ?? el);
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toContain('query={service=~".+"} | pattern_id="p10"');
});

const engineTime = "2026-07-18T14:00:00Z,2026-07-18T14:10:00Z";
const focusTitle = () => el.ownerDocument.querySelector("[aria-label=Focus] h2")?.textContent;
const focusQuery = () => el.ownerDocument.querySelector("[aria-label=Focus] code")?.textContent;

// expectCommon checks what the pattern's lines have in common: status=201, as its share
// here against its share in the other lines, the counts behind both on hover.
function expectCommon() {
  const row = button("status = 201");
  const common = row?.closest("section");
  expect(common?.querySelector("h3")?.textContent).toBe("What these lines have in common");
  expect(common?.querySelector("p")?.textContent).toBe(
    "Compared with the other 2 lines in this range.",
  );
  expect(row?.textContent).toBe("status = 201here80% · 4elsewhere0%");
  expect(row?.title).toBe("4 of these 5 lines; 0 of the other 2");
}

test("A pattern opens Focus, and a value in it focuses deeper", async () => {
  navigate({ view: "logs", query: engineQuery, mode: "patterns", time: engineTime });
  await expect.poll(() => el.querySelector(".pats > p")?.textContent).toBe("2 of 2 patterns");
  await expect.poll(() => el.querySelector(".pats .pat-n")?.textContent).toBe("5 new");
  await userEvent.click(el.querySelector('.pats button[title="9ef0c50864614821"]') ?? el);
  await expect.poll(focusTitle).toBe("POST /users <*> <*> ms");
  expect(focusQuery()).toBe(`${engineQuery} | pattern_id="9ef0c50864614821"`);
  await expect.poll(() => button("status = 201")).toBeTruthy();
  expectCommon();
  expect(button("Open trace")).toBeTruthy();
  await Promise.all(document.getAnimations().map((a) => a.finished));
  expect(
    (await axe.run(el.ownerDocument.querySelector("[aria-label=Focus]") ?? el)).violations,
  ).toEqual([]);
  await userEvent.click(button("status = 201") ?? el);
  await expect.poll(focusTitle).toBe("status=201");
  expect(focusQuery()).toBe(`${engineQuery} | pattern_id="9ef0c50864614821" | status="201"`);
  await expect.poll(() => el.ownerDocument.querySelector(".focus.stale")).toBeNull();
  expect(onEngine.missed).toEqual([]);
  await userEvent.keyboard("{Escape}");
});

test("Explain lists the shifts in a pattern's lines with the lines behind each share", async () => {
  navigate({ view: "logs", query: engineQuery, mode: "patterns", time: engineTime });
  const row = () => el.querySelector('.pats button[title="9ef0c50864614821"]')?.closest("li");
  await expect.poll(() => row()?.querySelector(".chip")?.textContent).toBe("new");
  await userEvent.click(row()?.querySelector(".pat-x") ?? el);
  await expect
    .poll(() => [...(row()?.querySelectorAll("p button") ?? [])].map((b) => b.textContent))
    .toEqual(["status=201 80% (4) vs 0% (0)", "status=503 20% (1) vs 0% (0)"]);
  expect(onEngine.missed).toEqual([]);
});

test("A slow query keeps the last result dimmed under a progress mark, until superseded", async () => {
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  await expect.poll(() => el.querySelector(".lrow")).toBeTruthy();
  navigate({ view: "logs", query: "slow", mode: "lines" });
  await expect.poll(() => el.querySelector(".logs-b.stale .lrow")).toBeTruthy();
  expect(el.querySelector("progress.busy")).toBeTruthy();
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  await expect.poll(() => slow.at(-1)?.aborted).toBe(true);
  await expect.poll(() => el.querySelector("progress.busy")).toBeNull();
});

test("Compare draws the baseline's volume as a line over the histogram's, in its bins", async () => {
  history.replaceState(null, "", "/ui/logs/time=2026-10-20T13:00:00Z,2026-10-20T15:00:00Z");
  navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
  const aggregates = vi.spyOn(client, "call");
  const compare = () => el.querySelector<HTMLSelectElement>(".hist-h select[aria-label=Compare]");
  await expect.poll(compare).toBeTruthy();
  await userEvent.selectOptions(compare() ?? el, "7d");
  await expect
    .poll(() => el.querySelector(".hist-k.was")?.textContent)
    .toBe("line: a week earlier");
  const baseline = () =>
    aggregates.mock.calls
      .map(([, req]) => JSON.stringify(req))
      .find((r) => r.includes('"from":"2026-10-13T13:00:00.000Z"'));
  await expect.poll(baseline).toBeTruthy();
  // The range's bins are a minute wide over its two hours, and the baseline's are too.
  expect(baseline()).toContain('"bins":"1m"');
  expect(baseline()).toContain('"group_by":"level"');
  await expect.poll(() => el.querySelector(".hist-vs")?.textContent).toMatch(/a week earlier/);
  expect(el.querySelector(".hist [role=img]")?.getAttribute("aria-label")).toMatch(
    /by level, beside [\d,]+ a week earlier$/,
  );
  expect((await axe.run(el)).violations).toEqual([]);
  aggregates.mockRestore();
  await userEvent.selectOptions(compare() ?? el, "");
  await expect.poll(() => el.querySelector(".hist-k.was")).toBeNull();
});

test("A copied link reads the same snapshot over the same absolute range", async () => {
  const copied = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  history.replaceState(null, "", "/ui/logs");
  navigate({ view: "logs", query: '{service="db-3"}', mode: "patterns" });
  await expect.poll(() => el.querySelector(".pats li button[title]")).toBeTruthy();
  await userEvent.click(button("Copy link") ?? el);
  const link = new URL(String(copied.mock.calls[0]?.[0]));
  copied.mockRestore();
  expect(decodeURIComponent(link.pathname)).toMatch(/\/time=\d{4}-[^,]+Z,\d{4}-[^/]+Z\/snap=s$/);
  const reads = vi.spyOn(client, "call");
  history.pushState(null, "", link.pathname);
  dispatchEvent(new PopStateEvent("popstate"));
  const query = () => JSON.stringify(reads.mock.calls.find(([tool]) => tool === "query")?.[1]);
  await expect.poll(query).toContain('"snapshot":"s"');
  expect(query()).toMatch(/"time":\{"from":"\d{4}-/);
  reads.mockRestore();
});

test("Lines and the volume show while the fields are still being ranked", async () => {
  holdFields = true;
  // A range no test ranked fields over before, so they are asked for rather than held.
  const time = "2026-10-20T12:00:00Z,2026-10-20T13:00:00Z";
  navigate({ view: "logs", query: '{service="db-3"} |= "held"', mode: "lines", time });
  await expect.poll(() => el.querySelector(".frail")?.textContent).toBe("FieldsLoading…");
  await expect.poll(() => el.querySelector(".live")?.textContent).toContain("pool timeout 59");
  expect(el.querySelector(".hist [role=img]")).toBeTruthy();
  holdFields = false;
});

test("A number's facet shows its spread, and a bound on it narrows the query", async () => {
  navigate({ view: "logs", query: "timeout", mode: "lines" });
  const cells = (sel: string) => [...el.querySelectorAll(sel)].map((d) => d.textContent);
  await expect.poll(() => cells(".fac-n tbody td")).toEqual(["1", "120", "480", "2.4k"]);
  expect(cells(".fac-n thead th")).toEqual(["service", "min", "p50", "p95", "max"]);
  await expect.poll(() => el.querySelector(".stale")).toBeNull();
  expect((await axe.run(el)).violations).toEqual([]);
  // The test page is narrower than the rail shows at, so the bound is submitted unseen.
  const atLeast = el.querySelector<HTMLInputElement>("input[aria-label='duration_ms >=']");
  if (atLeast) {
    atLeast.value = "500";
  }
  atLeast?.form?.requestSubmit();
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toContain("query=timeout duration_ms:>=500");
});

test("The volume's bars are five minutes wide over a day and an hour wide over a week", async () => {
  const calls = vi.spyOn(client, "call");
  // asked is whether an aggregate went over a span ms long, as bounds, on bins.
  const asked = (span: number, bins: string) =>
    calls.mock.calls.some(([tool, req]) => {
      const r = JSON.stringify(req);
      const [, from = "", to = ""] = /"from":"([^"]+)","to":"([^"]+)"/.exec(r) ?? [];
      return (
        tool === "aggregate" &&
        r.includes(`"bins":"${bins}"`) &&
        Date.parse(to) - Date.parse(from) === span
      );
    });
  for (const [from, span, bins] of [
    ["now-24h", 864e5, "5m"],
    ["now-7d", 7 * 864e5, "1h"],
  ] as const) {
    history.replaceState(null, "", `/ui/logs/time=${from},now`);
    navigate({ view: "logs", query: '{service="db-3"}', mode: "lines" });
    await expect.poll(() => asked(span, bins)).toBe(true);
  }
  calls.mockRestore();
});
