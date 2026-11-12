import { type FixtureClient, fixtureClient, type Requests, type ToolName } from "@datafocal/api";
import { Shell } from "@datafocal/shell";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterAll, afterEach, expect, test, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import engine from "../fixtures/engine.json";
import { ViewsView } from "./index";

// The factory corpus with the sample view, deploy/definitions/view/querier.yaml.
const time = `time=${encodeURIComponent("2026-07-27T00:00:00Z,2026-07-28T00:00:00Z")}`;
const { data: sample } = await fixtureClient(engine).call("get_view", { name: "querier" });
let root: Root | undefined;
let fixtures: FixtureClient | undefined;
const used = new Set<unknown>();
afterEach(async () => {
  expect(await fixtures?.faults(), "each call sent once, a frame's at once").toEqual([]);
  root?.unmount();
  expect(fixtures?.missed, "every request the view sent is in requests.json").toEqual([]);
  for (const a of fixtures?.used ?? []) {
    used.add(a);
  }
});
afterAll(() => {
  const unused = engine.filter((a) => !used.has(a)).map((a) => ({ tool: a.tool, req: a.req }));
  expect(unused, "every request in requests.json is one a test sends").toEqual([]);
});

type Answer = (tool: ToolName, req: Requests[ToolName]) => unknown;

// render opens path on the engine's answers, or answer's to the requests it answers.
function render(path: string, answer?: Answer) {
  for (const a of fixtures?.used ?? []) {
    used.add(a); // a test rendering twice reads through two clients
  }
  history.replaceState(null, "", `/ui/views/${path}`);
  fixtures = fixtureClient(engine, {
    answer: (tool, req) => {
      const data = answer?.(tool, req);
      return data instanceof Error ? Promise.reject(data) : data && { data, meta: {} };
    },
  });
  const routes = [{ path: "/ui/views", view: ViewsView }];
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(createElement(Shell, { routes, client: fixtures }));
}

const queries = () => (fixtures?.sent ?? []).filter((s) => s.tool === "query");
const panelCalls = () => (fixtures?.sent ?? []).filter((s) => s.view);
const titles = () =>
  [...document.querySelectorAll(".vw-cell h3")].map((h) => h.firstChild?.textContent);
const click = (name: string) => userEvent.click(page.getByRole("button", { name, exact: true }));
// withPanels is the sample view holding panels in place of its own.
const withPanels = (...panels: readonly object[]) => ({
  ...sample,
  view: { ...sample.view, panels },
});
const series = {
  series: [
    { key: "", start: Date.parse("2026-07-18T14:00:00Z"), step: 60_000, values: [1, 5, 2, 7] },
  ],
};
const chart = (q: string) => ({
  canonical: q,
  call: { tool: "aggregate", args: { group_by: "" } },
  result: series,
});

test("a built view is saved, then listed", async () => {
  let saved = "";
  render(`${time}/mode=new`, (tool, req) => {
    if ("dry_run" in req && req.dry_run === false) {
      saved = req.content;
      return { documents: [], content: "", saved: true };
    }
    const added = { name: "gateway-errors", title: "Gateway errors", panels: 1 };
    return tool === "list_views" && saved ? { views: [added], writable: true } : undefined;
  });
  await userEvent.fill(page.getByLabelText("Title"), "Gateway errors");
  await click("Add panel");
  await userEvent.fill(page.getByLabelText("Query"), '{service="querier"} | status>=500');
  await click("Save");
  await expect.poll(() => saved).not.toBe("");
  expect(JSON.parse(saved)).toMatchObject({
    metadata: { name: "gateway-errors" },
    spec: { panels: [{ kind: "lines" }] },
  });
  await expect.element(page.getByRole("button", { name: "gateway-errors" })).toBeVisible();
});

test("changing a variable rewrites every panel, and its permalink reproduces them", async () => {
  render(`${time}/name=querier`);
  await expect.poll(() => panelCalls().length).toBe(sample.view.panels.length);
  // Every panel draws its first answer before the variable changes, so none is aborted.
  await expect
    .poll(() => document.querySelector(".vw-grid")?.textContent)
    .not.toContain("Loading…");
  const select = page.getByRole("group", { name: "Variables" }).getByRole("combobox");
  await userEvent.selectOptions(select, "All");
  await expect
    .poll(() => queries().filter((s) => JSON.stringify(s.req).includes('"path":"*"')).length)
    .toBe(5);
  const link = location.pathname;
  expect(decodeURIComponent(link)).toContain("/var-path=*");
  root?.unmount();
  render(link.replace("/ui/views/", ""));
  await expect.poll(() => panelCalls().length).toBe(sample.view.panels.length);
  expect(
    panelCalls().every(
      (s) => JSON.stringify(s.req).includes('"path":"*"') || s.tool === "list_slos",
    ),
  ).toBe(true);
});

test("each panel's call names it, and a failed panel leaves the others drawn", async () => {
  render(`${time}/name=querier`, (tool, req) =>
    tool === "query" && "q" in req && req.q.includes("patterns")
      ? new Error("no patterns today")
      : undefined,
  );
  await expect.element(page.getByRole("alert")).toHaveTextContent("no patterns today");
  await expect
    .poll(() => document.querySelectorAll(".vw-cell .panel > :last-child:not(.failure)").length)
    .toBe(5);
  expect(
    panelCalls()
      .map((s) => s.view ?? "")
      .sort((a, b) => a.localeCompare(b)),
  ).toEqual([0, 1, 2, 3, 4, 5].map((i) => `querier/${i}`));
});

test("two panels with the same query make one call", async () => {
  const p = sample.view.panels[2] ?? {};
  render(`${time}/name=querier`, (tool) =>
    tool === "get_view" ? withPanels(p, { ...p, title: "Again" }) : undefined,
  );
  await expect.poll(titles).toEqual(["Failed requests", "Again"]);
  await expect.poll(() => document.querySelectorAll(".vw-cell .big").length).toBe(2);
  expect(queries()).toHaveLength(1);
});

test("a panel below the fold waits to be scrolled to, and one off screen skips a refresh until back", async () => {
  vi.useFakeTimers({ toFake: ["setInterval", "Date"], now: Date.parse("2026-07-18T14:10:00Z") });
  const q = (title: string) => ({
    title,
    kind: "timeseries",
    query: `count_over_time({level="${title}"} [1m])`,
    grid: { x: 0, y: title === "Top" ? 0 : 60, w: 12, h: 4 },
  });
  render("time=now-10m%2Cnow/name=querier/var-path=*", (tool) =>
    tool === "get_view"
      ? withPanels(q("Top"), q("Bottom"))
      : tool === "query"
        ? chart("q")
        : tool === "aggregate"
          ? series
          : undefined,
  );
  const reads = (i: number) => panelCalls().filter((s) => s.view === `querier/${i}`).length;
  await expect.poll(() => reads(0)).toBe(1);
  expect(reads(1)).toBe(0);
  document.querySelector(".shell > main")?.scrollTo(0, 1e5);
  await expect.poll(() => reads(1)).toBe(1);
  document.querySelector(".shell > main")?.scrollTo(0, 0);
  await expect.poll(() => document.querySelectorAll(".vw-cell.stale").length).toBe(0);
  await new Promise((r) => setTimeout(r, 100)); // for the observer to see it leave
  vi.advanceTimersByTime(30_000);
  await expect.poll(() => reads(0)).toBe(2);
  expect(reads(1)).toBe(1);
  await expect
    .poll(() => document.querySelector(".vw-cell.stale h3")?.firstChild?.textContent)
    .toBe("Bottom");
  document.querySelector(".shell > main")?.scrollTo(0, 1e5);
  await expect.poll(() => reads(1)).toBe(2);
  vi.useRealTimers();
});

test("with the directory read-only, Save is Export, of the file export_definitions writes", async () => {
  const urls = vi.spyOn(URL, "createObjectURL");
  render(`${time}/name=querier/mode=edit`, (tool) =>
    tool === "list_slos" ? { slos: [], total: 0, writable: false } : undefined,
  );
  await click("Export");
  expect(document.querySelector("[role=note]")?.textContent).toContain("read-only (mounted image)");
  const file = urls.mock.calls[0]?.[0];
  const want = await fixtures?.call("export_definitions", { names: ["View/querier"] });
  expect(file instanceof Blob && (await file.text())).toBe(want?.data.content);
});

test("a timeseries brush narrows the range, and a pick opens a Moment", async () => {
  const p = sample.view.panels[0] ?? {};
  render(`${time}/name=querier/var-path=*`, (tool) =>
    tool === "get_view"
      ? withPanels(p)
      : tool === "query"
        ? chart("q")
        : tool === "aggregate"
          ? series
          : tool === "focus"
            ? new Error("no focus")
            : undefined,
  );
  await expect.poll(() => document.querySelector(".u-over")).toBeTruthy();
  const over = document.querySelector(".u-over");
  const box = over?.getBoundingClientRect() ?? new DOMRect();
  const at = (type: string, f: number) =>
    over?.dispatchEvent(
      new MouseEvent(type, {
        clientX: box.left + f * box.width,
        clientY: box.top + 10,
        movementX: 1,
        bubbles: true,
      }),
    );
  at("mousedown", 0.25);
  at("mousemove", 0.75);
  at("mouseup", 0.75);
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toMatch(/\/time=2026-07-18T14:0[0-3]:[\d.]+Z,2026-07-18T14:0[0-3]:[\d.]+Z/);
  for (const type of ["mousemove", "mousedown", "mouseup", "click"]) {
    at(type, 1);
  }
  await expect.element(page.getByLabelText("Focus")).toBeVisible();
});

test.each([
  ["a Logs query", '{service="gateway"} |~ "timeout"', "lines"],
  ["an AI view chart", "sum by (model) (sum_over_time({} | unwrap cost_usd [1h]))", "timeseries"],
])("Add to view from %s opens the builder with its panel appended", async (_, query, kind) => {
  render(`${time}/query=${encodeURIComponent(query)}`);
  await click("querier");
  const last = page.getByRole("group", { name: `Panel ${sample.view.panels.length + 1}` });
  await expect.element(last.getByLabelText("Query")).toHaveValue(query);
  await expect.element(last.getByLabelText("Kind")).toHaveValue(kind);
});
