import {
  type FixtureClient,
  type FixtureOptions,
  fixtureClient,
  type Requests,
} from "@datafocal/api";
import { Shell } from "@datafocal/shell";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterAll, afterEach, expect, test, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import engine from "../fixtures/engine.json";
import { SLOsView } from "./index";

// The incident corpus with gateway, grouped by endpoint, and gateway-prometheus (slo_test.go).
const time = `time=${encodeURIComponent("2026-07-18T14:00:00Z,2026-07-18T14:10:00Z")}`;
const { data: listed } = await fixtureClient(engine).call("list_slos", {
  limit: 60,
  trend: true,
});
let root: Root | undefined;
let fixtures: FixtureClient | undefined;
// used is every engine answer a test read, which by the last test is every one in requests.json.
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

// render opens path on the engine's answers, or answer's to the requests it answers, each
// tool's answer taking its delay.
function render(
  path: string,
  answer?: (tool: keyof Requests, req: Requests[keyof Requests]) => unknown,
  delays: FixtureOptions["delays"] = {},
) {
  history.replaceState(null, "", `/ui/slos/${time}${path}`);
  const client = fixtureClient(engine, {
    answer: (tool, req) => {
      const data = answer?.(tool, req);
      return data === undefined ? undefined : { data, meta: {} };
    },
    delays,
  });
  fixtures = client;
  const routes = [
    { path: "/ui/slos", view: SLOsView },
    { path: "/ui/logs", view: () => null },
  ];
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(createElement(Shell, { routes, client }));
}

const cards = () =>
  [...document.querySelectorAll('[aria-label="SLOs"] article')].map((c) => c.ariaLabel);
const rows = (label: string) =>
  [...document.querySelectorAll(`table[aria-label="${label}"] tbody tr`)].map((r) => r.textContent);
const click = (name: string) => userEvent.click(page.getByRole("button", { name, exact: true }));

test("a built SLO is saved, then listed", async () => {
  let saved = "";
  render(`/mode=new/query=${encodeURIComponent('{service="gateway"}')}`, (tool, req) => {
    if ("dry_run" in req && req.dry_run === false) {
      saved = req.content;
      return { documents: [], content: "", saved: true };
    }
    const added = { ...listed.slos[0], name: "gateway-5xx", groups: [] };
    return tool === "list_slos" && saved ? { ...listed, slos: [...listed.slos, added] } : undefined;
  });
  await userEvent.fill(page.getByLabelText("Name"), "gateway-5xx");
  await click("Save");
  await expect.poll(() => saved).not.toBe("");
  expect(JSON.parse(saved)).toMatchObject({ metadata: { name: "gateway-5xx" } });
  await expect.poll(cards).toContain("gateway-5xx");
});

test("with the directory read-only, Save is Export, of the file export_definitions writes", async () => {
  const urls = vi.spyOn(URL, "createObjectURL");
  render("/slo=gateway/mode=edit", (tool) =>
    tool === "list_slos" ? { ...listed, writable: false } : undefined,
  );
  await click("Export");
  expect(document.querySelector("[role=note]")?.textContent).toContain("read-only (mounted image)");
  const file = urls.mock.calls[0]?.[0];
  const want = await fixtures?.call("export_definitions", { names: ["gateway"] });
  expect(file instanceof Blob && (await file.text())).toBe(want?.data.content);
});

test("the badge counts the firing SLOs and opens them", async () => {
  const firing = listed.slos
    .filter((s) => s.name === "gateway")
    .map((s) => ({ ...s, state: "firing" }));
  render("", (_, req) =>
    "state" in req && req.state ? { ...listed, slos: firing, total: firing.length } : undefined,
  );
  await userEvent.click(page.getByRole("link", { name: "1 firing" }));
  expect(location.pathname).toContain("/state=firing");
  await expect.poll(cards).toEqual(["gateway"]);
  expect(document.querySelector("article .slo-state")?.textContent).toBe("Firing");
});

const tiles = () => [...document.querySelectorAll(".kpi small")].map((k) => k.textContent);
const path = () => decodeURIComponent(location.pathname);

test("a part whose answer is slow holds back only itself", async () => {
  render("/slo=gateway", undefined, { explain_spike: 2000 });
  const shared = page.getByRole("region", { name: "endpoint", exact: true });
  const drawn = performance.now();
  await expect.poll(() => rows("Groups").length, { timeout: 1500 }).toBe(2);
  expect(tiles()).toContain("SLI");
  expect(shared.query(), "the part explain_spike draws").toBeNull();
  expect(performance.now() - drawn).toBeLessThan(2000);
  await expect.element(shared, { timeout: 3000 }).toBeVisible();
});

test("an event SLO's page drills from what its bad events share to one of them, each step undone by Back", async () => {
  render("/slo=gateway");
  // Its burn tile is its alert condition's, over that condition's windows.
  await expect
    .poll(tiles)
    .toEqual(["SLI", "Error budget", "Budget in 1h", "Burn 1h · fast", "Bad events"]);
  expect(document.querySelector("[aria-label='Groups'] th:nth-child(2)")?.textContent).toBe(
    "endpoint",
  );
  expect(rows("Conditions")).toEqual(["fastokpage2.6× · 3.6×14.4×—"]);
  expect(rows("Groups").map((r) => r?.match(/(\/[a-z]+)/)?.[1])).toEqual(["/users", "/health"]);
  const shared = page.getByRole("region", { name: "endpoint", exact: true });
  await userEvent.click(shared.getByRole("button", { name: /^\/users/ }));
  expect(path()).toContain('/filter=[{"field":"endpoint","op":"eq","value":"/users"}]');
  await expect.element(page.getByRole("button", { name: "endpoint=/users ×" })).toBeVisible();

  // A bad event opens in the inspector, its place in the URL, with the links every view shares.
  await expect.poll(() => rows("Bad events").length).toBeGreaterThan(0);
  await userEvent.click(page.getByRole("table", { name: "Bad events" }).getByRole("row").nth(1));
  expect(path()).toMatch(/\/pick=[^/]+\/\d+/);
  const links = page.getByRole("group", { name: "Open in" });
  await expect.element(links.getByRole("button", { name: "Logs", exact: true })).toBeVisible();
  await expect.element(page.getByRole("table", { name: "The request's steps" })).toBeVisible();

  // Back closes it, then takes the filter off.
  history.back();
  await expect.poll(path).not.toContain("/pick=");
  await expect
    .element(page.getByRole("table", { name: "The request's steps" }))
    .not.toBeInTheDocument();
  history.back();
  await expect.poll(path).not.toContain("/filter=");
});

test("a time-slice SLO's page lists its bad slices, one narrowing the page, and what changed in it", async () => {
  render("/slo=gateway-clean");
  // With no alert policy, its burn is over 1h and 6h.
  await expect
    .poll(tiles)
    .toEqual(["SLI", "Error budget", "Budget in 1h", "Burn 1h", "Burn 6h", "Bad slices"]);
  await expect.poll(() => rows("Bad slices").length).toBeGreaterThan(0);
  const before = path();
  await userEvent.click(page.getByRole("table", { name: "Bad slices" }).getByRole("row").nth(1));
  await expect.poll(path).not.toBe(before);
  expect(path()).toMatch(/\/time=[\dT:.Z-]+,[\dT:.Z-]+/);
  await expect
    .poll(
      () => [...document.querySelectorAll("section[aria-label$='against as long before']")].length,
    )
    .toBeGreaterThan(0);
  history.back();
  await expect.poll(path).toBe(before);
});

test("over more than a day, what reads lines reads the range's last day, and says so", async () => {
  const spans: number[] = [];
  const long = `time=${encodeURIComponent("2026-07-16T14:10:00Z,2026-07-18T14:10:00Z")}`;
  // Only the spans are checked, so what reads lines answers empty; the SLO and its definition
  // are the engine's.
  const empty: Partial<Record<keyof Requests, unknown>> = {
    aggregate: { series: [] },
    search_logs: { mode: "lines", lines: [] },
    explain_spike: { shifts: [], lines: 0, rest: 0 },
    rank_fields: { fields: [] },
    resource_usage: {
      start: 0,
      step: 0,
      hosts: [],
      groups: [],
      total: { key: "", summary: {} },
      columns: [],
    },
  };
  render(`/slo=gateway/${long}`, (tool, req) => {
    const time = "selector" in req ? req.selector?.time : undefined;
    if (time?.from && time.to && tool !== "slo_status") {
      spans.push(Date.parse(time.to) - Date.parse(time.from));
    }
    return empty[tool];
  });
  await expect.element(page.getByRole("note")).toHaveTextContent(/the range's last day/);
  await expect.poll(() => spans.length).toBeGreaterThan(0);
  expect(Math.max(...spans)).toBeLessThanOrEqual(24 * 3_600_000);
});
