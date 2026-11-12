import {
  countingClient,
  createFakeClient,
  fixtureClient,
  type DataFocalClient,
  type ServiceGraphRequest,
  ToolError,
} from "@datafocal/api";
import { navigate, Shell } from "@datafocal/shell";
import axe from "axe-core";
import { createElement } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, expect, test } from "vitest";
import { page, userEvent } from "vitest/browser";
import anomalies from "../../api/fixtures/find_anomalies.golden.json";
import rcaWalk from "../../api/fixtures/rca_walk.golden.json";
import { GraphView } from "./index";

const meta = { snapshot: "s", coverage: 1, approx: false, cost: { gets: 0, bytes_read: 0 } };
const answer = (data: unknown) => ({ data, meta: { ...meta, token_estimate: 0 } });
const edge = (from: string, to: string) => ({
  from,
  to,
  method: "co-occurrence",
  weight: 1,
  confidence: 0.9,
});
const names = ["web", "checkout", "db", ...Array.from({ length: 100 }, (_, i) => `svc-${i}`)];
const graph = fixtureClient([
  {
    tool: "service_graph",
    answer: answer({
      nodes: ["web", "checkout", "db"].map((id) => ({ id, kind: "workload" })),
      edges: [
        {
          ...edge("web", "checkout"),
          shared: [{ field: "trace_id", value: "t1", from_lines: 3, to_lines: 2 }],
        },
        edge("checkout", "db"),
      ],
    }),
  },
  {
    tool: "aggregate",
    answer: answer({ series: names.map((key) => ({ key, start: 0, step: 1 })) }),
  },
  { tool: "focus", answer: answer({ errors: {} }) },
  { tool: "find_anomalies", answer: anomalies },
  { tool: "rca_walk", answer: rcaWalk },
  {
    tool: "search_logs",
    answer: answer({
      mode: "scan",
      lines: [
        {
          timestamp: "2026-10-20T14:00:01Z",
          level: "error",
          message: "checkout: payment declined",
          pattern_id: "p1",
          service: "checkout",
          row: 1,
        },
      ],
    }),
  },
]);
// Unfocused, the graph is one group of two services.
const grouped = fixtureClient([
  {
    tool: "service_graph",
    answer: answer({
      nodes: [
        {
          id: "group:svc",
          kind: "group",
          group: "svc",
          members: 2,
          member_ids: ["svc-0", "svc-1"],
        },
      ],
      edges: [],
    }),
  },
]);
const graphs: ServiceGraphRequest[] = [];
const walks: unknown[] = [];
const client = createFakeClient({
  callFn: (tool, req, opts) => {
    if (tool === "rca_walk") {
      walks.push(req);
    }
    if (tool === "service_graph" && "max_nodes" in req) {
      graphs.push(req);
    }
    const group = tool === "service_graph" && !("focus" in req);
    return (group ? grouped : graph).call(tool, req, opts);
  },
});

const routes = [
  { path: "/ui/graph", view: GraphView },
  { path: "/ui/logs", view: () => createElement("p", null, "logs") },
  { path: "/ui/trace", view: () => createElement("p", null, "trace") },
];
history.replaceState(null, "", "/ui/graph");
const el = document.body.appendChild(document.createElement("div"));
el.style.height = "600px";
const counted = countingClient(client);
createRoot(el).render(createElement(Shell, { routes, client: counted }));
// Every call reaches the fixtures through a loader, which sends each once, a frame's at once.
afterEach(async () => {
  expect(await counted.faults(), "each call sent once, a frame's at once").toEqual([]);
});

const at = () => decodeURIComponent(location.pathname);
const stage = async () => {
  await expect.poll(() => el.querySelector(".g-stage circle")).toBeTruthy();
  return el.querySelector<HTMLElement>(".g-stage") ?? el;
};

test("grouping refetches the graph at the level chosen, never over max_nodes", async () => {
  await stage();
  const group = (name: string) =>
    userEvent.click(el.querySelector(`[aria-label="Group by"] button:nth-child(${name})`) ?? el);
  // The last hour goes as its bounds at the page's pinned now.
  const hour = (to = "") => ({ from: new Date(Date.parse(to) - 3_600_000).toISOString(), to });
  await expect
    .poll(() => graphs.at(-1))
    .toEqual({
      selector: { time: hour(graphs.at(-1)?.selector.time?.to) },
      max_nodes: 400,
      group_by: "auto",
    });
  await group("3");
  await expect.poll(() => graphs.at(-1)?.group_by).toBe("");
  await group("1");
  await expect.poll(() => graphs.at(-1)?.group_by).toBe("domain");
  expect(graphs.every((g) => g.max_nodes === 400)).toBe(true);
  await group("2");
});

test("the wheel zooms the drawing in place, and asks for no other graph", async () => {
  const s = await stage();
  const asked = graphs.length;
  const layer = () => s.querySelector(".g-map > g")?.getAttribute("transform");
  s.querySelector(".g-map")?.dispatchEvent(
    new WheelEvent("wheel", { bubbles: true, cancelable: true, deltaY: -100 }),
  );
  await expect.poll(layer).toMatch(/scale\(1\.3/);
  await userEvent.click(s.querySelector("[aria-label^='Fit the graph']") ?? s);
  expect(layer()).toBe("translate(0 0) scale(1)");
  expect(graphs.length).toBe(asked);
});

test("choosing a search result focuses that service", async () => {
  await userEvent.fill(el.querySelector("input[type=search]") ?? el, "CHECK");
  await userEvent.click(el.querySelector(".g-search li button") ?? el);
  expect(at()).toBe("/ui/graph/focus=checkout");
  await expect.poll(() => graphs.at(-1)).toMatchObject({ focus: "checkout", hops: 1 });
});

test("L opens the focused service's lines", async () => {
  navigate({ view: "graph", focus: "checkout", title: "From trace" });
  const s = await stage();
  await expect.poll(() => el.querySelector(".g-act")).toBeTruthy();
  expect(el.querySelector(".g-tb")?.textContent).toContain("From traceBack");
  s.focus();
  await userEvent.keyboard("l");
  expect(at()).toBe('/ui/logs/query={service="checkout"}/mode=lines/title=checkout');
});

test("an edge, drawn or listed, opens how it was found, each end's lines and theirs", async () => {
  navigate({ view: "graph", focus: "checkout" });
  const s = await stage();
  expect(el.querySelector(".top")?.textContent).not.toContain("Compare");
  // A line level across the map has no height, which a pointer's click waits on in vain.
  s.querySelector(".g-e")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
  await expect.poll(() => el.querySelector(".drawer h2")?.textContent).toContain(" → ");
  const links = [...el.querySelectorAll(".g-side .g-list button")];
  await userEvent.click(links.find((b) => b.textContent === "web → checkout") ?? el);
  await expect.poll(() => el.querySelector(".drawer h2")?.textContent).toBe("web → checkout");
  expect(el.querySelector(".g-why h3")?.textContent).toBe("Why they are linked · co-occurrence");
  expect(el.querySelector(".g-why dl")?.textContent).toContain("Confidence0.90");
  expect(el.querySelector(".drawer .ln")?.textContent).toContain("checkout: payment declined");
  await userEvent.keyboard("{Escape}");
  await expect.poll(() => el.querySelector(".drawer")).toBeNull();
});

test("a value an edge's ends share, listed with their lines, focuses on both ends' lines with it", async () => {
  navigate({ view: "graph", focus: "checkout" });
  await stage();
  const links = [...el.querySelectorAll(".g-side .g-list button")];
  await userEvent.click(links.find((b) => b.textContent === "web → checkout") ?? el);
  const shared = () => [...el.querySelectorAll(".drawer-b li")].at(-1);
  await expect.poll(() => shared()?.textContent).toBe("trace_id = t13 / 2 lines");
  await userEvent.click(shared()?.querySelector("button") ?? el);
  const focus = () => el.ownerDocument.querySelector("[aria-label=Focus]");
  await expect.poll(() => focus()?.querySelector("h2")?.textContent).toBe("trace_id=t1");
  expect(focus()?.querySelector("code")?.textContent).toBe(
    '{service=~"^(web|checkout)$"} | trace_id="t1"',
  );
  await userEvent.keyboard("{Escape}");
});

test("Traces opens the trace view on the traces touching both ends of an edge", async () => {
  navigate({ view: "graph", focus: "checkout" });
  const s = await stage();
  await expect.poll(() => el.querySelector(".g-act")).toBeTruthy();
  await userEvent.click(s.querySelector('[data-id="db"] circle') ?? s);
  await expect.poll(() => el.querySelector(".g-act")?.textContent).toBe("LogsTracesSummary");
  expect((await axe.run(el)).violations).toEqual([]);
  await userEvent.click(el.querySelector(".g-act button:nth-child(2)") ?? el);
  expect(at()).toBe(
    '/ui/trace/where=service="checkout" service="db"/title=From service graph · checkout → db',
  );
});

test("Summary opens Focus on the selection's lines, which a click on a node does not", async () => {
  navigate({ view: "graph", focus: "checkout" });
  const s = await stage();
  const focus = () => el.ownerDocument.querySelector("[aria-label=Focus] h2")?.textContent;
  await userEvent.click(s.querySelector('[data-id="db"] circle') ?? s);
  await expect.poll(() => el.querySelector(".g-pick h3")?.textContent).toBe("db");
  expect(focus()).toBeUndefined();
  await userEvent.click(el.querySelector(".g-act button:nth-child(3)") ?? el);
  await expect.poll(focus).toBe("checkout → db");
  await userEvent.keyboard("{Escape}");
});

test("a group node offers its members' lines", async () => {
  navigate({ view: "graph" });
  const s = await stage();
  await expect.poll(() => s.querySelector('[data-id="group:svc"] circle')).toBeTruthy();
  await userEvent.click(s.querySelector('[data-id="group:svc"] circle') ?? s);
  await expect.poll(() => el.querySelector(".g-act")?.textContent).toBe("LogsSummary");
  await userEvent.click(el.querySelector(".g-act button") ?? el);
  expect(at()).toBe('/ui/logs/query={service=~"^(svc-0|svc-1)$"}/mode=lines/title=svc');
});

test("Investigate waits for the user, then lists rca_walk's hops in order with their lines", async () => {
  // The window holds the seeded incident: db's spike at 14:02:15.
  history.pushState(null, "", "/ui/graph/time=2026-07-18T13:30:00Z,2026-07-18T14:30:00Z");
  dispatchEvent(new PopStateEvent("popstate"));
  const s = await stage();
  await expect.poll(() => el.querySelector(".g-banner")?.textContent).toContain("db new_pattern");
  await expect.poll(() => graphs.at(-1)?.selector.time?.from).toBe("2026-07-18T13:30:00Z");
  expect([walks.length, at().includes("mode="), el.querySelector(".g-inv")]).toEqual([
    0,
    false,
    null,
  ]);
  s.focus();
  expect(el.querySelector(".g-tb select[aria-label=Compare]")).toBeNull();
  await userEvent.keyboard("r");
  expect(at()).toContain("/mode=investigate");
  await expect.poll(() => el.querySelector(".g-tb select[aria-label=Compare]")).toBeTruthy();
  await expect
    .poll(() => walks)
    .toEqual([
      {
        selector: { time: { from: "2026-07-18T13:30:00Z", to: "2026-07-18T14:30:00Z" } },
        symptom: { pattern_id: "168b725e27267cf" },
      },
    ]);
  const { chain } = rcaWalk.data;
  for (const [i, hop] of chain.entries()) {
    await expect.poll(() => el.querySelectorAll(".g-inv li").length).toBe(chain.length);
    await userEvent.click(el.querySelectorAll(".g-inv li")[i]?.querySelector("button") ?? el);
    expect(at()).toContain(
      `/ui/logs/query={service="${hop.to}"} | pattern_id="${hop.pattern_to}"/mode=lines/title=Hop ${i + 1} · ${hop.to}`,
    );
    history.back();
    await expect.poll(at).toContain("/ui/graph/");
  }
});

test("an empty graph names its time window", async () => {
  const time = { from: "2026-07-27T00:00:00Z", to: "2026-07-28T00:00:00Z" };
  const empty = fixtureClient([
    { tool: "find_anomalies", req: { selector: { time } }, answer: answer({ anomalies: [] }) },
    {
      tool: "service_graph",
      req: { selector: { time }, max_nodes: 400, group_by: "auto" },
      answer: answer({ nodes: [], edges: [] }),
    },
    {
      tool: "aggregate",
      req: { selector: { time }, measure: "count", group_by: "service", limit: 1e6 },
      answer: answer({ series: [] }),
    },
  ]);
  history.replaceState(null, "", `/ui/graph/time=${encodeURIComponent(`${time.from},${time.to}`)}`);
  const host = document.body.appendChild(document.createElement("div"));
  const root = createRoot(host);
  root.render(createElement(Shell, { routes, client: empty }));
  await expect
    .poll(() => host.querySelector('.g-stage [role="status"]')?.textContent)
    .toBe(`No services logged between ${time.from} and ${time.to}.`);
  root.unmount();
  host.remove();
  expect(empty.missed).toEqual([]);
});

// investigated mounts the graph investigating the day of 2026-07-27 on client, and returns
// the root-cause section's and the banner's text once one settles, then unmounts.
async function investigated(client: DataFocalClient, settled: string) {
  const time = encodeURIComponent("2026-07-27T00:00:00Z,2026-07-28T00:00:00Z");
  history.replaceState(null, "", `/ui/graph/time=${time}/mode=investigate`);
  const host = document.body.appendChild(document.createElement("div"));
  const root = createRoot(host);
  root.render(createElement(Shell, { routes, client }));
  const text = () => host.querySelector(".graph")?.textContent ?? "";
  await expect.poll(text).toContain(settled);
  const shown = text();
  root.unmount();
  host.remove();
  return shown;
}

// quiet answers every call of an investigation over a quiet day but find_anomalies' and
// rca_walk's, and the names aggregate only to its own request, so a lanes aggregate misses.
const quiet = [
  { tool: "service_graph", answer: answer({ nodes: [], edges: [] }) },
  {
    tool: "aggregate",
    req: {
      selector: { time: { from: "2026-07-27T00:00:00Z", to: "2026-07-28T00:00:00Z" } },
      measure: "count",
      group_by: "service",
      limit: 1e6,
    },
    answer: answer({ series: [] }),
  },
];

test("a walk with no chain says why, and asks for no lanes", async () => {
  const at = "2026-07-27T12:00:00Z";
  const rejected = ["p2", "p3"].map((id) => ({ pattern_to: id, rejected_by: "no_attribution" }));
  const anomalies = [{ kind: "spike", pattern_id: "p1", service: "db", at }];
  const walk = fixtureClient([
    ...quiet,
    { tool: "find_anomalies", answer: answer({ anomalies }) },
    { tool: "rca_walk", answer: answer({ chain: null, rejected }) },
  ]);
  expect(await investigated(walk, "No causal chain found")).toContain(
    `No causal chain found for db's spike at ${at}. 2 moved with it but share no trace, correlation value or host with it.`,
  );
  expect(walk.missed).toEqual([]);
});

test("investigating a window with no anomalies says there is no incident", async () => {
  const none = fixtureClient([
    ...quiet,
    { tool: "find_anomalies", answer: answer({ anomalies: [] }) },
  ]);
  await investigated(none, "No incident in this window.");
  expect(none.missed).toEqual([]);
});

test("a failed find_anomalies says it failed, not that there is no incident", async () => {
  const rest = fixtureClient(quiet);
  const failing = createFakeClient({
    callFn: (tool, req, opts) =>
      tool === "find_anomalies"
        ? Promise.reject(new ToolError({ code: "internal", message: "down", retryable: false }))
        : rest.call(tool, req, opts),
  });
  expect(await investigated(failing, "Failed: internal: down")).not.toContain("No incident");
});

// The 12-service call tree service_graph draws from nested timing.
const calls = [
  ["frontend", "api-gateway"],
  ["api-gateway", "auth"],
  ["api-gateway", "cart"],
  ["cart", "redis"],
  ["api-gateway", "checkout"],
  ["checkout", "inventory"],
  ["inventory", "postgres"],
  ["checkout", "payment"],
  ["payment", "fraud"],
  ["payment", "stripe-proxy"],
  ["checkout", "email"],
];
const tree = fixtureClient([
  {
    tool: "service_graph",
    answer: answer({
      nodes: ["frontend", ...calls.map(([, to]) => to)].map((id) => ({ id, kind: "workload" })),
      edges: calls.map(([from = "", to = ""], i) => ({
        ...edge(from, to),
        method: "call",
        weight: 300,
        calls: 300,
        error_ratio: i === 7 ? 0.1 : 0,
        p50_ms: 20 + i,
        p95_ms: 40 + i,
      })),
    }),
  },
  {
    tool: "aggregate",
    answer: answer({
      series: rcaWalk.data.chain.map((h) => ({
        key: h.pattern_to,
        start: 0,
        step: 60,
        values: [1, 4, 2],
      })),
    }),
  },
]);

// overlap reports whether boxes a and b share any area.
const overlap = (a: DOMRect, b: DOMRect) =>
  a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;

// crosses reports whether a point along path, every 2 units, falls in box.
function crosses(path: SVGPathElement, box: DOMRect): boolean {
  const ctm = path.getScreenCTM() ?? new DOMMatrix();
  for (let l = 0; l <= path.getTotalLength(); l += 2) {
    const { x, y } = path.getPointAtLength(l).matrixTransform(ctm);
    if (x > box.left && x < box.right && y > box.top && y < box.bottom) {
      return true;
    }
  }
  return false;
}

test("the 12-service call tree reads at 1280 px: no label crosses an edge or a label, and each opens", async () => {
  await page.viewport(1280, 800);
  el.style.display = "none";
  const window = "time=2026-07-18T13:30:00Z,2026-07-18T14:30:00Z";
  history.replaceState(null, "", `/ui/graph/${window}/mode=investigate`);
  const host = document.body.appendChild(document.createElement("div"));
  host.style.height = "800px";
  const client = createFakeClient({
    callFn: (tool, req, opts) =>
      (tool === "service_graph" || tool === "aggregate" ? tree : graph).call(tool, req, opts),
  });
  const root = createRoot(host);
  root.render(createElement(Shell, { routes, client }));
  await expect.poll(() => host.querySelectorAll(".g-lanes canvas").length).toBeGreaterThan(0);
  const boxes = (q: string) => [...host.querySelectorAll(q)].map((n) => n.getBoundingClientRect());
  const names = boxes(".g-n text");
  const labels = [...names, ...boxes(".g-el:not(.short)")];
  const edges = [...host.querySelectorAll<SVGPathElement>(".g-e")];
  for (const [i, box] of names.entries()) {
    expect(edges.filter((e) => crosses(e, box))).toEqual([]);
    expect(labels.filter((l, j) => i !== j && overlap(box, l))).toEqual([]);
  }
  const header = boxes(".g-tb > *, .g-inv > :not(ol, .g-lanes), .g-inv li, .g-lanes > *");
  expect(header.filter((a, i) => header.some((b, j) => i < j && overlap(a, b)))).toEqual([]);
  for (const node of host.querySelectorAll(".g-n")) {
    const id = node.getAttribute("data-id");
    await userEvent.click(node.querySelector(".nm") ?? node);
    await expect.poll(() => host.querySelector(".g-pick h3")?.textContent).toBe(id);
  }
  root.unmount();
  host.remove();
  el.style.display = "";
});
