import { createFakeClient, type FakeClient, fixtureClient } from "@datafocal/api";
import { Annotation, Chart, Inspector, Menu, NormalityChip } from "@datafocal/ui";
import axe from "axe-core";
import { createElement, type ReactElement, useEffect } from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, test, vi } from "vitest";
import { userEvent } from "vitest/browser";
import { Compare, navigate, Shell, useEvidence, useFocus, useParams, useTimeRange } from "./index";

const start = location.pathname;
let root: Root | undefined;
afterEach(() => {
  root?.unmount();
  document.querySelector("#app")?.remove();
  delete document.documentElement.dataset["theme"];
  history.replaceState(null, "", start);
});

const focusTarget = {
  title: "slow query <*> ms",
  query: '{service=~".+"} | pattern_id="p1"',
  selector: { time: { from: "now-1h", to: "now" } },
  slice: [{ field: "pattern_id", op: "eq", value: "p1" }],
};

// Probe renders its view's name and query, with the controls the tests drive, compare's
// among them.
function Probe({ name }: { name: string }): ReactElement {
  const params = useParams();
  const showEvidence = useEvidence();
  const focus = useFocus();
  const [, setRange] = useTimeRange();
  const evidence = { title: "db-3 errors", lines: [], computed: "count", query: "{}" };
  return createElement(
    "div",
    null,
    createElement("h1", null, [name, params["query"]].filter(Boolean).join(" ")),
    createElement("button", { onClick: () => showEvidence(evidence) }, "Evidence"),
    createElement("button", { onClick: () => focus(focusTarget) }, "Focus"),
    createElement(
      "button",
      { onClick: () => setRange({ from: "2026-10-20T13:30:00Z", to: "2026-10-20T14:30:00Z" }) },
      "Zoom",
    ),
    createElement(Compare),
  );
}

const routes = ["ai", "logs", "graph", "trace", "resources"].map((name) => ({
  path: `/ui/${name}`,
  view: () => createElement(Probe, { name }),
}));

function render(
  path: string,
  view?: () => ReactElement,
  client: FakeClient = createFakeClient({}),
): HTMLElement {
  history.replaceState(null, "", path);
  const el = document.body.appendChild(document.createElement("div"));
  el.id = "app";
  root = createRoot(el);
  const rs = view ? [{ path, view }] : routes;
  flushSync(() => root?.render(createElement(Shell, { routes: rs, client })));
  return el;
}

const heading = () => document.querySelector("h1")?.textContent;
const button = (name: string) =>
  [...document.querySelectorAll("button")].find((b) => b.textContent?.startsWith(name));

test("Shell renders its view with no accessibility violations", async () => {
  const el = render("/", () => createElement("h1", null, "Logs"));
  expect(el.querySelector("main")?.textContent).toBe("Logs");
  expect((await axe.run(el)).violations).toEqual([]);
});

test("keys 1–5 switch views", async () => {
  render("/ui/ai");
  for (const [key, name] of [
    ["2", "logs"],
    ["3", "graph"],
    ["4", "trace"],
    ["5", "resources"],
    ["1", "ai"],
  ]) {
    await userEvent.keyboard(key ?? "");
    await expect.poll(heading).toBe(name);
  }
  expect(document.querySelector("[aria-current=page]")?.getAttribute("aria-label")).toBe(
    "AI systems",
  );
});

test("the top bar reads the range in words and copies a link, and opens no palette", async () => {
  const copied = vi.spyOn(navigator.clipboard, "writeText").mockResolvedValue();
  render("/ui/ai");
  expect(document.querySelector(".top")?.textContent).toBe("15m1h6h24h7d30dlast 1hCopy link");
  await userEvent.keyboard("{Control>}k{/Control}");
  expect(document.querySelector("dialog")).toBeNull();
  await userEvent.click(button("Copy link") ?? document.body);
  await expect.poll(() => button("Link copied")).toBeTruthy();
  expect(decodeURIComponent(String(copied.mock.calls[0]?.[0]))).toMatch(
    /\/ui\/ai\/time=\d{4}-[^,]+Z,\d{4}-/,
  );
  copied.mockRestore();
});

test("Esc closes the evidence drawer", async () => {
  render("/ui/ai");
  await userEvent.click(button("Evidence") ?? document.body);
  await expect.poll(() => document.querySelector(".drawer h2")?.textContent).toBe("db-3 errors");
  await userEvent.keyboard("{Escape}");
  await expect.poll(() => document.querySelector(".drawer")).toBeNull();
});

test("a permalink restores the view, selection, time range and compare state", async () => {
  render("/ui/graph");
  flushSync(() => navigate({ view: "logs", query: '{service="db-3"}', mode: "patterns" }));
  await userEvent.click(button("Zoom") ?? document.body);
  const compare = () => document.querySelector<HTMLSelectElement>("select[aria-label=Compare]");
  await userEvent.selectOptions(compare() ?? document.body, "7d");
  await expect.poll(() => location.pathname).toContain("/vs=7d");
  const link = location.pathname;
  root?.unmount();
  document.querySelector("#app")?.remove();

  render(link);
  expect(heading()).toBe('logs {service="db-3"}');
  expect(compare()?.value).toBe("7d");
  expect(document.querySelector(".range")?.textContent).toBe("10-20 13:30 → 10-20 14:30 UTC");
});

test("compare against a range starts as the period before and takes typed bounds", async () => {
  render("/ui/logs");
  await userEvent.click(button("Zoom") ?? document.body);
  const compare = document.querySelector<HTMLSelectElement>("select[aria-label=Compare]");
  await userEvent.selectOptions(compare ?? document.body, "custom");
  const from = () =>
    document.querySelector<HTMLInputElement>("input[aria-label='Compare from (UTC)']");
  await expect.poll(() => from()?.value).toBe("2026-10-20T12:30");
  expect(decodeURIComponent(location.pathname)).toContain(
    "/vs=2026-10-20T12:30:00.000Z,2026-10-20T13:30:00.000Z",
  );
  await userEvent.fill(from() ?? document.body, "2026-10-19T12:00");
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toContain("/vs=2026-10-19T12:00:00Z,2026-10-20T13:30:00.000Z");
  expect((await axe.run(document.body)).violations).toEqual([]);
});

test("navigate lands on Logs with the query and a Back link to the origin", async () => {
  render("/ui/graph");
  const origin = location.href;
  flushSync(() => navigate({ view: "logs", query: '{service="db-3"}' }));
  expect(heading()).toBe('logs {service="db-3"}');
  const back = document.querySelector<HTMLAnchorElement>(".top a");
  expect(back?.href).toBe(origin);
  await userEvent.click(back ?? document.body);
  await expect.poll(heading).toBe("graph");
});

// RangeProbe records each range it is shown, as a view refetching over it would.
function RangeProbe({ read }: { read: string[] }): ReactElement {
  const [range] = useTimeRange();
  useEffect(() => {
    read.push(`${range.from},${range.to}`);
  }, [read, range]);
  return createElement("h1", null, "logs");
}

test("each range preset sets the URL and the view reads the new range", async () => {
  const read: string[] = [];
  render("/ui/logs", () => createElement(RangeProbe, { read }));
  const presets = ["15m", "1h", "6h", "24h", "7d", "30d"];
  for (const p of presets) {
    await userEvent.click(button(p) ?? document.body);
    expect(location.pathname).toBe(`/ui/logs/time=${encodeURIComponent(`now-${p},now`)}`);
    expect(button(p)?.getAttribute("aria-pressed")).toBe("true");
  }
  expect(read).toEqual(["now-1h,now", ...presets.map((p) => `now-${p},now`)]);
});

// Gallery shows each shared component, for the accessibility checks.
function Gallery(): ReactElement {
  const tabs = [
    { id: "summary", label: "Summary", render: () => "p99 latency rose 4×" },
    { id: "logs", label: "Logs", render: () => "lines" },
  ];
  return createElement(
    "div",
    null,
    createElement(Probe, { name: "gallery" }),
    createElement(Annotation, {
      parts: ["Errors rose after ", { text: "the deploy", onClick: () => {} }, "."],
      source: "rca_walk",
    }),
    createElement(NormalityChip, { percentile: 97, peers: 1200, window: "6h" }),
    createElement(NormalityChip, { percentile: 85, peers: 40, window: "6h" }),
    createElement(Menu, {
      label: "More options",
      sections: [{ title: "Overlays", items: [{ label: "Errors", run: () => {}, checked: true }] }],
    }),
    createElement(Inspector, { title: "db-3", tabs }),
    createElement(Chart, {
      kind: "sparkline",
      data: [
        [1, 2, 3],
        [3, 1, 2],
      ],
      width: 120,
      height: 24,
      label: "errors",
    }),
  );
}

test("both themes pass axe, drawer and shared components included", async () => {
  const el = render("/ui/ai", Gallery);
  await userEvent.click(button("Evidence") ?? document.body);
  for (const theme of ["light", "dark"]) {
    document.documentElement.dataset["theme"] = theme;
    expect((await axe.run(el)).violations).toEqual([]);
  }
});

const scanError =
  "scan bdde41862b2c6990fa701e5e35491271 row group 0: segment: read column level/0: storage: get local/2026/10/28/17/f27733b24cbfa5a7/bdde41862b2c6990fa701e5e35491271.seg: context deadline exceeded";

// shownWhole reports whether el shows all of its text: wrapped, not clipped, and inside the
// panel's visible body without scrolling.
function shownWhole(el: Element | null | undefined): boolean {
  const body = document.querySelector("[aria-label=Focus] .focus-b");
  if (!el || !body) {
    return false;
  }
  const r = el.getBoundingClientRect();
  const b = body.getBoundingClientRect();
  return el.scrollWidth <= el.clientWidth && r.top >= b.top && r.bottom <= b.bottom;
}

test("Focus shows a failed section's whole error at the top of its body, with Copy", async () => {
  // Forty patterns fill the body past its fold, below the failure.
  const patterns = Array.from({ length: 40 }, (_, i) => ({ pattern_id: `p${i}`, count: i }));
  const data = { errors: { lift: `internal: ${scanError}` }, related: { patterns } };
  const client = fixtureClient([{ tool: "focus", answer: { data, meta: { snapshot: "e1.s1" } } }]);
  render("/ui/logs", undefined, client);
  await userEvent.click(button("Focus") ?? document.body);
  const failure = () => document.querySelector("[aria-label=Focus] [role=alert] p");
  await expect
    .poll(() => failure()?.textContent)
    .toBe(`Couldn't load what these lines have in common: internal: ${scanError}`);
  expect(shownWhole(failure())).toBe(true);
  expect(document.querySelector("[aria-label=Focus] [role=alert] button")?.textContent).toBe(
    "Copy",
  );
  // The panel fades in; mid-fade its text is fainter than it stays.
  await Promise.all(document.getAnimations().map((a) => a.finished));
  expect(
    (await axe.run(document.querySelector("[aria-label=Focus]") ?? document.body)).violations,
  ).toEqual([]);
});

test("Focus shows what a structured example says when it has no message", async () => {
  const said = {
    "message.content": '["ran the tests and they pass"]',
    "message.role": "assistant",
  };
  const example = {
    timestamp: "2026-10-28T23:32:21Z",
    service: "workspace",
    message: "",
    attributes: said,
  };
  const data = { examples: { mode: "scan", lines: [example] } };
  const client = fixtureClient([{ tool: "focus", answer: { data, meta: { snapshot: "e1.s1" } } }]);
  render("/ui/logs", undefined, client);
  await userEvent.click(button("Focus") ?? document.body);
  await expect
    .poll(() => document.querySelector("[aria-label=Focus] .ln > :last-child")?.textContent)
    .toBe("content: ran the tests and they pass");
});

test("Focus shows the fields its lines share beside a message every line repeats", async () => {
  const example = {
    timestamp: "2026-10-28T23:32:21Z",
    service: "querier",
    message: "request",
    attributes: { method: "POST", duration_ms: "2345", path: "/v1/rca_walk" },
  };
  const shift = { field: "method", value: "POST", slice_share: 1, baseline_share: 0.1 };
  const data = {
    lift: { shifts: [{ ...shift, slice_count: 4, baseline_count: 18 }], lines: 4, rest: 180 },
    related: { patterns: [{ pattern_id: "p1", pattern: "request", service: "querier", count: 4 }] },
    examples: { mode: "scan", lines: [example] },
  };
  const client = fixtureClient([{ tool: "focus", answer: { data, meta: { snapshot: "e1.s1" } } }]);
  render("/ui/logs", undefined, client);
  await userEvent.click(button("Focus") ?? document.body);
  await expect
    .poll(() => document.querySelector("[aria-label=Focus] .ln > :last-child")?.textContent)
    .toBe("requestmethod=POST");
  expect(
    document.querySelector("[aria-label=Focus]")?.textContent,
    "one pattern is every line",
  ).not.toContain("Patterns");
});

test("Focus says so when the whole call fails", async () => {
  const client = createFakeClient({ callFn: () => Promise.reject(new Error(scanError)) });
  render("/ui/logs", undefined, client);
  await userEvent.click(button("Focus") ?? document.body);
  const failure = () => document.querySelector("[aria-label=Focus] [role=alert] p");
  await expect.poll(() => failure()?.textContent).toBe(`Failed: ${scanError}`);
  expect(shownWhole(failure())).toBe(true);
});

test("Focus closes with its ×, and for good when the view it was opened on is left", async () => {
  render("/ui/logs");
  const panel = () => document.querySelector("[aria-label=Focus]");
  await userEvent.click(button("Focus") ?? document.body);
  expect(panel()).toBeTruthy();
  await userEvent.click(
    document.querySelector("[aria-label=Focus] [aria-label=Close]") ?? document.body,
  );
  expect(panel()).toBeNull();
  await userEvent.click(button("Focus") ?? document.body);
  navigate({ view: "graph" });
  await expect.poll(heading).toBe("graph");
  expect(panel()).toBeNull();
  navigate({ view: "logs", query: "" });
  await expect.poll(heading).toBe("logs");
  expect(panel()).toBeNull();
});
