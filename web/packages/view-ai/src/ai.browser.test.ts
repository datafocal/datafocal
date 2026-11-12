import { type FixtureClient, fixtureClient, type Sent } from "@datafocal/api";
import { Shell, useParams } from "@datafocal/shell";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, test } from "vitest";
import engine from "../fixtures/engine.json";
import { AIView } from "./index";

// time is the factory corpus's last day, the range every request in requests.json reads.
const time = "2026-07-27T00:00:00Z,2026-07-28T00:00:00Z";
let root: Root | undefined;
let fixtures: FixtureClient | undefined;
afterEach(async () => {
  expect(await fixtures?.faults(), "each call sent once, a frame's at once").toEqual([]);
  root?.unmount();
  expect(fixtures?.missed, "every request the view sent is in requests.json").toEqual([]);
});

function Logs() {
  return createElement("h1", null, useParams()["query"]);
}

// render opens the AI view over the factory corpus's last day on the engine's answers, and
// returns the calls sent to them.
function render(path = ""): readonly Sent[] {
  history.replaceState(null, "", `/ui/ai/time=${encodeURIComponent(time)}${path}`);
  const client = fixtureClient(engine);
  fixtures = client;
  const routes = [
    { path: "/ui/ai", view: AIView },
    { path: "/ui/logs", view: Logs },
    { path: "/ui/resources", view: () => createElement("p", null, "Resources") },
  ];
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(createElement(Shell, { routes, client }));
  return client.sent;
}

const find = async (selector: string, text: string) => {
  let el: HTMLElement | undefined;
  await expect
    .poll(() => {
      const all = [...document.querySelectorAll<HTMLElement>(selector)];
      el = all.find((e) => e.textContent?.startsWith(text));
      return el;
    })
    .toBeTruthy();
  return el;
};
const spendRow = (key: string) => find('[aria-label^="Spend by"] button', key);
const logsOf = async (table: string, key: string) =>
  (await find(`[aria-label="${table}"] tr`, key))?.querySelector<HTMLElement>(
    "td:last-child button",
  );
const heading = () => document.querySelector("h1")?.textContent;

test("pivoting model → harness → tool adds a chip a step and refetches only what changed", async () => {
  const sent = render();
  (await find(".ai-dims button", "Harness version"))?.click();
  await spendRow("2.1.4");
  const before = sent.length;
  (await find(".ai-dims button", "Model"))?.click();
  await spendRow("claude-opus-4-5");
  expect(sent.length, "switching back to a dimension it has is no call").toBe(before);
  for (const [i, key] of ["claude-opus-4-5", "2.1.4", "Read"].entries()) {
    (await spendRow(key))?.click();
    await expect.poll(() => document.querySelectorAll(".chip").length).toBe(i + 1);
  }
  const ranked = () => sent.filter((s) => s.tool === "rank_fields").length;
  // The window once, and the lines of runs once per set of filters.
  await expect.poll(ranked).toBe(5);
});

test("a spend split names its dimension by role and sums to the Spend tile", async () => {
  render();
  const tile = await find(".kpi", "Spend");
  await spendRow("claude-opus-4-5");
  const spend = tile?.querySelector("strong")?.textContent ?? "";
  const rows = [...document.querySelectorAll('[aria-label="Spend by Model"] tbody tr')];
  const total = rows.reduce(
    (n, r) => n + Number(r.children[1]?.textContent?.replace(/[$,]/g, "")),
    0,
  );
  expect(Number(spend.replace(/[$,]/g, ""))).toBeCloseTo(total, 1);
});

test.each([
  ["a cost row", '{model="claude-opus-4-5"}', () => logsOf("Spend by Model", "claude-opus-4-5")],
  [
    "a tool",
    '{tool_name="Read"}',
    async () => {
      (await find('[role="tab"]', "Tools"))?.click();
      return logsOf("Tools", "Read");
    },
  ],
])("%s opens exactly its slice in Logs", async (_, query, target) => {
  render();
  (await target())?.click();
  await expect.poll(heading).toBe(query);
});

// The factory's transcripts report no durations, so turns and tool calls are timed by the
// lines sharing their promptId or call id (internal/e2e pins these answers to the generator's).
test("tool latency and turn time come from the times of each call's and turn's lines", async () => {
  render("/tab=tools");
  expect((await find('table[aria-label="Tools"] tr', "Bash"))?.textContent).toContain("20.3 s");
  const rest = await find('table[aria-label="Turn time"] tr', "model and other");
  expect(rest?.textContent).toContain("24m 10s");
});

const tile = async (label: string) =>
  (await find(".kpi", label))?.querySelector("strong")?.textContent;

test("every lever's page shows its ledger, or names what the logs lack", async () => {
  render();
  for (const [tab, shown] of [
    ["Context", 'table[aria-label="Context makeup"] tr'],
    ["Tools", 'table[aria-label="Tools"] tr'],
    ["Skills", '[aria-label="Skill ledger"] [role="status"]'],
    ["Cache", 'table[aria-label="Changes"] tr'],
    ["Models", 'table[aria-label="Models"] tr'],
    ["Compare", 'table[aria-label="Comparison"] tr'],
  ] as const) {
    (await find('[role="tab"]', tab))?.click();
    await find(shown, "");
    expect(document.querySelector('[role="alert"]'), tab).toBeNull();
  }
});

test("the context's parts add up to the input the Tokens tile counts", async () => {
  render();
  const input = (await find(".kpi", "Tokens"))?.querySelector("span")?.textContent ?? "";
  await expect.poll(() => tile("Peak context")).toMatch(/^\d/);
  (await find('[role="tab"]', "Context"))?.click();
  const note = await find(".ai-note", "Shares come from");
  expect(note?.textContent).toContain(`${/input ([\d.]+\w?)/.exec(input)?.[1]} input tokens`);
});

test("opening a tool drills into its calls, and Compare sets its runs against the rest", async () => {
  render();
  (await find('[role="tab"]', "Tools"))?.click();
  (await find('table[aria-label="Tools"] button', "Read"))?.click();
  await find(".ai-crumbs b", "Read");
  await find('table[aria-label="Largest outputs"] tr', "");
  expect(location.pathname).toContain("/pick=Read");
  (await find('[role="tab"]', "Models"))?.click();
  (await find('table[aria-label="Models"] button', "claude-opus-4-5"))?.click();
  await find('table[aria-label="Comparison"] tr', "Cost per run");
  expect(decodeURIComponent(location.pathname)).toContain(
    'split={"field":"model","value":"claude-opus-4-5"}',
  );
});

// The run Runs lists first, the factory corpus's costliest session.
const session = "842e33af-9917-f20e-e46e-5bec950ddc23";

// Each call says what it did, the tools it called or its reply, and how long it took: the
// transcript logs no duration, so from the run's line before it to its last.
test("a run is its model calls in order, each saying what it did and opening its line", async () => {
  render();
  (await find('[role="tab"]', "Runs"))?.click();
  (await find(".ai-runs-b [role=row]", ""))?.click();
  await find('table[aria-label="Calls"] button', "#1");
  const column = (name: string) => {
    const head = [...document.querySelectorAll('table[aria-label="Calls"] th')].map(
      (h) => h.textContent,
    );
    const at = head.indexOf(name);
    return [...document.querySelectorAll('table[aria-label="Calls"] tbody tr')].map(
      (r) => r.children[at]?.textContent ?? "",
    );
  };
  await expect.poll(() => column("Took").filter((t) => t !== "—").length).toBeGreaterThan(0);
  expect(column("What it did").filter((d) => d === "—")).toEqual([]);
  expect(column("What it did").some((d) => /^\w+ · /.test(d))).toBe(true);
  (await find('table[aria-label="Calls"] button', "#1"))?.click();
  await find(".insp dt", "input_tokens");
  (await find(".insp button", "Open in Logs"))?.click();
  await expect.poll(heading).toBe(`{run_id="${session}"}`);
});

const tab = () => document.querySelector('[role="tab"][aria-selected="true"]')?.textContent;

test("the tab, run and filter are the URL's, so Back steps back through them", async () => {
  render();
  (await spendRow("claude-opus-4-5"))?.click();
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toContain('/filter=[{"field":"model","op":"eq","value":"claude-opus-4-5"}]');
  history.back();
  await expect.poll(() => document.querySelectorAll(".chip").length).toBe(0);
  (await find('[role="tab"]', "Runs"))?.click();
  (await find(".ai-runs-b [role=row]", ""))?.click();
  await expect.poll(tab).toBe("Run");
  expect(location.pathname).toContain(`/pick=${session}`);
  history.back();
  await expect.poll(tab).toBe("Runs");
});

// A link from before the drill-down names its run as run=.
test.each([`/pick=${session}`, `/run=${session}`])("a link to a run opens it: %s", async (pick) => {
  render(`/tab=run${pick}`);
  await expect.poll(tab).toBe("Run");
  await find('table[aria-label="Calls"] tr', "#1");
});

test("a link from before the drill-down opens on its filters, and a chip takes one off", async () => {
  render(`/filters=${encodeURIComponent('[{"field":"model","value":"claude-opus-4-5"}]')}`);
  (await find(".chip", "model=claude-opus-4-5"))?.click();
  await expect.poll(() => document.querySelectorAll(".chip").length).toBe(0);
  expect(location.pathname).not.toContain("filter");
});

// The factory's transcripts were not read through a pod's root, so they name no pod.
test("a run whose lines name no pod opens its service's resources over the run", async () => {
  render(`/tab=run/run=${session}`);
  await find('table[aria-label="Calls"] tr', "#1");
  (await find(".pivots button", "Resources"))?.click();
  await expect
    .poll(() => decodeURIComponent(location.pathname))
    .toBe("/ui/resources/service=workspace/time=2026-07-27T19:04:00.415Z,2026-07-27T19:09:11.384Z");
});

test("a run in a per-run summary opens its calls", async () => {
  render("/tab=cache");
  (await find('table[aria-label="Runs by share of input read from cache"] button', ""))?.click();
  await expect.poll(tab).toBe("Run");
  await find('table[aria-label="Calls"] tr', "#1");
});
