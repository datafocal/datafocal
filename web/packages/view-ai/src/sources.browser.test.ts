import { type FixtureClient, fixtureClient } from "@datafocal/api";
import { Shell } from "@datafocal/shell";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, test } from "vitest";
import sources from "../fixtures/sources.json";
import { AIView } from "./index";

// The view on every agent source in internal/e2e/testdata/ai ingested together: Claude Code,
// Codex, Copilot, Gemini and pi transcripts, a gateway's log, and OpenTelemetry records and
// spans. They share no field but the columns ingest discovers, and most name no session, so
// every one must be counted without one.
// time spans the sources, recorded from 2025-09 to 2026-10.
const time = "2025-09-01T00:00:00Z,2026-11-01T00:00:00Z";
let root: Root | undefined;
let fixtures: FixtureClient | undefined;
afterEach(async () => {
  expect(await fixtures?.faults(), "each call sent once, a frame's at once").toEqual([]);
  root?.unmount();
  expect(fixtures?.missed, "every request the view sent is in requests.sources.json").toEqual([]);
});

function render(path = "") {
  history.replaceState(null, "", `/ui/ai/time=${encodeURIComponent(time)}${path}`);
  const client = fixtureClient(sources);
  fixtures = client;
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(createElement(Shell, { routes: [{ path: "/ui/ai", view: AIView }], client }));
}

const find = async (selector: string, text: string) => {
  let el: HTMLElement | undefined;
  await expect
    .poll(() => {
      el = [...document.querySelectorAll<HTMLElement>(selector)].find((e) =>
        e.textContent?.startsWith(text),
      );
      return el;
    })
    .toBeTruthy();
  return el;
};
const rows = (table: string) =>
  [...document.querySelectorAll(`table[aria-label="${table}"] tbody tr`)].map(
    (r) => r.children[0]?.textContent ?? "",
  );

// spend is the sum of each source's cost_usd, as TestAIViewQueries pins it per source.
const spend = "$0.57";

test("spend counts every source, whether or not it names a session", async () => {
  render();
  const tile = await find(".kpi", "Spend");
  await expect.poll(() => tile?.querySelector("strong")?.textContent).toBe(spend);
});

// models are each source's: Claude Code, Codex, OpenTelemetry's Claude Code, pi, Gemini,
// Copilot, the gateway and Strands.
const models = [
  "claude-opus-4-1-20250805",
  "claude-sonnet-4-20250514",
  "gpt-6.1-sol",
  "claude-haiku-4-5-20251001",
  "claude-opus-4-5",
  "gemini-3-flash-preview",
  "gpt-5-mini",
  "claude-opus-5-5",
  "gpt-4o",
  "openai/gpt-6-luna",
];

test("every source's models are in the Models ledger, each with its calls", async () => {
  render("/tab=models");
  await find('table[aria-label="Models"] tr', "");
  expect(rows("Models").sort()).toEqual([...models].sort());
  const calls = [...document.querySelectorAll('table[aria-label="Models"] tbody tr')].map((r) =>
    Number(r.children[1]?.textContent),
  );
  expect(calls.filter((n) => !(n > 0))).toEqual([]);
});

test("every source's tools are in the Tools ledger", async () => {
  render("/tab=tools");
  await find('table[aria-label="Tools"] tr', "");
  expect(rows("Tools").length).toBeGreaterThan(3);
});

// memories are the memory files the fixtures load, read and write.
const memories = ["MEMORY.md", "no-commit-attribution.md", "run-tests-first.md"];

test("every memory loaded, read or written is in the Memory ledger", async () => {
  render("/tab=memory");
  await find('table[aria-label="Memories"] tr', "");
  expect(rows("Memories").sort()).toEqual(memories);
});

test("the overview counts memory use", async () => {
  render();
  const card = await find('section[aria-label="Memory"]', "Memory");
  await expect.poll(() => card?.querySelector(".big")?.firstChild?.textContent).toBe("3");
});

test("a run lists its memory operations in order", async () => {
  render("/tab=run/pick=00000000-0000-4000-8000-0000000000a1");
  await find('table[aria-label="Memory operations"] tr', "");
  const ops = [...document.querySelectorAll('table[aria-label="Memory operations"] tbody tr')].map(
    (r) => r.children[2]?.textContent,
  );
  expect(ops).toEqual(["load", "read", "write"]);
});

test("the context tab shows what compaction removed and the cache it wrote again", async () => {
  render("/tab=context");
  const facts = async (label: string) =>
    (await find(".ai-facts div", label))?.querySelector("b")?.textContent;
  await expect.poll(() => facts("Compactions")).toBe("1");
  expect(await facts("Tokens removed")).toBe("346.7K");
  expect(await facts("Cache rewritten after")).toBe("30.1K");
  // Off the cursor every legend reads a value: a line's last, a bar chart's total.
  const legends = () =>
    [...document.querySelectorAll(".u-legend .u-value")].map((v) => v.textContent);
  await expect.poll(() => legends().length).toBeGreaterThan(0);
  expect(legends().filter((v) => v === "—" || v === "")).toEqual([]);
});

// windows are the subscription windows the Codex, Copilot and Claude Code statusline fixtures
// report, each source's its own account's; Copilot's premium window allows nothing.
const windows = [
  "claude-code-statusline · 5h",
  "claude-code-statusline · 7d",
  "codex-quota · 5h",
  "codex-quota · 7d",
  "copilot-cli · chat",
  "copilot-cli · completions",
];

test("every reported subscription window is in the Quota tab, each with its share", async () => {
  render("/tab=quota");
  await find('table[aria-label="Quota windows"] tr', "");
  expect(rows("Quota windows").sort()).toEqual(windows);
});

test("the Quota tab ranks runs by how far they saw the first window climb", async () => {
  render("/tab=quota");
  await find('table[aria-label="Quota by run"] tr', "");
  const first = document.querySelector('table[aria-label="Quota by run"] tbody tr');
  expect({
    runs: rows("Quota by run"),
    climbs: [...(first?.querySelectorAll(".meter") ?? [])].map((m) => m.textContent),
  }).toEqual({
    runs: [
      "00000000-0000-4000-8000-0000000000c1",
      "00000000-0000-4000-8000-0000000000b1",
      "488f34ee-b5ff-4555-bedb-cdded66578ec",
    ],
    climbs: ["+88.0 pts", "+16.0 pts", "—", "—"],
  });
});

test("the overview shows the window closest to its limit", async () => {
  render();
  const tile = await find(".kpi", "Quota");
  await expect.poll(() => tile?.querySelector("strong")?.textContent).toBe("100.0%");
});

test("a run shows the quota its lines report", async () => {
  render("/tab=run/pick=00000000-0000-4000-8000-0000000000c1");
  const fact = await find(".ai-facts div", "Quota 5h");
  expect(fact?.querySelector("b")?.textContent).toBe("100.0%");
});
