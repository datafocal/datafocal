import type { AggregateResponse } from "@datafocal/api";
import { logql } from "@datafocal/query";
import { Absent, type Answer, Chart, tileText, Meter } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode, useState } from "react";
import { type Contributor, contributors, makeupRequest } from "./context";
import type { Field } from "./fields";
import { type MemoryStat, useMemory, uses } from "./memory";
import { type ModelStat, modelRequest, modelStats } from "./models";
import { type Account, highest, label, share, useQuota } from "./quota";
import {
  breakdown,
  eq,
  measures,
  money,
  num,
  Panel,
  pct,
  perBin,
  type Row,
  sum,
  type Tab,
  type TabProps,
  Table,
  type Usage,
  usageMeasure,
  tokens,
  usd,
  useAggregate,
  useUsage,
} from "./panel";
import { Stack } from "./parts";
import { quantile, type RunStat, useAfterCompaction, useRunStats } from "./stats";
import { type ToolStat, useTools } from "./tools";

const value = <T>(a: Answer<T>): T | undefined => (a instanceof Error ? undefined : a);

type Tile = readonly [label: string, value: string, detail: string, tab: Tab];

// tiles are the KPIs: what the window spent, per run, from cache, at its peak and in tokens.
function tiles(p: {
  scope: TabProps["scope"];
  usage: Usage;
  runs: Answer<readonly RunStat[]>;
  quota: Answer<Account>;
}): Tile[] {
  const { usage } = p;
  const f = p.scope.fields;
  const runs = value(p.runs);
  const of = (pick: (r: RunStat) => number, q: number) =>
    runs?.length ? quantile(runs.map(pick), q) : Number.NaN;
  const later = (show: (rs: readonly RunStat[]) => string) =>
    runs ? show(runs) : tileText(p.runs instanceof Error ? p.runs : undefined);
  const [input, output] = [sum(usage.input), sum(usage.output)];
  const cost = (q: number) =>
    money(
      f,
      of((r) => r.cost, q),
    );
  return [
    [
      "Spend",
      money(f, sum(usage.cost)),
      later((rs) => `${num(rs.length)} runs · ${num(sum(rs.map((r) => r.calls)))} model calls`),
      "models",
    ],
    [
      "Cost per run",
      later((rs) => (rs.some((r) => r.cost > 0) ? cost(0.5) : "—")),
      later((rs) =>
        rs.some((r) => r.cost > 0)
          ? `median · p90 ${cost(0.9)}`
          : "no line with a cost names a run",
      ),
      "compare",
    ],
    [
      "Cache hit",
      pct(sum(usage.cache) / (input || 1)),
      `${num(sum(usage.cache))} tokens read from cache`,
      "cache",
    ],
    [
      "Peak context",
      later(() => num(of((r) => r.peak, 0.5))),
      later(() => `median per run · p90 ${num(of((r) => r.peak, 0.9))}`),
      "context",
    ],
    ["Tokens", num(input + output), `input ${num(input)} · output ${num(output)}`, "context"],
    ...quotaTile(p.quota),
  ];
}

// quotaTile is the subscription window closest to its limit, when the logs report any.
function quotaTile(quota: Answer<Account>): Tile[] {
  const a = value(quota);
  const top = a && highest(a);
  if (!a || !top) {
    return [];
  }
  const limited = a.limited ? ` · ${num(a.limited)} limits hit` : "";
  return [["Quota", share(top.now), `of the ${label(a, top)} window used${limited}`, "quota"]];
}

function Kpis(
  p: TabProps & { usage: Usage; runs: Answer<readonly RunStat[]>; quota: Answer<Account> },
): ReactElement {
  return createElement(
    "div",
    { className: "kpis" },
    tiles(p).map(([label, v, detail, tab]) =>
      createElement(
        "button",
        {
          key: label,
          type: "button",
          className: "kpi",
          title: label === "Tokens" ? p.usage.source : undefined,
          onClick: () => p.open(tab),
        },
        createElement("small", null, label),
        createElement("strong", null, v),
        createElement("span", null, detail),
      ),
    ),
  );
}

// Evidence is what the lever cards show.
interface Evidence {
  readonly parts: readonly Contributor[];
  readonly tools: readonly ToolStat[];
  readonly runs: readonly RunStat[];
  readonly models: readonly ModelStat[];
  readonly memory: readonly MemoryStat[];
}

// Card is one lever at a glance: its headline number, a picture and its top rows.
function Card(p: {
  title: string;
  big: string;
  unit: string;
  go: () => void;
  children?: ReactNode;
  rows: readonly (readonly [string, string, number, () => void])[];
}): ReactElement {
  return createElement(
    "section",
    { className: "panel card", "aria-label": p.title },
    createElement(
      "h3",
      null,
      p.title,
      createElement("button", { type: "button", className: "link", onClick: p.go }, "Open ›"),
    ),
    createElement("div", { className: "big" }, p.big, createElement("small", null, p.unit)),
    p.children,
    p.rows.map(([label, figure, share, go]) =>
      createElement(
        "button",
        { key: label, type: "button", className: "prow", onClick: go },
        createElement(Meter, { text: label, share }),
        createElement("span", null, figure),
      ),
    ),
  );
}

function CacheCard(p: {
  scope: TabProps["scope"];
  usage: Usage;
  runs: readonly RunStat[];
  open: TabProps["open"];
}): ReactElement {
  const { usage, open } = p;
  const switches = sum(p.runs.map((r) => r.modelChanges));
  const compactions = sum(p.runs.map((r) => r.compactions));
  const after = value(useAfterCompaction(p.scope));
  const spent = after?.calls ? ` · ${money(p.scope.fields, after.cost)} rewriting the cache` : "";
  return createElement(
    Card,
    {
      title: "Cache",
      big: pct(sum(usage.cache) / (sum(usage.input) || 1)),
      unit: "of input from cache",
      go: () => open("cache"),
      rows: [
        [`${num(switches)} model switches mid-run`, num(switches), 0, () => open("cache")],
        [`${num(compactions)} compactions${spent}`, num(compactions), 0, () => open("context")],
      ],
    },
    createElement(Chart, {
      kind: "sparkline",
      data: [usage.bins, usage.cache.map((v, i) => v / (usage.input[i] || 1))],
      width: 280,
      height: 40,
      label: "Cache hit rate over time",
    }),
  );
}

// MemoryCard is how much the agents use their memory, and the memories they use most.
function MemoryCard(p: { memory: readonly MemoryStat[]; open: TabProps["open"] }): ReactElement {
  const { memory, open } = p;
  const all = sum(memory.map(uses)) || 1;
  const writes = sum(memory.map((m) => m.write));
  return createElement(
    Card,
    {
      title: "Memory",
      big: num(sum(memory.map(uses))),
      unit: "memory loads, reads and writes",
      go: () => open("memory"),
      rows: memory
        .slice(0, 3)
        .map(
          (m) => [m.memory, num(uses(m)), uses(m) / all, () => open("memory", m.memory)] as const,
        ),
    },
    createElement(
      "p",
      { className: "ai-note" },
      memory.length
        ? `${tokens(sum(memory.map((m) => m.chars)))} tokens loaded · ${num(writes)} writes`
        : "No memory use logged.",
    ),
  );
}

function Cards(p: TabProps & { e: Evidence; usage: Usage }): ReactElement {
  const { e, usage, open } = p;
  const out = sum(e.tools.map((t) => t.out || 0)) || 1;
  const spend = sum(e.models.map((m) => m.cost)) || 1;
  return createElement(
    "div",
    { className: "g3" },
    createElement(
      Card,
      {
        title: "Context",
        big: num(sum(usage.input)),
        unit: "input tokens",
        go: () => open("context"),
        rows: e.parts
          .slice(0, 3)
          .map((c) => [c.label, pct(c.share), c.share, () => open(c.to ?? "context")] as const),
      },
      e.parts.length
        ? createElement(Stack, { parts: e.parts })
        : createElement("p", { className: "ai-note" }, "No content lengths logged."),
    ),
    createElement(Card, {
      title: "Tools",
      big: num(out / 4),
      unit: "output tokens",
      go: () => open("tools"),
      rows: e.tools
        .slice(0, 4)
        .map((t) => [t.tool, num(t.out / 4), t.out / out, () => open("tools", t.tool)] as const),
    }),
    createElement(CacheCard, { scope: p.scope, usage, runs: e.runs, open }),
    createElement(Card, {
      title: "Models",
      big: num(e.models.length),
      unit: "models",
      go: () => open("models"),
      rows: e.models
        .slice(0, 4)
        .map(
          (m) =>
            [m.model, money(p.scope.fields, m.cost), m.cost / spend, () => open("models")] as const,
        ),
    }),
    createElement(MemoryCard, { memory: e.memory, open }),
  );
}

// costRows are r's groups by spend, with their share and tokens; picking one narrows the page
// to it. The lines carrying no value of the dimension are one row too, so the rows add up to
// the Spend tile: a tool's own lines carry a fraction of the spend.
function costRows(
  r: AggregateResponse,
  at: { cost: number; tokens: number; inputs: number },
  dim: Field,
  p: TabProps,
): Row[] {
  const byInput = Array.from({ length: at.inputs }, (_, j) => breakdown(r, at.tokens + j));
  const all = breakdown(r, at.cost);
  const paid = all.some((s) => s.value > 0) ? all.filter((s) => s.value > 0) : all;
  return paid.map((s): Row => {
    const i = all.indexOf(s);
    const slice = eq(dim.name, s.key);
    const cells = [usd(s.value), pct(s.share), num(sum(byInput.map((b) => b[i]?.value ?? 0)))];
    return s.key === ""
      ? { key: `No ${dim.label.toLowerCase()} (unattributed)`, cells }
      : {
          key: s.key,
          cells,
          query: logql([...p.scope.filters, slice]),
          onPick: () => p.pivot(slice),
        };
  });
}

// SpendBy splits spend by the chosen dimension; picking a row narrows the page to it, which
// moves the split on to the next dimension.
function SpendBy(p: TabProps & { usage: Usage }): ReactElement {
  const f = p.scope.fields;
  const dims = f.dims.filter((d) => !p.scope.filters.some((x) => x.field === d.name));
  const [chosen, setChosen] = useState<string>();
  const dim = dims.find((d) => d.name === chosen) ?? dims[0];
  const m = measures({ cost: usageMeasure(f, "sum", f.cost) });
  const tokens = p.usage.inputs.flatMap((x) => usageMeasure(f, "sum", x) ?? []);
  const costs = useAggregate(
    p.scope,
    dim ? { group_by: dim.name, measures: [...m.list, ...tokens], bins: "auto" } : null,
  );
  const at = { cost: m.at("cost"), tokens: m.list.length, inputs: p.usage.inputs.length };
  return createElement(Panel<AggregateResponse>, {
    title: "Spend by",
    answer: f.cost
      ? costs
      : new Absent("no cost: neither a logged cost nor token counts for a priced model"),
    render: (r) => [
      createElement(
        "div",
        { key: "d", className: "ai-dims" },
        dims.map((d) =>
          createElement(
            "button",
            {
              key: d.name,
              type: "button",
              className: "tbtn",
              title: d.source,
              "aria-pressed": d === dim,
              onClick: () => setChosen(d.name),
            },
            d.label,
          ),
        ),
      ),
      createElement(Chart, {
        key: "c",
        kind: "sparkline",
        data: perBin(r, m.at("cost")),
        width: 160,
        height: 32,
        label: "Spend over time",
      }),
      dim &&
        createElement(Table, {
          key: "t",
          label: `Spend by ${dim.label}`,
          columns: [dim.label, "Spend", "Share", "Input tokens"],
          rows: costRows(r, at, dim, p),
        }),
    ],
  });
}

function Body(p: TabProps & { usage: Usage }): ReactElement {
  const f = p.scope.fields;
  const runs = useRunStats(p.scope);
  const makeup = useAggregate(p.scope, makeupRequest(f));
  const tools = useTools(p.scope);
  const models = useAggregate(p.scope, modelRequest(f));
  const memory = useMemory(p.scope);
  const quota = useQuota(p.scope);
  const e: Evidence = {
    parts: value(makeup) ? contributors(value(makeup) ?? { series: [] }, f) : [],
    tools: value(tools) ?? [],
    runs: value(runs) ?? [],
    models: value(models) ? modelStats(value(models) ?? { series: [] }, f) : [],
    memory: value(memory) ?? [],
  };
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Kpis, { ...p, runs, quota }),
    createElement(Cards, { ...p, e }),
    createElement(SpendBy, p),
  );
}

export function OverviewTab(p: TabProps): ReactElement {
  const usage = useUsage(p.scope);
  return createElement(Panel<Usage>, {
    title: "What it costs",
    answer: usage,
    render: (u) => createElement(Body, { ...p, usage: u }),
  });
}
