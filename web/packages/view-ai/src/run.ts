import type { LogLine, Predicate } from "@datafocal/api";
import { Pivots, type Subject } from "@datafocal/shell";
import { agentStep, Inspector, useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement, useState } from "react";
import { type Fields, usageFields } from "./fields";
import {
  button,
  eq,
  num,
  openLogs,
  Panel,
  pct,
  type Scope,
  selectorOf,
  sliceQuery,
  sum,
  type TabProps,
  Table,
  tokens,
  usd,
} from "./panel";
import { type Fact, Facts, Plot } from "./parts";
import { type Account, label, share, useQuota } from "./quota";
import { inMs, ms } from "./stats";

// Call is one model call of a run, read from the lines logging its usage: what it did, and
// how long it took, its own duration where logged, else from the run's line before it to its
// last.
interface Call {
  readonly line: LogLine;
  readonly lines: readonly LogLine[];
  readonly did: string;
  readonly at: number;
  readonly model: string;
  // input is all the input the call was billed for, read from cache or not.
  readonly input: number;
  readonly cacheRead: number;
  readonly output: number;
  readonly cost: number;
  readonly duration: number;
}

const number = (l: LogLine, field: string | undefined): number =>
  field ? Number(l.attributes?.[field] ?? 0) || 0 : 0;

function callOf(f: Fields, l: LogLine, lines: readonly LogLine[]): Call {
  const at = (x: { name: string } | undefined) => number(l, x?.name);
  const [input, read, write] = [at(f.input), at(f.cacheRead), at(f.cacheWrite)];
  // Reading more from cache than all input is only possible when input excludes it.
  const excludes = f.excludesCache || read > input;
  return {
    line: l,
    lines,
    did: did(lines),
    at: Date.parse(l.timestamp),
    model: (f.model && l.attributes?.[f.model.name]) ?? "",
    input: excludes ? input + read + write : input,
    cacheRead: read,
    output: at(f.output),
    cost: at(f.cost),
    duration: f.duration ? inMs(f.duration.name, at(f.duration)) : Number.NaN,
  };
}

// did is what a call's lines did: the tools it called, else its reply, else its thinking.
function did(lines: readonly LogLine[]): string {
  const steps = lines.flatMap((l) => agentStep(l) ?? []);
  const called = steps.filter((s) => s.kind === "call").map((s) => s.label);
  const replied = steps.find((s) => s.kind === "message");
  return called.join(" + ") || replied?.label || (steps.length > 0 ? "thinking" : "");
}

// calls are a run's usage lines in time order, a call once where its lines share a call id,
// read from all of them.
function callsOf(f: Fields, lines: readonly LogLine[]): Call[] {
  const byCall = new Map<string, LogLine[]>();
  const sorted = [...lines].sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp));
  sorted.forEach((l, i) => {
    const id = (f.call && l.attributes?.[f.call.name]) || `line ${i}`;
    byCall.set(id, [...(byCall.get(id) ?? []), l]);
  });
  return [...byCall.values()].flatMap((ls) => (ls[0] ? [callOf(f, ls[0], ls)] : []));
}

// took is how long c took: its own duration, else from the latest of the run's lines before
// it, the input it answered, to its last line.
function took(c: Call, before: readonly number[]): number {
  if (Number.isFinite(c.duration)) {
    return c.duration;
  }
  const end = Math.max(...c.lines.map((l) => Date.parse(l.timestamp)));
  const start = before.filter((t) => t < c.at).at(-1);
  return start === undefined ? Number.NaN : end - start;
}

// useCalls reads the lines of run id that carry usage.
function useCalls(scope: Scope, id: string): Call[] | Error | undefined {
  const f = scope.fields;
  const any = usageFields(f).map((x): Predicate => ({ field: x.name, op: "exists" }));
  const s = selectorOf(scope);
  const selector = {
    ...s,
    where: [...(s.where ?? []), { field: f.run?.name ?? "", op: "eq", value: id }, { any }],
  };
  return useAnswer(JSON.stringify(selector), () =>
    scope.client
      .call("search_logs", { selector, limit: 10_000 })
      .then((e) => callsOf(f, e.data.lines ?? [])),
  );
}

// useBefore is when each of run id's lines was logged, in order, which a call's time is
// measured from when it logs none of its own; none until read.
function useBefore(scope: Scope, id: string): readonly number[] {
  const s = selectorOf(scope);
  const selector = {
    ...s,
    where: [...(s.where ?? []), { field: scope.fields.run?.name ?? "", op: "eq", value: id }],
  };
  // Only when each line was logged is read: a run's lines whole can be megabytes.
  const columns = [scope.fields.run?.name ?? ""];
  const lines = useAnswer(JSON.stringify(["times", selector]), () =>
    scope.client
      .call("search_logs", { selector, limit: 10_000, columns })
      .then((e) => e.data.lines ?? []),
  );
  return Array.isArray(lines)
    ? lines.map((l) => Date.parse(l.timestamp)).sort((a, b) => a - b)
    : [];
}

// RunTab is the run picked in Runs as the model calls it made, in order: what each sent,
// read from cache, answered and cost, and how its context grew.
export function RunTab({ scope, picked: run }: TabProps): ReactElement {
  if (run === undefined) {
    return createElement("p", null, "Pick a run in Runs.");
  }
  return createElement(Panel<Call[]>, {
    title: `Run ${run}`,
    answer: useCalls(scope, run),
    render: (calls) => createElement(RunBody, { scope, id: run, calls }),
  });
}

// subjectOf is run id as the other views read it: its lines under the page's filters, its
// session or run by the run field, and the pod its lines name and their service, from a minute
// before its first call to a minute after its last.
function subjectOf(scope: Scope, id: string, calls: readonly Call[]): Subject {
  const field = scope.fields.run?.name ?? "";
  const line = calls[0]?.line;
  const times = calls.map((c) => c.at);
  const at = (t: number) => new Date(t).toISOString();
  return {
    where: [...scope.filters, eq(field, id)],
    id: { field, value: id },
    service: line?.service,
    pod: line?.attributes?.["pod"],
    time: line && { from: at(Math.min(...times) - 60_000), to: at(Math.max(...times) + 60_000) },
    title: `Run ${id}`,
  };
}

function RunBody({ scope, id, calls }: { scope: Scope; id: string; calls: Call[] }): ReactElement {
  const [picked, setPicked] = useState<Call>();
  const t0 = calls[0]?.at ?? 0;
  const models = [...new Set(calls.map((c) => c.model).filter(Boolean))];
  const steps = calls.map((_, i) => i + 1);
  const cumulative = calls.map((_, i) => sum(calls.slice(0, i + 1).map((c) => c.cost)));
  const quota = useRunQuota(scope, id);
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Pivots, { subject: subjectOf(scope, id, calls), from: "ai" }),
    createElement(Facts, {
      facts: [
        ["Model calls", num(calls.length)],
        ["Spend", usd(sum(calls.map((c) => c.cost)))],
        ["Took", ms((calls.at(-1)?.at ?? t0) - t0), "first call to last"],
        ["Peak context", num(Math.max(0, ...calls.map((c) => c.input))), "tokens"],
        ["Models", models.join(", ") || "—"],
        ...quotaFacts(quota),
      ],
    }),
    createElement(
      "div",
      { className: "g2" },
      createElement(Plot, {
        kind: "line",
        xs: steps,
        ys: [calls.map((c) => c.input), calls.map((c) => c.cacheRead)],
        series: ["input", "read from cache"],
        label: "Input per call",
        x: { label: "call" },
        y: { label: "tokens", format: num },
      }),
      createElement(Plot, {
        kind: "line",
        xs: steps,
        ys: [cumulative],
        series: ["spent"],
        label: "Spend over the run",
        x: { label: "call" },
        y: { label: "spend so far", format: usd },
      }),
    ),
    createElement(Calls, { calls, t0, before: useBefore(scope, id), pick: setPicked }),
    picked && createElement(CallInspector, { scope, id, call: picked }),
    scope.fields.memoryOp && createElement(RunMemory, { scope, id, t0 }),
  );
}

// RunMemory is each memory the run loaded, read or wrote, in order.
function RunMemory(p: { scope: Scope; id: string; t0: number }): ReactElement {
  const f = p.scope.fields;
  const s = selectorOf(p.scope);
  const selector = {
    ...s,
    where: [
      ...(s.where ?? []),
      { field: f.run?.name ?? "", op: "eq", value: p.id },
      { field: f.memoryOp?.name ?? "", op: "exists" },
    ],
  };
  const lines = useAnswer(JSON.stringify(selector), () =>
    p.scope.client
      .call("search_logs", { selector, limit: 1_000 })
      .then((e) =>
        [...(e.data.lines ?? [])].sort((a, b) => Date.parse(a.timestamp) - Date.parse(b.timestamp)),
      ),
  );
  const attr = (l: LogLine, field: { name: string } | undefined) =>
    (field && l.attributes?.[field.name]) ?? "";
  return createElement(Panel<LogLine[]>, {
    title: "Memory in this run",
    answer: lines,
    render: (ls) =>
      createElement(Table, {
        label: "Memory operations",
        columns: ["Step", "At", "Operation", "Memory", "Tokens loaded"],
        rows: ls.map((l, i) => ({
          key: `#${i + 1}`,
          cells: [
            `+${ms(Date.parse(l.timestamp) - p.t0)}`,
            attr(l, f.memoryOp),
            attr(l, f.memory),
            attr(l, f.memoryChars) ? tokens(Number(attr(l, f.memoryChars))) : "—",
          ],
        })),
      }),
  });
}

// useRunQuota is the quota the run's own lines report.
function useRunQuota(scope: Scope, id: string) {
  const s = selectorOf(scope);
  return useQuota(scope, {
    ...s,
    where: [...(s.where ?? []), { field: scope.fields.run?.name ?? "", op: "eq", value: id }],
  });
}

// quotaFacts are each subscription window's share as the run last read it, and the limits
// it hit.
function quotaFacts(a: Account | Error | undefined): Fact[] {
  if (a instanceof Error || !a) {
    return [];
  }
  const windows = a.windows.map(
    (w): Fact => [`Quota ${label(a, w)}`, share(w.now), "used at the end"],
  );
  return a.limited ? [...windows, ["Limits hit", num(a.limited)]] : windows;
}

// shown is the most calls the table lists, the costliest of a longer run.
const shown = 100;

function Calls(p: {
  calls: Call[];
  t0: number;
  before: readonly number[];
  pick: (c: Call) => void;
}): ReactElement {
  const listed =
    p.calls.length > shown
      ? [...p.calls]
          .sort((a, b) => b.cost - a.cost)
          .slice(0, shown)
          .sort((a, b) => a.at - b.at)
      : p.calls;
  return createElement(
    "div",
    null,
    p.calls.length > shown &&
      createElement(
        "p",
        { className: "ai-note" },
        `The ${shown} costliest of ${num(p.calls.length)} calls, in order.`,
      ),
    createElement(Table, {
      label: "Calls",
      columns: [
        "Call",
        "What it did",
        "At",
        "Model",
        "Input",
        "From cache",
        "Output",
        "Cost",
        "Took",
      ],
      rows: listed.map((c) => ({
        key: `#${p.calls.indexOf(c) + 1}`,
        cells: [
          c.did || "—",
          `+${ms(c.at - p.t0)}`,
          c.model,
          num(c.input),
          pct(c.input ? c.cacheRead / c.input : Number.NaN),
          num(c.output),
          usd(c.cost),
          ms(took(c, p.before)),
        ],
        onPick: () => p.pick(c),
      })),
    }),
  );
}

// CallInspector shows the line a call was read from, every field it carries.
function CallInspector(p: { scope: Scope; id: string; call: Call }): ReactElement {
  const attrs = Object.entries(p.call.line.attributes ?? {}).sort(([a], [b]) => a.localeCompare(b));
  return createElement(Inspector, {
    title: `${p.call.model || "call"} at ${p.call.line.timestamp}`,
    actions: button("Open in Logs", () =>
      openLogs(sliceQuery(p.scope, p.scope.fields.run, p.id), `Run ${p.id}`),
    ),
    tabs: [
      {
        id: "fields",
        label: "Fields",
        render: () =>
          createElement(
            "dl",
            null,
            attrs.flatMap(([k, v]) => [
              createElement("dt", { key: k }, k),
              createElement("dd", { key: `${k}=` }, v),
            ]),
          ),
      },
    ],
  });
}
