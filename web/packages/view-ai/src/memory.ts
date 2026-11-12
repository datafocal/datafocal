import type { AggregateResponse, MeasureSpec, Predicate } from "@datafocal/api";
import { Absent, Meter } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import type { Field, Fields } from "./fields";
import {
  button,
  grouped,
  measures,
  num,
  Panel,
  pct,
  perBin,
  type Scope,
  selectorOf,
  sliceQuery,
  spec,
  sum,
  type TabProps,
  Table,
  tokens,
  useAggregate,
  useRunCount,
  valueAt,
} from "./panel";
import { Crumbs, Facts, Lede, Plot } from "./parts";

// Op is what a line does with a memory: loads it into the context, or a tool call reads or
// writes it.
type Op = "load" | "read" | "write";
const ops: readonly Op[] = ["load", "read", "write"];

const opCount = (op: Field, value: Op): MeasureSpec => ({
  measure: "count",
  where: [{ field: op.name, op: "eq", value }],
});

// memoryMeasures count each operation, the runs doing any and the text the loads put in the
// context.
const memoryMeasures = (f: Fields) =>
  measures({
    load: f.memoryOp && opCount(f.memoryOp, "load"),
    read: f.memoryOp && opCount(f.memoryOp, "read"),
    write: f.memoryOp && opCount(f.memoryOp, "write"),
    runs: spec("distinct", f.run),
    chars: spec("sum", f.memoryChars),
  });

// MemoryStat is a memory's line of the ledger.
export interface MemoryStat {
  readonly memory: string;
  readonly load: number;
  readonly read: number;
  readonly write: number;
  readonly runs: number;
  readonly chars: number;
}

export const uses = (m: MemoryStat): number => m.load + m.read + m.write;

// usesMeasure counts the lines using memory, whatever they do with it.
export const usesMeasure = (f: Fields): MeasureSpec | undefined =>
  f.memoryOp && { measure: "count", where: [{ field: f.memoryOp.name, op: "exists" }] };

// useMemory is each memory's operations, most used first.
export function useMemory(scope: Scope): readonly MemoryStat[] | Error | undefined {
  const f = scope.fields;
  const m = memoryMeasures(f);
  const answer = useAggregate(
    scope,
    f.memory && f.memoryOp ? { group_by: f.memory.name, measures: m.list, bins: "all" } : null,
  );
  if (answer instanceof Error || !answer) {
    return answer;
  }
  return grouped(answer, "")[0]
    .series.map((s) => {
      const get = (k: Parameters<typeof m.at>[0]) =>
        m.at(k) < 0 ? 0 : (valueAt(s, m.at(k), 0) ?? 0);
      return {
        memory: s.key,
        load: get("load"),
        read: get("read"),
        write: get("write"),
        runs: get("runs"),
        chars: get("chars"),
      };
    })
    .sort((a, b) => uses(b) - uses(a) || a.memory.localeCompare(b.memory));
}

// perLoad is the tokens one load of a memory puts in the context.
const perLoad = (m: MemoryStat): string => (m.load && m.chars ? tokens(m.chars / m.load) : "—");

function Ledger(p: TabProps & { stats: readonly MemoryStat[]; runs: number }): ReactElement {
  const { stats } = p;
  const top = Math.max(1, ...stats.map(uses));
  const unread = stats.filter((m) => m.write > 0 && m.read + m.load === 0);
  return createElement(
    "div",
    null,
    createElement(Facts, {
      facts: [
        ["Memories", num(stats.length)],
        ["Loads", num(sum(stats.map((m) => m.load))), "into the context"],
        ["Reads", num(sum(stats.map((m) => m.read))), "by a tool call"],
        ["Writes", num(sum(stats.map((m) => m.write))), "by a tool call"],
        ["Tokens loaded", tokens(sum(stats.map((m) => m.chars))), "at four characters each"],
        ["Written, never read", num(unread.length), "in the window"],
      ],
    }),
    createElement(Table, {
      label: "Memories",
      columns: ["Memory", "Uses", "Loads", "Reads", "Writes", "Tokens per load", "Share of runs"],
      rows: stats.map((m) => ({
        key: m.memory,
        cells: [
          createElement(Meter, { text: num(uses(m)), share: uses(m) / top }),
          num(m.load),
          num(m.read),
          num(m.write),
          perLoad(m),
          pct(p.runs ? m.runs / p.runs : Number.NaN),
        ],
        query: sliceQuery(p.scope, p.scope.fields.memory, m.memory),
        onPick: () => p.open("memory", m.memory),
      })),
    }),
  );
}

// Trend is each operation per bin, so a memory written but no longer loaded shows.
function Trend({ scope }: { scope: Scope }): ReactElement {
  const op = scope.fields.memoryOp;
  const answer = useAggregate(
    scope,
    op ? { group_by: op.name, measure: "count", bins: "auto" } : null,
  );
  return createElement(Panel<AggregateResponse>, {
    title: "Memory over time",
    answer,
    render: (r) => {
      const [xs] = perBin(r);
      const of = (o: Op) => {
        const [ts, vs] = perBin({ series: r.series.filter((s) => s.key === o) });
        const byBin = new Map(ts.map((t, j) => [t, vs[j] ?? 0]));
        return xs.map((t) => byBin.get(t) ?? 0);
      };
      return createElement(Plot, {
        kind: "line",
        xs,
        ys: ops.map(of),
        series: ops.map((o) => `${o}s`),
        label: "Memory operations over time",
        x: { label: "time", time: true },
        y: { label: "operations", format: num },
      });
    },
  });
}

// MemoryPage is the runs that use a memory and what each did with it.
function MemoryPage(p: TabProps & { memory: string }): ReactElement {
  const f = p.scope.fields;
  const where: Predicate[] = [
    ...(selectorOf(p.scope).where ?? []),
    { field: f.memory?.name ?? "", op: "eq", value: p.memory },
  ];
  const m = memoryMeasures(f);
  const runs = useAggregate(
    p.scope,
    f.run
      ? { group_by: f.run.name, measures: m.list.slice(0, 3), bins: "all", limit: 10_000 }
      : null,
    { ...selectorOf(p.scope), where },
  );
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Crumbs, { steps: [["Memory", () => p.open("memory")], [p.memory]] }),
    button("Compare runs that use it ›", () =>
      p.compare({ field: f.memory?.name ?? "", value: p.memory }),
    ),
    createElement(Panel<AggregateResponse>, {
      title: "Runs that use it",
      answer: runs,
      render: (r) =>
        createElement(Table, {
          label: "Runs using the memory",
          columns: ["Run", "Loads", "Reads", "Writes"],
          rows: grouped(r, "")[0].series.map((s) => ({
            key: s.key,
            cells: ops.map((op) => num(valueAt(s, m.at(op), 0) ?? 0)),
            onPick: () => p.openRun(s.key),
          })),
        }),
    }),
  );
}

export function MemoryTab(p: TabProps): ReactElement {
  const f = p.scope.fields;
  const stats = useMemory(p.scope);
  const runs = useRunCount(p.scope);
  if (p.picked !== undefined) {
    return createElement(MemoryPage, { ...p, memory: p.picked });
  }
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Memory",
      text: "What the agents remember across sessions: each memory file loaded into the context, read or written by a tool call, and the runs doing it.",
    }),
    createElement(Panel<readonly MemoryStat[]>, {
      title: "Memory ledger",
      answer:
        f.memory && f.memoryOp
          ? stats
          : new Absent(
              "no memory use. Memory shows when the logs carry a memory file loaded into the context, a memory tool call, or a read or write of a file in a memory directory",
            ),
      render: (s) => createElement(Ledger, { ...p, stats: s, runs }),
    }),
    f.memory && f.memoryOp && createElement(Trend, { scope: p.scope }),
  );
}
