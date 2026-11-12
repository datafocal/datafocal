import type { AggregateResponse, DataFocalClient, Selector, TimeSpec } from "@datafocal/api";
import { suggest } from "@datafocal/query";
import { navigate, resolve, useParams, useQueryRange } from "@datafocal/shell";
import {
  Checked,
  type Choice,
  Choices,
  duration,
  QueryField,
  Show,
  useAnswer,
} from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode, useId, useState } from "react";
import { at, exported, list, type Path, pct, put } from "./doc";

const src = (query: string) => ({ metricSource: { type: "DataFocal", spec: { query } } });
const queryAt = (...p: string[]): Path => [...p, "metricSource", "spec", "query"];
const ratio = ["spec", "indicator", "spec", "ratioMetric"];
const [total, good] = [queryAt(...ratio, "total"), queryAt(...ratio, "good")];
const metric = queryAt("spec", "indicator", "spec", "thresholdMetric");
const objective = ["spec", "objectives", 0];
const policies = ["spec", "alertPolicies"];

const ratioOf = (q: string, narrower: string) => ({
  budgetingMethod: "Occurrences",
  objectives: [{ target: 0.999 }],
  indicator: { spec: { ratioMetric: { total: src(q), good: src(`${q} | ${narrower}`) } } },
});
const sliced = (objectives: object, query: string) => ({
  budgetingMethod: "Timeslices",
  objectives: [objectives],
  indicator: { spec: { thresholdMetric: src(query) } },
});

// shapes are the four SLIs, each over a log query's events.
const shapes: Readonly<Record<string, (q: string) => object>> = {
  "Event ratio": (q) => ratioOf(q, "status < 500"),
  "Latency threshold": (q) => ratioOf(q, "duration_ms <= 300"),
  "Time-slice": (q) =>
    sliced(
      { op: "lt", value: 300, target: 0.995, timeSliceTarget: 0.995, timeSliceWindow: "1m" },
      `quantile_over_time(0.99, ${q} | unwrap duration_ms [1m])`,
    ),
  Absence: (q) =>
    sliced(
      { op: "gt", value: 0, target: 0.99, timeSliceWindow: "5m" },
      `count_over_time(${q} [5m])`,
    ),
};

const shapeOf = (doc: unknown) =>
  at(doc, ["spec", "budgetingMethod"]) === "Occurrences"
    ? /duration/.test(String(at(doc, good)))
      ? "Latency threshold"
      : "Event ratio"
    : String(at(doc, metric)).startsWith("count_over_time")
      ? "Absence"
      : "Time-slice";

const fresh = (q: string) => ({
  apiVersion: "openslo/v1",
  kind: "SLO",
  metadata: { name: "" },
  spec: {
    service: /service="([^"]+)"/.exec(q)?.[1] ?? "",
    timeWindow: [{ duration: "28d", isRolling: true }],
    ...shapes["Event ratio"]?.(q),
  },
});

// policy is one burn-rate alert policy of conditions [threshold, lookback, short window, severity].
const policy = (name: string, conditions: [number, string, string, string][]) => [
  {
    kind: "AlertPolicy",
    metadata: { name },
    spec: {
      conditions: conditions.map(([threshold, lookbackWindow, short, severity], i) => ({
        kind: "AlertCondition",
        metadata: {
          name: `${severity}-${i + 1}`,
          annotations: { "datafocal/short-window": short },
        },
        spec: { severity, condition: { kind: "burnrate", op: "gte", threshold, lookbackWindow } },
      })),
    },
  },
];
const alerts: Readonly<Record<string, unknown>> = {
  none: undefined,
  standard: policy("standard-burn", [
    [14.4, "1h", "5m", "page"],
    [6, "6h", "30m", "page"],
    [1, "3d", "6h", "ticket"],
  ]),
  custom: policy("burn", [[14.4, "1h", "5m", "page"]]),
};
const alertsOf = (doc: unknown) => {
  const name = at(doc, [...policies, 0, "metadata", "name"]);
  return name === undefined ? "none" : name === "standard-burn" ? "standard" : "custom";
};

const isAggregate = (r: unknown): r is AggregateResponse =>
  typeof r === "object" && r !== null && "series" in r;
const hasSelector = (a: unknown): a is { selector: Selector } =>
  typeof a === "object" && a !== null && "selector" in a;

// counted is how many events q matches over time, through aggregate, and its selector.
async function counted(client: DataFocalClient, q: string, time: TimeSpec) {
  const { data } = await client.call("query", { q: `sum(count_over_time(${q} [1h]))`, time });
  const series = isAggregate(data.result) ? data.result.series : [];
  const n = series.flatMap((s) => s.values ?? []).reduce<number>((a, v) => a + (v ?? 0), 0);
  return { n, selector: hasSelector(data.call.args) ? data.call.args.selector : {} };
}

// countsOf is the valid and good counts of total and good over time, and the fields of
// valid's events to group by.
async function countsOf(client: DataFocalClient, [v, g]: unknown[], time: TimeSpec) {
  if (typeof v !== "string" || typeof g !== "string") {
    return undefined;
  }
  const [valid, ok] = await Promise.all([counted(client, v, time), counted(client, g, time)]);
  const { fields } = (await client.call("rank_fields", { selector: valid.selector })).data;
  return { valid: valid.n, good: ok.n, fields };
}

// useDay is the 24 hours to the range's end, which the builder previews over.
function useDay(): TimeSpec {
  const { to } = useQueryRange();
  return { from: new Date(Date.parse(to) - 864e5).toISOString(), to };
}

// useCounts is countsOf doc's queries over time.
function useCounts(client: DataFocalClient, doc: unknown, time: TimeSpec) {
  const queries = [at(doc, total), at(doc, good)];
  return useAnswer(JSON.stringify([queries, time]), () => countsOf(client, queries, time));
}

// useServices is the services that logged over time, the Service field's choices.
function useServices(client: DataFocalClient, time: TimeSpec) {
  return useAnswer(JSON.stringify(time), async () => {
    const q = "sum by (service) (count_over_time({} [1h]))";
    const { data } = await client.call("query", { q, time });
    return isAggregate(data.result) ? data.result.series.map((s) => s.key).filter(Boolean) : [];
  });
}

// Fields are a form section's document, its setter, the counts over its queries, and the
// services to choose from.
interface Fields {
  readonly doc: unknown;
  readonly set: (d: unknown) => void;
  readonly counts: Awaited<ReturnType<typeof countsOf>>;
  readonly services: readonly string[];
}

// Plain is a text or number input, offering choices as it's typed.
function Plain(p: {
  value: string | number;
  number: boolean;
  choices: readonly Choice[];
  set: (v: string) => void;
}) {
  const id = useId();
  return [
    createElement("input", {
      key: "i",
      value: p.value,
      type: p.number ? "number" : "text",
      step: "any",
      list: p.choices.length > 0 ? id : undefined,
      onChange: (e: { target: { value: string } }) => p.set(e.target.value),
    }),
    p.choices.length > 0 && createElement(Choices, { key: "c", id, choices: p.choices }),
  ];
}

// Input is how a field is typed: a number, a log query, or text offering choices.
interface Input {
  readonly number?: boolean;
  readonly query?: boolean;
  readonly choices?: readonly Choice[];
}

const field = (f: Fields, label: string, path: Path, input: Input = {}) => {
  const v = at(f.doc, path);
  const value = typeof v === "string" || typeof v === "number" ? v : "";
  const set = (s: string) => f.set(put(f.doc, path, input.number ? Number(s) : s || undefined));
  return createElement(
    "label",
    { key: label },
    createElement("span", null, label),
    input.query
      ? createElement(QueryField, {
          value: String(value),
          words: (q: string, cursor: number) =>
            suggest(q, cursor, f.counts?.fields.map((s) => s.name) ?? []),
          set,
        })
      : createElement(Plain, {
          value,
          number: input.number === true,
          choices: input.choices ?? [],
          set,
        }),
  );
};

const select = (label: string, value: string, options: object, pick: (v: string) => void) =>
  createElement(
    "label",
    { key: label },
    createElement("span", null, label),
    createElement(
      "select",
      { value, onChange: (e: { target: { value: string } }) => pick(e.target.value) },
      Object.keys(options).map((o) => createElement("option", { key: o }, o)),
    ),
  );

// hint is a line under a section's fields, aligned with the inputs.
const hint = (key: string, ...text: ReactNode[]) =>
  createElement(
    "p",
    { key, className: "slo-hint", role: key === "n" ? "status" : undefined },
    ...text,
  );

// section is a titled group of the form's fields.
const section = (title: string, ...children: ReactNode[]) =>
  createElement("fieldset", { key: title }, createElement("legend", null, title), ...children);

// sli is the shape, its queries, and their counts, flagged when good doesn't narrow total.
function sli(f: Fields): ReactNode[] {
  const { doc, set, counts: c } = f;
  const t = at(doc, total);
  const base =
    typeof t === "string" ? t : (/\(([^[]*) \[/.exec(String(at(doc, metric)))?.[1] ?? "{}");
  const ratios = at(doc, metric) === undefined;
  return [
    select("Shape", shapeOf(doc), shapes, (s) =>
      set(put(doc, ["spec"], { ...Object(at(doc, ["spec"])), ...shapes[s]?.(base) })),
    ),
    ...(ratios
      ? [
          field(f, "Total query", total, { query: true }),
          field(f, "Good query", good, { query: true }),
        ]
      : [
          field(f, "Metric query", metric, { query: true }),
          field(f, "Slice value", [...objective, "value"], { number: true }),
        ]),
    c &&
      hint(
        "n",
        `${c.good} good of ${c.valid} valid events over 24h`,
        c.valid > 0 &&
          c.good >= c.valid &&
          createElement("b", { className: "slo-flag" }, " · the good query doesn't narrow total"),
      ),
  ];
}

// objective is the target and window, and the budget they allow.
function objectiveOf(f: Fields): ReactNode[] {
  const target = Number(at(f.doc, [...objective, "target"]));
  const window = String(at(f.doc, ["spec", "timeWindow", 0, "duration"]));
  const ms = -resolve(`now-${window}`, 0);
  const events = f.counts?.valid && Math.round(((1 - target) * f.counts.valid * ms) / 864e5);
  return [
    field(f, "Target", [...objective, "target"], { number: true }),
    field(f, "Window", ["spec", "timeWindow", 0, "duration"], {
      choices: ["1d", "7d", "28d", "30d"].map((value) => ({ value })),
    }),
    ms > 0 &&
      hint(
        "w",
        `${pct(1 - target)} of ${window} is ${duration((1 - target) * ms)}`,
        events ? `, or about ${events} bad events at current volume` : "",
      ),
  ];
}

// grouping is the column to group by, suggested by rank_fields, and the groups it tracks.
function grouping(f: Fields): ReactNode[] {
  const groupBy = ["metadata", "annotations", "datafocal/group-by"];
  const groups = f.counts?.fields.find((s) => s.name === at(f.doc, groupBy))?.cardinality;
  const choices = (f.counts?.fields ?? []).map((s) => ({
    value: s.name,
    note: `${s.cardinality} values`,
  }));
  return [
    field(f, "Group by", groupBy, { choices }),
    hint(
      "g",
      groups === undefined
        ? "Optional: one budget per value of a field."
        : `tracks ${Math.min(groups, 500)} groups`,
    ),
  ];
}

// alerting is none, the standard preset, or custom conditions' burn rates and lookbacks.
function alerting(f: Fields): ReactNode[] {
  const conditions = at(f.doc, [...policies, 0, "spec", "conditions"]);
  const custom = alertsOf(f.doc) === "custom" ? list(conditions) : [];
  return [
    select("Alerts", alertsOf(f.doc), alerts, (a) => f.set(put(f.doc, policies, alerts[a]))),
    ...custom.flatMap((_, i) => {
      const path = [...policies, 0, "spec", "conditions", i, "spec", "condition"];
      return [
        field(f, `Condition ${i + 1} burn rate`, [...path, "threshold"], { number: true }),
        field(f, `Condition ${i + 1} lookback`, [...path, "lookbackWindow"]),
      ];
    }),
  ];
}

// Builder is an SLO's form, its name and service, SLI, objective, grouping and alerts,
// beside its classification, status over the last 24 hours and normalized YAML. Without name
// it builds a new SLO, from the query param when given.
export function Builder(p: { client: DataFocalClient; name: string | undefined }): ReactElement {
  const { client, name } = p;
  const { query = "{}" } = useParams();
  const loaded = useAnswer(name ?? "", async () => (name ? exported(client, name) : fresh(query)));
  const [edited, set] = useState<unknown>();
  const doc = edited ?? (loaded instanceof Error ? undefined : loaded);
  const time = useDay();
  const counts = useCounts(client, doc, time);
  const services = useServices(client, time);
  const f = {
    doc,
    set,
    counts: counts instanceof Error ? undefined : counts,
    services: Array.isArray(services) ? services : [],
  };
  return createElement(
    "div",
    { className: "slos slo-builder" },
    createElement("header", null, createElement("h2", null, name ? `Edit ${name}` : "New SLO")),
    doc === undefined
      ? createElement(Show<unknown>, { answer: loaded, render: () => null })
      : createElement(
          "div",
          { className: "slo-grid" },
          createElement(
            "form",
            { onSubmit: (e: { preventDefault: () => void }) => e.preventDefault() },
            section(
              "SLO",
              field(f, "Name", ["metadata", "name"]),
              field(f, "Service", ["spec", "service"], {
                choices: f.services.map((value) => ({ value })),
              }),
            ),
            section("Indicator", ...sli(f)),
            section("Objective", ...objectiveOf(f)),
            section("Grouping", ...grouping(f)),
            section("Alerts", ...alerting(f)),
          ),
          createElement(Checked, {
            client,
            content: JSON.stringify(doc),
            file: `${String(at(doc, ["metadata", "name"]))}.yaml`,
            onSaved: () => navigate({ view: "slos" }),
          }),
        ),
  );
}
