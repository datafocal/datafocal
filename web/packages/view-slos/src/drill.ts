import {
  type ExplainSpikeResponse,
  type DataFocalClient,
  type LogLine,
  type Predicate,
  type SLOStatusResponse,
  type TimeSpec,
} from "@datafocal/api";
import { type Drill as DrillState, DrillChips, useDrill } from "@datafocal/shell";
import {
  type Answer,
  BarRow,
  type Kpi,
  Kpis,
  lineText,
  Meter,
  Panel,
  Show,
  useAnswer,
} from "@datafocal/ui";
import { createElement as h, type ReactElement, type ReactNode } from "react";
import { Changed } from "./changed";
import { pct } from "./doc";
import { boundOf, FieldChart } from "./measure";
import { badTile, Overview, unitOf } from "./overview";
import { clock, heading, isDuration, rate, same, shown, valueOf } from "./parts";
import { Projection } from "./projection";
import { RequestInspector } from "./request";
import { Resources } from "./resources";
import { type Scopes, scopesOf } from "./scopes";
import { BadSlices, type Slice, SliceChart, useSlices } from "./slices";
import { type Analysis, useAnalysis, WindowStrip } from "./window";

// An SLO's drill-down, built of what its definition says it counts:
// the tiles, its unit over time beside the budget left, for a time-slice SLO its bad slices,
// for one with bad events what they share and the events themselves, one open in the
// inspector, else what changed, and the resources of the pods that served it. Every step of
// it, a filter, a slice, an event, is a page of the browser's history.

type Detail = SLOStatusResponse["slo"];

// lineOf is the picked line's place, as "segment/row".
const lineOf = (picked: string | undefined) => {
  const cut = picked?.lastIndexOf("/") ?? -1;
  return picked && cut > 0
    ? { segment: picked.slice(0, cut), row: Number(picked.slice(cut + 1)) }
    : undefined;
};
const refOf = (l: LogLine) => `${l.segment ?? ""}/${l.row}`;

// burnTiles are the burn rates the definition alerts on, each condition's over its lookback
// with its short window's and its threshold; with no alert policy, the burn over 1h and 6h.
function burnTiles(d: Detail): Kpi[] {
  const conditions = d.conditions ?? [];
  if (conditions.length === 0) {
    const tile = (label: string, v: number | undefined): Kpi => ({
      label,
      value: rate(v),
      detail: "1× spends the budget over the window",
      tone: v != null && v >= 1 ? "bad" : undefined,
    });
    return [tile("Burn 1h", d.burn_rate_1h), tile("Burn 6h", d.burn_rate_6h)];
  }
  return conditions.map((c) => ({
    label: `Burn ${c.lookback}${c.name ? ` · ${c.name}` : ""}`,
    value: rate(c.burn_rate),
    detail: `${c.short_window ? `${rate(c.short_burn_rate)} over ${c.short_window} · ` : ""}alerts at ${rate(c.threshold)}`,
    tone: c.state === "ok" ? undefined : "bad",
  }));
}

const when = (t: string) =>
  new Date(Date.parse(t)).toLocaleString([], {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });

// forecastTile is the budget a horizon on at the burn over twice the horizon before, and when
// that burn spends it.
function forecastTile(d: Detail): Kpi | undefined {
  const f = d.forecast;
  if (!f) {
    return undefined;
  }
  const b = f.budget_remaining;
  return {
    label: `Budget in ${f.horizon}`,
    value: b == null ? "—" : b > 0 ? pct(b, 0) : "spent",
    detail: `at ${rate(f.burn_rate)}, the last ${f.lookback}'s burn${f.exhausted_at ? ` · runs out ${when(f.exhausted_at)}` : ""}`,
    tone: b != null && b <= 0 ? "bad" : undefined,
  };
}

// Tiles are the SLO's SLI, budget, burn, forecast and bad units against what each is measured by.
function tiles(d: Detail, bad: Kpi): Kpi[] {
  const b = d.budget_remaining;
  const forecast = forecastTile(d);
  return [
    {
      label: "SLI",
      value: pct(d.sli),
      detail: `objective ${pct(d.objective)}`,
      tone: d.sli != null && d.sli < d.objective ? "bad" : undefined,
    },
    b == null || b > 0
      ? { label: "Error budget", value: b == null ? "—" : pct(b, 0), detail: "left" }
      : { label: "Error budget", value: rate(1 - b), detail: "the budget spent", tone: "bad" },
    ...(forecast ? [forecast] : []),
    ...burnTiles(d),
    bad,
  ];
}

const settled = <T>(a: Answer<T | undefined>) => (a instanceof Error ? undefined : a);

// byField is explain_spike's shifts grouped by their field, in its order.
function byField(spike: ExplainSpikeResponse) {
  const fields = new Map<string, ExplainSpikeResponse["shifts"]>();
  for (const s of spike.shifts) {
    fields.set(s.field, [...(fields.get(s.field) ?? []), s]);
  }
  return [...fields];
}

// Shared is a panel per field whose values the bad events over-represent, each a filter.
function Shared(p: {
  spike: ExplainSpikeResponse;
  filter: readonly Predicate[];
  toggle: (f: Predicate) => void;
}) {
  const fields = byField(p.spike);
  if (fields.length === 0) {
    return h(
      Panel,
      { title: "What the bad events share" },
      h("p", null, "No field value is over-represented among them."),
    );
  }
  return h(
    "div",
    { className: "g3" },
    fields.map(([field, shifts]) =>
      h(
        Panel,
        {
          key: field,
          title: field,
          big: [pct(shifts[0]?.slice_share, 0), `of bad events hold ${shifts[0]?.value ?? ""}`],
        },
        shifts.map((s) => {
          const f: Predicate = { field, op: "eq", value: s.value };
          return h(BarRow, {
            key: s.value,
            label: s.value,
            share: s.slice_share,
            figure: `${pct(s.slice_share, 0)} · ${pct(s.baseline_share, 1)} of the rest`,
            pressed: p.filter.some((g) => same(f, g)),
            onClick: () => p.toggle(f),
          });
        }),
      ),
    ),
  );
}

// cellsOf are a bad event's cells under columns, a duration with a bar of its share of the
// slowest.
const cellsOf = (l: LogLine, columns: readonly string[], slowest: number) =>
  columns.map((c) =>
    h(
      "td",
      { key: c },
      isDuration(c)
        ? h(Meter, { text: shown(c, valueOf(l, c)), share: (Number(valueOf(l, c)) || 0) / slowest })
        : shown(c, valueOf(l, c)),
    ),
  );

const isPicked = (l: LogLine, picked: string | undefined) => refOf(l) === picked;

// Events are the bad events, newest first, under the filter: their time, the fields that make
// them bad and those they share, each opening in the inspector.
function Events(p: {
  lines: readonly LogLine[];
  columns: readonly string[];
  picked: string | undefined;
  pick: (l: LogLine) => void;
}): ReactElement {
  if (p.lines.length === 0) {
    return h(Panel, { title: "Bad events" }, h("p", null, "No bad events in range."));
  }
  const timed = p.columns.find(isDuration);
  const slowest = Math.max(1, ...p.lines.map((l) => Number(valueOf(l, timed ?? "")) || 0));
  return h(
    Panel,
    {
      title: "Bad events",
      big: [String(p.lines.length), p.lines.length === 20 ? "newest shown" : "in range"],
    },
    h(
      "table",
      { "aria-label": "Bad events", className: "slo-requests" },
      h(
        "thead",
        null,
        h(
          "tr",
          null,
          ["At", ...p.columns].map((n) => h("th", { key: n }, heading(n))),
        ),
      ),
      h(
        "tbody",
        null,
        p.lines.map((l) =>
          h(
            "tr",
            {
              key: `${l.segment ?? ""}/${l.row}`,
              "aria-selected": isPicked(l, p.picked),
              tabIndex: 0,
              onClick: () => p.pick(l),
              onKeyDown: (e: { key: string }) => e.key === "Enter" && p.pick(l),
            },
            h("td", null, clock(l.timestamp)),
            ...cellsOf(l, p.columns, slowest),
          ),
        ),
      ),
    ),
  );
}

// useDrill loads what the drill-down reads: the scopes, the slices, and for an SLO with bad
// events explain_spike over them and the events under the filter.
function useLoads(p: {
  client: DataFocalClient;
  d: Detail;
  doc: unknown;
  filter: readonly Predicate[];
  a: Analysis;
}) {
  const { client, d, doc, filter } = p;
  const { time } = p.a;
  const scopes = useAnswer(JSON.stringify(["scopes", d.name, d.group, time]), () =>
    scopesOf(client, doc, d, time),
  );
  const s = settled(scopes);
  const slices = useSlices(client, s?.slices, time);
  const events = s !== undefined && s.bad.length > 0;
  const spike = useAnswer(JSON.stringify(["spike", s, time, filter]), async () =>
    events
      ? (
          await client.call("explain_spike", {
            selector: { time, where: [...s.valid, ...filter] },
            slice: [...s.bad, ...filter],
          })
        ).data
      : undefined,
  );
  const lines = useAnswer(JSON.stringify(["lines", s, time, filter]), async () =>
    events
      ? once(
          (
            await client.call("search_logs", {
              selector: { time, where: [...s.bad, ...filter] },
              limit: 20,
            })
          ).data.lines ?? [],
          s.distinctBy,
        )
      : undefined,
  );
  return { time, scopes, s, slices, spike, lines };
}

type Drilled = ReturnType<typeof useLoads>;

// once is lines with each value of field once, its first line kept, and every line without
// one, as the SLO counts them.
const once = (lines: readonly LogLine[], field: string | undefined): LogLine[] => {
  const seen = new Set<string>();
  return lines.filter((l) => {
    const v = field ? valueOf(l, field) : "";
    return v === "" || (!seen.has(v) && Boolean(seen.add(v)));
  });
};

// columnsOf are a bad event's columns after its time: what makes it bad, what the events
// share, and what the filter names.
const columnsOf = (
  s: Scopes | undefined,
  spike: ExplainSpikeResponse | undefined,
  filter: readonly Predicate[],
) =>
  [
    ...new Set([
      ...(s?.badness ?? []).map((b) => b.field ?? ""),
      ...(spike ? byField(spike).map(([f]) => f) : []),
      ...filter.map((f) => f.field ?? ""),
    ]),
  ].filter(Boolean);

interface Props {
  readonly client: DataFocalClient;
  readonly d: Detail;
  readonly loads: Drilled;
  readonly drill: DrillState;
  readonly columns: readonly string[];
}

// Measure is what the SLO judges over the window: a time-slice SLO's metric a slice against
// its objective, or the number a bound on which makes an event bad, against the bound.
function Measure(p: Props): ReactNode {
  const { s, slices, time } = p.loads;
  const brush = p.drill.narrow;
  const sliced = settled(slices);
  if (s?.slices) {
    return sliced && h(SliceChart, { s: s.slices, slices: sliced, brush });
  }
  const bound = s && boundOf(s.badness);
  return (
    s &&
    bound &&
    h(FieldChart, { client: p.client, time, valid: [...s.valid, ...p.drill.filter], bound, brush })
  );
}

// Detail is what the bad units are: the bad slices, and the bad events and what they share,
// or with none to list what changed.
function Breakdown(p: Props): ReactNode {
  const { s, slices, spike, lines, time } = p.loads;
  if (!s) {
    return null;
  }
  const { drill } = p;
  const sliced = settled(slices);
  return [
    s.slices &&
      sliced &&
      h(Projection, { key: "p", client: p.client, s: s.slices, slices: sliced, time }),
    s.slices &&
      sliced &&
      h(BadSlices, {
        key: "b",
        s: s.slices,
        slices: sliced,
        open: (x: Slice) => drill.narrow(x.from, x.to),
      }),
    s.bad.length === 0
      ? // With no bad events to list, what changed explains the bad slices, when there are any.
        sliced?.some((x) => x.state === "bad") &&
        h(Changed, { key: "c", client: p.client, time, valid: s.valid })
      : [
          h(DrillChips, { key: "f", drill }),
          h(Show<ExplainSpikeResponse | undefined>, {
            key: "s",
            answer: spike,
            render: (r) => r && h(Shared, { spike: r, filter: drill.filter, toggle: drill.toggle }),
          }),
          h(Show<readonly LogLine[] | undefined>, {
            key: "e",
            answer: lines,
            render: (ls) =>
              ls &&
              h(Events, {
                lines: ls,
                columns: p.columns,
                picked: drill.picked,
                pick: (l) => drill.pick(refOf(l)),
              }),
          }),
        ],
  ];
}

// usePicked is the chosen event: from the events listed, else read from its place.
function usePicked(
  client: DataFocalClient,
  picked: string | undefined,
  lines: readonly LogLine[] | undefined,
  time: TimeSpec,
) {
  const listed = lines?.find((l) => isPicked(l, picked));
  return useAnswer(JSON.stringify(["picked", picked, Boolean(listed)]), async () => {
    const at = lineOf(picked);
    if (!at || listed) {
      return listed;
    }
    const res = await client.call("search_logs", {
      selector: { time, where: [] },
      around: { ...at, before: 0, after: 0 },
      limit: 1,
    });
    return res.data.lines?.find((l) => isPicked(l, picked));
  });
}

// titleOf names a bad event by what makes it bad and the first field the bad events share,
// as their columns lead with, else by its text.
const titleOf = (l: LogLine, columns: readonly string[]) =>
  columns
    .slice(0, 2)
    .map((c) => shown(c, valueOf(l, c)))
    .filter(Boolean)
    .join(" · ") || lineText(l);

// Drill is the SLO's page below its header: the tiles and the overview over the range, from the
// store's counts, then what reads lines, over the analysis window.
export function Drill(p: { client: DataFocalClient; d: Detail; doc: unknown }): ReactNode {
  const drill = useDrill();
  const a = useAnalysis();
  const loads = useLoads({ client: p.client, d: p.d, doc: p.doc, filter: drill.filter, a });
  const columns = columnsOf(loads.s, settled(loads.spike), drill.filter);
  const picked = settled(usePicked(p.client, drill.picked, settled(loads.lines), loads.time));
  const props: Props = { ...p, loads, drill, columns };
  const unit = unitOf(p.doc);
  return h(
    "div",
    { className: "slo-drill" },
    h(
      "div",
      { className: "slo-drill-main" },
      h(Kpis, { tiles: tiles(p.d, badTile(p.d, unit)) }),
      h(Overview, { d: p.d, unit, brush: drill.narrow }),
      h(WindowStrip, { a }),
      h(Measure, props),
      h(Breakdown, props),
      p.d.service &&
        h(Resources, {
          client: p.client,
          time: loads.time,
          where: [{ field: "service", op: "eq", value: p.d.service }],
          service: p.d.service,
        }),
    ),
    picked &&
      h(RequestInspector, {
        key: refOf(picked),
        client: p.client,
        l: picked,
        title: titleOf(picked, columns),
        close: () => drill.pick(undefined),
      }),
  );
}
