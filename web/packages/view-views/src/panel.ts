import {
  binStarts,
  type GetViewResponse,
  type DataFocalClient,
  type ListSLOsResponse,
  type LogLine,
  type Pattern,
  type QueryRequest,
  type QueryResponse,
  type Selector,
  type Series,
  type TimeSpec,
} from "@datafocal/api";
import { navigate, useFocus, useSnapshot, useTimeRange } from "@datafocal/shell";
import { Card, Chart, Failure, lineText, Menu, Panel, useAnswer, useLast } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode, useEffect, useState } from "react";

type PanelDef = GetViewResponse["view"]["panels"][number];

// Drawn is what a panel draws: a query's answer, or for an slo panel its SLO's status.
type Drawn = QueryResponse | ListSLOsResponse["slos"][number];

// row and column are a grid unit's height and width in pixels, with the gap after it.
const [row, column] = [70, 96];
const iso = (ms: number) => new Date(ms).toISOString();
const fmt = (v: number) => (Number.isInteger(v) ? String(v) : v.toFixed(2));
const total = (s?: Series) => (s?.values ?? []).reduce<number>((a, v) => a + (v ?? 0), 0);

// listed is v's array at key, of what the query's tool answers.
function listed<T>(v: unknown, key: string): T[] {
  const x: unknown = typeof v === "object" && v !== null ? Reflect.get(v, key) : undefined;
  const isList = (y: unknown): y is T[] => Array.isArray(y);
  return isList(x) ? x : [];
}

// useCell places a panel on the grid, and says whether it is within a screen of the view.
function useCell({ x, y, w, h }: PanelDef["grid"]) {
  const [el, setEl] = useState<HTMLElement | null>(null);
  const [on, setOn] = useState(false);
  useEffect(() => {
    el?.style.setProperty("grid-area", `${y + 1} / ${x + 1} / span ${h} / span ${w}`);
  }, [el, x, y, w, h]);
  useEffect(() => {
    const options = { root: el?.closest("main") ?? null, rootMargin: "100% 0px" };
    const watch = new IntersectionObserver(([e]) => setOn(e?.isIntersecting === true), options);
    if (el) {
      watch.observe(el);
    }
    return () => watch.disconnect();
  }, [el]);
  return { ref: setEl, on, width: el?.clientWidth };
}

// TimeChart is a timeseries panel's lines, one per group: a brush narrows the page's range and
// a pick opens the Moment at its bin.
function TimeChart(props: { p: PanelDef; res: QueryResponse; width: number }): ReactNode {
  const { p, res } = props;
  const [, setRange] = useTimeRange();
  const focus = useFocus();
  const [snapshot] = useSnapshot();
  const series = listed<Series>(res.result, "series");
  const args: { selector?: Selector } =
    typeof res.call.args === "object" ? { ...res.call.args } : {};
  const pick = (at: number) =>
    focus({
      title: `${p.title} at ${iso(at).slice(11, 16)}`,
      query: res.canonical,
      selector: { ...args.selector, time: { from: iso(at), to: iso(at + (series[0]?.step ?? 0)) } },
      slice: [],
      ...(snapshot && { snapshot }),
    });
  return createElement(Chart, {
    kind: "line",
    data: [series[0] ? binStarts(series[0]) : [], ...series.map((s) => s.values ?? [])],
    width: props.width - 34,
    height: p.grid.h * row - 110,
    label: p.title,
    x: { time: true },
    series: series.map((s) => s.key || p.title),
    onBrush: (from, to) => setRange({ from: iso(from), to: iso(to) }),
    onPick: pick,
  });
}

const tr = (cells: readonly ReactNode[], i: number, cell = "td") =>
  createElement(
    "tr",
    { key: i },
    cells.map((c, j) => createElement(cell, { key: j }, c)),
  );

// table is rows of cells under head, as a panel's groups, patterns and lines are drawn.
export const table = (label: string, rows: ReactNode[][], head?: string[]): ReactElement =>
  rows.length === 0
    ? createElement("p", { className: "vw-hint", role: "status" }, "Nothing over this range.")
    : createElement(
        "table",
        { "aria-label": label },
        head && createElement("thead", null, tr(head, 0, "th")),
        createElement(
          "tbody",
          null,
          rows.map((r, i) => tr(r, i)),
        ),
      );

// draw is the panel's answer drawn as its kind draws it.
function draw(p: PanelDef, d: Drawn, width: number): ReactNode {
  if (!("canonical" in d)) {
    return createElement(Card, { s: d, open: (slo: string) => navigate({ view: "slos", slo }) });
  }
  const series = listed<Series>(d.result, "series");
  const kinds: Record<string, () => ReactNode> = {
    timeseries: () => createElement(TimeChart, { p, res: d, width }),
    stat: () => createElement("div", { className: "big" }, fmt(total(series[0]))),
    table: () =>
      table(
        p.title,
        series.map((s) => [s.key, fmt(total(s))]),
      ),
    patterns: () =>
      table(
        p.title,
        listed<Pattern>(d.result, "patterns").map((t) => [t.pattern, t.count]),
      ),
    lines: () =>
      table(
        p.title,
        listed<LogLine>(d.result, "lines").map((l) => [lineText(l)]),
      ),
  };
  return kinds[p.kind]?.();
}

export interface ViewPanelProps {
  readonly client: DataFocalClient;
  readonly view: string; // view/index is the panel's X-DataFocal-View
  readonly index: number;
  readonly panel: PanelDef;
  readonly time: TimeSpec;
  // now is the tick the time was resolved against, or undefined for the page's pinned now.
  readonly now: number | undefined;
  // vars are the variables' values, undefined until each has one.
  readonly vars: Readonly<Record<string, string>> | undefined;
}

// useRead asks once shown, and again on a change while shown; until then the panel is stale.
function useRead(p: ViewPanelProps, shown: boolean) {
  const [snapshot] = useSnapshot();
  const { panel: def, client } = p;
  const { req, want } = ask(p, snapshot);
  const [key, setKey] = useState<string | null>(null);
  if (shown && want && key !== want) {
    setKey(want);
  }
  const answer = useAnswer<Drawn>(key, (signal) =>
    read(client, def, req, {
      signal,
      view: `${p.view}/${p.index}`,
      ...(p.now !== undefined && { now: p.now }),
    }),
  );
  return { answer, stale: key !== null && key !== want, asked: key !== null };
}

// ask is the query panel p reads under snapshot, and the key it is asked by, or "" for a
// panel with nothing to read yet, as a new one in the builder, which asks nothing.
function ask(p: ViewPanelProps, snapshot: string | null): { req: QueryRequest; want: string } {
  const def = p.panel;
  const req: QueryRequest = {
    q: def.query ?? "",
    time: p.time,
    vars: p.vars ?? {},
    ...(snapshot && { snapshot }),
  };
  const asks = p.vars !== undefined && Boolean(def.slo || def.query);
  return { req, want: asks ? JSON.stringify(def.slo ? [def.slo, p.now] : req) : "" };
}

// read is what panel def draws: its SLO, or its query's answer to req.
async function read(
  client: DataFocalClient,
  def: PanelDef,
  req: QueryRequest,
  opts: Parameters<DataFocalClient["call"]>[2],
): Promise<Drawn> {
  if (!def.slo) {
    return (await client.call("query", req, opts)).data;
  }
  const { slos } = (await client.call("list_slos", { query: def.slo, trend: true }, opts)).data;
  return slos.find((s) => s.name === def.slo) ?? Promise.reject(new Error(`no SLO ${def.slo}`));
}

// ViewPanel is one panel, read once it is within a screen of the view and again on a change
// while it is, keeping its last answer; one off screen is stale until it comes back. Its menu
// opens its query in Logs, with the variables substituted, or the view's builder.
export function ViewPanel(p: ViewPanelProps): ReactNode {
  const { panel: def } = p;
  const { ref, on, width = p.panel.grid.w * column } = useCell(def.grid);
  const { answer, stale, asked } = useRead(p, on);
  const last = useLast(answer);
  const logs = last && "canonical" in last ? last.canonical : undefined;
  const items = [
    ...(logs
      ? [{ label: "Open in Logs", run: () => navigate({ view: "logs", query: logs }) }]
      : []),
    { label: "Edit", run: () => navigate({ view: "views", name: p.view, mode: "edit" }) },
  ];
  const body =
    answer instanceof Error
      ? createElement(Failure, { what: "Failed", message: answer.message })
      : last
        ? draw(def, last, width)
        : asked && "Loading…";
  const menu = createElement(Menu, { label: `${def.title} actions`, sections: [{ items }] });
  return createElement(
    "div",
    { ref, className: stale ? "vw-cell stale" : "vw-cell" },
    createElement(Panel, { title: def.title }, menu, body),
  );
}
