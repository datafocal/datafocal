import type { DataFocalClient, LogLine, Span } from "@datafocal/api";
import type { Renderer } from "@datafocal/render-core";
import { createTimelineRenderer, type TimelineModel } from "@datafocal/render-timeline";
import { createFlameRenderer, createWaterfallRenderer } from "@datafocal/render-waterfall";
import { type Destination, navigate, type TimeRange } from "@datafocal/shell";
import { cssColor, duration } from "@datafocal/ui";
import {
  createElement,
  type ReactElement,
  type ReactNode,
  useEffect,
  useEffectEvent,
  useMemo,
  useState,
} from "react";
import { button, Shape, SpanInspector, TraceSummary } from "./detail";
import { type Gesture, Plot, type PlotHit, type PlotProps, type RowName } from "./plot";
import {
  flameModel,
  instantLeaves,
  isCall,
  lanesModel,
  lineLogs,
  ms,
  colorIn,
  shown,
  spanLogs,
  type Trace,
  traceLogs,
  waterfallModel,
} from "./trace";

// modes are the ways a trace is drawn: its tree against time, its tree by depth, each
// service's spans on a lane, and how it compares with traces like it.
const modes = ["Waterfall", "Flame", "Services", "Compare"] as const;
type Mode = (typeof modes)[number];

interface Menu {
  readonly id: number;
  readonly x: number;
  readonly y: number;
  readonly items: readonly (readonly [string, Destination])[];
}

export interface TraceBodyProps {
  readonly client: DataFocalClient;
  readonly range: TimeRange;
  readonly t: Trace;
  // start is the span selected on arrival, -1 for none.
  readonly start: number;
}

// found are the spans a find over t keeps: those whose name, service, note or first line says
// q, ignoring case, and the spans above them; all of them for no q.
function found(t: Trace, q: string): ReadonlySet<number> | undefined {
  const want = q.trim().toLowerCase();
  if (!want) {
    return undefined;
  }
  const keep = new Set<number>();
  t.spans.forEach((s, i) => {
    const said = [
      s.name,
      s.service,
      t.notes[i],
      ...Object.values(t.lines[i]?.[0]?.attributes ?? {}),
    ];
    if (said.some((x) => x?.toLowerCase().includes(want))) {
      for (let p = i; p >= 0 && !keep.has(p); p = t.spans[p]?.parent ?? -1) {
        keep.add(p);
      }
    }
  });
  return keep;
}

// useRows is the spans the waterfall shows: those under no collapsed span a find keeps, less
// the instant steps it folds until they are asked for, but for the selection; hidden are
// those it folds.
function useRows(t: Trace, collapsed: ReadonlySet<number>, sel: number, q: string) {
  const [instants, setInstants] = useState(false);
  const folded = useMemo(() => instantLeaves(t), [t]);
  const kept = useMemo(() => found(t, q), [t, q]);
  const rows = useMemo(
    () =>
      shown(t, kept ? new Set() : collapsed).filter(
        (i) => (kept?.has(i) ?? true) && (instants || !folded.has(i) || i === sel),
      ),
    [t, collapsed, instants, folded, sel, kept],
  );
  return { rows, folded, hidden: instants ? undefined : folded, instants, setInstants };
}

// TraceBody shows a trace in the chosen mode beside the selected span, and opens in Logs
// exactly the lines a row, tick, bar or key names.
// useSelection is the selected span, from start; the spans folded, a conversation's turns but
// the selection's at first; whether its panel is open, which selecting a span reopens; and
// select and toggle, which change them.
function useSelection(t: Trace, start: number) {
  const [sel, setSel] = useState(start);
  const [collapsed, setCollapsed] = useState<ReadonlySet<number>>(() => closedTurns(t, start));
  const [inspecting, setInspecting] = useState(true);
  const select = (i: number | undefined) => {
    if (i === undefined || i < 0) {
      return;
    }
    setInspecting(true);
    const open = new Set(collapsed);
    for (let p = t.spans[i]?.parent ?? -1; p >= 0; p = t.spans[p]?.parent ?? -1) {
      open.delete(p);
    }
    // An unchanged set keeps the model, so selecting a shown span draws nothing.
    if (open.size < collapsed.size) {
      setCollapsed(open);
    }
    setSel(i);
  };
  const toggle = (i: number) => {
    const c = new Set(collapsed);
    if (!c.delete(i)) {
      c.add(i);
    }
    setCollapsed(c);
  };
  return { sel, select, collapsed, setCollapsed, toggle, inspecting, setInspecting };
}

export function TraceBody({ client, range, t, start }: TraceBodyProps): ReactElement {
  const [mode, setMode] = useState<Mode>("Waterfall");
  const [win, setWin] = useState<readonly [number, number]>();
  const [menu, setMenu] = useState<Menu>();
  const [q, setQ] = useState("");
  const { sel, select, collapsed, setCollapsed, toggle, inspecting, setInspecting } = useSelection(
    t,
    start,
  );
  const { rows, folded, hidden, instants, setInstants } = useRows(t, collapsed, sel, q);
  const span = t.spans[sel];
  useKeys(t, sel, rows, { collapsed, setCollapsed, select, setWin, win, setInspecting });
  const close = () => setInspecting(false);
  const onHit = (g: Gesture, h: PlotHit, order: readonly number[]) => {
    const i = order[h.index] ?? -1;
    const s = t.spans[i];
    const line = mode === "Waterfall" ? lineAt(t, i, h) : undefined;
    if (g === "menu") {
      const items = menuItems(t, s, line);
      setMenu((m) => ({ id: (m?.id ?? 0) + 1, x: h.x, y: h.y, items }));
    } else if (s) {
      const to = drill(g, h.alt, s, line);
      if (to) {
        navigate(to);
      } else {
        select(i);
      }
    }
  };
  const canvas = { t, rows, win, sel, onHit, folded: hidden, collapsed, toggle };
  return createElement(
    "div",
    { className: "tr-trace" },
    createElement(TraceSummary, { t }),
    createElement(
      Toolbar,
      { t, mode, setMode, q, setQ },
      mode !== "Compare" && fold(folded.size, instants, setInstants, t.turns.length > 0),
      span &&
        button(
          win ? "Whole trace" : "Zoom to span",
          () => fit(t, span, win, setWin),
          "Fit the selection (F)",
        ),
    ),
    mode === "Compare"
      ? createElement(Shape, { client, range, t })
      : createElement(Canvas, { ...canvas, mode }),
    span && inspecting && createElement(SpanInspector, { client, t, i: sel, range, close }),
    menu && createElement(ContextMenu, { key: menu.id, menu }),
  );
}

// fit zooms to span s, or back to the whole trace when zoomed.
function fit(
  t: Trace,
  s: Span,
  win: readonly [number, number] | undefined,
  setWin: (w: readonly [number, number] | undefined) => void,
): void {
  setWin(win ? undefined : [s.start_ms - t.t0, s.start_ms - t.t0 + Math.max(s.dur_ms, 1)]);
}

// closedTurns are a conversation's turns but the one holding start, so the trace opens on
// what each prompt asked and did, and on the steps of the one arrived at, and the other
// services' records, which run beside the conversation rather than in it.
function closedTurns(t: Trace, start: number): ReadonlySet<number> {
  const open = new Set<number>();
  for (let p = start; p >= 0; p = t.spans[p]?.parent ?? -1) {
    open.add(p);
  }
  return new Set([...t.turns, ...background(t)].filter((i) => !open.has(i)));
}

// background are a conversation's roots holding nothing of its services: what ran beside it,
// as a quota's samples, rather than in it.
function background(t: Trace): number[] {
  if (t.turns.length === 0) {
    return [];
  }
  const talking = new Set(t.turns.map((i) => t.spans[i]?.service));
  const holds = new Set<number>();
  t.spans.forEach((s, i) => {
    if (talking.has(s.service)) {
      for (let p = i; p >= 0 && !holds.has(p); p = t.spans[p]?.parent ?? -1) {
        holds.add(p);
      }
    }
  });
  return t.spans.flatMap((s, i) => (s.parent < 0 && !holds.has(i) ? [i] : []));
}

// fold is the toggle that shows or folds a trace's n instant steps, none when it has none; in
// a conversation they are its records that are no message, which it hides.
function fold(n: number, shown: boolean, set: (shown: boolean) => void, talk: boolean): ReactNode {
  const what = talk ? `hidden record${n === 1 ? "" : "s"}` : `instant step${n === 1 ? "" : "s"}`;
  return (
    n > 0 &&
    createElement(
      "button",
      {
        type: "button",
        className: "tbtn tr-fold",
        "aria-pressed": shown,
        title: talk
          ? "Records of the session that carry no message, such as its mode or title"
          : "Steps that take no time: a line logged on the way rather than work done",
        onClick: () => set(!shown),
      },
      `${shown ? "Hide" : "Show"} ${n} ${what}`,
    )
  );
}

// Toolbar is the modes a trace is drawn in, a find over its spans, then what acts on it.
function Toolbar(p: {
  t: Trace;
  mode: Mode;
  setMode: (m: Mode) => void;
  q: string;
  setQ: (q: string) => void;
  children?: ReactNode;
}): ReactElement {
  const { t } = p;
  return createElement(
    "div",
    { className: "tr-bar" },
    createElement(
      "div",
      { role: "group", "aria-label": "Mode", className: "tr-seg" },
      modes.map((m) =>
        createElement(
          "button",
          {
            key: m,
            type: "button",
            "aria-pressed": m === p.mode,
            onClick: () => p.setMode(m),
          },
          m,
        ),
      ),
    ),
    p.mode !== "Compare" &&
      createElement("input", {
        type: "search",
        className: "tr-find",
        "aria-label": "Find spans in this trace",
        placeholder: "Find spans: name, service, field value",
        value: p.q,
        onChange: (e: { target: HTMLInputElement }) => p.setQ(e.target.value),
      }),
    (t.coverage < 1 || t.res.total > t.spans.length) &&
      createElement(
        "span",
        null,
        `${t.spans.length} of ${t.coverage < 1 ? "at least " : ""}${t.res.total} spans`,
      ),
    createElement(
      "div",
      { className: "tr-acts" },
      p.children,
      button("Trace logs", () => navigate(traceLogs(t)), "Every line of this trace (T)"),
    ),
  );
}

// lineAt is the line of span i whose tick is within 3 px of the hit.
const lineAt = (t: Trace, i: number, h: PlotHit) =>
  t.lines[i]?.find((l) => Math.abs(ms(l) - t.t0 - h.t) * h.px <= 3);

// drill is where a click on span s goes: its lines on a double- or Alt-click, else the line
// whose tick it hit, if any.
function drill(
  g: Gesture,
  alt: boolean,
  s: Span,
  line: LogLine | undefined,
): Destination | undefined {
  if (g === "open" || alt) {
    return spanLogs(s);
  }
  return line && lineLogs(s, line);
}

function menuItems(t: Trace, s: Span | undefined, line: LogLine | undefined): Menu["items"] {
  return [
    ...(s && line ? [["Logs at this line", lineLogs(s, line)] as const] : []),
    ...(s ? [["Logs for this span", spanLogs(s)] as const] : []),
    ["Logs for this trace", traceLogs(t)],
  ];
}

interface CanvasProps {
  readonly t: Trace;
  readonly mode: Exclude<Mode, "Compare">;
  readonly rows: number[];
  readonly win: readonly [number, number] | undefined;
  readonly sel: number;
  readonly onHit: (g: Gesture, h: PlotHit, order: readonly number[]) => void;
  // folded are the instant steps left out of rows, which their parents' names count.
  readonly folded: ReadonlySet<number> | undefined;
  readonly collapsed: ReadonlySet<number>;
  readonly toggle: (i: number) => void;
}

// badges are a conversation's steps' marks by what they are.
const badges: Readonly<Record<string, { text: string; color: string }>> = {
  user: { text: "U", color: "#5b9dff" },
  thinking: { text: "…", color: "#b48cf2" },
  call: { text: "$", color: "#2fc4b2" },
  result: { text: "↩", color: "#8a93a3" },
  assistant: { text: "A", color: "#e8a050" },
};

// label is span i's name, as many as it stands for, and the instants folded under it.
function label(t: Trace, i: number, under: number): string {
  const s = t.spans[i];
  const name = s?.group ? `${s.name} ×${s.group.count}` : (s?.name ?? "");
  return under && !t.notes[i] ? `${name} · +${under} instant` : name;
}

// aside is what is said muted beside span i's name: what it did in a conversation, else its
// service where its parent's is another.
const aside = (t: Trace, i: number): string => {
  const s = t.spans[i];
  return t.notes[i] || (t.spans[s?.parent ?? -1]?.service === s?.service ? "" : (s?.service ?? ""));
};

// rowName is what the waterfall's name column says of span i: its name, and muted its
// service where it is not its parent's, or what it did in a conversation, after its colour,
// a call's arrow or a step's badge, and whether what it holds shows.
function rowName(
  t: Trace,
  i: number,
  p: { under: number; collapsed: ReadonlySet<number> },
): RowName | undefined {
  const s = t.spans[i];
  if (!s) {
    return undefined;
  }
  const holds = t.spans[i + 1]?.parent === i;
  const badge = badges[t.kinds[i] ?? ""];
  return {
    name: label(t, i, p.under),
    service: aside(t, i),
    indent: t.indent[i] ?? 0,
    error: s.error,
    color: colorIn(t, s.service),
    call: isCall(t.lines[i]?.[0]),
    ...(badge && { badge }),
    ...(holds && { open: !p.collapsed.has(i) }),
  };
}

// foldedUnder is how many of folded each span holds.
function foldedUnder(t: Trace, folded: ReadonlySet<number> | undefined): Map<number, number> {
  const under = new Map<number, number>();
  for (const i of folded ?? []) {
    const p = t.spans[i]?.parent ?? -1;
    under.set(p, (under.get(p) ?? 0) + 1);
  }
  return under;
}

// themed is a renderer's creator given the page's ink, muted ink and font, read where the
// canvas sits.
const themed =
  <R>(create: (c: HTMLCanvasElement, theme: { ink: string; muted: string; font: string }) => R) =>
  (c: HTMLCanvasElement): R => {
    const at = c.parentElement ?? c;
    const family = getComputedStyle(at).getPropertyValue("--sans").trim() || "sans-serif";
    return create(c, {
      ink: cssColor("--ink", at),
      muted: cssColor("--ink-3", at),
      font: `500 11.5px ${family}`,
    });
  };
const waterfall = themed(createWaterfallRenderer);
const flame = themed(createFlameRenderer);
const timeline: (c: HTMLCanvasElement) => Renderer<TimelineModel> = themed(createTimelineRenderer);

// Canvas plots the shown spans as a waterfall or a flame graph, or every span on its
// service's lane.
function Canvas(p: CanvasProps): ReactElement {
  const { t, mode, rows, win, sel, onHit, folded } = p;
  const wf = useMemo(() => waterfallModel(t, rows), [t, rows]);
  const fm = useMemo(() => flameModel(wf), [wf]);
  const under = useMemo(() => foldedUnder(t, folded), [t, folded]);
  const byStart = useMemo(
    () =>
      [...t.spans.keys()].sort((a, b) => (t.spans[a]?.start_ms ?? 0) - (t.spans[b]?.start_ms ?? 0)),
    [t],
  );
  const lanes = useMemo(() => lanesModel(t, byStart), [t, byStart]);
  const end = Math.max(1, ...t.spans.map((s) => s.start_ms + s.dur_ms - t.t0));
  const plot = <M>(
    q: Pick<PlotProps<M>, "create" | "model" | "rows" | "row" | "name" | "head" | "onToggle">,
    order: number[],
  ) => {
    const tip = (i: number) => {
      const s = t.spans[order[i] ?? -1];
      return s ? `${s.name} · ${duration(s.dur_ms)}` : "";
    };
    return createElement(Plot<M>, {
      ...q,
      key: mode,
      win,
      end,
      tip,
      onHit: (g, h) => onHit(g, h, order),
    });
  };
  if (mode === "Services") {
    const row = lanes.lanes.findIndex((l) => l.id === t.spans[sel]?.service);
    const name = laneName(t, lanes);
    return plot(
      { create: timeline, model: lanes, rows: lanes.lanes.length, row, name, head: "Service" },
      byStart,
    );
  }
  if (mode === "Flame") {
    const row = fm.spans.depth[rows.indexOf(sel)] ?? -1;
    return plot({ create: flame, model: fm, rows: Math.max(...fm.spans.depth) + 1, row }, rows);
  }
  return plot({ create: waterfall, model: wf, ...named(p, under) }, rows);
}

// laneName names a lane by its service, in its colour.
const laneName = (t: Trace, lanes: TimelineModel) => (r: number) => {
  const id = lanes.lanes[r]?.id;
  return id === undefined
    ? undefined
    : { name: id, service: "", indent: 0, error: false, color: colorIn(t, id) };
};

// named is the waterfall's rows, its selection's and what its name column says of each,
// under its head, a chevron folding what each holds.
function named(p: CanvasProps, under: ReadonlyMap<number, number>) {
  const { t, rows } = p;
  return {
    rows: rows.length,
    row: rows.indexOf(p.sel),
    name: (row: number) => {
      const i = rows[row] ?? -1;
      return rowName(t, i, { under: under.get(i) ?? 0, collapsed: p.collapsed });
    },
    head: t.turns.length > 0 ? "Turn · step" : "Operation",
    onToggle: (row: number) => p.toggle(rows[row] ?? -1),
  };
}

interface Nav {
  readonly collapsed: ReadonlySet<number>;
  readonly setCollapsed: (c: ReadonlySet<number>) => void;
  readonly select: (i: number | undefined) => void;
  readonly win: readonly [number, number] | undefined;
  readonly setWin: (w: readonly [number, number] | undefined) => void;
  readonly setInspecting: (open: boolean) => void;
}

// useKeys binds the mockup's keys: j/k move, h/l collapse and expand, n and c step to the
// next error and critical span, f fits the selection, L and T open its and the trace's lines,
// and Escape closes the selected span's panel.
function useKeys(t: Trace, sel: number, rows: readonly number[], nav: Nav): void {
  const span = t.spans[sel];
  const step = (d: number) =>
    nav.select(rows[Math.min(rows.length - 1, Math.max(0, rows.indexOf(sel) + d))]);
  const next = (pick: (s: Span) => boolean) => {
    const all = t.spans.flatMap((s, i) => (pick(s) ? [i] : []));
    nav.select(all.find((i) => i > sel) ?? all[0]);
  };
  const fold = (on: boolean) => {
    const c = new Set(nav.collapsed);
    if (on) {
      c.add(sel);
    } else {
      c.delete(sel);
    }
    nav.setCollapsed(c);
  };
  const leaf = t.spans[sel + 1]?.parent !== sel;
  const keys: Readonly<Record<string, () => void>> = {
    j: () => step(1),
    k: () => step(-1),
    h: () => span && (leaf || nav.collapsed.has(sel) ? nav.select(span.parent) : fold(true)),
    l: () => (nav.collapsed.has(sel) ? fold(false) : step(1)),
    n: () => next((s) => s.error),
    c: () => next((s) => s.critical),
    f: () =>
      nav.setWin(
        nav.win || !span ? undefined : [span.start_ms - t.t0, span.start_ms - t.t0 + span.dur_ms],
      ),
    L: () => navigate(span ? spanLogs(span) : traceLogs(t)),
    T: () => navigate(traceLogs(t)),
    Escape: () => nav.setInspecting(false),
  };
  const onKey = useEffectEvent((e: KeyboardEvent) => {
    if (!e.metaKey && !e.ctrlKey && !e.altKey && !(e.target instanceof HTMLInputElement)) {
      keys[e.key]?.();
    }
  });
  useEffect(() => {
    const listen = (e: KeyboardEvent) => onKey(e);
    addEventListener("keydown", listen);
    return () => removeEventListener("keydown", listen);
  }, []);
}

// ContextMenu is the right-click menu at the pointer, a popover so a click elsewhere closes it.
function ContextMenu({ menu }: { menu: Menu }): ReactElement {
  return createElement(
    "div",
    {
      role: "menu",
      popover: "auto",
      className: "menu tr-menu",
      ref: (el: HTMLDivElement | null) => {
        el?.style.setProperty("left", `${menu.x}px`);
        el?.style.setProperty("top", `${menu.y}px`);
        if (el && !el.matches(":popover-open")) {
          el.showPopover();
        }
      },
    },
    menu.items.map(([label, to]) =>
      createElement(
        "button",
        { key: label, type: "button", role: "menuitem", onClick: () => navigate(to) },
        label,
      ),
    ),
  );
}
