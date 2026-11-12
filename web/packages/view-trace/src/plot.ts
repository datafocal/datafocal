import type { Renderer } from "@datafocal/render-core";
import { duration } from "@datafocal/ui";
import {
  createElement,
  type MouseEvent,
  type ReactElement,
  useCallback,
  useEffect,
  useLayoutEffect,
  useRef,
  useState,
} from "react";

// rowHeight is a span's row, in CSS pixels; tickGap is the fewest pixels between the
// ruler's ticks.
const rowHeight = 26;
const tickGap = 90;

export type Gesture = "click" | "open" | "menu";

// PlotHit is where a gesture landed: the item under it (-1 for none), its time in the
// model's milliseconds, the pixels a millisecond spans, and the page point.
export interface PlotHit {
  readonly index: number;
  readonly t: number;
  readonly px: number;
  readonly x: number;
  readonly y: number;
  readonly alt: boolean;
}

// RowName is what the name column says of a row: its span's name and, muted, its service
// or what it did, indented, after its colour, or an arrow for a call to another service, or a
// badge saying what a conversation's step is; open says whether the spans under it show,
// undefined for one holding none.
export interface RowName {
  readonly name: string;
  readonly service: string;
  readonly indent: number;
  readonly error: boolean;
  readonly color?: string;
  readonly call?: boolean;
  readonly badge?: { readonly text: string; readonly color: string };
  readonly open?: boolean;
}

export interface PlotProps<M> {
  readonly create: (c: HTMLCanvasElement) => Renderer<M>;
  readonly model: M;
  readonly rows: number;
  // win is the time shown, all of [0, end] when unset.
  readonly win: readonly [number, number] | undefined;
  readonly end: number;
  // row is the selection's, -1 for none.
  readonly row: number;
  // name names a row in a column beside the bars, under head; without it the canvas is the
  // whole width.
  readonly name?: (row: number) => RowName | undefined;
  readonly head?: string;
  // tip is what hovering an item says of it.
  readonly tip?: (index: number) => string;
  readonly onHit: (g: Gesture, h: PlotHit) => void;
  // onToggle opens or folds the spans under a row's.
  readonly onToggle?: (row: number) => void;
  // onBox takes the times and rows a box dragged over the canvas spans, ordered.
  readonly onBox?: (t0: number, t1: number, row0: number, row1: number) => void;
}

// ticks are the ruler's times across [x0, x1], a round step apart and at least tickGap
// pixels of width apart.
function ticks(x0: number, x1: number, width: number): number[] {
  const span = x1 - x0;
  if (span <= 0 || width <= 0) {
    return [];
  }
  const raw = (span * tickGap) / width;
  const pow = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= raw) ?? raw;
  const out: number[] = [];
  for (let t = Math.ceil(x0 / step) * step; t <= x1; t += step) {
    out.push(t);
  }
  return out;
}

// Ruler is the time axis over the bars, from the trace's start; the last tick's label runs
// into the margin beside the bars.
function Ruler(p: { x0: number; x1: number; head: string | undefined }): ReactElement {
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  const width = el?.clientWidth ?? 0;
  return createElement(
    "div",
    { className: "tr-ruler", "aria-hidden": true },
    p.head !== undefined && createElement("div", { className: "tr-nh" }, p.head),
    createElement(
      "div",
      { ref: setEl, className: "tr-axis" },
      ticks(p.x0, p.x1, width).map((t) =>
        createElement(
          "span",
          {
            key: t,
            ref: (s: HTMLElement | null) =>
              s?.style.setProperty("left", `${((t - p.x0) / (p.x1 - p.x0)) * 100}%`),
          },
          t === 0 ? "0" : duration(t),
        ),
      ),
    ),
  );
}

interface NamesProps {
  readonly name: (row: number) => RowName | undefined;
  readonly first: number;
  readonly last: number;
  readonly onPick: (row: number, e: MouseEvent) => void;
  readonly onToggle?: ((row: number) => void) | undefined;
}

// Names are the name column's rows in view, from first to last, each a click from selecting.
// arrow marks a call to another service.
const arrow = createElement(
  "svg",
  { className: "tr-call", width: 12, height: 12, viewBox: "0 0 24 24", "aria-hidden": true },
  createElement("path", { d: "M5 12h14M13 6l6 6-6 6" }),
);

// mark is what leads a row's name: a step's badge, a call's arrow, or its service's colour.
function mark(n: RowName): ReactElement {
  if (n.badge) {
    const { text, color } = n.badge;
    return createElement(
      "span",
      {
        className: "tr-badge",
        ref: (el: HTMLElement | null) => el?.style.setProperty("--c", color),
      },
      text,
    );
  }
  if (n.call) {
    return arrow;
  }
  return createElement("span", {
    className: "tr-sq",
    ref: (el: HTMLElement | null) => el?.style.setProperty("--c", n.color ?? "currentColor"),
  });
}

// NameRow is row r's name, a click from selecting it, after a chevron folding what it holds.
function NameRow(p: { r: number; n: RowName } & Omit<NamesProps, "first" | "last" | "name">) {
  const { r, n } = p;
  return createElement(
    "div",
    {
      className: `tr-n${n.error ? " err" : ""}${n.call ? " call" : ""}`,
      title: n.service ? `${n.name} · ${n.service}` : n.name,
      ref: (el: HTMLElement | null) => {
        el?.style.setProperty("top", `${r * rowHeight}px`);
        el?.style.setProperty("padding-left", `${10 + Math.min(n.indent, 12) * 14}px`);
      },
      onClick: (e: MouseEvent) => p.onPick(r, e),
    },
    n.open === undefined
      ? createElement("span", { className: "tr-chev" })
      : createElement(
          "button",
          {
            type: "button",
            className: "tr-chev",
            "aria-label": n.open ? `Fold ${n.name}` : `Open ${n.name}`,
            "aria-expanded": n.open,
            onClick: (e: MouseEvent) => {
              e.stopPropagation();
              p.onToggle?.(r);
            },
          },
          n.open ? "▾" : "▸",
        ),
    mark(n),
    createElement("span", { className: "tr-nm" }, n.name),
    n.service && createElement("span", { className: "tr-sv" }, n.service),
  );
}

function Names(p: NamesProps): ReactElement {
  const rows: ReactElement[] = [];
  for (let r = p.first; r < p.last; r++) {
    const n = p.name(r);
    if (n) {
      rows.push(createElement(NameRow, { key: r, r, n, onPick: p.onPick, onToggle: p.onToggle }));
    }
  }
  return createElement("div", { className: "tr-names" }, rows);
}

// Grid is the ruler's ticks drawn down through the rows, behind the bars.
function Grid(p: { x0: number; x1: number }): ReactElement {
  const [el, setEl] = useState<HTMLDivElement | null>(null);
  return createElement(
    "div",
    { ref: setEl, className: "tr-grid", "aria-hidden": true },
    ticks(p.x0, p.x1, el?.clientWidth ?? 0).map((t) =>
      createElement("span", {
        key: t,
        ref: (s: HTMLElement | null) =>
          s?.style.setProperty("left", `${((t - p.x0) / (p.x1 - p.x0)) * 100}%`),
      }),
    ),
  );
}

// Plot draws a model in a scroller as tall as its rows, under a time ruler and beside its
// rows' names, redrawing only what scrolls into the canvas, so selecting a row draws nothing.
export function Plot<M>(p: PlotProps<M>): ReactElement {
  const [scroller, setScroller] = useState<HTMLDivElement | null>(null);
  const [band, setBand] = useState<HTMLDivElement | null>(null);
  const [top, setTop] = useState(0);
  const [drawn, attach] = useRenderer(p.create);
  useDraw(p, drawn, scroller, top);
  const [x0, x1] = p.win ?? [0, p.end];
  useLayoutEffect(() => {
    band?.style.setProperty("top", `${p.row * rowHeight}px`);
    band?.scrollIntoView({ block: "nearest" });
  }, [band, p.row]);
  const { hit, handlers } = useGestures(p, drawn, top);
  const height = scroller?.clientHeight ?? 0;
  return createElement(
    "div",
    { className: p.name ? "tr-plot named" : "tr-plot" },
    createElement(Ruler, { x0, x1, head: p.name && (p.head ?? "Span") }),
    createElement(
      "div",
      {
        ref: setScroller,
        className: "tr-scroll",
        onScroll: () => setTop(scroller?.scrollTop ?? 0),
      },
      createElement(
        "div",
        {
          className: "tr-tall",
          ref: (el) => el?.style.setProperty("height", `${p.rows * rowHeight}px`),
        },
        createElement(Grid, { x0, x1 }),
        p.name &&
          createElement(Names, {
            name: p.name,
            first: Math.max(0, Math.floor(top / rowHeight) - 5),
            last: Math.min(p.rows, Math.ceil((top + height) / rowHeight) + 5),
            onPick: (row, e) => hit("click", e, row, -1),
            ...(p.onToggle && { onToggle: p.onToggle }),
          }),
        createElement("canvas", {
          ref: attach,
          role: "img",
          "aria-label": "Trace",
          ...handlers,
        }),
        p.row >= 0 && createElement("div", { ref: setBand, className: "tr-row" }),
      ),
    ),
  );
}

// useGestures is the canvas's handlers, each telling p what a gesture landed on, and hit,
// which tells it of a row picked beside the canvas. A press dragged into a box is no click.
function useGestures<M>(p: PlotProps<M>, drawn: Drawn<M> | undefined, top: number) {
  const [x0, x1] = p.win ?? [0, p.end];
  const press = useRef<{ t: number; row: number; x: number }>(undefined);
  const dragged = useRef(false);
  const px = () => (drawn?.c.clientWidth ?? 1) / (x1 - x0);
  const hit = (g: Gesture, e: MouseEvent, index: number, offsetX: number) =>
    p.onHit(g, {
      index,
      t: x0 + offsetX / px(),
      px: px(),
      x: e.clientX,
      y: e.clientY,
      alt: e.altKey,
    });
  const under = ({ nativeEvent: n }: MouseEvent) =>
    drawn?.r.hitTest(n.offsetX, n.offsetY)?.index ?? -1;
  const at = (e: MouseEvent) => ({
    t: x0 + e.nativeEvent.offsetX / px(),
    row: (e.nativeEvent.offsetY + top) / rowHeight,
    x: e.clientX,
  });
  const on = (g: Gesture) => (e: MouseEvent) => {
    e.preventDefault();
    if (!dragged.current) {
      hit(g, e, under(e), e.nativeEvent.offsetX);
    }
    dragged.current = false;
  };
  const drop = (e: MouseEvent) => {
    const [a, b] = [press.current, at(e)];
    dragged.current = a !== undefined && p.onBox !== undefined && Math.abs(b.x - a.x) > 4;
    if (a && dragged.current) {
      p.onBox?.(
        Math.min(a.t, b.t),
        Math.max(a.t, b.t),
        Math.min(a.row, b.row),
        Math.max(a.row, b.row),
      );
    }
  };
  const handlers = {
    onClick: on("click"),
    onDoubleClick: on("open"),
    onContextMenu: on("menu"),
    onMouseMove: (e: MouseEvent<HTMLCanvasElement>) => {
      e.currentTarget.title = p.tip?.(under(e)) ?? "";
    },
    onMouseDown: (e: MouseEvent) => {
      press.current = at(e);
    },
    onMouseUp: drop,
  };
  return { hit, handlers };
}

// useRenderer is the renderer create makes for the canvas the ref it returns attaches.
function useRenderer<M>(create: (c: HTMLCanvasElement) => Renderer<M>) {
  const [drawn, setDrawn] = useState<Drawn<M>>();
  const attach = useCallback(
    (c: HTMLCanvasElement | null) => {
      if (!c) {
        return;
      }
      const r = create(c);
      setDrawn({ c, r });
      return () => r.dispose();
    },
    [create],
  );
  return [drawn, attach] as const;
}

interface Drawn<M> {
  readonly c: HTMLCanvasElement;
  readonly r: Renderer<M>;
}

// useDraw draws the rows scrolled top pixels into view, again whenever the scroller's width
// changes, as when the panel beside it closes.
function useDraw<M>(
  p: PlotProps<M>,
  drawn: Drawn<M> | undefined,
  scroller: HTMLDivElement | null,
  top: number,
): void {
  const { model, rows } = p;
  const [x0, x1] = p.win ?? [0, p.end];
  const [width, setWidth] = useState(0);
  useEffect(() => {
    if (!scroller) {
      return;
    }
    const observer = new ResizeObserver(() => setWidth(scroller.clientWidth));
    observer.observe(scroller);
    return () => observer.disconnect();
  }, [scroller]);
  useLayoutEffect(() => {
    if (!drawn || !scroller) {
      return;
    }
    const { c, r } = drawn;
    const height = Math.min(scroller.clientHeight, rows * rowHeight);
    const [y0, y1] = [top / rowHeight, (top + height) / rowHeight];
    const view = { x0, x1, y0, y1, width: c.clientWidth, height, dpr: devicePixelRatio };
    c.style.setProperty("height", `${height}px`);
    r.resize(view.width, height, view.dpr);
    performance.mark("draw", { detail: r.render(model, view) });
  }, [drawn, scroller, model, rows, x0, x1, top, width]);
}
