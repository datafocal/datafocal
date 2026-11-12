import type { GraphEdge } from "@datafocal/api";
import { duration } from "@datafocal/ui";
import {
  createElement,
  Fragment,
  type MouseEvent,
  type ReactElement,
  useEffect,
  useRef,
  useState,
} from "react";
import { fits, health, labelLine, type Shown, stat } from "./model";
import { nodeLines } from "./side";

// pad is the room around the nodes, in layout units, so labels under the edge nodes fit.
const pad = 100;

export interface MapProps {
  readonly shown: Shown;
  // lines is how many lines each service logged in the window.
  readonly lines: ReadonlyMap<string, number>;
  readonly selected: string | undefined;
  readonly onSelect: (id: string | undefined) => void;
  // onEdge opens what backs a link.
  readonly onEdge: (e: GraphEdge) => void;
}

// neighbours is the selected node and every node an edge joins it to, by model index.
function neighbours(shown: Shown, selected: number): Set<number> {
  const { from, to } = shown.laid.model.edges;
  const near = new Set([selected]);
  from.forEach((a, e) => {
    const b = to[e] ?? -1;
    if (a === selected) {
      near.add(b);
    } else if (b === selected) {
      near.add(a);
    }
  });
  return near;
}

// Lit is the selection by model index, and every node an edge joins it to; null when nothing
// is selected.
type Lit = { readonly at: number; readonly near: Set<number> } | null;

// touches is whether link e joins the selection.
const touches = (shown: Shown, e: number, lit: Lit) =>
  lit !== null &&
  (shown.laid.model.edges.from[e] === lit.at || shown.laid.model.edges.to[e] === lit.at);

// edgeClass is link e's classes: its method's dash, its health, ghosted when removed, lit
// when it joins the selection and dimmed when it does not.
function edgeClass(shown: Shown, e: number, lit: Lit): string {
  const { flags, method } = shown.laid.model.edges;
  const ghost = ((flags[e] ?? 0) & 1) !== 0;
  const on = touches(shown, e, lit);
  const h = `h${health(shown.graph.edges[e]?.error_ratio)}`;
  return ["g-e", `m${method[e] ?? 0}`, h, ghost && "ghost", on && "lit", lit && !on && "far"]
    .filter(Boolean)
    .join(" ");
}

// callFacts is what call edge g carries: its calls and, when any, its errors, then its p95.
function callFacts(g: GraphEdge): string[] {
  const errors = g.error_ratio ?? 0;
  const share = errors > 0 ? ` · ${(errors * 100).toFixed(errors < 0.1 ? 1 : 0)}% errors` : "";
  const calls = `${(g.calls ?? 0).toLocaleString("en")} call${g.calls === 1 ? "" : "s"}${share}`;
  return (g.p95_ms ?? 0) > 0 ? [calls, `p95 ${duration(g.p95_ms ?? 0)}`] : [calls];
}

// edgeTitle is what hovering link e says: which services it joins and what it rests on.
function edgeTitle(p: MapProps, e: number): string {
  const { shown } = p;
  const { from, to } = shown.laid.model.edges;
  const name = (i: number | undefined) => shown.laid.model.labels[i ?? -1] ?? "";
  const g = shown.graph.edges[e];
  const why = !g
    ? ""
    : g.calls
      ? callFacts(g).join(" · ")
      : `${g.method}, confidence ${g.confidence.toFixed(2)}`;
  return `${name(from[e])} → ${name(to[e])}: ${why}. Click for why.`;
}

type Point = { readonly x: number; readonly y: number };
const origin: Point = { x: 0, y: 0 };

// rounded is the path along route with each bend rounded off, so a route reads as one line.
function rounded(route: readonly Point[], radius = 14): string {
  return route
    .map((q, i) => {
      const [prev, next] = [route[i - 1], route[i + 1]];
      if (!prev || !next) {
        return `${i ? "L" : "M"}${q.x} ${q.y}`;
      }
      const cut = (to: Point) => {
        const len = Math.hypot(to.x - q.x, to.y - q.y) || 1;
        const k = Math.min(radius, len / 2) / len;
        return `${q.x + (to.x - q.x) * k} ${q.y + (to.y - q.y) * k}`;
      };
      return `L${cut(prev)}Q${q.x} ${q.y} ${cut(next)}`;
    })
    .join("");
}

// edge is link e along its route, as wide as its weight, coloured by its errors and as
// opaque as its confidence, under a wider invisible stroke that takes its clicks. A call's
// label shows its calls, errors and p95 when it joins the selection, else on hover.
function edge(p: MapProps, e: number, lit: Lit): ReactElement {
  const { shown } = p;
  const d = rounded(shown.laid.routes[e] ?? []);
  const g = shown.graph.edges[e];
  const mark = shown.laid.marks[e] ?? origin;
  return createElement(
    "g",
    {
      key: e,
      className: "g-ed",
      onClick: (ev: MouseEvent) => {
        ev.stopPropagation();
        if (g) {
          p.onEdge(g);
        }
      },
    },
    createElement("title", null, edgeTitle(p, e)),
    createElement("path", { className: "g-hit", d }),
    createElement("path", {
      className: edgeClass(shown, e, lit),
      d,
      strokeWidth: 1.5 + 3 * (shown.laid.model.edges.weight[e] ?? 0),
      strokeOpacity: 0.35 + 0.65 * (shown.laid.model.edges.confidence[e] ?? 0),
    }),
    g?.calls && edgeLabel(callFacts(g), mark, touches(shown, e, lit)),
  );
}

// edgeLabel is a call's figures at mark, shown when its edge joins the selection and they fit
// the room the layout left, else on hover.
function edgeLabel(text: string[], mark: Point, lit: boolean): ReactElement {
  const shown = lit && fits(text);
  return createElement(
    "text",
    { className: shown ? "g-el" : "g-el short", x: mark.x, y: mark.y - labelLine / 2 },
    text.map((t, k) => createElement("tspan", { key: t, x: mark.x, dy: k * labelLine }, t)),
  );
}

// Edges are the links but those from a service to itself, which have no line to draw; the
// selection's are drawn last, over the rest.
function Edges(p: MapProps & { lit: Lit }): ReactElement {
  const { from, to } = p.shown.laid.model.edges;
  const drawn = [...from.keys()].filter((e) => from[e] !== to[e]);
  const order = [
    ...drawn.filter((e) => !touches(p.shown, e, p.lit)),
    ...drawn.filter((e) => touches(p.shown, e, p.lit)),
  ];
  return createElement(
    "g",
    { className: "g-edges" },
    order.map((e) => edge(p, e, p.lit)),
  );
}

// labelOf is node i's name, with how many services it stands for when more than one.
function labelOf({ laid }: Shown, i: number): string {
  const members = laid.model.nodes.members[i] ?? 1;
  return `${laid.model.labels[i] ?? laid.ids[i] ?? ""}${members > 1 ? ` (${members})` : ""}`;
}

// facts is what node i logged in the window: its lines and, when any, its errors' share.
function facts(p: MapProps, i: number): { text: string; bad: boolean; errors: number } {
  const id = p.shown.laid.ids[i];
  const n = p.shown.graph.nodes.find((x) => x.id === id);
  if (!n) {
    return { text: "", bad: false, errors: 0 };
  }
  const errors = n.error_ratio ?? 0;
  return { text: stat(nodeLines(n, p.lines), errors), bad: errors > 0.01, errors };
}

// errorRing is an arc around a disc of radius r at x, y as long as its errors' share of a
// full turn, at least a sliver, so a service's errors show on the map and not only in its
// colour.
function errorRing(x: number, y: number, r: number, errors: number): ReactElement {
  const turn = Math.max(errors * 100, 4);
  return createElement("circle", {
    key: "e",
    className: "g-ring",
    cx: x,
    cy: y,
    r: r + 4,
    pathLength: 100,
    strokeDasharray: `${turn} ${100 - turn}`,
    transform: `rotate(-90 ${x} ${y})`,
  });
}

// node is service i: a disc coloured by health with its label under it, ringed when
// selected and dimmed when far from the selection.
// disc is node i's centre and radius.
function disc({ nodes }: Shown["laid"]["model"], i: number): [number, number, number] {
  const [x = 0, y = 0, r = 0] = [nodes.x[i], nodes.y[i], nodes.r[i]];
  return [x, y, r];
}

// rings are a disc's halo when selected, the disc, and its errors' arc when it has any.
function rings([x, y, r]: [number, number, number], sel: boolean, errors: number) {
  return [
    sel && createElement("circle", { key: "h", className: "g-halo", cx: x, cy: y, r: r + 6 }),
    createElement("circle", { key: "d", className: "g-disc", cx: x, cy: y, r }),
    errors > 0 && errorRing(x, y, r, errors),
  ];
}

function node(p: MapProps, i: number, far: boolean): ReactElement {
  const { model, ids } = p.shown.laid;
  const id = ids[i] ?? "";
  const [x, y, r] = disc(model, i);
  const label = labelOf(p.shown, i);
  const stat = facts(p, i);
  const sel = id === p.selected;
  const classes = ["g-n", `h${model.nodes.health[i] ?? 0}`, sel && "sel", far && "far"];
  return createElement(
    "g",
    {
      key: id,
      className: classes.filter(Boolean).join(" "),
      "data-id": id,
      onClick: (e: MouseEvent) => {
        e.stopPropagation();
        p.onSelect(id);
      },
    },
    createElement("title", null, `${label}: ${stat.text}. Click for its lines and traces.`),
    rings([x, y, r], sel, stat.errors),
    createElement(
      "text",
      { x, y: y + r },
      createElement("tspan", { className: "nm" }, label),
      createElement("tspan", { className: stat.bad ? "st bad" : "st", x, dy: "1.35em" }, stat.text),
    ),
  );
}

// Nodes are the services, the selection's neighbours lit and the rest dimmed.
function Nodes(p: MapProps & { lit: Lit }): ReactElement {
  return createElement(
    "g",
    { className: "g-nodes" },
    p.shown.laid.ids.map((_, i) => node(p, i, p.lit !== null && !p.lit.near.has(i))),
  );
}

// Legend says what a disc's colour and the arc around it mean.
function Legend(): ReactElement {
  const key = (className: string, text: string) =>
    createElement(
      "li",
      { key: text },
      createElement("i", { className, "aria-hidden": true }),
      text,
    );
  return createElement(
    "ul",
    { className: "g-legend", "aria-label": "Legend" },
    key("h0", "under 1% error lines"),
    key("h1", "1–5%"),
    key("h2", "over 5%"),
    key("ring", "arc: share of lines at error"),
  );
}

// Zoomer pans and zooms the drawing: by a factor about its middle, or back to fitting it.
interface Zoomer {
  readonly by: (factor: number) => void;
  readonly fit: () => void;
}

const clamp = (v: number, lo: number, hi: number) => Math.min(Math.max(v, lo), hi);

// camera is the pan and zoom over layer, written straight to the page so moving it renders
// nothing, with --u on svg set to the layout units one screen pixel spans, so labels and
// strokes keep their size however far in it is.
function camera(svg: SVGSVGElement, layer: SVGGElement, width: number, height: number) {
  const z = { k: 1, x: 0, y: 0, fitted: 1 };
  const draw = () => {
    layer.setAttribute("transform", `translate(${z.x} ${z.y}) scale(${z.k})`);
    svg.style.setProperty("--u", String(1 / (z.fitted * z.k)));
  };
  const user = (cx: number, cy: number) =>
    new DOMPoint(cx, cy).matrixTransform(svg.getScreenCTM()?.inverse() ?? new DOMMatrix());
  return {
    measure: () => {
      z.fitted = Math.min(svg.clientWidth / width, svg.clientHeight / height) || 1;
      draw();
    },
    // zoomAt scales by factor about the screen point cx, cy, which stays where it is.
    zoomAt: (cx: number, cy: number, factor: number) => {
      const p = user(cx, cy);
      const k = clamp(z.k * factor, 0.25, 12);
      z.x = p.x - ((p.x - z.x) * k) / z.k;
      z.y = p.y - ((p.y - z.y) * k) / z.k;
      z.k = k;
      draw();
    },
    // grab returns how to move the drawing by screen pixels from where it is now.
    grab: () => {
      const [x0, y0] = [z.x, z.y];
      return (dx: number, dy: number) => {
        [z.x, z.y] = [x0 + dx / z.fitted, y0 + dy / z.fitted];
        draw();
      };
    },
    fit: () => {
      Object.assign(z, { k: 1, x: 0, y: 0 });
      draw();
    },
  };
}

type Camera = ReturnType<typeof camera>;

// dragging pans cam while the pointer that went down moves past a few pixels, and reports
// whether the last press panned, so the click that ends it can be dropped.
function dragging(svg: SVGSVGElement, cam: Camera) {
  let moved = false;
  const onDown = (e: PointerEvent) => {
    if (e.button !== 0) {
      return;
    }
    const [x0, y0, pan] = [e.clientX, e.clientY, cam.grab()];
    moved = false;
    const move = (m: PointerEvent) => {
      const [dx, dy] = [m.clientX - x0, m.clientY - y0];
      moved ||= Math.hypot(dx, dy) > 4;
      if (moved) {
        svg.classList.add("panning");
        pan(dx, dy);
      }
    };
    const up = () => {
      svg.classList.remove("panning");
      removeEventListener("pointermove", move);
      removeEventListener("pointerup", up);
    };
    addEventListener("pointermove", move);
    addEventListener("pointerup", up);
  };
  return { onDown, panned: () => moved };
}

// panZoom makes the wheel zoom layer about the pointer and a drag pan it; a click that ends a
// drag selects nothing, and a double-click fits the graph back. It returns how to zoom from
// controls, and how to stop.
function panZoom(svg: SVGSVGElement, layer: SVGGElement, width: number, height: number) {
  const cam = camera(svg, layer, width, height);
  const drag = dragging(svg, cam);
  const onWheel = (e: WheelEvent) => {
    e.preventDefault();
    cam.zoomAt(e.clientX, e.clientY, Math.exp(-clamp(e.deltaY, -100, 100) * 0.003));
  };
  const onClick = (e: Event) => drag.panned() && e.stopPropagation();
  const observer = new ResizeObserver(cam.measure);
  observer.observe(svg);
  svg.addEventListener("wheel", onWheel, { passive: false });
  svg.addEventListener("pointerdown", drag.onDown);
  svg.addEventListener("click", onClick, true);
  svg.addEventListener("dblclick", cam.fit);
  const middle = () => {
    const r = svg.getBoundingClientRect();
    return [r.left + r.width / 2, r.top + r.height / 2] as const;
  };
  const zoomer: Zoomer = { by: (f) => cam.zoomAt(...middle(), f), fit: cam.fit };
  const stop = () => {
    observer.disconnect();
    svg.removeEventListener("wheel", onWheel);
    svg.removeEventListener("pointerdown", drag.onDown);
    svg.removeEventListener("click", onClick, true);
    svg.removeEventListener("dblclick", cam.fit);
  };
  return { zoomer, stop };
}

// usePanZoom pans and zooms layer within svg while both are drawn, refitting when the
// drawing's extent changes.
function usePanZoom(
  svg: SVGSVGElement | null,
  layer: SVGGElement | null,
  width: number,
  height: number,
) {
  const zoomer = useRef<Zoomer>(undefined);
  useEffect(() => {
    if (!svg || !layer) {
      return;
    }
    const { zoomer: z, stop } = panZoom(svg, layer, width, height);
    zoomer.current = z;
    return stop;
  }, [svg, layer, width, height]);
  return zoomer;
}

// ZoomControls zoom in and out about the middle, and fit the graph back in its box.
function ZoomControls({ zoomer }: { zoomer: { current: Zoomer | undefined } }): ReactElement {
  const control = (label: string, text: string, run: (z: Zoomer) => void) =>
    createElement(
      "button",
      {
        key: label,
        type: "button",
        "aria-label": label,
        title: label,
        onClick: () => zoomer.current && run(zoomer.current),
      },
      text,
    );
  return createElement(
    "div",
    { className: "g-zoom", role: "group", "aria-label": "Zoom" },
    control("Zoom in", "+", (z) => z.by(1.4)),
    control("Zoom out", "−", (z) => z.by(1 / 1.4)),
    control("Fit the graph (double-click)", "⤢", (z) => z.fit()),
  );
}

// GraphMap draws the laid-out graph as SVG, fitted to its box, that the wheel zooms and a drag
// pans: a click on a node selects it, on the background clears the selection. A selection
// lights its neighbours and dims the rest.
export function GraphMap(p: MapProps): ReactElement {
  const { x, y } = p.shown.laid.model.nodes;
  const [x0, x1] = [Math.min(0, ...x) - pad, Math.max(0, ...x) + pad];
  const [y0, y1] = [Math.min(0, ...y) - pad, Math.max(0, ...y) + pad];
  const [svg, setSvg] = useState<SVGSVGElement | null>(null);
  const [layer, setLayer] = useState<SVGGElement | null>(null);
  const zoomer = usePanZoom(svg, layer, x1 - x0, y1 - y0);
  const at = p.shown.laid.ids.indexOf(p.selected ?? "");
  const lit: Lit = at < 0 ? null : { at, near: neighbours(p.shown, at) };
  return createElement(
    Fragment,
    null,
    createElement(
      "svg",
      {
        className: "g-map",
        viewBox: `${x0} ${y0} ${x1 - x0} ${y1 - y0}`,
        preserveAspectRatio: "xMidYMid meet",
        ref: setSvg,
        role: "img",
        "aria-label": `Service graph of ${p.shown.laid.ids.length} services`,
        onClick: () => p.onSelect(undefined),
      },
      createElement(
        "g",
        { ref: setLayer },
        createElement(Edges, { ...p, lit }),
        createElement(Nodes, { ...p, lit }),
      ),
    ),
    createElement(ZoomControls, { zoomer }),
    createElement(Legend),
  );
}
