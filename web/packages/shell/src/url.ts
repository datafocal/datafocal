import type { DataFocalClient, LineRef, Predicate, TimeSpec } from "@datafocal/api";
import { type ComponentType, useMemo, useSyncExternalStore } from "react";

export interface ViewProps {
  readonly client: DataFocalClient;
}

// Route mounts a view at a path whose last segment names it: "/ui/logs" is the logs view.
export interface Route {
  readonly path: string;
  readonly view: ComponentType<ViewProps>;
  // keep are the view's own params a navigation within it carries, as the shell's are.
  readonly keep?: readonly string[];
}

export type View = "ai" | "logs" | "graph" | "trace" | "resources" | "slos" | "views";

export type Destination =
  | {
      view: "logs";
      query: string;
      mode?: "lines" | "patterns" | "insights";
      focus?: LineRef;
      from?: string;
      title?: string;
      // time is the range to open on, "from,to", where the page's own would not hold it.
      time?: string;
    }
  | {
      view: "trace";
      field?: string;
      traceId?: string;
      where?: string;
      span?: string;
      title?: string;
      // The list's filters: duration bounds in ms, start bounds as "from,to" in epoch ms, errors
      // only, a Logs search, and its order.
      min?: string;
      start?: string;
      max?: string;
      errors?: string;
      q?: string;
      sort?: string;
    }
  | { view: "graph"; focus?: string; mode?: "explore" | "investigate"; title?: string }
  // time is the range to open on, "from,to", as a trace's or a run's own span; pod narrows to
  // one pod, as a run's sandbox.
  | { view: "resources"; service?: string; pod?: string; time?: string }
  // slo opens an SLO's page, or with mode its builder; query prefills a new SLO's.
  | {
      view: "slos";
      slo?: string;
      mode?: "edit" | "new" | "import";
      state?: string;
      group?: string;
      query?: string;
    }
  // name opens a saved view, or its builder; vars are var-<name> segments; query is a panel to add.
  | {
      view: "views";
      name?: string;
      mode?: "edit" | "new" | "import";
      vars?: Readonly<Record<string, string>>;
      query?: string;
    }
  | {
      view: "ai";
      tab?:
        | "overview"
        | "context"
        | "tools"
        | "skills"
        | "memory"
        | "cache"
        | "models"
        | "quota"
        | "compare"
        | "outcomes"
        | "runs"
        | "run"
        | "map";
      // pick and filter are the drill-down's (useDrill): pick is what the tab drills into, a
      // run, a tool, a skill, a memory.
      pick?: string;
      filter?: readonly Predicate[];
      // split is what Compare sets apart, as JSON: {"field","value"} or {"at"}.
      split?: string;
    };

// TimeRange bounds are RFC3339 or relative to now ("now-1h"), as the tools take them.
export interface TimeRange {
  readonly from: string;
  readonly to: string;
}

// Baseline is what compare shows the range against: the range moved back by an offset
// ("1d", "7d", or "period" for its own length, the window just before), or a range of its own.
export type Baseline = { readonly offset: string } | TimeRange;

const units: Readonly<Record<string, number>> = { s: 1e3, m: 6e4, h: 36e5, d: 864e5 };

// resolve turns an RFC3339 or now-relative ("now-15m") time into milliseconds.
export function resolve(t: string, now: number): number {
  const rel = /^now(?:-(\d+)([smhd]))?$/.exec(t);
  return rel ? now - Number(rel[1] ?? 0) * (units[rel[2] ?? ""] ?? 0) : Date.parse(t);
}

// baselineRange is the range b compares time against, its bounds made absolute.
export function baselineRange(time: TimeSpec, b: Baseline): TimeRange {
  if (!("offset" in b)) {
    return b;
  }
  const now = Date.now();
  const width = resolve(time.to ?? "now", now) - resolve(time.from ?? "now", now);
  const back = b.offset === "period" ? width : now - resolve(`now-${b.offset}`, now);
  const at = (t = "now") => new Date(resolve(t, now) - back).toISOString();
  return { from: at(time.from), to: at(time.to) };
}

// Params are the page's state, kept in the path after the route as key=value segments, so
// a permalink reproduces the picture: a destination's fields plus the shell's keys below.
type Params = Readonly<Record<string, string>>;
const shellKeys = ["time", "vs", "snap"];

// routes are the mounted Shell's, which navigate resolves destinations against.
let routes: readonly Route[] = [];
const listeners = new Set<() => void>();

export function mount(r: readonly Route[]): void {
  routes = r;
}

export function routeFor(view: View): Route | undefined {
  return routes.find((r) => r.path.endsWith(`/${view}`));
}

const trim = (path: string) => path.replace(/\/$/, "");

function locate(pathname: string): { route: Route | undefined; params: Params } {
  const route = routes.find((r) => pathname === r.path || pathname.startsWith(`${trim(r.path)}/`));
  const params: Record<string, string> = {};
  for (const segment of route ? pathname.slice(route.path.length).split("/") : []) {
    const eq = segment.indexOf("=");
    if (eq > 0) {
      params[segment.slice(0, eq)] = decodeURIComponent(segment.slice(eq + 1));
    }
  }
  return { route: route ?? routes[0], params };
}

function pathOf(route: Route, params: Params): string {
  const segments = Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`);
  return [trim(route.path), ...segments].join("/") || "/";
}

function notify(): void {
  for (const l of listeners) {
    l();
  }
}

function subscribe(l: () => void): () => void {
  listeners.add(l);
  addEventListener("popstate", l);
  return () => {
    listeners.delete(l);
    removeEventListener("popstate", l);
  };
}

// navigate opens a destination, keeping the time range, unless it names its own, the
// baseline, the snapshot and, within one view, the params its route keeps. Arriving
// from another view leaves a Back link to it.
export function navigate(to: Destination): void {
  const { view, ...fields } = to;
  const params: Record<string, string> = {};
  for (const [k, v] of Object.entries(fields)) {
    params[k] = typeof v === "string" ? v : JSON.stringify(v);
  }
  if ("vars" in to && to.vars) {
    delete params["vars"];
    for (const [k, v] of Object.entries(to.vars)) {
      params[`var-${k}`] = v;
    }
  }
  open(view, params);
}

// show switches to a view with nothing selected.
export function show(view: View): void {
  open(view, {});
}

function open(view: View, params: Record<string, string>): void {
  const route = routeFor(view);
  if (!route) {
    throw new Error(`no route for the ${view} view`);
  }
  const here = locate(location.pathname);
  const kept = here.route === route ? (route.keep ?? []) : [];
  for (const k of [...shellKeys, ...kept]) {
    const v = here.params[k];
    if (v !== undefined && params[k] === undefined) {
      params[k] = v;
    }
  }
  history.pushState(here.route === route ? null : location.href, "", pathOf(route, params));
  notify();
}

function setParam(key: string, value: string | null): void {
  const { route, params } = locate(location.pathname);
  if (!route) {
    return;
  }
  const next = Object.entries(params).filter(([k]) => k !== key);
  if (value !== null) {
    next.push([key, value]);
  }
  history.replaceState(history.state, "", pathOf(route, Object.fromEntries(next)));
  notify();
}

// stepTo changes the page's own params as a new entry in the browser's history, keeping the
// rest, so Back undoes it; a null value drops its param.
export function stepTo(changes: Readonly<Record<string, string | null>>): void {
  const { route, params } = locate(location.pathname);
  if (!route) {
    return;
  }
  const next = Object.entries(params).filter(([k]) => !(k in changes));
  for (const [k, v] of Object.entries(changes)) {
    if (v !== null) {
      next.push([k, v]);
    }
  }
  history.pushState(history.state, "", pathOf(route, Object.fromEntries(next)));
  notify();
}

// useLocation re-renders on every navigation.
export function useLocation(): { route: Route | undefined; params: Params } {
  const path = useSyncExternalStore(subscribe, () => location.pathname);
  return useMemo(() => locate(path), [path]);
}

// useParams returns the page's state, from which a view reads its selection.
export function useParams(): Params {
  return useLocation().params;
}

// useBack returns the URL navigate came from, when it came from another view.
export function useBack(): string | null {
  useLocation();
  const back: unknown = history.state;
  return typeof back === "string" ? back : null;
}

// pinned is the now the page's relative times resolve against. It moves only when the range
// changes or the page loads, so every view asks for the same bounds and the loader each answer
// once, however the page is drilled into.
let pinned: { time: string | undefined; now: number } | undefined;

export function pinnedNow(): number {
  const time = locate(location.pathname).params["time"];
  if (!pinned || pinned.time !== time) {
    pinned = { time, now: Date.now() };
  }
  return pinned.now;
}

const bound = (t: string, now: number) =>
  t.startsWith("now") ? new Date(resolve(t, now)).toISOString() : t;

// useQueryRange is the page's range resolved against the pinned now as RFC3339 bounds, which
// a view sends in place of its relative times.
export function useQueryRange(): TimeRange {
  const [range] = useTimeRange();
  const now = pinnedNow();
  return useMemo(() => ({ from: bound(range.from, now), to: bound(range.to, now) }), [range, now]);
}

export function useTimeRange(): [TimeRange, (r: TimeRange) => void] {
  const time = useParams()["time"];
  const range = useMemo(() => {
    const [from = "now-1h", to = "now"] = time?.split(",") ?? [];
    return { from, to };
  }, [time]);
  return [range, (r) => setParam("time", `${r.from},${r.to}`)];
}

// useParam is a view's own page state at key, kept in the URL so a reload or link keeps it,
// and a setter that replaces it, or with null drops it.
export function useParam(key: string): [string | undefined, (value: string | null) => void] {
  return [useParams()[key], (value) => setParam(key, value)];
}

// useCompare is the baseline the range is compared against, kept in the URL as vs: the
// offset, or a range as "from,to".
export function useCompare(): [Baseline | null, (b: Baseline | null) => void] {
  const vs = useParams()["vs"];
  const baseline = useMemo((): Baseline | null => {
    const [from, to] = vs?.split(",") ?? [];
    return vs === undefined ? null : from && to ? { from, to } : { offset: vs };
  }, [vs]);
  return [baseline, (b) => setParam("vs", b && ("offset" in b ? b.offset : `${b.from},${b.to}`))];
}

// permalink is the page's URL with its range made absolute and pinned to snapshot, so it
// shows the same picture later.
export function permalink(snapshot: string): string {
  const { route, params } = locate(location.pathname);
  const now = Date.now();
  const at = (t: string) => new Date(resolve(t, now)).toISOString();
  const [from = "now-1h", to = "now"] = params["time"]?.split(",") ?? [];
  const pinned = { ...params, time: `${at(from)},${at(to)}`, ...(snapshot && { snap: snapshot }) };
  return route ? location.origin + pathOf(route, pinned) : location.href;
}

// useSnapshot is the snapshot token pinning the data the page shows; a view sets it from
// its response.
export function useSnapshot(): [string | null, (token: string | null) => void] {
  return [useParams()["snap"] ?? null, (token) => setParam("snap", token)];
}
