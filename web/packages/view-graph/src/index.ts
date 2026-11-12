import type {
  Anomaly,
  GraphEdge,
  GraphNode,
  DataFocalClient,
  LogLine,
  RCAHop,
  RCARejection,
  Series,
  ServiceGraphRequest,
  ServiceGraphResponse,
  TimeSpec,
} from "@datafocal/api";
import {
  baselineRange,
  Compare,
  navigate,
  Pivots,
  useCompare,
  useEvidence,
  useFocus,
  useParams,
  useQueryRange,
  type ViewProps,
} from "@datafocal/shell";
import { type Answer, Chip, duration, lineText, Show } from "@datafocal/ui";
import {
  createElement,
  Fragment,
  type ReactElement,
  useCallback,
  useEffect,
  useMemo,
  useState,
} from "react";
import { Investigation, lanesRequest, onset } from "./investigate";
import { GraphMap } from "./map";
import { type Changes, diff, layout, overlay, type Shown, search, withAll } from "./model";
import { nodeLines, Side } from "./side";
import "./graph.css";

// levels are what each grouping step groups services by: domains, groups, then nothing.
const levels = ["domain", "auto", ""];
const levelNames = ["Domains", "Groups", "Services"];
// maxNodes keeps every level within what render-graph draws in a frame.
const maxNodes = 400;
const anyService = '{service=~".+"}';

// Target is what the selection's actions act on: a service, the edge between the focus and a
// neighbour as its caller and callee, or a group as its members, which offers only Logs.
interface Target {
  readonly id: string;
  readonly services: readonly string[];
  readonly label: string;
  readonly group?: boolean;
}

// groupTarget is a group's members; a group of one is its service.
function groupTarget(id: string, node: GraphNode): Target {
  const members = node.member_ids ?? [];
  const [only] = members;
  return members.length === 1 && only !== undefined
    ? { id: only, services: [only], label: only }
    : { id, services: members, label: node.group ?? id, group: true };
}

function targetOf(g: ServiceGraphResponse, id: string, focus?: string): Target | undefined {
  const node = g.nodes.find((n) => n.id === id);
  if (node?.kind === "group") {
    return groupTarget(id, node);
  }
  if (node?.kind !== "workload") {
    return undefined;
  }
  const edge = g.edges.find(
    (e) => (e.from === focus && e.to === id) || (e.from === id && e.to === focus),
  );
  const services = id !== focus && edge ? [edge.from, edge.to] : [id];
  return { id, services, label: services.join(" → ") };
}

// escaped is s with the characters a regex reads specially escaped.
const escaped = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

// logsQuery is the LogQL selecting t's services' lines: one matcher, as LogQL does not join
// selectors.
const logsQuery = ({ services }: Pick<Target, "services">) =>
  services.length === 1
    ? `{service=${JSON.stringify(services[0])}}`
    : `{service=~${JSON.stringify(`^(${services.map(escaped).join("|")})$`)}}`;

// servicesWhere matches the lines of services.
const servicesWhere = (services: readonly string[]) => ({
  field: "service",
  op: "regex",
  value: `^(${services.map(escaped).join("|")})$`,
});

// methodWords say, for each way an edge is found, what it means that two services are linked.
const methodWords: Readonly<Record<string, string>> = {
  call: "One calls the other: in the traces both appear in, the callee's spans sit under the caller's.",
  "co-occurrence":
    "Their lines carry the same correlation values, such as a request or trace id, in this window.",
  correlation: "Their line rates rise and fall together, one leading the other by a lag.",
  topology: "One runs on the other, as the scheduler's lines place it.",
};

// fact is one labelled figure in the evidence's summary.
const fact = (k: string, v: string) =>
  createElement("div", { key: k }, createElement("dt", null, k), createElement("dd", null, v));

// edgeFacts are an edge's figures: a call's numbers or its confidence, a correlation's lag,
// and how many lines each end logged.
function edgeFacts(e: GraphEdge, lines: ReadonlyMap<string, number>): ReactElement[] {
  const percent = `${((e.error_ratio ?? 0) * 100).toFixed(1)}%`;
  const call = e.calls
    ? [
        fact("Calls", e.calls.toLocaleString("en")),
        fact("Callee errors", percent),
        fact("Latency", `p50 ${duration(e.p50_ms ?? 0)} · p95 ${duration(e.p95_ms ?? 0)}`),
      ]
    : [fact("Confidence", e.confidence.toFixed(2))];
  const lag =
    e.rho === undefined
      ? []
      : [fact("Correlation", `${e.rho.toFixed(2)} at a lag of ${e.lag_seconds ?? 0}s`)];
  const ends = [e.from, e.to].map((s) =>
    fact(`${s} lines`, (lines.get(s) ?? 0).toLocaleString("en")),
  );
  return [...call, ...lag, ...ends];
}

interface EdgeWhy {
  readonly e: GraphEdge;
  readonly lines: ReadonlyMap<string, number>;
  readonly failed: string | undefined;
  readonly onShared: (field: string, value: string) => void;
}

// EdgeWhy says why an edge is drawn, its figures, the ways on to its traces and errors, and
// the values both ends share, each a click from the lines holding it.
function EdgeWhy({ e, lines, failed, onShared }: EdgeWhy): ReactElement {
  const label = `${e.from} → ${e.to}`;
  return createElement(
    "div",
    { className: "g-why" },
    createElement("h3", null, `Why they are linked · ${e.method}`),
    createElement("p", null, methodWords[e.method] ?? e.method),
    createElement("dl", null, edgeFacts(e, lines)),
    failed && createElement("p", { className: "bad" }, `Reading its lines failed: ${failed}`),
    e.calls &&
      createElement(
        "div",
        { className: "g-act" },
        button("Traces", () => openTraces({ services: [e.from, e.to], label })),
        button(`${e.to} error lines`, () =>
          openLogs({ services: [e.to], label: `${e.to} errors` }, "error"),
        ),
      ),
    (e.shared?.length ?? 0) > 0 &&
      createElement(
        Fragment,
        null,
        createElement("h3", null, "Values both carry"),
        createElement("p", null, "Click one to look into the lines holding it."),
        createElement(
          "ul",
          { className: "g-list" },
          e.shared?.map((v) =>
            createElement(
              "li",
              { key: `${v.field}=${v.value}` },
              createElement(
                "button",
                { type: "button", onClick: () => onShared(v.field, v.value) },
                `${v.field} = ${v.value}`,
              ),
              createElement(
                "span",
                null,
                `${v.from_lines.toLocaleString("en")} / ${v.to_lines.toLocaleString("en")} lines`,
              ),
            ),
          ),
        ),
      ),
  );
}

// endOf is an edge end's name and the services it stands for: a group's members, else itself.
function endOf(nodes: readonly GraphNode[], id: string): { label: string; services: string[] } {
  const n = nodes.find((x) => x.id === id);
  return n?.kind === "group"
    ? { label: n.group ?? id, services: [...(n.member_ids ?? [])] }
    : { label: id, services: [id] };
}

// useEdgeEvidence opens the evidence drawer on an edge once its lines are read: why it is
// drawn, with the lines of both ends beside it.
function useEdgeEvidence(
  client: DataFocalClient,
  time: TimeSpec,
  lines: ReadonlyMap<string, number>,
  nodes: readonly GraphNode[],
): (e: GraphEdge) => void {
  const show = useEvidence();
  const focus = useFocus();
  return (edge) => {
    const [a, b] = [endOf(nodes, edge.from), endOf(nodes, edge.to)];
    const e = { ...edge, from: a.label, to: b.label };
    const services = [...new Set([...a.services, ...b.services])];
    const onShared = (field: string, value: string) =>
      focus({
        title: `${field}=${value}`,
        query: `${logsQuery({ services })} | ${field}=${JSON.stringify(value)}`,
        selector: { time },
        slice: [servicesWhere(services), { field, op: "eq", value }],
      });
    const open = (found: readonly LogLine[], failed?: string) =>
      show({
        title: `${e.from} → ${e.to}`,
        lines: found.map((l) => ({
          time: l.timestamp.slice(11, 19),
          service: l.service ?? "",
          message: lineText(l),
          error: l.level === "error",
        })),
        computed: createElement(EdgeWhy, { e, lines, failed, onShared }),
        query: logsQuery({ services }),
      });
    const req = { selector: { time, where: [servicesWhere(services)] }, limit: 20 };
    client.call("search_logs", req).then(
      (env) => open(env.data.lines ?? []),
      (err: unknown) => open([], err instanceof Error ? err.message : String(err)),
    );
  };
}

// useAsync is what run last resolved or failed with.
function useAsync<T>(run: () => Promise<T>): T | Error | undefined {
  const [value, setValue] = useState<T | Error>();
  useEffect(() => {
    let live = true;
    const settle = (v: T | Error) => live && setValue(v);
    void run().then(settle, (e: unknown) => settle(e instanceof Error ? e : new Error(String(e))));
    return () => {
      live = false;
    };
  }, [run]);
  return value;
}

const button = (label: string, onClick: () => void, pressed?: boolean) =>
  createElement(
    "button",
    { type: "button", key: label, className: "tbtn", "aria-pressed": pressed, onClick },
    label,
  );

function Search({ names }: { names: readonly string[] }): ReactElement {
  const [q, setQ] = useState("");
  const found = useMemo(() => search(names, q), [names, q]);
  return createElement(
    "div",
    { className: "g-search" },
    createElement("input", {
      type: "search",
      "aria-label": "Search services",
      placeholder: "Search services",
      value: q,
      onChange: (e: { target: HTMLInputElement }) => setQ(e.target.value),
    }),
    found.length > 0 &&
      createElement(
        "ul",
        { "aria-label": "Services found" },
        found.map((n) =>
          createElement(
            "li",
            { key: n },
            button(n, () => navigate({ view: "graph", focus: n })),
          ),
        ),
      ),
  );
}

interface StageProps {
  readonly time: TimeSpec;
  readonly shown: Shown | undefined;
  readonly lines: ReadonlyMap<string, number>;
  readonly selected: string | undefined;
  readonly onSelect: (id: string | undefined) => void;
  readonly onEdge: (e: GraphEdge) => void;
  readonly onKey: (e: { key: string }) => void;
}

// Stage draws the graph, a click on a node selecting it.
function Stage(p: StageProps): ReactElement {
  const { shown } = p;
  return createElement(
    "div",
    {
      className: "g-stage",
      role: "group",
      tabIndex: 0,
      "aria-label": "Service graph",
      onKeyDown: p.onKey,
    },
    shown?.graph.nodes.length === 0 &&
      createElement(
        "p",
        { role: "status" },
        `No services logged between ${p.time.from} and ${p.time.to ?? "now"}.`,
      ),
    shown &&
      shown.graph.nodes.length > 0 &&
      createElement(GraphMap, {
        shown,
        lines: p.lines,
        selected: p.selected,
        onSelect: p.onSelect,
        onEdge: p.onEdge,
      }),
  );
}

// openLogs lists t's lines, at level when given, or every service's without t.
const openLogs = (t: Pick<Target, "services" | "label"> | undefined, level?: string) =>
  navigate(
    t
      ? {
          view: "logs",
          query: `${logsQuery(t)}${level ? ` | level=${JSON.stringify(level)}` : ""}`,
          mode: "lines",
          title: t.label,
        }
      : { view: "logs", query: anyService },
  );

// openTraces lists the traces touching every service of t.
const openTraces = (t: Pick<Target, "services" | "label">) =>
  navigate({
    view: "trace",
    where: t.services.map((s) => `service=${JSON.stringify(s)}`).join(" "),
    title: `From service graph · ${t.label}`,
  });

// Links are where the selection's lines and use are: a service's are the links every view
// shares, and a group's Logs reads all of its services.
function Links({ target, time }: { target: Target; time: TimeSpec }): ReactElement {
  const [service] = target.services;
  return target.group || target.services.length !== 1 || !service
    ? button("Logs", () => openLogs(target))
    : createElement(Pivots, {
        from: "graph",
        subject: { service, time: { from: time.from ?? "now-1h", to: time.to ?? "now" } },
      });
}

interface ActionsProps {
  readonly target: Target;
  readonly focus: string | undefined;
  readonly time: TimeSpec;
  // errors is the share of the selection's lines at level error, which colours its node.
  readonly errors: number;
}

const errorLevel = { field: "level", op: "eq", value: "error" };

// Actions are the action bar over the selection: Summary opens Focus on its services' lines,
// and Errors, when it logs any, on just its error lines.
function Actions({ target, focus, time, errors }: ActionsProps): ReactElement {
  const service = !target.group && target.services.length === 1 && target.id !== focus;
  const openFocus = useFocus();
  const share = `${(errors * 100).toFixed(errors < 0.1 ? 1 : 0)}%`;
  const why =
    errors > 0.01
      ? `${share} of these lines are at level error, which is why the graph colours ${target.label}. Errors looks at just those.`
      : undefined;
  const summary = () =>
    openFocus({
      title: target.label,
      query: logsQuery(target),
      selector: { time },
      slice: [servicesWhere(target.services)],
      ...(why && { why }),
    });
  const errorFocus = () =>
    openFocus({
      title: `${target.label} errors`,
      query: `${logsQuery(target)} | level="error"`,
      selector: { time },
      slice: [servicesWhere(target.services), errorLevel],
      why: `The ${share} of ${target.label}'s lines at level error: what they say and what they share.`,
    });
  return createElement(
    "div",
    { className: "g-act", role: "toolbar", "aria-label": "Actions for the selection" },
    createElement(Links, { key: "links", target, time }),
    !target.group && button("Traces", () => openTraces(target)),
    button("Summary", summary),
    errors > 0 && button("Errors", errorFocus),
    service && button("Focus", () => navigate({ view: "graph", focus: target.id })),
    service &&
      button("Explain", () => navigate({ view: "graph", focus: target.id, mode: "investigate" })),
  );
}

interface ToolbarProps {
  readonly names: Answer<readonly string[]>;
  readonly target: Target | undefined;
  readonly focus: string | undefined;
  readonly hops: number;
  readonly onHops: (h: number) => void;
  readonly level: number;
  readonly onLevel: (l: number) => void;
}

const switchMode = (focus: string | undefined, mode: "explore" | "investigate") =>
  navigate({ view: "graph", ...(focus && { focus }), mode });

// Grouping is what the map groups services by, unless it shows one service's neighbourhood,
// which is how many hops out it reaches.
function Grouping(p: ToolbarProps): ReactElement {
  return createElement(
    "div",
    { className: "g-seg", role: "group", "aria-label": p.focus ? "Hops" : "Group by" },
    p.focus
      ? [1, 2, 3].map((h) => button(`${h} hop${h > 1 ? "s" : ""}`, () => p.onHops(h), p.hops === h))
      : levelNames.map((name, l) => button(name, () => p.onLevel(l), p.level === l)),
  );
}

function Toolbar(p: ToolbarProps): ReactElement {
  const { names, target, focus } = p;
  const { title, mode } = useParams();
  const investigating = mode === "investigate";
  return createElement(
    "div",
    { className: "g-tb" },
    createElement(
      "div",
      { className: "g-seg", role: "group", "aria-label": "Mode" },
      button("Explore", () => switchMode(focus, "explore"), !investigating),
      button("Investigate", () => switchMode(focus, "investigate"), investigating),
    ),
    createElement(Search, { names: names instanceof Error ? [] : (names ?? []) }),
    names instanceof Error &&
      createElement(Show<readonly string[]>, { answer: names, render: () => null }),
    createElement(Grouping, p),
    button("Logs", () => openLogs(target)),
    // Investigating correlates the services, which compare diffs against the baseline's.
    investigating && createElement(Compare, { key: "compare" }),
    title && createElement(Chip, { key: "title" }, title),
    title && button("Back", () => history.back()),
  );
}

const unchanged: Changes = { added: [], removed: [] };

// useGraph is the graph req asks for and, investigating with a baseline, its changes since.
function useGraph(client: DataFocalClient, req: ServiceGraphRequest) {
  const [compare] = useCompare();
  const time = req.selector.time;
  const base = useMemo(
    () =>
      req.correlation && compare && time
        ? { ...req, selector: { time: baselineRange(time, compare) } }
        : undefined,
    [req, compare, time],
  );
  return useAsync(
    useCallback(async () => {
      const { data } = await client.call("service_graph", req);
      const was = base && (await client.call("service_graph", base)).data;
      return { graph: data, changes: was ? diff(data, was) : unchanged };
    }, [client, req, base]),
  );
}

// useIncident is the window's top anomaly that is not routine, or the focus's when it has
// one; null when there is none.
function useIncident(
  client: DataFocalClient,
  time: TimeSpec,
  focus: string | undefined,
): Answer<Anomaly | null> {
  const found = useAsync(
    useCallback(
      async () => (await client.call("find_anomalies", { selector: { time } })).data.anomalies,
      [client, time],
    ),
  );
  if (found instanceof Error || !found) {
    return found;
  }
  const listed = found.filter((a) => !a.routine);
  return listed.find((a) => a.service === focus) ?? listed[0] ?? null;
}

interface Chain {
  readonly hops: readonly RCAHop[];
  readonly lanes: readonly Series[];
  readonly rejected: readonly RCARejection[];
}

const noChain: Chain = { hops: [], lanes: [], rejected: [] };

// useChain is rca_walk's chain from incident's pattern, each hop's lines over time and the
// candidates it rejected.
function useChain(
  client: DataFocalClient,
  time: TimeSpec,
  incident: Anomaly | undefined,
): Answer<Chain> {
  return useAsync(
    useCallback(async () => {
      if (!incident) {
        return noChain;
      }
      const req = { selector: { time }, symptom: { pattern_id: incident.pattern_id } };
      const { chain, rejected = [] } = (await client.call("rca_walk", req)).data;
      const hops = chain ?? [];
      const lanes =
        hops.length > 0
          ? (await client.call("aggregate", lanesRequest(hops, time))).data.series
          : [];
      return { hops, lanes, rejected };
    }, [client, time, incident]),
  );
}

// useInvestigation is the incident in the window and, while investigating, its chain and
// the replay's playhead, before which a hop has not begun.
function useInvestigation(
  client: DataFocalClient,
  time: TimeSpec,
  focus: string | undefined,
  investigating: boolean,
) {
  const incident = useIncident(client, time, focus);
  const anomaly = incident instanceof Error ? undefined : (incident ?? undefined);
  const chain = useChain(client, time, investigating ? anomaly : undefined);
  // The graph draws the chain's hops once it has them; a failed chain shows where it's drawn.
  const hops = chain instanceof Error ? [] : (chain?.hops ?? []);
  const [playhead, setPlayhead] = useState<number>();
  const lit = useCallback(
    (h: RCAHop) =>
      playhead === undefined || (anomaly !== undefined && onset(anomaly, h) <= playhead),
    [anomaly, playhead],
  );
  return { incident, anomaly, chain, hops, playhead, onPlayhead: setPlayhead, lit };
}

// Drawing is how to draw a graph: at which level, with which chain over it, and how many
// lines each service logged, which sizes its node.
interface Drawing {
  readonly kind: number;
  readonly inv: ReturnType<typeof useInvestigation>;
  readonly lines: ReadonlyMap<string, number>;
}

// useShown is the graph req asks for laid out at kind, with inv's chain drawn over it.
function useShown(
  client: DataFocalClient,
  req: ServiceGraphRequest,
  { kind, inv, lines }: Drawing,
): Shown | Error | undefined {
  const result = useGraph(client, req);
  const { hops, lit } = inv;
  const loaded = result instanceof Error ? undefined : result;
  const shown = useMemo(() => {
    const graph = loaded && withAll(loaded.graph, hops, loaded.changes.removed);
    if (!graph) {
      return undefined;
    }
    const byId = new Map(graph.nodes.map((n) => [n.id, n]));
    const of = (id: string) => {
      const n = byId.get(id);
      return n ? nodeLines(n, lines) : 0;
    };
    return { graph, laid: layout(graph, kind, req.focus, of) };
  }, [loaded, hops, kind, req.focus, lines]);
  const drawn = useMemo(
    () => shown && loaded && overlay(shown, hops, lit, loaded.changes),
    [shown, loaded, hops, lit],
  );
  return result instanceof Error ? result : drawn;
}

// useLines is how many lines each service in the window logged; its names are the only
// thing search holds.
function useLines(client: DataFocalClient, time: TimeSpec): Answer<ReadonlyMap<string, number>> {
  return useAsync(
    useCallback(async () => {
      const all = { selector: { time }, measure: "count", group_by: "service", limit: 1e6 };
      const { series } = (await client.call("aggregate", all)).data;
      return new Map(
        series.map((s) => [s.key, (s.values ?? []).reduce((sum: number, v) => sum + (v ?? 0), 0)]),
      );
    }, [client, time]),
  );
}

const noLines: ReadonlyMap<string, number> = new Map();

// useCounts is each service's lines in the window, none until they arrive, and its names.
function useCounts(client: DataFocalClient, time: TimeSpec) {
  const counted = useLines(client, time);
  const lines = counted instanceof Error || !counted ? noLines : counted;
  const names = useMemo(
    (): Answer<readonly string[]> =>
      counted instanceof Error || !counted ? counted : [...counted.keys()],
    [counted],
  );
  return { lines, names };
}

// clock is a time's hour, minute and second.
const clock = (at: string) => new Date(at).toISOString().slice(11, 19);

interface BannerProps {
  readonly incident: Answer<Anomaly | null>;
  readonly investigating: boolean;
  readonly onInvestigate: () => void;
}

// Banner offers to investigate the incident in the window, until the user does, and says
// when investigating finds none.
const Banner = ({ incident, investigating, onInvestigate }: BannerProps) =>
  incident !== undefined &&
  createElement(Show<Anomaly | null>, {
    answer: incident,
    render: (a) =>
      investigating
        ? a === null && createElement("p", { role: "status" }, "No incident in this window.")
        : a &&
          createElement(
            "p",
            { className: "g-banner", role: "status" },
            `Incident · ${a.service} ${a.kind} at ${clock(a.at)} `,
            button("Investigate", onInvestigate),
          ),
  });

// useSelection is the node clicked since the focus last changed, or until then the focus,
// what it can open, and how to select another.
function useSelection(shown: Shown | undefined, focus: string | undefined) {
  const [pick, setPick] = useState<{ focus?: string; id?: string }>({});
  const selected = pick.focus === focus ? pick.id : focus;
  const target = useMemo(
    () => (shown && selected ? targetOf(shown.graph, selected, focus) : undefined),
    [shown, selected, focus],
  );
  const select = (id?: string) => setPick({ ...(focus && { focus }), ...(id && { id }) });
  return { selected, target, select };
}

const noNodes: readonly GraphNode[] = [];
const nodesOf = (shown: Shown | undefined) => shown?.graph.nodes ?? noNodes;

// errorsOf is the share of the selected node's lines at level error.
const errorsOf = (shown: Shown | undefined, id: string | undefined) =>
  nodesOf(shown).find((n) => n.id === id)?.error_ratio ?? 0;

// stageKeys are the stage's keys: L opens the selection's lines, T its traces, R investigates.
const stageKeys = (investigate: () => void): Record<string, (t?: Target) => void> => ({
  l: (t) => t && openLogs(t),
  t: (t) => t && !t.group && openTraces(t),
  r: investigate,
});

// GraphView is the service graph. Explore mode is the map at a zoom level, or one service's
// callers and callees, with drill-downs from the selection to its lines and traces.
// Investigate mode, which only the user turns on, draws an incident's root-cause chain over
// it and diffs it against the baseline when comparing.
export function GraphView({ client }: ViewProps): ReactElement {
  const { focus, mode } = useParams();
  const investigating = mode === "investigate";
  const time = useQueryRange();
  const [level, setLevel] = useState(1);
  const [hops, setHops] = useState(1);
  const req = useMemo(
    () => ({
      selector: { time },
      max_nodes: maxNodes,
      ...(focus ? { focus, hops } : { group_by: levels[level] ?? "" }),
      ...(investigating && { correlation: true }),
    }),
    [time, focus, hops, level, investigating],
  );
  const inv = useInvestigation(client, time, focus, investigating);
  const { lines, names } = useCounts(client, time);
  const kind = focus ? levels.length - 1 : level;
  const result = useShown(client, req, { kind, inv, lines });
  const shown = result instanceof Error ? undefined : result;
  const { selected, target, select } = useSelection(shown, focus);
  const onEdge = useEdgeEvidence(client, time, lines, nodesOf(shown));
  const pick = { selected, onSelect: select, onEdge };
  const investigate = () => switchMode(focus, "investigate");
  const errors = errorsOf(shown, selected);
  const actions = target && createElement(Actions, { target, focus, time, errors });
  const keys = stageKeys(investigate);
  const bar = { names, target, focus, hops, onHops: setHops, level, onLevel: setLevel };
  return createElement(
    "div",
    { className: "graph" },
    createElement(Toolbar, bar),
    createElement(Show<Shown>, { answer: result, render: () => null }),
    createElement(Banner, { incident: inv.incident, investigating, onInvestigate: investigate }),
    createElement(
      "div",
      { className: "g-body" },
      createElement(Stage, {
        time,
        shown,
        lines,
        ...pick,
        onKey: (e: { key: string }) => keys[e.key.toLowerCase()]?.(target),
      }),
      shown && createElement(Side, { graph: shown.graph, lines, ...pick, actions }),
    ),
    investigating &&
      inv.anomaly &&
      createElement(Show<Chain>, {
        answer: inv.chain,
        render: (c) =>
          inv.anomaly && createElement(Investigation, { ...inv, ...c, incident: inv.anomaly }),
      }),
  );
}
