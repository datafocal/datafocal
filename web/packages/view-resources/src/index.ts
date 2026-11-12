import type {
  DataFocalClient,
  Predicate,
  ResourceUsageResponse,
  ResourceUse,
} from "@datafocal/api";
import {
  Pivots,
  type TimeRange,
  useParam,
  useQueryRange,
  useTimeRange,
  type ViewProps,
} from "@datafocal/shell";
import { type Answer, Show } from "@datafocal/ui";
import {
  createElement,
  type ReactElement,
  type ReactNode,
  useEffect,
  useEffectEvent,
  useState,
} from "react";
import { type Brush, Charts, DetailCharts } from "./charts";
import { FilterBar, parseFilter, withTerm } from "./filter";
import { type LogsOf, logsOf } from "./logs";
import { bytes, cores, type Detail, details, detailNames, isDetail } from "./format";
import { button, columnsOf, parseSort, type Sort, Table } from "./table";
import "./resources.css";

// useUsage is resource_usage's answer over the range under where, grouped by groupBy and
// with detail's parts, loaded again when any of them changes. A new range keeps the last
// answer while the next loads; a new scope does not, as its rows are other things.
function useUsage(
  client: DataFocalClient,
  range: TimeRange,
  where: Predicate[],
  { groupBy, detail }: { groupBy: string; detail: Detail | undefined },
): { answer: Answer<ResourceUsageResponse>; pending: boolean } {
  // Disk has no parts to ask for.
  const parts = detail && detail !== "disk" ? { detail } : {};
  const req = { selector: { time: range, where }, group_by: groupBy, ...parts };
  const [key, scope] = [JSON.stringify(req), JSON.stringify([where, groupBy, parts])];
  const [answer, setAnswer] = useState<{
    key: string;
    scope: string;
    value: ResourceUsageResponse | Error;
  }>();
  const load = useEffectEvent(() => client.call("resource_usage", req).then((e) => e.data));
  useEffect(() => {
    let live = true;
    const set = (value: ResourceUsageResponse | Error) => live && setAnswer({ key, scope, value });
    load().then(set, (e: unknown) => set(e instanceof Error ? e : new Error(String(e))));
    return () => {
      live = false;
    };
  }, [key, scope]);
  const kept = answer?.scope === scope ? answer.value : undefined;
  return { answer: kept, pending: kept !== undefined && answer?.key !== key };
}

// Facts are the scope's totals over the window, a tile each: the mean, and under it the peak.
function Facts(p: { total: ResourceUse; hosts: number; disk: boolean }): ReactElement {
  const fact = (label: string, value: string, under: string) =>
    createElement(
      "div",
      { key: label },
      createElement("small", null, label),
      createElement("b", null, value),
      createElement("span", null, under),
    );
  const [s, n] = [p.total.summary, (v: number | null | undefined) => v ?? null];
  return createElement(
    "div",
    { className: "res-facts" },
    p.hosts > 0 && fact("Hosts", String(p.hosts), "sampled"),
    fact("CPU", cores(n(s.cpu_cores_mean)), `mean · peak ${cores(n(s.cpu_cores_peak))}`),
    fact("Memory", bytes(n(s.memory_bytes_mean)), `mean · peak ${bytes(n(s.memory_bytes_peak))}`),
    fact("Network in", bytes(n(s.rx_bytes_per_sec_mean), true), "mean"),
    fact("Network out", bytes(n(s.tx_bytes_per_sec_mean), true), "mean"),
    p.disk && fact("Disk used", bytes(n(s.disk_bytes_last)), "latest"),
  );
}

const bins = (r: ResourceUsageResponse): number[] =>
  Array.from({ length: r.total.cpu_cores?.length ?? 0 }, (_, i) => r.start + i * r.step);

// Scope is what the page narrows to: a service, or a pod of one, as a run's sandbox; the
// filter; and the resource whose page is open.
interface Scope {
  readonly service: string | undefined;
  readonly pod: string | undefined;
  readonly filter: string;
  readonly detail: Detail | undefined;
}

// Rows are what the page breaks the scope's use into: the groups, or where the samples hold
// none, as from a host running no containers, each host's own use, under its label.
interface Rows {
  readonly label: string;
  readonly uses: readonly ResourceUse[];
  // hosts is whether the rows are the hosts, which then need no table of their own.
  readonly hosts: boolean;
}

// rowsOf are the scope's rows; off Kubernetes a service's are its containers, naming no pod.
function rowsOf(usage: ResourceUsageResponse, scope: Scope): Rows {
  if (usage.groups.length === 0) {
    return { label: "Hosts", uses: usage.hosts, hosts: true };
  }
  const { label, by } = groupOf(scope);
  const pods = by !== "pod" || usage.columns.includes("pod");
  return { label: pods ? label : "Containers", uses: usage.groups, hosts: false };
}

// groupOf is what the scope's rows are: every service, a service's pods, or a pod's
// containers, its sandbox carrying the pod's network.
const groupOf = (s: Scope): { label: string; by: string } =>
  s.pod
    ? { label: "Containers", by: "container" }
    : s.service
      ? { label: "Pods", by: "pod" }
      : { label: "Services", by: "service" };

// Actions are what the page's rows and charts do: open a service, narrow to a host, sort the
// tables, narrow the range, and open a resource's page or, with null, the overview.
interface Actions {
  readonly pick: (service: string) => void;
  readonly host: (host: string) => void;
  readonly sort: Sort;
  readonly onSort: (s: string) => void;
  readonly brush: Brush;
  readonly open: (d: Detail | null) => void;
}

// Tables are the rows the scope sums, and each host's own use unless those are the rows.
function Tables(p: {
  usage: ResourceUsageResponse;
  rows: Rows;
  logs: LogsOf;
  scope: Scope;
  act: Actions;
  disk: boolean;
}): ReactNode {
  const { usage, rows, logs, scope, act } = p;
  const table = (key: string, label: string, uses: readonly ResourceUse[], more: object) =>
    createElement(
      "section",
      { key, className: "res-panel" },
      createElement(Table, {
        label,
        rows: uses,
        columns: columnsOf(scope.detail, uses, p.disk),
        sort: act.sort,
        onSort: act.onSort,
        ...more,
      }),
    );
  const hosts = { pick: act.host, logs: logs.host };
  if (rows.hosts) {
    return table("h", "Hosts", usage.hosts, hosts);
  }
  return [
    table("t", rows.label, rows.uses, {
      ...(!scope.service && !scope.pod && { pick: act.pick }),
      logs: logs.row,
    }),
    usage.hosts.length > 0 && table("h", "Hosts", usage.hosts, hosts),
  ];
}

const empty = createElement(
  "p",
  { className: "res-note", role: "status" },
  "No resource samples in this window. Run cAdvisor with --storage_driver=stdout beside the collector (deploy/k8s/base/cadvisor.yaml) and each host's, container's or systemd unit's CPU, memory, network and disk use shows here.",
);

// Graphs are the overview's charts, or the open resource's.
function Graphs(p: {
  usage: ResourceUsageResponse;
  rows: Rows;
  logs: LogsOf;
  scope: Scope;
  act: Actions;
  disk: boolean;
}) {
  const { usage, rows, logs, scope, act } = p;
  const t = bins(usage);
  const rowLogs = { row: rows.hosts ? logs.host : logs.row, rest: logs.scope };
  if (!scope.detail) {
    return createElement(Charts, {
      bins: t,
      rows: rows.uses,
      logs: rowLogs,
      disk: p.disk,
      brush: act.brush,
      open: act.open,
    });
  }
  return createElement(DetailCharts, {
    detail: scope.detail,
    bins: t,
    rows: rows.uses,
    total: usage.total,
    // Rows that are the hosts are charted once.
    hosts: rows.hosts ? [] : usage.hosts,
    label: rows.label.toLowerCase(),
    logs: rowLogs,
    scope: logs,
    brush: act.brush,
  });
}

// Body is the scope's facts, charts and tables.
function Body(p: { usage: ResourceUsageResponse; scope: Scope; act: Actions }): ReactNode {
  const { usage, scope, act } = p;
  const rows = rowsOf(usage, scope);
  if (rows.uses.length === 0) {
    return empty;
  }
  const logs = logsOf({
    ...(scope.service && { service: scope.service }),
    ...(scope.pod && { pod: scope.pod }),
    pods: usage.columns.includes("pod"),
    where: parseFilter(scope.filter).where,
  });
  // cAdvisor writes no disk use for a container under containerd, so a scope may have none.
  const disk = usage.total.summary.disk_bytes_last != null;
  return [
    createElement(Facts, { key: "f", total: usage.total, hosts: usage.hosts.length, disk }),
    usage.capped &&
      createElement("p", { key: "c", className: "res-note" }, "The 20 busiest hosts only."),
    createElement(Graphs, { key: "g", usage, rows, logs, scope, act, disk }),
    createElement(Tables, { key: "t", usage, rows, logs, scope, act, disk }),
  ];
}

// Crumbs is the way back to every service, and the links every view shares from the scope.
function Crumbs(p: { scope: Scope; back: () => void }): ReactElement {
  const { service, pod } = p.scope;
  const [time] = useTimeRange();
  return createElement(
    "nav",
    { className: "res-crumbs", "aria-label": "Drill path" },
    button("All services", p.back),
    createElement("span", { "aria-hidden": true }, "›"),
    createElement("b", null, pod ?? service ?? ""),
    createElement(Pivots, { from: "resources", subject: { service, pod, time } }),
  );
}

// Tabs switch between the overview and each resource's page.
function Tabs(p: { detail: Detail | undefined; open: (d: Detail | null) => void }): ReactElement {
  const tab = (d: Detail | null, label: string) =>
    createElement(
      "button",
      {
        key: label,
        type: "button",
        className: "tbtn",
        "aria-pressed": (p.detail ?? null) === d,
        onClick: () => p.open(d),
      },
      label,
    );
  return createElement(
    "div",
    { className: "seg", role: "group", "aria-label": "Resource" },
    tab(null, "Overview"),
    detailNames.map((d) => tab(d, details[d].title)),
  );
}

// useScope is the page's scope, from the URL, and what its rows and charts do.
function useScope(): {
  scope: Scope;
  act: Actions;
  setFilter: (q: string) => void;
  back: () => void;
} {
  const [, setRange] = useTimeRange();
  const [service, setService] = useParam("service");
  const [pod, setPod] = useParam("pod");
  const [q, setQ] = useParam("q");
  const [stat, setStat] = useParam("stat");
  const [sort, setSort] = useParam("sort");
  const filter = q ?? "";
  const iso = (t: number) => new Date(t).toISOString();
  return {
    scope: { service, pod, filter, detail: isDetail(stat) ? stat : undefined },
    act: {
      pick: setService,
      host: (h) => setQ(withTerm(filter, "host", h)),
      sort: parseSort(sort),
      onSort: setSort,
      brush: (from, to) => setRange({ from: iso(from), to: iso(to) }),
      open: setStat,
    },
    setFilter: (v) => setQ(v === "" ? null : v),
    back: () => {
      setService(null);
      setPod(null);
    },
  };
}

// ResourcesView charts each service's CPU, memory, network and disk use over the range, from
// the samples a sampler such as cAdvisor logs, each service's pods once one is picked, and
// each resource's parts on its own page; the filter narrows every sample it reads.
export function ResourcesView({ client }: ViewProps): ReactElement {
  const range = useQueryRange();
  const { scope, act, setFilter, back } = useScope();
  const { service, pod } = scope;
  const where: Predicate[] = [
    ...(service ? [{ field: "service", op: "eq" as const, value: service }] : []),
    ...(pod ? [{ field: "pod", op: "eq" as const, value: pod }] : []),
    ...parseFilter(scope.filter).where,
  ];
  const { answer: usage, pending } = useUsage(client, range, where, {
    groupBy: groupOf(scope).by,
    detail: scope.detail,
  });
  return createElement(
    "div",
    { className: pending ? "res stale" : "res", "aria-busy": pending },
    pending && createElement("progress", { className: "busy", "aria-label": "Loading" }),
    createElement(
      "header",
      { className: "res-lede" },
      createElement("h2", null, "Resources"),
      createElement(
        "p",
        null,
        "CPU, memory, network and disk use per service over the range, from the samples logged for each container or systemd unit, and each host's own. Pick a service for its pods, a host to narrow to it, or a resource for its parts. Click a line on a chart for its logs around then, or drag across one to narrow the range.",
      ),
    ),
    createElement(
      "div",
      { className: "res-bar" },
      createElement(FilterBar, { key: scope.filter, q: scope.filter, onRun: setFilter }),
      createElement(Tabs, { detail: scope.detail, open: act.open }),
    ),
    (service || pod) && createElement(Crumbs, { scope, back }),
    createElement(Show<ResourceUsageResponse>, {
      answer: usage,
      render: (u) => createElement(Body, { usage: u, scope, act }),
    }),
  );
}
