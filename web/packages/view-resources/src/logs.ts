import type { Predicate } from "@datafocal/api";
import { navigate } from "@datafocal/shell";

// LogsOf are the Logs queries of what the page charts: a row, a host's own use, and the
// scope as a whole, which the rest of the rows and the parts are.
export interface LogsOf {
  readonly row: (key: string) => string;
  readonly host: (host: string) => string;
  readonly scope: string;
}

// stageOps are the LogQL label filters of the filter's predicates.
const stageOps: Readonly<Record<string, string>> = {
  eq: "=",
  ne: "!=",
  regex: "=~",
  noregex: "!~",
};

// stages are the filter's predicates as LogQL label filters, then a line filter leaving out
// the sampler's own lines about each container, which alone hold no message: what the page
// charts comes from them, and the logs are what the containers themselves wrote. A field
// cannot tell them apart, as ingest carries a sample's container.id onto the lines its
// container logs.
function stages(where: readonly Predicate[]): string {
  const filters = where.flatMap((w) =>
    w.field && w.op && stageOps[w.op]
      ? [` | ${w.field}${stageOps[w.op]}${JSON.stringify(w.value ?? "")}`]
      : [],
  );
  return `${filters.join("")} |~ "."`;
}

const stream = (labels: Readonly<Record<string, string>>) =>
  `{${Object.entries(labels)
    .map(([k, v]) => `${k}=${JSON.stringify(v)}`)
    .join(", ")}}`;

// rowStream is the stream of a row's lines: a service; a pod's container; a service's pod; or,
// off Kubernetes, one of a service's containers, a cgroup the journal names each line's or a
// runtime's name, which is the container's service.
function rowStream(p: { service?: string; pod?: string; pods: boolean }, key: string): string {
  if (p.pod) {
    return stream({ pod: p.pod, container: key });
  }
  if (!p.service) {
    return stream({ service: key });
  }
  if (p.pods) {
    return stream({ pod: key });
  }
  return key.startsWith("/")
    ? `${stream({ service: p.service })} | _SYSTEMD_CGROUP=${JSON.stringify(key)}`
    : stream({ service: key });
}

// logsOf are the scope's Logs queries: a row's (rowStream); a host's, every line it logged;
// and the scope's, the service or pod open or every line; each under the page's filter.
export function logsOf(p: {
  service?: string;
  pod?: string;
  pods: boolean;
  where: readonly Predicate[];
}): LogsOf {
  const tail = stages(p.where);
  const open = p.pod ? { pod: p.pod } : p.service ? { service: p.service } : undefined;
  return {
    row: (key) => `${rowStream(p, key)}${tail}`,
    host: (host) => `{host=${JSON.stringify(host)}}${tail}`,
    scope: `${open ? stream(open) : '{service=~".+"}'}${tail}`,
  };
}

// openLogs opens query in Logs over the bin starting at, step ms wide, and one either side,
// so the lines leading into a spike and out of it show too.
export function openLogs(query: string, at: number, step: number): void {
  const iso = (t: number) => new Date(t).toISOString();
  navigate({ view: "logs", query, time: `${iso(at - step)},${iso(at + 2 * step)}` });
}
