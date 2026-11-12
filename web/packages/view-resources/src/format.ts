import type { ResourceUse } from "@datafocal/api";

const units = ["B", "KiB", "MiB", "GiB", "TiB"];

// bytes prints a size in binary units, and with perSecond as a rate.
export function bytes(n: number | null, perSecond = false): string {
  if (n === null) {
    return "—";
  }
  let unit = 0;
  for (let v = n; v >= 1024 && unit < units.length - 1; v /= 1024) {
    unit++;
  }
  const v = n / 1024 ** unit;
  return `${v.toFixed(v < 10 && unit > 0 ? 1 : 0)} ${units[unit]}${perSecond ? "/s" : ""}`;
}

// cores prints CPU in cores, to two significant digits under a tenth, so a container using
// a few millicores reads as that rather than as nothing.
export function cores(n: number | null): string {
  if (n === null) {
    return "—";
  }
  if (n > 0 && n < 0.1) {
    return `${n.toPrecision(2)} cores`;
  }
  return `${n.toFixed(n < 10 ? 2 : 1)} cores`;
}

// perSecond prints a count a second, such as the network's errors.
const perSecond = (n: number | null): string =>
  n === null ? "—" : `${n < 10 ? n.toPrecision(2) : Math.round(n)}/s`;

// Stat is a series of a use the charts draw.
export type Stat =
  | "cpu_cores"
  | "memory_bytes"
  | "rx_bytes_per_sec"
  | "tx_bytes_per_sec"
  | "disk_bytes";

// stats are how each stat reads, and the summary value the charts rank rows by.
export const stats: Readonly<
  Record<
    Stat,
    { title: string; format: (n: number | null) => string; rank: (u: ResourceUse) => number }
  >
> = {
  cpu_cores: { title: "CPU", format: cores, rank: (u) => u.summary.cpu_cores_mean ?? -1 },
  memory_bytes: {
    title: "Memory (working set)",
    format: (n) => bytes(n),
    rank: (u) => u.summary.memory_bytes_mean ?? -1,
  },
  rx_bytes_per_sec: {
    title: "Network in",
    format: (n) => bytes(n, true),
    rank: (u) => u.summary.rx_bytes_per_sec_mean ?? -1,
  },
  tx_bytes_per_sec: {
    title: "Network out",
    format: (n) => bytes(n, true),
    rank: (u) => u.summary.tx_bytes_per_sec_mean ?? -1,
  },
  disk_bytes: {
    title: "Disk used",
    format: (n) => bytes(n),
    rank: (u) => u.summary.disk_bytes_last ?? -1,
  },
};

// Detail is a resource the page can open on its own, with its parts.
export type Detail = "cpu" | "memory" | "network" | "disk";

// details are each resource's page: its name, the stats it charts by group, and how its
// parts print. Disk has no parts, so its page asks for none.
export const details: Readonly<
  Record<Detail, { title: string; stats: readonly Stat[]; part: (n: number | null) => string }>
> = {
  cpu: { title: "CPU", stats: ["cpu_cores"], part: cores },
  memory: { title: "Memory", stats: ["memory_bytes"], part: (n) => bytes(n) },
  network: { title: "Network", stats: ["rx_bytes_per_sec", "tx_bytes_per_sec"], part: perSecond },
  disk: { title: "Disk", stats: ["disk_bytes"], part: (n) => bytes(n) },
};

// detailNames are the resources' pages, in the order their tabs stand.
export const detailNames: readonly Detail[] = ["cpu", "memory", "network", "disk"];

export const isDetail = (s: string | undefined): s is Detail => detailNames.some((d) => d === s);

// partTitles are how the engine's part names read.
export const partTitles: Readonly<Record<string, string>> = {
  user: "user",
  system: "system",
  rss: "RSS",
  cache: "cache",
  swap: "swap",
  mapped_file: "mapped files",
  rx_errors: "errors in",
  tx_errors: "errors out",
};
