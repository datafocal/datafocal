import { createElement, lazy } from "react";
import { createRoot } from "react-dom/client";
import {
  createHttpClient,
  fixtureClient,
  type DataFocalClient,
  type KeyedAnswer,
} from "@datafocal/api";
import { Shell } from "@datafocal/shell";

// delays are the milliseconds each tool's fixture answer takes, as ?delays=tool:ms,…, which the
// perf scenarios set.
const delays = (): Record<string, number> =>
  Object.fromEntries(
    (new URLSearchParams(location.search).get("delays") ?? "")
      .split(",")
      .filter(Boolean)
      .map((d) => [d.split(":")[0] ?? "", Number(d.split(":")[1])]),
  );

// The fixtures build (vite build --mode fixtures) answers from the Go goldens, so it runs
// as static files with no querier.
async function createClient(): Promise<DataFocalClient> {
  if (import.meta.env.MODE !== "fixtures") {
    return createHttpClient({ baseUrl: "" });
  }
  const goldens = import.meta.glob<unknown>("../../api/fixtures/*.golden.json", {
    import: "default",
  });
  const fixtures: KeyedAnswer[] = [];
  for (const [path, load] of Object.entries(goldens)) {
    fixtures.push({ tool: path.replace(/^.*\/(.*)\.golden\.json$/, "$1"), answer: await load() });
  }
  return fixtureClient(fixtures, { delays: delays() });
}

// Each view loads on first visit, so its code stays out of the app shell's bundle budget.
// The first is home: Logs, which with no query tails every service's lines.
const routes = [
  {
    path: "/ui/logs",
    view: lazy(async () => ({ default: (await import("@datafocal/view-logs")).LogsView })),
    keep: ["by"],
  },
  {
    path: "/ui/ai",
    view: lazy(async () => ({ default: (await import("@datafocal/view-ai")).AIView })),
  },
  {
    path: "/ui/graph",
    view: lazy(async () => ({ default: (await import("@datafocal/view-graph")).GraphView })),
  },
  {
    path: "/ui/trace",
    view: lazy(async () => ({ default: (await import("@datafocal/view-trace")).TraceView })),
  },
  {
    path: "/ui/resources",
    view: lazy(async () => ({
      default: (await import("@datafocal/view-resources")).ResourcesView,
    })),
  },
  {
    path: "/ui/slos",
    view: lazy(async () => ({ default: (await import("@datafocal/view-slos")).SLOsView })),
  },
  {
    path: "/ui/views",
    view: lazy(async () => ({ default: (await import("@datafocal/view-views")).ViewsView })),
  },
];

const root = document.getElementById("root");
if (!root) {
  throw new Error("index.html has no #root element");
}
createRoot(root).render(createElement(Shell, { routes, client: await createClient() }));
