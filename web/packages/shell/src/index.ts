import type { DataFocalClient } from "@datafocal/api";
import { type Evidence, EvidenceDrawer, useAnswer, useLast } from "@datafocal/ui";
import {
  createContext,
  createElement,
  type ReactElement,
  Suspense,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import "./shell.css";
import { FocusPanel, type FocusTarget } from "./focus";
import { Presets, RangeText } from "./range";
import {
  mount,
  navigate,
  permalink,
  pinnedNow,
  type Route,
  routeFor,
  show,
  useBack,
  useLocation,
  type View,
} from "./url";

export { Compare, describeBaseline } from "./compare";
export { type Drill, DrillChips, useDrill } from "./drill";
export { around, type Pivot, Pivots, pivotsOf, type Subject, traceable, traceOf } from "./pivot";
export type { FocusTarget } from "./focus";
export {
  type Baseline,
  baselineRange,
  type Destination,
  navigate,
  resolve,
  type Route,
  type TimeRange,
  useCompare,
  useParam,
  useParams,
  useQueryRange,
  useSnapshot,
  useTimeRange,
  type ViewProps,
} from "./url";

export interface ShellProps {
  readonly routes: readonly Route[];
  readonly client: DataFocalClient;
}

// views is the rail, in the order keys 1–7 select.
const views: readonly { id: View; label: string; icon: string }[] = [
  {
    id: "ai",
    label: "AI systems",
    icon: "M7 6h10a3 3 0 0 1 3 3v6a3 3 0 0 1-3 3H7a3 3 0 0 1-3-3V9a3 3 0 0 1 3-3M9 11v2M15 11v2M12 3v3M8 21h8",
  },
  { id: "logs", label: "Logs", icon: "M4 6h16M4 10h10M4 14h16M4 18h8" },
  {
    id: "graph",
    label: "Service graph",
    icon: "M3 6a2 2 0 1 0 4 0a2 2 0 1 0-4 0M3 18a2 2 0 1 0 4 0a2 2 0 1 0-4 0M17 12a2 2 0 1 0 4 0a2 2 0 1 0-4 0M7 7l4 4M7 17l4-4M11 12h6",
  },
  { id: "trace", label: "Trace", icon: "M3 5h12M6 10h10M9 15h6M9 20h11" },
  { id: "resources", label: "Resources", icon: "M4 20V10M10 20V4M16 20v-7M22 20H2" },
  { id: "slos", label: "SLOs", icon: "M12 3a9 9 0 1 0 9 9h-9zM15 3.5A9 9 0 0 1 20.5 9H15z" },
  { id: "views", label: "Views", icon: "M4 4h7v9H4zM13 4h7v5h-7zM13 11h7v9h-7zM4 15h7v5H4z" },
];

// refreshMs is the querier's default refresh interval, at which SLO status changes.
const refreshMs = 30_000;
const refreshTick = () => Math.floor(Date.now() / refreshMs) * refreshMs;

// useTick is a now of its own, advanced at each refresh and rounded down to it, so what is read
// again on it leaves the page's pinned now put.
export function useTick(): number {
  const [now, setNow] = useState(refreshTick);
  useEffect(() => {
    const timer = setInterval(() => setNow(refreshTick()), refreshMs);
    return () => clearInterval(timer);
  }, []);
  return now;
}

// FiringBadge counts the firing SLOs, read again at each refresh, and links to them.
function FiringBadge({ client }: { client: DataFocalClient }): ReactElement {
  const now = useTick();
  const firing = useLast(
    useAnswer(
      String(now),
      async () =>
        (await client.call("list_slos", { state: "firing", limit: 1 }, { now })).data.total,
    ),
  );
  return createElement(
    "a",
    {
      className: firing ? "badge firing" : "badge",
      href: "/ui/slos/state=firing",
      onClick: (e: { preventDefault: () => void }) => {
        e.preventDefault();
        navigate({ view: "slos", state: "firing" });
      },
    },
    `${firing ?? "…"} firing`,
  );
}

interface ShellContext {
  readonly showEvidence: (e: Evidence | null) => void;
  readonly focus: (t: FocusTarget | null) => void;
}

const Context = createContext<ShellContext>({
  showEvidence: () => {},
  focus: () => {},
});

// useEvidence returns the function that opens the evidence drawer, or closes it with null.
export function useEvidence(): (e: Evidence | null) => void {
  return useContext(Context).showEvidence;
}

// useFocus returns the function that opens Focus on a target, or closes it with null.
export function useFocus(): (t: FocusTarget | null) => void {
  return useContext(Context).focus;
}

// pinning is client making each call against the pinned now, and noting the snapshot of each
// answer in seen, which a copied link pins.
function pinning(client: DataFocalClient, seen: { snapshot: string }): DataFocalClient {
  return {
    ...client,
    async call(tool, req, opts) {
      const env = await client.call(tool, req, { now: pinnedNow(), ...opts });
      seen.snapshot = env.meta.snapshot;
      return env;
    },
  };
}

const typing = (t: EventTarget | null) =>
  t instanceof HTMLInputElement ||
  t instanceof HTMLTextAreaElement ||
  t instanceof HTMLSelectElement;

// useKeys binds Esc to closing the drawer and 1–7 to the views, while no field has focus.
function useKeys(closeDrawer: () => void): void {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (typing(e.target) || e.metaKey || e.ctrlKey || e.altKey) {
        return;
      }
      const view = views[Number(e.key) - 1];
      if (e.key === "Escape") {
        closeDrawer();
      } else if (view && routeFor(view.id)) {
        show(view.id);
      }
    };
    addEventListener("keydown", onKey);
    return () => removeEventListener("keydown", onKey);
  }, [closeDrawer]);
}

function Rail({ current }: { current: Route | undefined }): ReactElement {
  return createElement(
    "nav",
    { className: "rail", "aria-label": "Views" },
    views.map((v, i) => {
      const route = routeFor(v.id);
      return (
        route &&
        createElement(
          "button",
          {
            key: v.id,
            type: "button",
            "aria-label": v.label,
            "aria-current": route === current ? "page" : undefined,
            title: `${v.label} (${i + 1})`,
            onClick: () => show(v.id),
          },
          createElement(
            "svg",
            { viewBox: "0 0 24 24", "aria-hidden": true },
            createElement("path", { d: v.icon }),
          ),
        )
      );
    }),
  );
}

// TopBar is the way back to the view navigate came from, the range, a link to the page and,
// where SLOs are routed, the firing badge.
function TopBar({ seen, client }: { seen: { snapshot: string }; client: DataFocalClient }) {
  const back = useBack();
  const [copied, setCopied] = useState(false);
  return createElement(
    "header",
    { className: "top" },
    back &&
      createElement(
        "a",
        {
          href: back,
          onClick: (e: { preventDefault: () => void }) => {
            e.preventDefault();
            history.back();
          },
        },
        "← Back",
      ),
    createElement(Presets),
    createElement(RangeText),
    createElement(
      "button",
      {
        type: "button",
        className: "tbtn",
        title: "Copy a link that reopens this page on the same data",
        onClick: () =>
          void navigator.clipboard.writeText(permalink(seen.snapshot)).then(() => setCopied(true)),
        onBlur: () => setCopied(false),
      },
      copied ? "Link copied" : "Copy link",
    ),
    routeFor("slos") && createElement(FiringBadge, { client }),
  );
}

// usePanel is what is open beside or below the view, evidence or Focus but never both, and the
// context views open them through. Leaving the view they were opened on closes both.
function usePanel(route: Route | undefined) {
  const [evidence, setEvidence] = useState<Evidence | null>(null);
  const [focused, setFocused] = useState<FocusTarget | null>(null);
  const [on, setOn] = useState(route);
  if (on !== route) {
    setOn(route);
    setEvidence(null);
    setFocused(null);
  }
  const focus = useCallback((t: FocusTarget | null) => {
    setEvidence(null);
    setFocused(t);
  }, []);
  const context = useMemo(
    () => ({
      showEvidence: (e: Evidence | null) => {
        setFocused(null);
        setEvidence(e);
      },
      focus,
    }),
    [focus],
  );
  return { evidence, focused, focus, context };
}

// Shell lays out the rail, the top bar, the view routed at the page's path (or the first
// route's on any other path), and the evidence drawer or Focus.
export function Shell({ routes, client }: ShellProps): ReactElement {
  mount(routes);
  const { route } = useLocation();
  const { evidence, focused, focus, context } = usePanel(route);
  const [seen] = useState(() => ({ snapshot: "" }));
  const pinned = useMemo(() => pinning(client, seen), [client, seen]);
  const closeDrawer = useCallback(() => focus(null), [focus]);
  useKeys(closeDrawer);
  return createElement(
    Context.Provider,
    { value: context },
    createElement(
      "div",
      { className: "shell" },
      createElement(Rail, { current: route }),
      createElement(TopBar, { seen, client: pinned }),
      createElement(
        "main",
        null,
        route && createElement(Suspense, null, createElement(route.view, { client: pinned })),
      ),
      focused &&
        createElement(FocusPanel, {
          client: pinned,
          target: focused,
          onFocus: focus,
          onClose: closeDrawer,
        }),
      evidence &&
        createElement(EvidenceDrawer, {
          evidence,
          onClose: closeDrawer,
          onOpenLogs: (query: string) => {
            closeDrawer();
            navigate({ view: "logs", query, title: evidence.title });
          },
        }),
    ),
  );
}
