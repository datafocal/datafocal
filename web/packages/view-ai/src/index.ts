import type { DataFocalClient, Predicate } from "@datafocal/api";
import {
  type Destination,
  DrillChips,
  navigate,
  type TimeRange,
  useDrill,
  useParams,
  useQueryRange,
  type ViewProps,
} from "@datafocal/shell";
import { useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement, useMemo } from "react";
import "./ai.css";
import { resolveFields, usageFields } from "./fields";
import { CacheTab } from "./cache";
import { CompareTab } from "./compare";
import { ContextTab } from "./context";
import { MapTab } from "./map";
import { MemoryTab } from "./memory";
import { ModelsTab } from "./models";
import { OutcomesTab } from "./outcomes";
import { OverviewTab } from "./overview";
import { QuotaTab } from "./quota";
import { eq, Panel, type Scope, type Split, type Tab, type TabProps } from "./panel";
import { RunTab } from "./run";
import { RunsTab } from "./runs";
import { SkillsTab } from "./skills";
import { ToolsTab } from "./tools";

// tabs are one per lever, each a ledger of what can be changed, then Compare, which checks a
// change helped, and the runs every ledger's evidence opens into.
const overview = { id: "overview", label: "Overview", view: OverviewTab } as const;
const tabs: readonly { id: Tab; label: string; view: (p: TabProps) => ReactElement }[] = [
  overview,
  { id: "context", label: "Context", view: ContextTab },
  { id: "tools", label: "Tools", view: ToolsTab },
  { id: "skills", label: "Skills", view: SkillsTab },
  { id: "memory", label: "Memory", view: MemoryTab },
  { id: "cache", label: "Cache", view: CacheTab },
  { id: "models", label: "Models", view: ModelsTab },
  { id: "quota", label: "Quota", view: QuotaTab },
  { id: "compare", label: "Compare", view: CompareTab },
  { id: "runs", label: "Runs", view: RunsTab },
  { id: "run", label: "Run", view: RunTab },
  { id: "outcomes", label: "Outcomes", view: OutcomesTab },
  { id: "map", label: "Map", view: MapTab },
];

// useFields reads which columns the logs carry: every role from the window, then the
// patterns of the usage roles again over the lines carrying usage under the filter, whose
// fields spend also splits by.
function useFields(client: DataFocalClient, range: TimeRange, filter: readonly Predicate[]) {
  const ranked = useAnswer(JSON.stringify(range), () =>
    client.call("rank_fields", { selector: { time: range } }).then((e) => e.data),
  );
  const unscoped = useMemo(
    () => (ranked instanceof Error || !ranked ? ranked : resolveFields(ranked.fields)),
    [ranked],
  );
  const usage = unscoped instanceof Error || !unscoped ? [] : usageFields(unscoped);
  const any = usage.map((f): Predicate => ({ field: f.name, op: "exists" }));
  const selector = any.length > 0 ? { time: range, where: [...filter, { any }] } : null;
  const key = selector && JSON.stringify(selector);
  const scoped = useAnswer(key, () =>
    client.call("rank_fields", { selector: selector ?? {} }).then((e) => e.data),
  );
  return useMemo(() => {
    if (!key || !ranked || ranked instanceof Error) {
      return unscoped;
    }
    return scoped instanceof Error || !scoped
      ? scoped
      : resolveFields(ranked.fields, scoped.fields);
  }, [ranked, key, unscoped, scoped]);
}

const isFilter = (f: unknown): f is { field: string; value: string } =>
  f instanceof Object &&
  "field" in f &&
  typeof f.field === "string" &&
  "value" in f &&
  typeof f.value === "string";

// legacyFilter is the filter a link from before the shell's drill-down names as filters=, its
// equalities as {field, value}; none when it names them in no form this view wrote.
function legacyFilter(json: string | undefined): Predicate[] {
  try {
    const parsed: unknown = JSON.parse(json ?? "[]");
    return Array.isArray(parsed) ? parsed.filter(isFilter).map((f) => eq(f.field, f.value)) : [];
  } catch {
    return [];
  }
}

// TabBar is the tabs, each opening its page.
function TabBar(p: { tab: Tab; open: (tab: Tab) => void }): ReactElement {
  return createElement(
    "div",
    { role: "tablist", className: "ai-tabs" },
    tabs.map((t) =>
      createElement(
        "button",
        {
          key: t.id,
          type: "button",
          role: "tab",
          "aria-selected": t.id === p.tab,
          onClick: () => p.open(t.id),
        },
        t.label,
      ),
    ),
  );
}

// AIView shows the chosen tab narrowed by the drill-down's filter, each predicate pivoting added
// a chip that removes it, and drilled into its pick. The tab and the drill-down are the URL's, so
// Back undoes a pivot or an open run. A link from before the drill-down, naming its filters, run
// or item as params of their own, still opens, and its next step writes them as the drill-down's.
export function AIView({ client }: ViewProps): ReactElement {
  const range = useQueryRange();
  const params = useParams();
  const drill = useDrill();
  const tab = tabs.find((t) => t.id === params["tab"]) ?? overview;
  const filter = drill.filter.length > 0 ? drill.filter : legacyFilter(params["filters"]);
  const picked = drill.picked ?? params["run"] ?? params["item"];
  const split = params["split"];
  const fields = useFields(client, range, filter);
  // go carries the filter but no pick or split, which only the step naming them keeps.
  const go = ({ filter: next = filter, ...to }: Partial<Extract<Destination, { view: "ai" }>>) =>
    navigate({ view: "ai", tab: tab.id, ...(next.length > 0 && { filter: next }), ...to });
  const pivot = (p: Predicate) => go({ filter: [...filter, p] });
  const openRun = (id: string) => go({ tab: "run", pick: id });
  const open = (to: Tab, it?: string) => go({ tab: to, ...(it !== undefined && { pick: it }) });
  const compare = (s: Split) => go({ tab: "compare", split: JSON.stringify(s) });
  const chips = {
    ...drill,
    filter,
    toggle: (p: Predicate) => go({ filter: filter.filter((g) => g !== p) }),
  };
  return createElement(
    "div",
    { className: "ai" },
    createElement(TabBar, { tab: tab.id, open }),
    createElement(DrillChips, { drill: chips }),
    createElement(Panel<ReturnType<typeof resolveFields>>, {
      title: tab.label,
      answer: fields,
      render: (f) => {
        const scope: Scope = { client, range, filters: filter, fields: f };
        return createElement(tab.view, { scope, pivot, picked, openRun, open, split, compare });
      },
    }),
  );
}
