import type { GetViewResponse, DataFocalClient, ListViewsResponse, TimeSpec } from "@datafocal/api";
import {
  navigate,
  resolve,
  useParams,
  useQueryRange,
  useTick,
  useTimeRange,
  type ViewProps,
} from "@datafocal/shell";
import { button, download, ImportForm, Show, useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode, useEffect, useState } from "react";
import { Builder } from "./builder";
import { table, ViewPanel } from "./panel";
import "./views.css";

type View = GetViewResponse["view"];
type Variable = NonNullable<View["variables"]>[number];
type Change = { target: { value: string } };

// exportViews downloads the named views as one file.
const exportViews = (client: DataFocalClient, names: readonly string[]) =>
  void client
    .call("export_definitions", { names: names.map((n) => `View/${n}`) })
    .then((e) => download(`${names.length === 1 ? names[0] : "views"}.yaml`, e.data.content));

// List is the saved views matching the search, to open or export; given a query, opening one
// adds it as a panel in the builder, as New view does.
function List({ client }: ViewProps): ReactElement {
  const { query } = useParams();
  const [search, setSearch] = useState("");
  const [picked, setPicked] = useState<string[]>([]);
  const req = search ? { query: search } : {};
  const list = useAnswer(search, async () => (await client.call("list_views", req)).data);
  const open = (name: string) =>
    navigate({ view: "views", name, ...(query && { mode: "edit" as const, query }) });
  const pick = (name: string) =>
    createElement("input", {
      type: "checkbox",
      "aria-label": `Select ${name}`,
      checked: picked.includes(name),
      onChange: () =>
        setPicked(picked.includes(name) ? picked.filter((n) => n !== name) : [...picked, name]),
    });
  const rows = (l: ListViewsResponse) =>
    l.views.map((v) => [pick(v.name), button(v.name, () => open(v.name)), v.title, v.panels]);
  const create = () => navigate({ view: "views", mode: "new", ...(query && { query }) });
  return createElement(
    "div",
    { className: "views" },
    createElement(
      "header",
      null,
      createElement("h2", null, query ? `Add ${query} to a view` : "Views"),
      createElement("input", {
        type: "search",
        "aria-label": "Search views",
        placeholder: "Search by name or title",
        value: search,
        onChange: (e: Change) => setSearch(e.target.value),
      }),
      createElement(
        "div",
        null,
        picked.length > 0 && button(`Export ${picked.length}`, () => exportViews(client, picked)),
        button("Import", () => navigate({ view: "views", mode: "import" })),
        button("New view", create, true),
      ),
    ),
    createElement(Show<ListViewsResponse>, {
      answer: list,
      render: (l) => table("Views", rows(l), ["", "Name", "Title", "Panels"]),
    }),
  );
}

// useVars are the variables' values, from the URL, a default, the column's top value or All,
// undefined until each has one, and the bar choosing them.
function useVars(client: DataFocalClient, view: View, time: TimeSpec) {
  const params = useParams();
  const variables = view.variables ?? [];
  const top = async (group_by: string) => {
    const req = { selector: { time }, measure: "count", group_by, bins: "all", limit: 20 };
    return (await client.call("aggregate", req)).data.series.map((s) => s.key).filter(Boolean);
  };
  const answer = useAnswer(JSON.stringify([variables, time]), () =>
    Promise.all(variables.map((v) => top(v.column))),
  );
  const choices = Array.isArray(answer) ? answer : undefined;
  const valueOf = (v: Variable, i: number) =>
    params[`var-${v.name}`] ?? v.default ?? choices?.[i]?.[0] ?? (v.allowAll && choices && "*");
  const values: Record<string, string> = {};
  for (const [i, v] of variables.entries()) {
    const value = valueOf(v, i);
    if (value) {
      values[v.name] = value;
    }
  }
  const bar = variables.map((v, i) => {
    const onChange = (e: Change) =>
      navigate({ view: "views", name: view.name, vars: { ...values, [v.name]: e.target.value } });
    const options = [...new Set([v.allowAll ? "*" : "", values[v.name], ...(choices?.[i] ?? [])])]
      .filter((c) => c !== undefined && c !== "")
      .map((c) => createElement("option", { key: c, value: c }, c === "*" ? "All" : c));
    const select = createElement("select", { value: values[v.name] ?? "", onChange }, options);
    return createElement("label", { key: v.name }, `${v.name} `, select);
  });
  return { vars: variables.every((v) => v.name in values) ? values : undefined, bar };
}

// usePanelTime is the range panels read: pinned, or ending at now and resolved at each tick.
function usePanelTime(): { time: TimeSpec; now: number | undefined } {
  const [range] = useTimeRange();
  const pinned = useQueryRange();
  const tick = useTick();
  const at = (t: string) => new Date(resolve(t, tick)).toISOString();
  const ticking = { time: { from: at(range.from), to: at(range.to) }, now: tick };
  return range.to.startsWith("now") ? ticking : { time: pinned, now: undefined };
}

// Grid is the view's variables bar over its panels, each read on its own.
function Grid({ client, view }: { client: DataFocalClient; view: View }): ReactNode {
  const { time, now } = usePanelTime();
  const { vars, bar } = useVars(client, view, time);
  const props = { client, view: view.name, time, now, vars };
  const panels = view.panels.map((panel, index) =>
    createElement(ViewPanel, { key: index, index, panel, ...props }),
  );
  const variables = { key: "v", className: "vw-row", role: "group", "aria-label": "Variables" };
  return [
    createElement("div", variables, bar),
    createElement("div", { key: "g", className: "vw-grid" }, panels),
  ];
}

// Page is a saved view over the page's range, which opens on the view's own.
function Page({ client, name }: ViewProps & { name: string }): ReactElement {
  const { time } = useParams();
  const [, setRange] = useTimeRange();
  const got = useAnswer(name, async () => (await client.call("get_view", { name })).data);
  const opens = got instanceof Error || got === undefined || time ? undefined : got.view.time;
  useEffect(() => {
    if (opens) {
      setRange({ from: opens.from ?? "now-1h", to: opens.to ?? "now" });
    }
  }, [opens, setRange]);
  const actions = createElement(
    "div",
    null,
    button("Export", () => exportViews(client, [name])),
    button("Edit", () => navigate({ view: "views", name, mode: "edit" })),
  );
  const render = (g: GetViewResponse) => [
    createElement("header", { key: "h" }, createElement("h2", null, g.view.title), actions),
    time && createElement(Grid, { key: "g", client, view: g.view }),
  ];
  return createElement(
    "div",
    { className: "views" },
    createElement(Show<GetViewResponse>, { answer: got, render }),
  );
}

// ViewsView is the saved views' list, a view's page, its builder, and Import.
export function ViewsView({ client }: ViewProps): ReactElement {
  const { name, mode } = useParams();
  if (mode === "import") {
    return createElement(ImportForm, { client, onSaved: () => navigate({ view: "views" }) });
  }
  if (mode === "new" || (name && mode === "edit")) {
    const edited = mode === "edit" ? name : undefined;
    return createElement(Builder, { key: name ?? "", client, name: edited });
  }
  return name ? createElement(Page, { client, name }) : createElement(List, { client });
}
