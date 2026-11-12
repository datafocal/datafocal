import type { ListSLOsResponse, SLOStatusResponse } from "@datafocal/api";
import {
  navigate,
  useParam,
  useParams,
  useQueryRange,
  useTimeRange,
  type ViewProps,
} from "@datafocal/shell";
import {
  burnCell,
  Card,
  Chart,
  health,
  ImportForm,
  Panel,
  Show,
  sliCell,
  useAnswer,
} from "@datafocal/ui";
import { createElement, type ReactElement, type ReactNode, useState } from "react";
import { Builder } from "./builder";
import { Drill } from "./drill";
import { at, button, download, exported, list, pct } from "./doc";
import "./slos.css";

type Status = ListSLOsResponse["slos"][number];
type Detail = SLOStatusResponse["slo"];

const states = ["ok", "firing", "no_data", "unbound", "partial"];
const when = (t: string | undefined) => (t ? ` since ${t.slice(0, 16).replace("T", " ")}` : "");
const rate = (v: number | null | undefined) => (v == null ? "—" : `${v.toFixed(1)}×`);

const cell = (className: string, ...children: ReactNode[]) =>
  createElement("div", { className }, ...children);

// statusCell is the health, when it began, and how much of the window is counted.
const statusCell = (s: Status, coverage = true) => {
  const h = health(s);
  return [
    cell(`slo-state slo-${h.tone}`, createElement("i"), h.label),
    cell(
      "slo-sub",
      coverage && s.state === "partial" ? `coverage ${pct(s.coverage, 0)}` : when(s.since).trim(),
    ),
  ];
};

// budgetCell is the budget left as a bar, or how many budgets the window has spent.
const budgetCell = (s: Status) => {
  const b = s.budget_remaining;
  const fill = b == null ? 0 : Math.max(0, Math.min(1, b));
  return [
    createElement(
      "meter",
      { className: `slo-bar slo-${health(s).tone}`, min: 0, max: 1, value: fill },
      pct(b, 0),
    ),
    cell("slo-sub", b == null ? "—" : `used ${rate(1 - b)}`),
  ];
};

// columns are the list's: heading, the value it sorts by and the cell it shows.
const columns: readonly [string, (s: Status) => number | string, (s: Status) => ReactNode][] = [
  ["Status", (s) => health(s).rank, statusCell],
  ["SLO", (s) => s.group ?? s.name, () => null],
  ["SLI", (s) => s.sli ?? -1, sliCell],
  ["Error budget", (s) => s.budget_remaining ?? -Infinity, budgetCell],
  ["Burn rate", (s) => -(s.burn_rate_1h ?? -1), burnCell],
];

// exportNames downloads the named SLOs as one file.
const exportNames = (client: ViewProps["client"], names: string[]) =>
  void client
    .call("export_definitions", { names })
    .then((e) => download(`${names.length === 1 ? names[0] : "slos"}.yaml`, e.data.content));

const link = (text: ReactNode, onClick: () => void, label?: string) =>
  createElement(
    "button",
    { type: "button", className: "slo-link", onClick, "aria-label": label },
    text,
  );

const open = (slo: string) => navigate({ view: "slos", slo });

// nameCell opens s's SLO, or group, page, with its service.
const nameCell = (s: Status) => [
  link(s.group ?? s.name, () =>
    navigate({ view: "slos", slo: s.name, ...(s.group && { group: s.group }) }),
  ),
  cell("slo-sub", s.group ? "" : (s.service ?? "")),
];

// byHealth orders firing first, then pending, then by budget remaining.
const byHealth = (a: Status, b: Status) =>
  health(a).rank - health(b).rank || (a.budget_remaining ?? 2) - (b.budget_remaining ?? 2);

// Table is statuses, an SLO's groups, under headings that sort them.
// Table is statuses under headings that sort them; an SLO's groups head their names with the
// field they are grouped by, and leave the coverage they share to the page.
function Table(p: { rows: readonly Status[]; label: string; groupedBy?: string | undefined }) {
  const [sort, setSort] = useParam("sort");
  const by = columns[Number(sort)]?.[1];
  const rows = [...p.rows].sort(
    by ? (a, b) => (by(a) < by(b) ? -1 : by(a) > by(b) ? 1 : 0) : byHealth,
  );
  const row = (s: Status) =>
    createElement(
      "tr",
      { key: s.name + (s.group ?? "") },
      columns.map(([label, , show], i) =>
        createElement(
          "td",
          { key: label },
          i === 1 ? nameCell(s) : i === 0 && p.groupedBy ? statusCell(s, false) : show(s),
        ),
      ),
    );
  const heading = ([label]: (typeof columns)[number], i: number) =>
    createElement(
      "th",
      { key: label, "aria-sort": sort === String(i) ? "ascending" : undefined },
      link(i === 1 && p.groupedBy ? p.groupedBy : label, () =>
        setSort(sort === String(i) ? null : String(i)),
      ),
    );
  return createElement(
    "table",
    { className: "slo-table", "aria-label": p.label },
    createElement("thead", null, createElement("tr", null, columns.map(heading))),
    createElement("tbody", null, rows.map(row)),
  );
}

// ListHeader is the list's search, its state filter, and Export, Import and New SLO.
function ListHeader(p: {
  client: ViewProps["client"];
  state: string | undefined;
  query: string;
  picked: string[];
  search: (q: string) => void;
}): ReactElement {
  return createElement(
    "header",
    null,
    createElement("h2", null, "SLOs"),
    createElement("input", {
      type: "search",
      "aria-label": "Search SLOs",
      placeholder: "Search by name or service",
      value: p.query,
      onChange: (e: { target: { value: string } }) => p.search(e.target.value),
    }),
    createElement(
      "select",
      {
        "aria-label": "State",
        value: p.state ?? "",
        onChange: (e: { target: { value: string } }) =>
          navigate({ view: "slos", ...(e.target.value && { state: e.target.value }) }),
      },
      ["", ...states].map((s) => createElement("option", { key: s, value: s }, s || "every state")),
    ),
    createElement(
      "div",
      { className: "slo-actions" },
      p.picked.length > 0 &&
        button(`Export ${p.picked.length}`, () => exportNames(p.client, p.picked)),
      button("Import", () => navigate({ view: "slos", mode: "import" })),
      button("New SLO", () => navigate({ view: "slos", mode: "new" }), true),
    ),
  );
}

// page is how many cards the list shows at first, and how many more each Show more adds.
const page = 60;

// List is the SLOs' status, firing first, or those in the state picked or matching the
// search, a page of cards at a time, to open or export.
function List({ client }: ViewProps): ReactElement {
  const state = useParams()["state"];
  const [picked, setPicked] = useState<string[]>([]);
  const [query, setQuery] = useState("");
  const [limit, setLimit] = useState(page);
  const req = { ...(state && { state }), ...(query && { query }), limit, trend: true };
  const list = useAnswer(
    JSON.stringify(req),
    async () => (await client.call("list_slos", req)).data,
  );
  const pick = (s: Status) =>
    createElement("input", {
      type: "checkbox",
      "aria-label": `Select ${s.name}`,
      checked: picked.includes(s.name),
      onChange: () =>
        setPicked(
          picked.includes(s.name) ? picked.filter((n) => n !== s.name) : [...picked, s.name],
        ),
    });
  return createElement(
    "div",
    { className: "slos" },
    createElement(ListHeader, {
      client,
      state,
      query,
      picked,
      search: (q: string) => (setQuery(q), setLimit(page)),
    }),
    createElement(Show<ListSLOsResponse>, {
      answer: list,
      render: (l) =>
        l.slos.length === 0
          ? createElement("p", { role: "status" }, query ? "No SLO matches." : "No SLOs.")
          : [
              createElement(
                "div",
                { key: "c", className: "slo-cards", role: "list", "aria-label": "SLOs" },
                // In the server's order, firing first then by budget, so pages follow on.
                l.slos.map((s) => createElement(Card, { key: s.name, s, pick: pick(s), open })),
              ),
              l.total > l.slos.length &&
                cell(
                  "slo-more slo-sub",
                  `${l.slos.length} of ${l.total} · `,
                  link("Show more", () => setLimit(limit + page)),
                ),
            ],
    }),
  );
}

// line charts one series of d's points against a constant line, the target or a threshold;
// dragging across it narrows the time range.
function line({
  d,
  label,
  ys,
  level,
  format,
  onBrush,
}: {
  d: Detail;
  label: string;
  ys: (number | null)[];
  level?: number;
  format: (v: number) => string;
  onBrush: (from: number, to: number) => void;
}) {
  const t = d.points.map((p) => Date.parse(p.at));
  return createElement(
    Panel,
    { key: label, title: label },
    createElement(Chart, {
      kind: "line",
      data: [t, ys, ...(level === undefined ? [] : [t.map(() => level)])],
      width: 360,
      height: 160,
      label,
      x: { time: true },
      y: { format },
      series: [label, "target"],
      onBrush,
    }),
  );
}

// targets are each inline policy's notification targets, by policy name.
function targets(doc: unknown, policy: string): string {
  const found = list(at(doc, ["spec", "alertPolicies"])).find(
    (p) => at(p, ["metadata", "name"]) === policy,
  );
  const names = list(at(found, ["spec", "notificationTargets"])).map(
    (t) => at(t, ["targetRef"]) ?? at(t, ["metadata", "name"]),
  );
  return names.length ? `${names.join(", ")} (not delivered)` : "—";
}

// Conditions are each alert condition's state, severity, burn and notification targets.
const ConditionTable = ({ d, doc }: { d: Detail; doc: unknown }) =>
  createElement(
    "table",
    { className: "slo-table", "aria-label": "Conditions" },
    createElement(
      "thead",
      null,
      createElement(
        "tr",
        null,
        ["Condition", "State", "Severity", "Burn · short", "Threshold", "Targets"].map((h) =>
          createElement("th", { key: h }, h),
        ),
      ),
    ),
    createElement(
      "tbody",
      null,
      (d.conditions ?? []).map((c) =>
        createElement(
          "tr",
          { key: c.policy + c.name },
          [
            c.name ?? c.policy,
            c.state + when(c.since),
            c.severity ?? "",
            `${rate(c.burn_rate)} · ${rate(c.short_burn_rate)}`,
            rate(c.threshold),
            targets(doc, c.policy),
          ].map((v, i) => createElement("td", { key: i }, v)),
        ),
      ),
    ),
  );

const iso = (ms: number) => new Date(ms).toISOString();

// Groups are the SLO's groups, each its own SLI and budget, headed by the field the definition
// groups by, with the coverage they share said once.
function Groups({ d, doc }: { d: Detail; doc: unknown }): ReactElement {
  const by = at(doc, ["metadata", "annotations", "datafocal/group-by"]);
  const field = typeof by === "string" ? by : "group";
  return createElement(
    Panel,
    {
      title: `Groups by ${field}`,
      big: d.coverage < 1 ? [pct(d.coverage, 0), "of the window counted"] : undefined,
    },
    createElement(Table, { rows: d.groups ?? [], label: "Groups", groupedBy: field }),
  );
}

// Body is an SLO's drill-down over the time range, beside its budget left, then its alert
// conditions with each one's burn rate, and its groups.
function Body({ client, d, doc }: { client: ViewProps["client"]; d: Detail; doc: unknown }) {
  const [, setRange] = useTimeRange();
  const onBrush = (from: number, to: number) => setRange({ from: iso(from), to: iso(to) });
  return [
    createElement(Drill, { key: "d", client, d, doc }),
    (d.conditions ?? []).length > 0 &&
      createElement(
        Panel,
        { key: "k", title: "Alert conditions" },
        createElement(ConditionTable, { d, doc }),
        cell(
          "g3",
          (d.conditions ?? []).map((c, i) =>
            line({
              d,
              label: `Burn rate · ${c.name ?? c.policy}`,
              ys: d.points.map((p) => p.burn_rates[i] ?? null),
              level: c.threshold,
              format: rate,
              onBrush,
            }),
          ),
        ),
      ),
    d.groups && createElement(Groups, { key: "t", d, doc }),
  ];
}

// Page is an SLO's, or with group one group's, status over the time range, and what is
// causing its bad events.
function Page({ client, name, group }: ViewProps & { name: string; group: string | undefined }) {
  const range = useQueryRange();
  const detail = useAnswer(
    `${name}/${group ?? ""}/${range.from},${range.to}`,
    async () =>
      (await client.call("slo_status", { name, ...(group && { group }), time_range: range })).data
        .slo,
  );
  const doc = useAnswer(name, () => exported(client, name));
  return createElement(
    "div",
    { className: "slos" },
    createElement(
      "header",
      null,
      createElement("h2", null, group ? `${name} · ${group}` : name),
      createElement(Show<Detail>, {
        answer: detail,
        render: (d) =>
          cell("slo-sub", [d.service, `objective ${pct(d.objective)}`].filter(Boolean).join(" · ")),
      }),
      createElement(
        "div",
        { className: "slo-actions" },
        button("Export", () => exportNames(client, [name])),
        button("Edit", () => navigate({ view: "slos", slo: name, mode: "edit" })),
      ),
    ),
    // The page reads its scopes from the definition, so it waits for it rather than ask
    // anything unscoped.
    createElement(Show<Detail>, {
      answer: detail,
      render: (d) =>
        createElement(Show<unknown>, {
          answer: doc,
          render: (loaded) => createElement(Body, { client, d, doc: loaded }),
        }),
    }),
  );
}

// SLOsView is the SLO list, an SLO's page, its builder, and Import.
export function SLOsView({ client }: ViewProps): ReactElement {
  const { slo, mode, group } = useParams();
  if (mode === "import") {
    return createElement(ImportForm, { client, onSaved: () => navigate({ view: "slos" }) });
  }
  if (mode === "new" || (slo && mode === "edit")) {
    return createElement(Builder, {
      key: slo ?? "",
      client,
      name: mode === "edit" ? slo : undefined,
    });
  }
  return slo ? createElement(Page, { client, name: slo, group }) : createElement(List, { client });
}
