import type { GraphEdge, GraphNode, ServiceGraphResponse } from "@datafocal/api";
import { createElement, type ReactElement, type ReactNode } from "react";

export interface SideProps {
  readonly graph: ServiceGraphResponse;
  // lines is how many lines a service logged in the window.
  readonly lines: ReadonlyMap<string, number>;
  readonly selected: string | undefined;
  readonly onSelect: (id: string) => void;
  // onEdge opens what backs a link.
  readonly onEdge: (e: GraphEdge) => void;
  // actions are what the selection can open.
  readonly actions: ReactNode;
}

const count = (n: number) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(1)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}k` : String(n);
const percent = (r: number) => `${(r * 100).toFixed(r < 0.1 ? 1 : 0)}%`;

// membersOf is the services a node stands for: a group's members, else itself.
const membersOf = (n: GraphNode): readonly string[] => n.member_ids ?? [n.id];

const labelOf = (n: GraphNode) => n.group ?? n.id;

// nodeLines is how many lines a node's services logged.
export const nodeLines = (n: GraphNode, lines: ReadonlyMap<string, number>): number =>
  membersOf(n).reduce((sum, s) => sum + (lines.get(s) ?? 0), 0);

// Picked is the selected node: its services, busiest first, with their lines, why it is the
// colour it is, what it can open, and what it links to.
function Picked({
  node,
  graph,
  lines,
  actions,
  onEdge,
}: {
  node: GraphNode;
  graph: ServiceGraphResponse;
  lines: ReadonlyMap<string, number>;
  actions: ReactNode;
  onEdge: (e: GraphEdge) => void;
}) {
  const members = [...membersOf(node)].sort((a, b) => (lines.get(b) ?? 0) - (lines.get(a) ?? 0));
  return createElement(
    "section",
    { className: "g-pick" },
    createElement("h3", null, labelOf(node)),
    createElement(
      "p",
      { className: "g-sub" },
      `${count(nodeLines(node, lines))} lines`,
      node.error_ratio ? ` · ${percent(node.error_ratio)} errors` : "",
      members.length > 1 ? ` · ${members.length} services` : "",
    ),
    createElement(ErrorNote, { errors: node.error_ratio ?? 0 }),
    actions,
    members.length > 1 &&
      createElement(
        "ul",
        { className: "g-list" },
        members
          .slice(0, 12)
          .map((m) =>
            createElement(
              "li",
              { key: m },
              createElement("span", null, m),
              createElement("span", null, count(lines.get(m) ?? 0)),
            ),
          ),
      ),
    createElement(Own, { node, graph, onEdge }),
  );
}

// ErrorNote says why a node logging errors is drawn the colour it is, and how to read them.
function ErrorNote({ errors }: { errors: number }): ReactElement | null {
  if (errors <= 0.01) {
    return null;
  }
  const red = errors > 0.05;
  return createElement(
    "p",
    { className: red ? "g-note bad" : "g-note warn" },
    `${percent(errors)} of its lines are at level error, which is why it is drawn ${red ? "red" : "amber"}. Errors shows what they say.`,
  );
}

// Own is the links that join node, each naming the other end and how they are linked.
function Own({ node, graph, onEdge }: Pick<SideProps, "graph" | "onEdge"> & { node: GraphNode }) {
  const label = new Map(graph.nodes.map((n) => [n.id, labelOf(n)]));
  const own = graph.edges.filter(
    (e) => e.from !== e.to && (e.from === node.id || e.to === node.id),
  );
  return createElement(
    "section",
    null,
    createElement("h3", null, `Links of ${labelOf(node)}`),
    own.length === 0
      ? createElement(
          "p",
          { className: "g-sub" },
          "It shares no correlation value with another service.",
        )
      : createElement(
          "ul",
          { className: "g-list" },
          own.map((e) => {
            const out = e.from === node.id;
            const other = label.get(out ? e.to : e.from) ?? "";
            const errors =
              e.calls && (e.error_ratio ?? 0) > 0 ? ` · ${percent(e.error_ratio ?? 0)} errors` : "";
            return createElement(
              "li",
              { key: `${e.from} ${e.to} ${e.method}` },
              createElement(
                "button",
                { type: "button", title: "Why they are linked", onClick: () => onEdge(e) },
                `${out ? "→" : "←"} ${other}`,
              ),
              createElement(
                "span",
                { className: errors ? "bad" : undefined },
                `${e.calls ? `${count(e.calls)} calls` : e.method}${errors}`,
              ),
            );
          }),
        ),
  );
}

// Attention is the nodes logging errors, worst first, each a click from selecting it.
function Attention({
  graph,
  onSelect,
  selected,
}: Pick<SideProps, "graph" | "onSelect" | "selected">): ReactElement {
  const failing = graph.nodes
    .filter((n) => (n.error_ratio ?? 0) > 0.01)
    .sort((a, b) => (b.error_ratio ?? 0) - (a.error_ratio ?? 0));
  return createElement(
    "section",
    null,
    createElement("h3", null, "Needs attention"),
    createElement(
      "p",
      { className: "g-sub" },
      "Services logging errors on more than 1% of their lines.",
    ),
    failing.length === 0
      ? createElement(
          "p",
          { className: "g-sub" },
          "No service logs errors on more than 1% of its lines.",
        )
      : createElement(
          "ul",
          { className: "g-list" },
          failing.map((n) =>
            createElement(
              "li",
              { key: n.id, className: n.id === selected ? "on" : undefined },
              createElement(
                "button",
                { type: "button", onClick: () => onSelect(n.id) },
                labelOf(n),
              ),
              createElement(
                "span",
                { className: (n.error_ratio ?? 0) > 0.05 ? "bad" : "warn" },
                `${percent(n.error_ratio ?? 0)} errors`,
              ),
            ),
          ),
        ),
  );
}

// Links are the graph's edges, strongest first, and why each was drawn, each opening what
// backs it.
function Links({ graph, onEdge }: Pick<SideProps, "graph" | "onEdge">): ReactElement {
  const label = new Map(graph.nodes.map((n) => [n.id, labelOf(n)]));
  const edges = [...graph.edges]
    .filter((e) => e.from !== e.to)
    .sort((a, b) => b.weight - a.weight)
    .slice(0, 8);
  return createElement(
    "section",
    null,
    createElement("h3", null, "Strongest links"),
    edges.length === 0
      ? createElement(
          "p",
          { className: "g-sub" },
          "No two services share a correlation value in this window.",
        )
      : createElement(
          "ul",
          { className: "g-list" },
          edges.map((e) =>
            createElement(
              "li",
              { key: `${e.from} ${e.to}` },
              createElement(
                "button",
                { type: "button", onClick: () => onEdge(e) },
                `${label.get(e.from) ?? e.from} → ${label.get(e.to) ?? e.to}`,
              ),
              createElement("span", { title: `confidence ${e.confidence.toFixed(2)}` }, e.method),
            ),
          ),
        ),
  );
}

// Side is the graph told in words: how much it holds, the selection, what is failing and
// what links what.
export function Side(p: SideProps): ReactElement {
  const { graph, lines } = p;
  const services = graph.nodes.reduce((n, node) => n + membersOf(node).length, 0);
  const total = graph.nodes.reduce((n, node) => n + nodeLines(node, lines), 0);
  const linked = new Set(graph.edges.flatMap((e) => [e.from, e.to]));
  const alone = graph.nodes.filter((n) => !linked.has(n.id)).length;
  const node = graph.nodes.find((n) => n.id === p.selected);
  return createElement(
    "aside",
    { className: "g-side", "aria-label": "Graph summary" },
    createElement(
      "div",
      { className: "g-stats" },
      [
        [count(services), "services"],
        [String(graph.edges.length), "links"],
        [count(total), "lines"],
      ].map(([v, k]) =>
        createElement(
          "div",
          { key: k },
          createElement("b", null, v),
          createElement("span", null, k),
        ),
      ),
    ),
    node
      ? createElement(Picked, { node, graph, lines, actions: p.actions, onEdge: p.onEdge })
      : createElement(
          "p",
          { className: "g-sub" },
          "Click a service, or one listed here, to see its lines and traces.",
        ),
    node && createElement("h2", { className: "g-across" }, "Across the whole graph"),
    createElement(Attention, p),
    createElement(Links, p),
    alone > 0 &&
      createElement(
        "p",
        { className: "g-sub" },
        `${alone} of ${graph.nodes.length} stand alone: they share no correlation value with another service in this window.`,
      ),
  );
}
