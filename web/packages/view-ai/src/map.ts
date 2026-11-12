import type { ServiceGraphResponse } from "@datafocal/api";
import { useAnswer } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import { Panel, selectorOf, sliceQuery, type TabProps, Table } from "./panel";

// MapTab is the service graph grouped by the agent column: per agent, the models, tools and
// services it is made of, and those its edges reach outside it.
export function MapTab({ scope }: TabProps): ReactElement {
  const agent = scope.fields.agent;
  const selector = selectorOf(scope);
  const graph = useAnswer(JSON.stringify([selector, agent]), () =>
    scope.client
      .call("service_graph", { selector, group_by: agent?.name ?? "" })
      .then((e) => e.data),
  );
  return createElement(Panel<ServiceGraphResponse>, {
    title: "System map",
    answer: graph,
    render: (g) => {
      const groups = [...new Set(g.nodes.map((n) => n.group ?? ""))].sort();
      const rows = groups.map((group) => {
        const members = new Set(g.nodes.filter((n) => (n.group ?? "") === group).map((n) => n.id));
        const touched = new Set(
          g.edges.filter((e) => members.has(e.from) && !members.has(e.to)).map((e) => e.to),
        );
        return {
          key: group,
          cells: [[...members].join(", "), [...touched].join(", ")],
          query: sliceQuery(scope, agent, group),
        };
      });
      return createElement(Table, {
        label: "System map",
        columns: [agent?.name ?? "", "Made of", "Touches"],
        rows,
      });
    },
  });
}
