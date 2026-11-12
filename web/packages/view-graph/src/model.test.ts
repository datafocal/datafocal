import { expect, test } from "vitest";
import { diff, layout, overlay, search, withAll } from "./model";

test("search finds a name among 102,416 within 50 ms", () => {
  const names = Array.from({ length: 102_416 }, (_, i) => `service-${i}`);
  const start = performance.now();
  const found = search(names, "SERVICE-102415");
  expect(performance.now() - start).toBeLessThanOrEqual(50);
  expect(found).toEqual(["service-102415"]);
});

const edge = (from: string, to: string) => ({
  from,
  to,
  method: "topology",
  weight: 1,
  confidence: 1,
});
const graph = (...edges: ReturnType<typeof edge>[]) => ({
  nodes: [...new Set(edges.flatMap((e) => [e.from, e.to]))].map((id) => ({ id, kind: "workload" })),
  edges,
});

test("diff is exactly the edges added and removed between two windows", () => {
  const before = graph(edge("web", "db"), edge("web", "cache"));
  const after = graph(edge("web", "db"), edge("web", "queue"), edge("queue", "db"));
  expect(diff(after, before)).toEqual({
    added: [edge("web", "queue"), edge("queue", "db")],
    removed: [edge("web", "cache")],
  });
});

test("overlay numbers the lit hops' causes in order, dims the rest and ghosts removed edges", () => {
  const hop = (from: string, to: string) => ({
    from,
    to,
    pattern_from: "p0",
    pattern_to: `p-${to}`,
    rho: 0.9,
    lag_seconds: -30,
    admitted_by: "topology",
  });
  const hops = [hop("web", "db"), hop("db", "disk")];
  const changes = { added: [], removed: [edge("web", "cache")] };
  const g = withAll(graph(edge("web", "db"), edge("web", "queue")), hops, changes.removed);
  const shown = { graph: g, laid: layout(g, 2) };
  const { model, ids } = overlay(shown, hops, (h) => h.to !== "disk", changes).laid;
  expect(ids).toEqual(["cache", "db", "disk", "queue", "web"]);
  expect(model.labels).toEqual(["cache Δ", "1. db", "disk", "queue", "web Δ"]);
  expect([...model.nodes.health]).toEqual([3, 0, 3, 3, 0]);
  // web → db, web → queue, then the removed web → cache and the drawn disk → db.
  expect([...model.edges.flags]).toEqual([0, 1, 1, 1]);
});
