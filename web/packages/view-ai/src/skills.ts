import type { AggregateRequest, AggregateResponse, Predicate } from "@datafocal/api";
import { Absent, Meter } from "@datafocal/ui";
import { createElement, type ReactElement } from "react";
import type { Fields } from "./fields";
import {
  button,
  grouped,
  num,
  Panel,
  pct,
  selectorOf,
  sliceQuery,
  spec,
  sum,
  type TabProps,
  Table,
  useAggregate,
  useRunCount,
  valueAt,
} from "./panel";
import { Crumbs, Facts, Lede } from "./parts";

// skillRequest counts each skill's loads and the runs loading it.
const skillRequest = (f: Fields): Partial<AggregateRequest> | null =>
  f.skill
    ? {
        group_by: f.skill.name,
        measures: [{ measure: "count" }, spec("distinct", f.run)].flatMap((m) => m ?? []),
        bins: "all",
      }
    : null;

function Ledger(p: TabProps & { r: AggregateResponse; runs: number }): ReactElement {
  const skills = grouped(p.r, "")[0].series.map((s) => ({
    key: s.key,
    loads: valueAt(s, 0, 0) ?? 0,
    runs: valueAt(s, 1, 0) ?? 0,
  }));
  const top = Math.max(1, ...skills.map((s) => s.loads));
  return createElement(
    "div",
    null,
    createElement(Facts, {
      facts: [
        ["Skills loaded", num(skills.length)],
        ["Loads", num(sum(skills.map((s) => s.loads)))],
        ["Runs in the window", num(p.runs)],
      ],
    }),
    createElement(Table, {
      label: "Skills",
      columns: ["Skill", "Loads", "Runs loading it", "Share of runs"],
      rows: [...skills]
        .sort((a, b) => b.loads - a.loads)
        .map((s) => ({
          key: s.key,
          cells: [
            createElement(Meter, { text: num(s.loads), share: s.loads / top }),
            num(s.runs),
            pct(p.runs ? s.runs / p.runs : Number.NaN),
          ],
          query: sliceQuery(p.scope, p.scope.fields.skill, s.key),
          onPick: () => p.open("skills", s.key),
        })),
    }),
  );
}

// SkillPage is the runs that load a skill, and the comparison of them against the rest.
function SkillPage(p: TabProps & { skill: string }): ReactElement {
  const f = p.scope.fields;
  const where: Predicate[] = [
    ...(selectorOf(p.scope).where ?? []),
    { field: f.skill?.name ?? "", op: "eq", value: p.skill },
  ];
  const runs = useAggregate(
    p.scope,
    f.run ? { group_by: f.run.name, measure: "count", bins: "all", limit: 10_000 } : null,
    { ...selectorOf(p.scope), where },
  );
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Crumbs, { steps: [["Skills", () => p.open("skills")], [p.skill]] }),
    button("Compare runs that load it ›", () =>
      p.compare({ field: f.skill?.name ?? "", value: p.skill }),
    ),
    createElement(Panel<AggregateResponse>, {
      title: "Runs that load it",
      answer: runs,
      render: (r) =>
        createElement(Table, {
          label: "Runs loading the skill",
          columns: ["Run", "Loads"],
          rows: grouped(r, "")[0].series.map((s) => ({
            key: s.key,
            cells: [num(valueAt(s, 0, 0) ?? 0)],
            onPick: () => p.openRun(s.key),
          })),
        }),
    }),
  );
}

export function SkillsTab(p: TabProps): ReactElement {
  const f = p.scope.fields;
  const answer = useAggregate(p.scope, skillRequest(f));
  const runs = useRunCount(p.scope);
  if (p.picked !== undefined) {
    return createElement(SkillPage, { ...p, skill: p.picked });
  }
  return createElement(
    "div",
    { className: "ai-tab" },
    createElement(Lede, {
      title: "Skills",
      text: "Skill loads and the runs that load each skill, from the lines that name a skill.",
    }),
    createElement(Panel<AggregateResponse>, {
      title: "Skill ledger",
      answer: f.skill
        ? answer
        : new Absent(
            "no skill loads. Skills show when the logs carry a skill-loading tool call, a read of a SKILL.md, or a skill name attribute",
          ),
      render: (r) => createElement(Ledger, { ...p, r, runs }),
    }),
  );
}
