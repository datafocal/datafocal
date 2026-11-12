import type {
  AggregateRequest,
  AggregateResponse,
  Envelope,
  FieldStat,
  FieldsRequest,
  FieldsResponse,
  KeyedAnswer,
} from "@datafocal/api";
import { expect, test } from "vitest";
import engine from "../fixtures/engine.json";
import { resolveFields } from "./fields";
import { inMs } from "./stats";
import { breakdown, grouped, total, usageMeasure } from "./panel";

// aggregates are the engine's answers to the aggregates the view sent on the factory corpus.
interface Aggregate extends KeyedAnswer {
  readonly req: AggregateRequest;
  readonly answer: Envelope<AggregateResponse>;
}
const isAggregate = (a: KeyedAnswer): a is Aggregate => a.tool === "aggregate";
const answers: readonly KeyedAnswer[] = engine;
const aggregates = answers.filter(isAggregate).map((a) => ({ req: a.req, res: a.answer.data }));
// unfiltered is the widest aggregate grouped by by in the default state, before any pivot.
const unfiltered = (by: string) =>
  aggregates
    .filter((a) => a.req.group_by === by && a.req.selector.where?.length === 0)
    .sort((a, b) => (b.req.measures?.length ?? 0) - (a.req.measures?.length ?? 0))[0];

test("the KPIs equal the sums of each breakdown's rows", () => {
  // The KPIs sum the usage the view reads by model over every group, the lines naming no
  // model included.
  const kpis = unfiltered("model");
  for (const by of ["version", "tool_name"]) {
    const rows = unfiltered(by);
    kpis?.req.measures?.forEach((m, i) => {
      // A distinct count is no sum: a run using two tools is in both their rows.
      if (!m.measure.startsWith("sum:")) {
        return;
      }
      const j = rows?.req.measures?.findIndex((n) => JSON.stringify(n) === JSON.stringify(m));
      if (rows && j !== undefined && j >= 0) {
        const sum = breakdown(rows.res, j).reduce((n, s) => n + s.value, 0);
        expect(sum, `${by} ${m.measure}`).toBeCloseTo(total(kpis.res, i));
      }
    });
  }
});

test("each breakdown's shares sum to 100%", () => {
  for (const { req, res } of aggregates.filter((a) => a.res?.series && total(a.res) > 0)) {
    const shares = breakdown(res).reduce((n, s) => n + s.share, 0);
    expect(shares, JSON.stringify(req)).toBeCloseTo(1);
  }
});

const stat = (name: string, present = 1, cardinality = 5): FieldStat => ({
  name,
  type: name.endsWith("id") || name === "task" ? "id" : "enum",
  cardinality,
  present,
  lines: present,
  signal: 0,
  high_signal: false,
});

test("roles come from the lines carrying usage, the run the unit of work on most of them", () => {
  const window = [stat("duration"), stat("request_id")];
  const scoped = ["model", "message.id", "param_1", "tpl_pattern", "pattern_id", "tier"].map((n) =>
    stat(n),
  );
  const units = [stat("task", 0.4), stat("session", 0.6)];
  const f = resolveFields(window, [...scoped, ...units, stat("rare", 0.01), stat("wide", 1, 400)]);
  expect([f.run?.name, f.call?.name, f.duration?.name]).toEqual([
    "session",
    "message.id",
    undefined,
  ]);
  expect(f.dims.map((d) => d.name)).toEqual(["model", "tier"]);
  expect(usageMeasure(f, "sum", f.model)).toEqual({
    measure: "sum:model",
    distinct_by: "message.id",
  });
});

// window and scoped are the engine's rank_fields of the window and of the lines carrying usage.
interface Ranking extends KeyedAnswer {
  readonly req: FieldsRequest;
  readonly answer: Envelope<FieldsResponse>;
}
const rankings = answers.filter((a): a is Ranking => a.tool === "rank_fields");
const [window, scoped] = [0, 1].map(
  (n) => rankings.find((a) => (a.req.selector.where?.length ?? 0) === n)?.answer.data.fields ?? [],
);

test("on the factory, dimensions are enums on a line in twenty and roles the agents' fields", () => {
  const f = resolveFields(window ?? [], scoped);
  const dims = f.dims.map((d) => scoped?.find((s) => s.name === d.name));
  expect(dims.filter((d) => d?.type !== "enum" || d.lines < 0.05)).toEqual([]);
  const notDims = /content\.(text|thinking)$|^cwd$|^pattern_id$|^param_\d+$|_pattern$/;
  expect(dims.filter((d) => notDims.test(d?.name ?? ""))).toEqual([]);
  expect(f.dims.length).toBeGreaterThan(0);
  // The columns only the factory's orchestrator, sandbox and system containers write.
  const theirs =
    /^(pod|container|namespace|level|took|caller|expected-duration|endpoint|http\..+|task|session|repo|harness|reason|source)$/;
  const roles = Object.values(f).flatMap((v) =>
    v && typeof v === "object" && "name" in v ? [v.name] : [],
  );
  expect(roles.filter((name) => theirs.test(name))).toEqual([]);
});

test("the lines lacking the group_by field are no group, only a count under the table", () => {
  const res = { series: ["", "Read"].map((key) => ({ key, start: 0, step: 1, values: [3] })) };
  const [kept, note] = grouped(res, "tool", 0);
  expect(kept.series.map((s) => s.key)).toEqual(["Read"]);
  expect(note).toBeTruthy();
  expect(grouped(kept, "tool", 0)[1]).toBe(false);
});

// agents and fields are what the view must learn from the data, not spell out; fields.ts
// holds the role patterns, so it alone may name fields.
const agents = /claude|codex|opencode|langgraph|anthropic|openai|transcript|jsonl/i;
const fields = /sessionId|parentUuid|message\.(usage|content|model)|gen_ai\./;

const sources = import.meta.glob<string>(["./*", "!./*.test.ts"], {
  query: "?raw",
  import: "default",
  eager: true,
});

test("nothing outside the role patterns names an agent, source format or field", () => {
  const offending = Object.entries(sources).flatMap(([file, source]) =>
    source
      .split("\n")
      .filter((line) => agents.test(line) || (file !== "./fields.ts" && fields.test(line)))
      .map((line) => `${file}: ${line.trim()}`),
  );
  expect(offending).toEqual([]);
});

// The factory's transcripts report no time to first token, so its unit is read off names.
test("time to first token is in the unit its field names, else OpenTelemetry's seconds", () => {
  expect(inMs("gen_ai.server.time_to_first_token", 1.855)).toBe(1855);
  expect(inMs("ttft_ms", 1855)).toBe(1855);
  expect(inMs("timeToFirstTokenMs", 1855)).toBe(1855);
});

// A store ingested partly before ingest discovered the canonical columns has them on its
// newest lines only; a role reads whichever of them and a source's own field covers more.
test("a role reads the canonical column, unless a source's own field is on more lines", () => {
  const usage = (input: number) => [
    stat("input_tokens", input),
    stat("gen_ai.usage.input_tokens", 0.4),
  ];
  expect(resolveFields(usage(0.001)).input?.name).toBe("gen_ai.usage.input_tokens");
  expect(resolveFields(usage(0.6)).input?.name).toBe("input_tokens");
});

// A gateway names no session; the pod it runs in is the run its calls belong to.
test("lines carrying usage but no session are runs of the instance they come from", () => {
  const gateway = [
    stat("gen_ai.usage.input_tokens"),
    stat("container", 1, 1),
    stat("pod", 1, 30),
    stat("session", 0.01, 3),
  ];
  expect(resolveFields(gateway).run?.name).toBe("pod");
});

// A gateway's calls logged before ingest discovered canonical columns carry only its own
// fields, while the canonical model is on many newer lines: a call's tokens, model and cost
// must still come from the same lines.
test("a call's roles all come from the canonical columns or all from a source's own fields", () => {
  const usage = [
    stat("gen_ai.usage.input_tokens", 0.4),
    stat("gen_ai.response.model", 0.4),
    stat("agw.ai.usage.cost.total", 0.4),
    stat("input_tokens", 0.1),
    stat("model", 0.9),
    stat("cost_usd", 0.9),
  ];
  const f = resolveFields(usage);
  expect([f.input?.name, f.model?.name, f.cost?.name]).toEqual([
    "gen_ai.usage.input_tokens",
    "gen_ai.response.model",
    "agw.ai.usage.cost.total",
  ]);
});
