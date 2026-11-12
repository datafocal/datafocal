import type { FieldStat } from "@datafocal/api";

// Field is a column the view reads, the name it shows it by, and where it came from, which
// the view shows on hover.
export interface Field {
  readonly name: string;
  readonly label: string;
  readonly source: string;
}

// outcome is the engine's pattern column: a run's outcome is the pattern it ends on.
export const outcome = "pattern_id";

// source is the engine's log source column: each source's quota is its own account's.
export const source = "service";

// roles are the columns the view reads: the canonical column ingest discovers when the logs
// carry it, else the first ranked field whose name matches. A role no field matches is
// absent, and the panels reading it say the logs lack it.
type Role =
  | "cost"
  | "input"
  | "output"
  | "cacheRead"
  | "cacheWrite"
  | "reasoning"
  | "run"
  | "call"
  | "model"
  | "harness"
  | "agent"
  | "tool"
  | "skill"
  | "ttft"
  | "duration"
  | "turn"
  | "toolCall"
  | "callId"
  | "contentType"
  | "contentRole"
  | "result"
  | "toolError"
  | "toolList"
  | "compaction"
  | "afterCompaction"
  | "memory"
  | "memoryOp"
  | "memoryChars"
  | "plan"
  | "quotaLimited"
  | "credits";

const roles: Readonly<Record<Role, RegExp>> = {
  cost: /cost/,
  input: /(^|\.)input_tokens$/,
  output: /output_tokens$/,
  cacheRead: /cache_read/,
  // A TTL breakdown of cache writes (ephemeral_5m_…) is part of its total, not the total.
  cacheWrite: /cache_(creation|write)[._](input_)?tokens$/,
  reasoning: /reasoning\w*tokens$/,
  // run is the unit of work the lines carrying usage name: a session or run id, else the
  // workload instance they come from, which ties a gateway's calls to the agent beside it.
  run: /(^|\.)(session|conversation|run|task|job)([._]?id)?$|(^|\.)(pod|container|instance)([._]?(name|id))?$/i,
  // call names the model call a line's usage belongs to, repeated on each line of its answer.
  call: /(^|\.)(message|request|response)[._]?id$/i,
  model: /model$/,
  harness: /(harness|version|provider\.name)$/,
  agent: /agent(\.name)?$/,
  tool: /tool(\.name)?$|content\.name$/,
  skill: /skill(\.name)?$/,
  ttft: /first_token/,
  duration: /duration(_ms)?$/,
  // turn and toolCall name the lines of one turn and of one tool call and its result, whose
  // times measure them when no duration column does.
  turn: /(^|\.)prompt[._]?id$/i,
  toolCall: /(^|\.)content\.id$/,
  // callId names a tool call on the one line that answers it: its result, or its span.
  callId: /tool_(use|call)_id$/,
  contentType: /content\.type$/,
  contentRole: /(^|\.)(content_)?role$/,
  result: /content\.content$/,
  toolError: /is_error$/,
  toolList: /tools?[._]?(hash|digest)$/,
  // compaction is the tokens a compaction removed, and afterCompaction the first model call
  // after one, which writes the compacted context to the cache again.
  compaction: /compaction\w*tokens$/,
  afterCompaction: /(^|\.)after_compaction$/,
  // memory is the memory file a line loads, reads or writes, memoryOp which of those it does,
  // and memoryChars the text a load puts in the context.
  memory: /(^|\.)memory[._]?(name|file)$/,
  memoryOp: /(^|\.)memory[._]?op$/,
  memoryChars: /(^|\.)memory\w*chars$/,
  // plan is the account's subscription, quotaLimited a limit a line hit, and credits what the
  // account has left; the windows themselves are Fields.quotas.
  plan: /(^|\.)(plan_type|subscription_type)$/,
  quotaLimited: /(^|\.)quota_limited$/,
  credits: /(^|\.)credits_remaining$/,
};

// canonical are the columns ingest derives from whatever names a source uses:
// input excludes the cache, and cost_usd prices them.
const canonical: Readonly<Partial<Record<Role, string>>> = {
  cost: "cost_usd",
  input: "input_tokens",
  output: "output_tokens",
  cacheRead: "cache_read_tokens",
  cacheWrite: "cache_write_tokens",
  reasoning: "reasoning_tokens",
  run: "run_id",
  model: "model",
  tool: "tool_name",
  callId: "tool_call_id",
  skill: "skill_name",
  contentRole: "content_role",
  toolList: "tools_hash",
  compaction: "compaction_tokens",
  afterCompaction: "after_compaction",
  memory: "memory_name",
  memoryOp: "memory_op",
  memoryChars: "memory_chars",
  plan: "plan",
  quotaLimited: "quota_limited",
  credits: "credits_remaining",
};

// labels name the roles a person picks between, so the page reads "Model", not a field.
const labels: Readonly<Partial<Record<Role, string>>> = {
  model: "Model",
  harness: "Harness version",
  agent: "Agent",
  tool: "Tool",
  skill: "Skill",
};

// kinds are the canonical lengths of each kind of block a line's content holds, which sum
// to its content: the parts of what fills the context.
const kinds = {
  text: "context_text_chars",
  thinking: "context_thinking_chars",
  tool: "context_tool_chars",
};
export type Kind = keyof typeof kinds;

export type Fields = { readonly [R in Role]?: Field } & {
  // dims are what spend can be split by.
  readonly dims: readonly Field[];
  // durations are every reported duration column, which turn time splits into.
  readonly durations: readonly string[];
  // blocks are the columns a block's own text fills: a text, thinking or tool result
  // block's text, and each value of a tool call's input.
  readonly blocks: readonly string[];
  // output is what a tool call's output length is read from: the canonical length of tool
  // content, or the length of the result block, whichever more lines carry.
  readonly toolOutput?: string;
  // kinds are the canonical block-length columns the logs carry, by kind of block.
  readonly kinds: Readonly<Partial<Record<Kind, string>>>;
  // excludesCache is whether input counts only what the cache did not serve.
  readonly excludesCache: boolean;
  // quotas are the subscription windows the logs report, each its used share and reset time.
  readonly quotas: readonly Quota[];
};

// Quota is a subscription window ingest discovered: 5h, 7d, chat and so on.
export interface Quota {
  readonly window: string;
  readonly used: string;
  readonly resets?: string;
}

const quotaUsed = /^quota_(.+)_used_pct$/;

const quotasOf = (window: readonly FieldStat[]): Quota[] =>
  window
    .flatMap((f) => {
      const w = quotaUsed.exec(f.name)?.[1];
      const resets = `quota_${w}_resets_at`;
      return w
        ? [{ window: w, used: f.name, ...(window.some((g) => g.name === resets) && { resets }) }]
        : [];
    })
    .sort((a, b) => a.window.localeCompare(b.window));

const dimRoles = ["model", "harness", "agent", "tool", "skill"] as const;
// usageRoles describe a model call, so their name patterns are matched on the lines carrying
// usage, where another program's request id or duration cannot pass for them, and the run is
// one those lines name, so a run's cost is whatever they log; the rest, and
// every canonical column, are read from the whole window. No role requires another: a line is
// counted for whatever it carries.
const usageRoles: readonly Role[] = [
  "run",
  "cost",
  "input",
  "output",
  "cacheRead",
  "cacheWrite",
  "reasoning",
  "model",
  "call",
  "harness",
  "agent",
  "ttft",
  "duration",
];
const notDims = /^pattern_id$|^param_\d+$|_pattern$|(^|\.)(content_)?role$/;
const blocks = /content\.(text|thinking|content)$|content\.input\.[^.]+$/;

const reported = (f: FieldStat, role?: Role): Field => ({
  name: f.name,
  label: (role && labels[role]) ?? f.name,
  source:
    role && canonical[role] === f.name
      ? `${f.name}, discovered at ingest from the source's own fields`
      : `${f.name}, reported by rank_fields`,
});

// resolve is the canonical column for role, or the first field of ranked matching role's
// pattern when that is on more lines of the window: a store ingested partly before ingest
// discovered canonical columns has them on its newest lines only. For run it is the matching
// field on the most lines, as a log may carry several units of work; a duration is a number.
function resolve(
  window: readonly FieldStat[],
  ranked: readonly FieldStat[],
  role: Role,
  shared: boolean,
): Field[] {
  const named = [...window, ...ranked].find((f) => f.name === canonical[role]);
  const found = matching(ranked, role);
  const best = callRoles.includes(role)
    ? shared
      ? (named ?? found)
      : (found ?? named)
    : wider(role === "run" ? ranked : window, named, found);
  return best ? [reported(best, role)] : [];
}

// wider is named, or found when that is on more lines of over: a run is weighed by the lines
// carrying usage it covers, any other role by the window's.
function wider(
  over: readonly FieldStat[],
  named: FieldStat | undefined,
  found: FieldStat | undefined,
): FieldStat | undefined {
  const lines = (f: FieldStat | undefined) => over.find((w) => w.name === f?.name)?.lines ?? 0;
  return named && lines(named) >= lines(found) ? named : (found ?? named);
}

// callRoles describe one model call, so they are read together: all from the canonical
// columns or all from the sources' own fields, never a call's tokens from one line and its
// model or cost from another. A store ingested partly before ingest discovered canonical
// columns has them on its newest lines only; whichever the input tokens are on more lines of
// is read.
const callRoles: readonly Role[] = [
  "cost",
  "input",
  "output",
  "cacheRead",
  "cacheWrite",
  "reasoning",
  "model",
];

function sharedWins(window: readonly FieldStat[], usage: readonly FieldStat[]): boolean {
  const lines = (name: string | undefined) => window.find((f) => f.name === name)?.lines ?? 0;
  return (
    lines(canonical.input) >= lines(matching(usage, "input")?.name) && lines(canonical.input) > 0
  );
}

function matching(ranked: readonly FieldStat[], role: Role): FieldStat | undefined {
  const answered = role === "model" ? ranked.find((f) => served.test(f.name)) : undefined;
  const all = ranked.filter(
    (f) =>
      roles[role].test(f.name) &&
      f.name !== canonical[role] &&
      (role !== "duration" || f.type === "num"),
  );
  // The run is the field on the most lines, the one with most values when several are on all.
  const widest = [...all].sort((a, b) => b.lines - a.lines || b.cardinality - a.cardinality)[0];
  return answered ?? (role === "run" ? widest : all[0]);
}

// served is the model that answered, preferred over the one requested when both are logged.
const served = /(^|\.)response[._]?model$/;

const roleNames = Object.keys(roles).filter((r): r is Role => r in roles);

function outputOf(window: readonly FieldStat[], result: Field | undefined) {
  const lines = (name: string | undefined) => window.find((f) => f.name === name)?.lines ?? 0;
  const own = result && `len(${result.name})`;
  const best = lines(kinds.tool) >= lines(result?.name) && lines(kinds.tool) > 0 ? kinds.tool : own;
  return best ? { toolOutput: best } : {};
}

const many = (f: FieldStat) => f.type === "enum" && f.cardinality >= 2 && f.cardinality <= 50;

// dimsOf are what spend splits by: the dimension roles the logs carry, then the enums of a few
// values on at least one line in twenty of those carrying usage, each labelled by its role
// when it plays one. The run is not one: the runs are their own ledger.
function dimsOf(
  fields: { [R in Role]?: Field },
  window: readonly FieldStat[],
  usage: readonly FieldStat[],
): Field[] {
  const roleOf = (name: string) => dimRoles.find((r) => fields[r]?.name === name);
  const first = dimRoles.flatMap((r) => window.find((f) => f.name === fields[r]?.name) ?? []);
  const dims = [...first.filter(many), ...usage.filter((f) => many(f) && f.lines >= 0.05)];
  return [
    ...new Map(
      dims
        .filter((f) => !notDims.test(f.name) && f.name !== fields.run?.name)
        .map((f) => [f.name, reported(f, roleOf(f.name))]),
    ).values(),
  ];
}

// usageFields are the fields usage is read from, whose lines the usage ranking reads.
export const usageFields = (f: Fields): Field[] =>
  [f.cost, f.input, f.output, f.cacheRead, f.cacheWrite].flatMap((x) => x ?? []);

// resolveFields maps rank_fields' answers onto the view's roles: window ranks every line in
// the window, and usage only the lines carrying usage (window again until that is known).
export function resolveFields(
  window: readonly FieldStat[],
  usage: readonly FieldStat[] = window,
): Fields {
  const shared = sharedWins(window, usage);
  const fields: { [R in Role]?: Field } = Object.fromEntries(
    roleNames.flatMap((role) =>
      resolve(window, usageRoles.includes(role) ? usage : window, role, shared).map((f) => [
        role,
        f,
      ]),
    ),
  );
  const has = (name: string) => window.some((f) => f.name === name);
  return {
    ...fields,
    dims: dimsOf(fields, window, usage),
    durations: usage
      .filter((f) => f.type === "num" && roles.duration.test(f.name))
      .map((f) => f.name),
    blocks: window.filter((f) => blocks.test(f.name)).map((f) => f.name),
    kinds: Object.fromEntries(Object.entries(kinds).filter(([, name]) => has(name))),
    excludesCache: fields.input?.name === canonical.input,
    quotas: quotasOf(window),
    ...outputOf(window, fields.result),
  };
}
