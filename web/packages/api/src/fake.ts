import {
  type Call,
  canonical,
  envelope,
  type DataFocalClient,
  loader,
  type Requests,
  ToolError,
  type ToolName,
} from "./client";
import type { AggregateRequest } from "./generated/tools";

// FakeClient is a DataFocalClient whose methods call the matching Fn field. An absent
// callFn answers an empty envelope and an absent followFn ends the stream at once.
export interface FakeClient extends DataFocalClient {
  callFn?: DataFocalClient["call"];
  followFn?: DataFocalClient["follow"];
}

// havingOps are the comparisons a having clause takes, as the engine lists them.
const havingOps = ["eq", "ge", "gt", "le", "lt", "ne"];
const sorts = ["value_desc", "value_asc", "key"];

// whole is whether s reads as a whole number in [min, n), as Go's strconv.Atoi reads it.
const whole = (s: string | undefined, n: number, min = 0) =>
  s !== undefined && /^[+-]?\d+$/.test(s) && Number(s) >= min && Number(s) < n;

// aggregateRules are the engine's refusals of an aggregate with n measures, in its order.
const aggregateRules: readonly ((r: AggregateRequest, n: number) => string | undefined)[] = [
  (r) => (r.measures?.length || r.measure ? undefined : "measure is empty"),
  (r) =>
    r.measures?.length && (r.measure || r.numerator?.length)
      ? "aggregate: measures replaces measure and numerator; set one or the other"
      : undefined,
  (r, n) => {
    const i = (r.having ?? []).findIndex(
      (h) => h.index < 0 || h.index >= n || !havingOps.includes(h.op),
    );
    return i < 0
      ? undefined
      : `aggregate: having[${i}] needs a measure index under ${n} and one of [${havingOps.join(" ")}]`;
  },
  (r, n) => {
    const term = (t: string) =>
      whole(t.split(":")[0], n) && ["asc", "desc"].includes(t.split(":")[1] ?? "");
    return !r.sort || sorts.includes(r.sort) || r.sort.split(",").every(term)
      ? undefined
      : `aggregate: sort must be ${sorts.join(", ")} or, with measures, <measure index>:desc|asc comma-separated, got ${JSON.stringify(r.sort)}`;
  },
  (r) =>
    !r.ordinal_of || whole(r.bins || "1", Number.MAX_SAFE_INTEGER, 1)
      ? undefined
      : `aggregate: with ordinal_of, bins is a width in positions, got ${JSON.stringify(r.bins)}`,
  (r, n) => {
    const by = r.rollup?.group_by;
    return !by || whole(/^label\[(.*)\]$/.exec(by)?.[1], n)
      ? undefined
      : `aggregate: rollup group_by must be label[i] with i under ${n}, got ${JSON.stringify(by)}`;
  },
];

// is narrows req to want's request when tool is want, as the engine decodes it.
const is = <T extends ToolName>(tool: ToolName, want: T, _req: unknown): _req is Requests[T] =>
  tool === want;

// checkRequest returns the refusal the engine gives a malformed request, for the rules
// request_rules.json pins against it, or undefined.
export function checkRequest(tool: ToolName, req: unknown): ToolError | undefined {
  let message: string | undefined;
  if (is(tool, "aggregate", req)) {
    const n = Math.max(req.measures?.length ?? 0, 1);
    message = aggregateRules.map((rule) => rule(req, n)).find((m) => m !== undefined);
  } else if (is(tool, "get_trace", req) && !req.value) {
    message = "get_trace: value is required";
  }
  return message === undefined
    ? undefined
    : new ToolError({ code: "invalid_request", message, retryable: false });
}

// createFakeClient returns the FakeClient calling fns, refusing first what the engine would.
export function createFakeClient(fns: Pick<FakeClient, "callFn" | "followFn">): FakeClient {
  return {
    ...fns,
    call(tool, req, opts) {
      const refused = checkRequest(tool, req);
      if (refused) {
        return Promise.reject(refused);
      }
      if (fns.callFn) {
        return fns.callFn(tool, req, opts);
      }
      return Promise.resolve(envelope({ data: {}, meta: {} }));
    },
    follow(req, onBatch, signal) {
      const refused = checkRequest("search_logs", req);
      if (refused) {
        return Promise.reject(refused);
      }
      return fns.followFn ? fns.followFn(req, onBatch, signal) : Promise.resolve();
    },
  };
}

// KeyedAnswer is a tool's answer to the request equal to req, or to any request without one;
// tool is a string so a JSON file of answers is one as it stands.
export interface KeyedAnswer {
  readonly tool: string;
  readonly req?: unknown;
  readonly answer: unknown;
}

// Sent is a call that reached the fixtures, with the pinned now it was made against, the send
// it went in (a batch, or a call alone) and the animation frame that send went out in.
export interface Sent {
  readonly tool: string;
  readonly req: unknown;
  readonly now: number | undefined;
  readonly view: string | undefined;
  readonly send: number;
  readonly frame: number;
}

// FixtureClient is a client answering from fixtures through a loader, which lists each request
// none answered, each answer it gave and each call the loader sent, so a test can tell a
// fixture no request reads any more and a view asking more than it draws.
export interface FixtureClient extends DataFocalClient {
  readonly missed: readonly unknown[];
  readonly used: ReadonlySet<KeyedAnswer>;
  readonly sent: readonly Sent[];
  // faults waits until no call is in flight and lists the calls sent twice against one pinned
  // now, the frames more than one send went out in, and each call sent while idle after.
  faults(): Promise<string[]>;
}

export interface FixtureOptions {
  // answer is the answer to a call the test answers itself, or undefined for the fixtures'.
  readonly answer?: (tool: ToolName, req: Requests[ToolName], signal: AbortSignal) => unknown;
  // delays are the milliseconds each tool's answer takes.
  readonly delays?: Readonly<Partial<Record<string, number>>>;
}

// frameCounter numbers the animation frames each call to it is made in: one it makes in a
// frame asks for the count to rise before the next frame's other callbacks run.
function frameCounter(): () => number {
  let frame = 0;
  let rising = false;
  return () => {
    if (!rising && typeof requestAnimationFrame === "function") {
      rising = true;
      requestAnimationFrame(() => {
        frame++;
        rising = false;
      });
    }
    return frame;
  };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
// idleMs is how long a view sends nothing before it counts as idle, and is watched for after.
const idleMs = 100;
// settleMs bounds the wait for the calls in flight, which a test may never answer.
const settleMs = 3000;

// faultsOf are the calls of sent made twice against one pinned now, but for those aborted
// before their answer, and the frames more than one send went out in.
function faultsOf(sent: readonly Sent[], aborted: ReadonlySet<Sent>): string[] {
  const calls = new Set<string>();
  const frames = new Map<number, number>();
  const out: string[] = [];
  for (const s of sent) {
    if ((frames.get(s.frame) ?? s.send) !== s.send) {
      out.push(`more than one send in the frame at ${s.frame}`);
    }
    frames.set(s.frame, s.send);
    const key = `${s.tool} ${canonical(s.req)} at ${s.now}`;
    if (calls.has(key)) {
      out.push(`sent twice: ${key}`);
    }
    if (!aborted.has(s)) {
      calls.add(key);
    }
  }
  return out;
}

// answerer answers a call with opts' answer, or the first of answers keyed to it, noting in
// missed each request none answers and in used each answer it gives.
function answerer(
  answers: readonly KeyedAnswer[],
  opts: FixtureOptions,
  missed: unknown[],
  used: Set<KeyedAnswer>,
) {
  return async (tool: ToolName, req: Requests[ToolName], signal: AbortSignal): Promise<unknown> => {
    await sleep(opts.delays?.[tool] ?? 0);
    const refused = checkRequest(tool, req);
    if (refused) {
      throw refused;
    }
    const own = await opts.answer?.(tool, req, signal);
    if (own !== undefined) {
      return own;
    }
    const key = canonical(req);
    const found = answers.find(
      (a) => a.tool === tool && (a.req === undefined || canonical(a.req) === key),
    );
    if (!found) {
      missed.push({ tool, req });
      throw new Error(`no answer for ${tool} ${key}`);
    }
    used.add(found);
    return found.answer;
  };
}

// fixtureClient answers each request with opts' answer, or the first answer keyed to it: the
// engine's own answers (engine.json) or the Go goldens make web-fixtures copies into fixtures/.
export function fixtureClient(
  answers: readonly KeyedAnswer[],
  opts: FixtureOptions = {},
): FixtureClient {
  const missed: unknown[] = [];
  const used = new Set<KeyedAnswer>();
  const answer = answerer(answers, opts, missed, used);
  const sent: Sent[] = [];
  const inFlight = new Set<Sent>();
  const aborted = new Set<Sent>();
  let checked = 0;
  const frame = frameCounter();
  const send = (calls: readonly Call[]) => {
    const at = { send: sent.length, frame: frame() };
    return calls.map((c) => {
      const s = { tool: c.tool, req: c.req, now: c.now, view: c.view, ...at };
      sent.push(s);
      // The perf scenarios count the calls a page sends by this mark.
      performance.mark("call", { detail: c.tool });
      inFlight.add(s);
      c.signal.addEventListener("abort", () => inFlight.delete(s) && aborted.add(s));
      return answer(c.tool, c.req, c.signal).finally(() => inFlight.delete(s));
    });
  };
  // faults checks the calls sent since it last did, once none is in flight.
  const faults = async () => {
    const until = Date.now() + settleMs;
    for (let seen = -1; (seen !== sent.length || inFlight.size > 0) && Date.now() < until; ) {
      seen = sent.length;
      await sleep(idleMs);
    }
    const idle = sent.length;
    await sleep(idleMs);
    const late = sent.slice(idle).map((s) => `sent while idle: ${s.tool} ${canonical(s.req)}`);
    const since = sent.slice(checked, idle);
    checked = sent.length;
    return [...faultsOf(since, aborted), ...late];
  };
  const client = loader({
    call: (c) => send([c])[0] ?? Promise.reject(new Error("no answer")),
    batch: send,
    async follow(req, onBatch, signal) {
      onBatch(envelope(await answer("search_logs", req, signal)));
    },
  });
  return { missed, used, sent, faults, ...client };
}

// countingClient is client through a loader, listing the calls that reach client as a
// FixtureClient does.
export function countingClient(client: DataFocalClient): FixtureClient {
  const counted = fixtureClient([], {
    answer: (tool, req, signal) => client.call(tool, req, { signal }),
  });
  return { ...counted, follow: (req, onBatch, signal) => client.follow(req, onBatch, signal) };
}
