import type { BatchResult } from "./generated/batch";
import type { APIError, Envelope } from "./generated/query";
import type * as tools from "./generated/tools";

// Requests and Responses map each registered tool's name to its request and response.
export interface Requests {
  aggregate: tools.AggregateRequest;
  attribute_writes: tools.AttributeWritesRequest;
  co_occurrence: tools.CoOccurrenceRequest;
  correlate: tools.CorrelateRequest;
  explain_spike: tools.ExplainSpikeRequest;
  find_anomalies: tools.FindAnomaliesRequest;
  focus: tools.FocusRequest;
  funnel: tools.FunnelRequest;
  search_logs: tools.SearchRequest;
  rank_fields: tools.FieldsRequest;
  query: tools.QueryRequest;
  list_patterns: tools.PatternsRequest;
  get_trace: tools.TraceRequest;
  trace_shape: tools.TraceShapeRequest;
  resource_usage: tools.ResourceUsageRequest;
  service_graph: tools.ServiceGraphRequest;
  rca_walk: tools.RCAWalkRequest;
  list_slos: tools.ListSLOsRequest;
  slo_status: tools.SLOStatusRequest;
  import_definitions: tools.ImportDefinitionsRequest;
  export_definitions: tools.ExportDefinitionsRequest;
  list_views: tools.ListViewsRequest;
  get_view: tools.GetViewRequest;
}

export interface Responses {
  aggregate: tools.AggregateResponse;
  attribute_writes: tools.AttributeWritesResponse;
  co_occurrence: tools.CoOccurrenceResponse;
  correlate: tools.CorrelateResponse;
  explain_spike: tools.ExplainSpikeResponse;
  find_anomalies: tools.FindAnomaliesResponse;
  focus: tools.FocusResponse;
  funnel: tools.FunnelResponse;
  search_logs: tools.SearchResponse;
  rank_fields: tools.FieldsResponse;
  query: tools.QueryResponse;
  list_patterns: tools.PatternsResponse;
  get_trace: tools.TraceResponse;
  trace_shape: tools.TraceShapeResponse;
  resource_usage: tools.ResourceUsageResponse;
  service_graph: tools.ServiceGraphResponse;
  rca_walk: tools.RCAWalkResponse;
  list_slos: tools.ListSLOsResponse;
  slo_status: tools.SLOStatusResponse;
  import_definitions: tools.ImportDefinitionsResponse;
  export_definitions: tools.ExportDefinitionsResponse;
  list_views: tools.ListViewsResponse;
  get_view: tools.GetViewResponse;
}

export type ToolName = keyof Requests;

export interface CallOptions {
  signal?: AbortSignal;
  // now is the pinned now the call is made against; an answer held from another is asked again.
  now?: number;
  // view is the saved view's panel making the call, "<name>/<panel index>".
  view?: string;
}

// DataFocalClient is the only way a view reaches the backend.
export interface DataFocalClient {
  call<T extends ToolName>(
    tool: T,
    req: Requests[T],
    opts?: CallOptions,
  ): Promise<Envelope<Responses[T]>>;
  // follow streams search_logs batches until the server ends the stream.
  follow(
    req: tools.SearchRequest,
    onBatch: (b: Envelope<tools.SearchResponse>) => void,
    signal: AbortSignal,
  ): Promise<void>;
}

// ToolError is a refusal from the tool surface, carrying what the caller needs to re-plan.
export class ToolError extends Error {
  readonly code: string;
  // reason is the engine's message, without the code.
  readonly reason: string;
  readonly hint: string | undefined;
  readonly suggestedFields: string[] | undefined;

  constructor(e: APIError) {
    super(`${e.code}: ${e.message}`);
    this.name = "ToolError";
    this.code = e.code;
    this.reason = e.message;
    this.hint = e.hint;
    this.suggestedFields = e.suggested_fields;
  }
}

function isAPIError(v: unknown): v is APIError {
  return typeof v === "object" && v !== null && "code" in v;
}

function isEnvelope<T>(v: unknown): v is Envelope<T> {
  return typeof v === "object" && v !== null && "data" in v && "meta" in v;
}

// envelope returns body as the answer it is, throwing the refusal it carries instead.
export function envelope<T>(body: unknown): Envelope<T> {
  if (isAPIError(body)) {
    throw new ToolError(body);
  }
  if (!isEnvelope<T>(body)) {
    throw new Error(`not a tool response: ${JSON.stringify(body)}`);
  }
  return body;
}

// readEvents calls onEvent with the data and name of each server-sent event in body.
async function readEvents(
  body: ReadableStream<Uint8Array<ArrayBuffer>>,
  onEvent: (data: string, event: string) => void,
) {
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  let buf = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) {
      return;
    }
    const events = (buf + value).split("\n\n");
    buf = events.pop() ?? "";
    for (const event of events) {
      onEvent(/^data: (.*)$/m.exec(event)?.[1] ?? "", /^event: (.*)$/m.exec(event)?.[1] ?? "");
    }
  }
}

// canonical is v's JSON with every object's keys sorted, so equal requests are equal strings.
export const canonical = (v: unknown): string =>
  JSON.stringify(v, (_, x: unknown) =>
    x && typeof x === "object" && !Array.isArray(x)
      ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : x,
  );

// Call is one call a loader sends, with the pinned now it was made against.
export interface Call {
  readonly tool: ToolName;
  readonly req: Requests[ToolName];
  readonly now: number | undefined;
  readonly view?: string | undefined;
  readonly signal: AbortSignal;
}

// Transport is what a loader sends its calls over: one at a time, or a frame's at once, each
// answer settling as it arrives.
export interface Transport {
  call(c: Call): Promise<unknown>;
  batch?(calls: readonly Call[], signal: AbortSignal): Promise<unknown>[];
  follow: DataFocalClient["follow"];
}

// heldChars bounds what a loader holds by the length of its answers' JSON.
const heldChars = 32 << 20;
const maxBatch = 32;

interface Load {
  readonly now: number | undefined;
  readonly answer: Promise<unknown>;
  readonly abort: AbortController;
  chars: number;
  waiters: number;
  settled: boolean;
}

interface Miss {
  readonly call: Call;
  readonly load: Load;
  readonly settle: (answer: Promise<unknown>) => void;
}

const nextFrame = (f: () => void) =>
  typeof requestAnimationFrame === "function" ? requestAnimationFrame(f) : setTimeout(f, 0);

// allAborted is a signal aborting once every load of loads has.
function allAborted(loads: readonly Load[]): AbortSignal {
  const abort = new AbortController();
  for (const l of loads) {
    l.abort.signal.addEventListener("abort", () => {
      if (loads.every((m) => m.abort.signal.aborted)) {
        abort.abort();
      }
    });
  }
  return abort.signal;
}

// Loader asks its transport each answer once, however many call for it: a call in flight is
// waited on, and an answer is held until the pinned now moves, or when its request names a
// snapshot until evicted. The calls neither held nor in flight in one frame go out together.
class Loader implements DataFocalClient {
  private readonly t: Transport;
  private readonly held = new Map<string, Load>();
  private chars = 0;
  private frame: Miss[] = [];
  private sentInFrame = false;

  constructor(t: Transport) {
    this.t = t;
  }

  async call<T extends ToolName>(
    tool: T,
    req: Requests[T],
    opts: CallOptions = {},
  ): Promise<Envelope<Responses[T]>> {
    // An import may save, which changes every answer, so none is held across it.
    const key = tool === "import_definitions" ? "" : `${tool} ${canonical(req)}`;
    let load = this.held.get(key);
    if (load && (("snapshot" in req && req.snapshot) || load.now === opts.now)) {
      this.held.delete(key);
      this.held.set(key, load);
    } else {
      load = this.miss(key, { tool, req, now: opts.now, view: opts.view });
    }
    const env = envelope<Responses[T]>(await this.wait(key, load, opts.signal));
    if ("dry_run" in req && req.dry_run === false) {
      this.held.clear();
      this.chars = 0;
    }
    return env;
  }

  follow(
    req: tools.SearchRequest,
    onBatch: (b: Envelope<tools.SearchResponse>) => void,
    signal: AbortSignal,
  ): Promise<void> {
    return this.t.follow(req, onBatch, signal);
  }

  private miss(key: string, c: Omit<Call, "signal">): Load {
    let settle: (answer: Promise<unknown>) => void = () => {};
    const answer = new Promise<unknown>((resolve) => {
      settle = resolve;
    });
    const load: Load = {
      now: c.now,
      answer,
      abort: new AbortController(),
      chars: 0,
      waiters: 0,
      settled: false,
    };
    if (key !== "") {
      this.held.set(key, load);
    }
    // A refusal is an answer like any other, but for one worth retrying; a call that failed to
    // arrive is asked again.
    answer.then(
      (body) =>
        isAPIError(body) && body.retryable ? this.drop(key, load) : this.keep(key, load, body),
      () => this.drop(key, load),
    );
    // A frame's first misses go once the task asking them ends; the rest wait for the next.
    if (this.frame.length === 0 && this.sentInFrame) {
      nextFrame(() => this.send());
    } else if (this.frame.length === 0) {
      queueMicrotask(() => this.send());
    }
    this.frame.push({ call: { ...c, signal: load.abort.signal }, load, settle });
    return load;
  }

  // send sends the frame's calls still waited on: one alone, more in batches.
  private send(): void {
    const misses = this.frame.filter((m) => !m.load.abort.signal.aborted);
    this.frame = [];
    this.sentInFrame = true;
    nextFrame(() => {
      this.sentInFrame = false;
    });
    const batch = this.t.batch?.bind(this.t);
    if (misses.length === 1 || !batch) {
      for (const m of misses) {
        m.settle(this.t.call(m.call));
      }
      return;
    }
    for (let i = 0; i < misses.length; i += maxBatch) {
      const part = misses.slice(i, i + maxBatch);
      const answers = batch(
        part.map((m) => m.call),
        allAborted(part.map((m) => m.load)),
      );
      part.forEach((m, j) => m.settle(answers[j] ?? Promise.reject(new Error("no answer"))));
    }
  }

  // wait is load's answer, or signal's reason once it aborts, the load aborted with its last
  // waiter.
  private wait(key: string, load: Load, signal: AbortSignal | undefined): Promise<unknown> {
    load.waiters++;
    if (!signal) {
      return load.answer;
    }
    return new Promise((resolve, reject) => {
      const aborted = () => {
        reject(signal.reason);
        if (--load.waiters === 0 && !load.settled) {
          load.abort.abort(signal.reason);
          this.drop(key, load);
        }
      };
      if (signal.aborted) {
        aborted();
        return;
      }
      signal.addEventListener("abort", aborted, { once: true });
      load.answer.then(resolve, reject).finally(() => signal.removeEventListener("abort", aborted));
    });
  }

  // keep holds load's answer, evicting the least recently used past heldChars.
  private keep(key: string, load: Load, answer: unknown): void {
    load.settled = true;
    if (this.held.get(key) !== load) {
      return;
    }
    load.chars = JSON.stringify(answer).length;
    this.chars += load.chars;
    for (const [k, l] of this.held) {
      if (this.chars <= heldChars) {
        return;
      }
      if (l.settled) {
        this.drop(k, l);
      }
    }
  }

  private drop(key: string, load: Load): void {
    if (this.held.get(key) === load) {
      this.held.delete(key);
      this.chars -= load.chars;
    }
  }
}

// loader is a DataFocalClient asking t each answer once, as a plain object so that a wrapper
// spreading it keeps its methods.
export function loader(t: Transport): DataFocalClient {
  const l = new Loader(t);
  return {
    call: (tool, req, opts) => l.call(tool, req, opts),
    follow: (req, onBatch, signal) => l.follow(req, onBatch, signal),
  };
}

const isBatchResult = (v: unknown): v is BatchResult =>
  typeof v === "object" && v !== null && "index" in v;

// retryAfterMs is how long res asks a refused call to wait, a second when it does not say.
const retryAfterMs = (res?: Response) => Number(res?.headers.get("Retry-After") ?? 1) * 1000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
// openMs is how long a batch stream may take to open before its calls go alone.
const openMs = 1000;

type Post = (path: string, req: unknown, signal?: AbortSignal, view?: string) => Promise<Response>;

// HTTP is the transport of the querier's POST /v1/<tool> routes, sending a batch over
// POST /v1/batch/stream until one fails, and a call refused busy again once after Retry-After.
class HTTP implements Transport {
  private readonly post: Post;
  private streams = true;

  constructor(post: Post) {
    this.post = post;
  }

  async call(c: Call): Promise<unknown> {
    const res = await this.post(c.tool, c.req, c.signal, c.view);
    const body: unknown = await res.json();
    if (!isAPIError(body) || body.code !== "busy") {
      return body;
    }
    await sleep(retryAfterMs(res));
    return this.plain(c);
  }

  batch(calls: readonly Call[], signal: AbortSignal): Promise<unknown>[] {
    // A panel's call names its view in a header, which a batch has none of for each call.
    if (!this.streams || calls.some((c) => c.view)) {
      return calls.map((c) => this.call(c));
    }
    const settles: ((answer: Promise<unknown>) => void)[] = [];
    const answers = calls.map(() => new Promise<unknown>((resolve) => settles.push(resolve)));
    const answered = new Set<number>();
    const cancel = new AbortController();
    const fail = (why: unknown) => {
      if (cancel.signal.aborted) {
        return;
      }
      cancel.abort(why);
      if (signal.aborted) {
        settles.forEach((s) => s(Promise.reject(signal.reason)));
        return;
      }
      this.fellBack(why);
      calls.forEach((c, i) => answered.has(i) || settles[i]?.(this.call(c)));
    };
    const opening = setTimeout(() => fail(new Error("batch stream did not open")), openMs);
    const onResult = (r: BatchResult) => {
      answered.add(r.index);
      const c = calls[r.index];
      settles[r.index]?.(c ? this.answer(c, r) : Promise.reject(new Error("no call")));
    };
    this.stream(calls, onResult, AbortSignal.any([signal, cancel.signal]), () =>
      clearTimeout(opening),
    )
      .then(() => answered.size < calls.length && fail(new Error("batch stream ended early")))
      .catch(fail)
      .finally(() => clearTimeout(opening));
    return answers;
  }

  async follow(
    req: tools.SearchRequest,
    onBatch: (b: Envelope<tools.SearchResponse>) => void,
    signal: AbortSignal,
  ): Promise<void> {
    const res = await this.post("search_logs/follow", req, signal);
    if (!res.ok || !res.body) {
      envelope(await res.json());
      return;
    }
    await readEvents(res.body, (data) => onBatch(envelope(JSON.parse(data))));
  }

  private async plain(c: Call): Promise<unknown> {
    const res = await this.post(c.tool, c.req, c.signal, c.view);
    return res.json();
  }

  // answer is c's answer from its result, asked again alone once a second on when refused busy.
  private answer(c: Call, r: BatchResult): Promise<unknown> {
    if (r.error?.code === "busy") {
      return sleep(retryAfterMs()).then(() => this.plain(c));
    }
    return Promise.resolve(r.error ?? r.envelope);
  }

  // stream sends calls in one POST /v1/batch/stream, calling opened once it opens and onResult
  // with each call's result as it arrives.
  private async stream(
    calls: readonly Call[],
    onResult: (r: BatchResult) => void,
    signal: AbortSignal,
    opened: () => void,
  ): Promise<void> {
    const body = { calls: calls.map((c) => ({ tool: c.tool, args: c.req })) };
    const res = await this.post("batch/stream", body, signal);
    if (!res.ok || !res.body) {
      throw new Error(`batch stream answered ${res.status}`);
    }
    await readEvents(res.body, (data, event) => {
      opened();
      const r: unknown = event === "open" ? undefined : JSON.parse(data);
      if (isBatchResult(r)) {
        onResult(r);
      }
    });
  }

  private fellBack(why: unknown): void {
    if (this.streams) {
      this.streams = false;
      // Reported once, as the browser reports an uncaught error, with why as its cause.
      reportError(
        new Error("batch stream failed; sending calls one at a time from now", { cause: why }),
      );
    }
  }
}

// createHttpClient calls the querier's POST /v1/<tool> routes under baseUrl through a loader.
export function createHttpClient(opts: { baseUrl: string }): DataFocalClient {
  return loader(
    new HTTP((path, req, signal, view) =>
      fetch(`${opts.baseUrl}/v1/${path}`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(view && { "X-DataFocal-View": view }) },
        body: JSON.stringify(req),
        signal: signal ?? null,
      }),
    ),
  );
}
