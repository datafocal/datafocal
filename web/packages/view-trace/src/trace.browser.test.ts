import {
  type FieldStat,
  type Series,
  type AggregateRequest,
  type FixtureClient,
  type FixtureOptions,
  fixtureClient,
  ToolError,
  type ToolName,
} from "@datafocal/api";
import { Shell, useParams } from "@datafocal/shell";
import { duration } from "@datafocal/ui";
import { createElement } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, expect, test, vi } from "vitest";
import { page } from "vitest/browser";
import engine from "../fixtures/engine.json";
import incidentAnswers from "../fixtures/search.json";
import { listRequest, TraceView } from "./index";
import { correlations, operationField, readTrace, traceEnds, waterfallModel } from "./trace";

// The view is laid out for a desktop's width, a list beside the trace beside its panel.
await page.viewport(1440, 900);

// time is the factory corpus's last day, the range every request in requests.json reads.
const time = "2026-07-27T00:00:00Z,2026-07-28T00:00:00Z";
const range = { from: "2026-07-27T00:00:00Z", to: "2026-07-28T00:00:00Z" };
// The corpora a test opens: the factory's, and the incident corpus, whose traces span services
// and err, on its answers in fixtures/search.json to the requests in requests.search.json.
const factory = { answers: engine, time };
const incident = { answers: incidentAnswers, time: "2026-07-18T14:00:00Z,2026-07-18T14:10:00Z" };
type Answers = Parameters<typeof fixtureClient>[0];
let root: Root | undefined;
let fixtures: FixtureClient | undefined;
afterEach(async () => {
  expect(await fixtures?.faults(), "each call sent once, a frame's at once").toEqual([]);
  root?.unmount();
  expect(fixtures?.missed, "every request the view sent is in its requests file").toEqual([]);
});

function Logs() {
  const p = useParams();
  return createElement("h1", null, [p["query"], p["focus"]].filter(Boolean).join(" "));
}

// requests are the requests the view sent, by tool, since the last render.
const requests = (tool: ToolName) =>
  (fixtures?.sent ?? []).filter((c) => c.tool === tool).map((c) => c.req);
// lists are the list's requests, apart from the one for the services to pick from and the one
// asking when the shown trace began and ended.
const lists = () =>
  requests("aggregate").filter(
    (r) =>
      typeof r === "object" &&
      r !== null &&
      "group_by" in r &&
      r.group_by !== "service" &&
      !JSON.stringify(r).includes('"measures":[{"measure":"min:ts"},{"measure":"max:ts"}]'),
  );

// The session the tests open, one Claude Code transcript gathered by sessionId and joined to
// the orchestrator's lines naming it run_id or session; the transcript's lines are its service's steps.
const session = "842e33af-9917-f20e-e46e-5bec950ddc23";
const answers = fixtureClient(engine);
const trace = (
  await answers.call("get_trace", {
    value: session,
    time: range,
    field: "sessionId",
    limit: 10_000,
  })
).data;
const firstLine = "3013cda7-ba99-f9c5-8a28-ed5dabeeb283";
const starts = trace.spans.map((s) => s.start_ms).sort((a, b) => a - b);
const t0 = starts[0] ?? 0;
const service = "workspace";
// The transcript's span starts ms into the trace, on row 2, after the orchestrator's span
// and its first step.
const ms = (trace.spans.find((s) => s.service === service)?.start_ms ?? 0) - t0;
const arrived = `+${duration(ms)}`;
// said is when the prompt was said, ms into the trace: the transcript's first step.
const said =
  (trace.spans.find((s) => s.service === service && s.name === "user · text" && s.dur_ms === 0)
    ?.start_ms ?? 0) - t0;

// render opens path over a corpus, the factory's last day unless named, on the engine's answers.
function render(
  path: string,
  on: { answers: Answers; time: string } = factory,
  opts: FixtureOptions = {},
): void {
  history.replaceState(null, "", `/ui/trace/time=${encodeURIComponent(on.time)}/${path}`);
  const client = fixtureClient(on.answers, opts);
  fixtures = client;
  const routes = [
    { path: "/ui/trace", view: TraceView },
    { path: "/ui/logs", view: Logs },
    { path: "/ui/resources", view: () => createElement("p", null, "Resources") },
  ];
  root = createRoot(document.body.appendChild(document.createElement("div")));
  root.render(createElement(Shell, { routes, client }));
}
const where = (w: string) => `where=${encodeURIComponent(w)}`;
const openSession = () =>
  render(`field=sessionId/traceId=${session}/${where(`service="${service}"`)}`);

// prompt is the session's one prompt: its turn's name, and the step that says it.
const prompt = "Implement the task in TASK.md.";
const names = () => [...document.querySelectorAll(".tr-nm")].map((n) => n.textContent ?? "");

// unfold shows the instant steps the waterfall folds on arrival, which these tests step through.
const unfold = async () => (await find(".tr-fold", ""))?.click();

const find = async (selector: string, text = "") => {
  let el: HTMLElement | undefined;
  await expect
    .poll(() => {
      el = [...document.querySelectorAll<HTMLElement>(selector)].find((e) =>
        e.textContent?.startsWith(text),
      );
      return el;
    })
    .toBeTruthy();
  return el;
};
const fact = (label: string) =>
  [...document.querySelectorAll(".insp dt")].find((dt) => dt.textContent === label)
    ?.nextElementSibling?.textContent;
const selected = () => fact("Start");
const heading = () => document.querySelector("h1")?.textContent;
const key = (k: string) => dispatchEvent(new KeyboardEvent("keydown", { key: k }));

// at fires type on the waterfall at ms into the trace, in the middle of row, a few pixels in
// from a bar starting then rather than on its edge.
const end = Math.max(...trace.spans.map((s) => s.start_ms + s.dur_ms)) - t0;
async function at(type: string, ms: number, row: number, init: MouseEventInit = {}) {
  const canvas = await find(".tr-trace canvas");
  const box = canvas?.getBoundingClientRect() ?? new DOMRect();
  const point = {
    clientX: box.left + (box.width * ms) / end + 3,
    clientY: box.top + row * 26 + 13,
  };
  canvas?.dispatchEvent(new MouseEvent(type, { bubbles: true, ...point, ...init }));
}

test("arriving with a trace and a service selects that service's first span", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  expect(document.querySelector(".tr-row")).toBeTruthy();
});

test("the span's panel closes on × or Escape and reopens on the next selection", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  const panel = () => document.querySelector(".tr-trace .insp");
  for (const close of [async () => (await find(".insp .insp-x"))?.click(), () => key("Escape")]) {
    await close();
    await expect.poll(panel).toBeNull();
    key("j");
    await expect.poll(panel).toBeTruthy();
  }
});

test("T lands in Logs on exactly the trace's lines", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  key("T");
  await expect
    .poll(heading)
    .toBe(`{} | sessionId="${session}" or run_id="${session}" or session="${session}"`);
});

// The transcript's span is its service's lines in the session, by either name.
const span = `{service="${service}"} | sessionId="${session}" or run_id="${session}" or session="${session}"`;
const quoted = span.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
const exactly = new RegExp(`^${quoted}$`);
test.each([
  [
    "the panel's Span logs button",
    exactly,
    () => find(".insp button", "Span logs").then((b) => b?.click()),
  ],
  ["L", exactly, () => key("L")],
  ["a double-click", exactly, () => at("dblclick", ms, 2)],
  ["an Alt-click", exactly, () => at("click", ms, 2, { altKey: true })],
  [
    "a log tick",
    new RegExp(
      `^\\{pattern_id="[0-9a-f]+", service="${service}"\\} \\| ${quoted.slice(quoted.indexOf("sessionId"))} \\{"segment":"[^"]+","row":\\d+\\}$`,
    ),
    async () => {
      // The prompt's step, a row of the open turn, at the height its name shows at.
      const name = await find(".tr-nm", `user: ${prompt}`);
      const box = (await find(".tr-trace canvas"))?.getBoundingClientRect() ?? new DOMRect();
      const y = name?.getBoundingClientRect() ?? new DOMRect();
      const point = { clientX: box.left + (box.width * said) / end, clientY: y.top + y.height / 2 };
      (await find(".tr-trace canvas"))?.dispatchEvent(
        new MouseEvent("click", { bubbles: true, ...point }),
      );
    },
  ],
  [
    "a right-click entry",
    exactly,
    async () => {
      await at("contextmenu", ms, 2);
      (await find('[role="menuitem"]', "Logs for this span"))?.click();
    },
  ],
])("%s lands in Logs on exactly the lines it names", async (_, query, act) => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  await unfold();
  await act();
  await expect.poll(heading).toMatch(query);
});

// The factory corpus logs no resource samples, so a span's service has none to show.
test("a span's service with no resource samples over the trace is offered no Resources", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  await expect.poll(() => requests("resource_usage")).toHaveLength(1);
  const pivots = () =>
    [...document.querySelectorAll(".insp .pivots button")].map((b) => b.textContent);
  await expect.poll(pivots).toEqual(["Graph"]);
});

// The first step's line is the user's prompt, a JSON record with no message field.
test("a span's attributes are one per item, and its lines say what a record says", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  key("j");
  await expect.poll(() => document.querySelector(".insp h2")?.textContent).toBe(`user: ${prompt}`);
  (await find('.insp [role="tab"]', "Attributes"))?.click();
  const items = () =>
    [...document.querySelectorAll<HTMLTableRowElement>(".insp .tr-attrs tr")].map(
      (tr) => `${tr.cells[0]?.textContent}=${tr.cells[1]?.querySelector("code")?.textContent}`,
    );
  await expect.poll(items).toContain(`uuid=${firstLine}`);
  (await find('.insp [role="tab"]', "Lines"))?.click();
  const lines = () => [...document.querySelectorAll(".tr-lines button")].map((b) => b.textContent);
  await expect.poll(lines).toContain(`19:04:56Z${service}text: Implement the task in TASK.md.`);
  // The inspector reopens the tab last chosen, which the other tests read Summary on.
  (await find('.insp [role="tab"]', "Overview"))?.click();
});

// The orchestrator's instant steps fold into its row; the transcript's records that are no
// message fold with them.
test("instant steps fold into their parent's row on arrival, which counts them, until shown", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  const fold = await find(".tr-fold");
  expect(fold?.textContent).toMatch(/^Show \d+ hidden records?$/);
  expect(names().some((n) => / · \+\d+ instant$/.test(n ?? ""))).toBe(true);
  expect(names()).not.toContain("ai-title");
  fold?.click();
  await expect.poll(names).toContain("ai-title");
  expect(fold?.getAttribute("aria-pressed")).toBe("true");
});

// From the transcript's first turn j steps through its steps, h climbs back and then folds
// it, and l unfolds it and steps in.
test("j/k/h/l move, fold and step through the trace", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  await unfold();
  const spanAndLines = () => `${document.querySelector(".insp h2")?.textContent} ${fact("Lines")}`;
  const turn = `1. ${prompt} 35`;
  for (const [k, shown] of [
    ["j", `user: ${prompt} 1`],
    ["j", "ai-title 1"],
    ["k", `user: ${prompt} 1`],
    ["h", turn],
    ["h", turn],
    ["l", turn],
    ["l", `user: ${prompt} 1`],
  ] as const) {
    key(k);
    await expect.poll(spanAndLines, { message: `after ${k}` }).toBe(shown);
  }
});

// A transcript reads as its prompts: a turn a row, named by what the person asked, with what
// it did in place of a service; its steps say what they did, and only its turn's are shown.
test("a conversation's turns name their prompt and sum up their steps, which say what they did", async () => {
  openSession();
  await expect.poll(selected).toBe(arrived);
  const row = (name: string) =>
    [...document.querySelectorAll(".tr-row, .tr-names > *")].find((r) =>
      r.querySelector(".tr-nm")?.textContent?.startsWith(name),
    );
  await expect
    .poll(() => row(`1. ${prompt}`)?.querySelector(".tr-sv")?.textContent)
    .toMatch(/^\d+ steps · (\w+ ×\d+ · ){3}\$\d+\.\d+$/);
  expect(names().filter((n) => /^(Write|Bash|Grep|Read|Edit) · /.test(n)).length).toBeGreaterThan(
    3,
  );
  expect(names().filter((n) => / result · /.test(n)).length).toBeGreaterThan(3);
  expect(names()).toContain("thinking · Read c9a94183 first.");
  expect(names().some((n) => /^(user|assistant) · /.test(n))).toBe(false);
});

const stat = (name: string, type: string, present = 1): FieldStat => ({
  name,
  type,
  cardinality: 5,
  present,
  lines: present,
  signal: 0,
  high_signal: false,
});

test("correlations are id-named and unit-of-work fields of many values, trace_id first, then units, then most lines a value", () => {
  const ranked = [
    stat("ts", "num"),
    stat("service", "enum"),
    stat("pattern_id", "str"),
    stat("uuid", "str"),
    stat("parentUuid", "str"),
    stat("message.id", "enum", 0.1),
    stat("task", "enum", 0.5),
    stat("sessionId", "enum", 0.4),
    { ...stat("request_id", "str", 0.9), cardinality: 900 },
    stat("latency_id", "num"),
    stat("trace_id", "id", 0.2),
    { ...stat("attachment.identity.modelId", "id", 0.9), cardinality: 1 },
    stat("container.id", "id", 0.7),
    stat("run_id", "id", 0.1),
  ];
  expect(correlations(ranked)).toEqual([
    "trace_id",
    "task",
    "sessionId",
    "run_id",
    "container.id",
    "message.id",
    "request_id",
  ]);
});

test("beside its bar a span says the numbers its lines carry, by field name", () => {
  const span = {
    name: "segment read",
    service: "api",
    start_ms: 0,
    dur_ms: 5,
    error: false,
    parent: -1,
    id: "svc:api",
    parent_method: "",
    duration_method: "observed",
    lines: 1,
    where: [],
    self_ms: 5,
    critical: true,
    critical_inferred: false,
  };
  const spans = [
    { ...span, sums: { gets: 4, bytes_read: 1234 } },
    { ...span, id: "step", parent: 0 },
  ];
  const t = readTrace({ field: "request_id", value: "r-1", spans, total: 2 }, [], 1);
  expect(waterfallModel(t, [0, 1]).facts).toEqual(["bytes_read 1.2K · gets 4", ""]);
});

test("an operation field is one naming an operation, not a resource, name first", () => {
  const ranked = [
    stat("http.method", "enum"),
    stat("k8s.pod.name", "str"),
    stat("service.name", "enum"),
    { ...stat("op", "enum"), cardinality: 1 },
    stat("name", "str", 0.6),
    stat("http.route", "enum", 0.9),
  ];
  expect(operationField(ranked)).toBe("name");
  expect(operationField(ranked.filter((f) => f.name !== "name"))).toBe("http.route");
  expect(operationField(ranked.slice(1, 4))).toBeUndefined();
});

// gathering is the fields the factory corpus's lines could gather traces by, best first.
const gathering = correlations(
  (await answers.call("rank_fields", { selector: { time: range } })).data.fields,
);
const select = async (): Promise<HTMLSelectElement> => {
  await find('select[aria-label="Gather traces by"]');
  return (
    document.querySelector<HTMLSelectElement>('select[aria-label="Gather traces by"]') ??
    document.createElement("select")
  );
};

test("arriving from another view with only a traceId keeps get_trace's default field", async () => {
  render(`traceId=${session}`);
  await find(".tr-list button[aria-current]");
  expect(lists()).toMatchObject([{ group_by: gathering[0] }]);
  expect(requests("get_trace")).toEqual([{ value: session, time: range, limit: 10_000 }]);
  expect([...(await select()).options].map((o) => o.value)).toEqual(gathering.slice(0, 5));
});

test("logs without trace_id list traces by their best correlation field and open the first by it", async () => {
  render("");
  const first = await find(".tr-list button[aria-current]");
  const id = first?.title;
  await expect
    .poll(() => requests("get_trace"))
    .toEqual([{ value: id, time: range, field: gathering[0], limit: 10_000 }]);
  expect(first?.getAttribute("aria-current")).toBe("true");
  first?.click();
  expect(lists()).toMatchObject([{ group_by: gathering[0] }]);
  expect(location.pathname).toContain(`/field=${gathering[0]}`);
  expect((await select()).value).toBe(gathering[0]);
});

test("picking another field lists traces by it", async () => {
  render("");
  const picker = await select();
  picker.value = "task";
  picker.dispatchEvent(new Event("change", { bubbles: true }));
  await expect.poll(() => lists().at(-1)).toMatchObject({ group_by: "task" });
  expect(location.pathname).toContain("/field=task");
});

test("several services list the traces touching each, by the field the logs carry", async () => {
  render(where('service="orchestrator" service="postgres" level="info"'));
  await find(".tr-list p, .tr-list button");
  const each = (value: string) => ({
    measure: "count",
    where: [{ field: "service", op: "eq", value }],
  });
  const req: object = lists()[0] ?? {};
  const read: unknown[] = "measures" in req && Array.isArray(req.measures) ? req.measures : [];
  const n = read.length;
  expect(read.slice(-2)).toEqual([each("orchestrator"), each("postgres")]);
  expect(req).toMatchObject({
    selector: { where: [{ field: "level", op: "eq", value: "info" }] },
    group_by: gathering[0],
    having: [
      { index: 3, op: "gt", value: 1 },
      { index: n - 2, op: "gt", value: 0 },
      { index: n - 1, op: "gt", value: 0 },
    ],
  });
});

test("a list scoped to a service tries fields until one gathers its traces", async () => {
  render(where('service="sandbox-s"'));
  await find(".tr-list button[aria-current]");
  expect(lists().at(-1)).toMatchObject({ group_by: "task" });
});

test("an empty list says which field no line carries", async () => {
  render(`field=task/${where('service="postgres"')}`);
  await find(".tr-list p", "No trace of more than one line in this range by task.");
});

// listed is the traces the list shows.
const listed = () =>
  [...document.querySelectorAll<HTMLButtonElement>(".tr-list button[aria-current]")].map(
    (b) => b.title,
  );
// own is the page's params but the range, which reopen renders afresh as a shared link would.
const own = () =>
  location.pathname
    .split("/")
    .slice(3)
    .filter((s) => !s.startsWith("time="));
const reopen = () => render(own().join("/"));
// sent is the list request the page's URL asks for, once the view has sent it, and the
// engine's answer to it.
async function sent(): Promise<{ req: AggregateRequest; series: Series[] }> {
  const params = new URLSearchParams(own().join("&"));
  const [from = "", to = ""] = factory.time.split(",");
  const ranked = await fixtureClient(engine).call("rank_fields", {
    selector: { time: { from, to } },
  });
  const ends = traceEnds(ranked.data.fields);
  const req = listRequest(
    { from, to },
    Object.fromEntries(params),
    params.get("field") ?? "",
    ends,
  );
  await expect.poll(lists).toContainEqual(req);
  const answer = await fixtureClient(engine).call("aggregate", req);
  return { req, series: answer.data.series.filter((s) => s.key !== "") };
}
const measured = (s: Series, i: number) => s.measure_values?.[i]?.[0] ?? 0;
// filter adds a span filter in the list's editor: a field, a comparison and a value.
const filter = async (field: string, op: string, value: string) => {
  const form = await find(".tr-add");
  const set = (name: string, v: string) => {
    const el = form?.querySelector<HTMLInputElement | HTMLSelectElement>(`[name="${name}"]`);
    if (el) {
      el.value = v;
    }
  };
  set("field", field);
  set("op", op);
  set("value", value);
  form?.querySelector<HTMLButtonElement>('[type="submit"]')?.click();
  await expect.poll(() => decodeURIComponent(location.pathname)).toContain(value);
};

test.each([
  [
    "a duration of at least 500 ms lists only traces of 500 ms or more",
    "min=500",
    () => filter("duration", ">=", "500"),
    (s: Series) => measured(s, 5) >= 500,
  ],
  [
    "two services list only traces touching each",
    'where=service="workspace" service="orchestrator"',
    async () => {
      await filter("service", ":", "workspace");
      await filter("service", ":", "orchestrator");
    },
    (s: Series, n: number) => measured(s, n - 2) > 0 && measured(s, n - 1) > 0,
  ],
  [
    "a word and a field that must not match list only traces with a line both find",
    "q=claimed -service:workspace",
    async () => {
      await filter("", ":", "claimed");
      await filter("service", "!", "workspace");
    },
    (s: Series, n: number) => measured(s, n - 1) > 0,
  ],
])("%s, round-trips through the URL, and goes with its chip", async (_, param, act, holds) => {
  render("");
  await find(".tr-list button[aria-current]");
  await act();
  await expect.poll(() => decodeURIComponent(location.pathname)).toContain(`/${param}`);
  const { req, series } = await sent();
  expect(series.length).toBeGreaterThan(0);
  // The measures a filter asks to have come last, after those every list reads.
  expect(series.filter((s) => !holds(s, req.measures?.length ?? 0))).toEqual([]);
  await expect.poll(listed).toEqual(series.map((s) => s.key));
  root?.unmount();
  reopen();
  await expect.poll(lists).toContainEqual(req);
  // Its last chip goes, and with it the filter it says.
  const chips = () => [...document.querySelectorAll<HTMLButtonElement>(".tr-chips button")];
  await expect.poll(() => chips().length).toBeGreaterThan(0);
  const before = chips().length;
  chips().at(-1)?.click();
  await expect.poll(() => chips().length).toBe(before - 1);
});

// A trace of network calls, whose spans report their own times, keeps its tree and its
// services: none of it reads as a conversation.
test("a trace of calls is no conversation: its spans keep their names, services and tree", async () => {
  render("", incident);
  await find(".tr-trace .tr-nm", "");
  const rows = (cls: string) =>
    [...document.querySelectorAll(`.tr-trace .${cls}`)].map((n) => n.textContent ?? "");
  expect(rows("tr-nm").filter((n) => /^\d+\. /.test(n))).toEqual([]);
  expect(rows("tr-sv").filter((n) => / steps|tool (call|result)/.test(n))).toEqual([]);
  expect(document.querySelector(".tr-fold")?.textContent ?? "").not.toMatch(/hidden records/);
});

// The incident's first trace, a user's, runs from inside the ten-minute range to past its end,
// so it is read again with no time, and its lines over the time its spans cover; when that
// read is refused the trace stays as the range holds it.
const anytime = (tool: string, req: unknown) =>
  tool === "get_trace" && typeof req === "object" && req !== null && !("time" in req);
test.each([
  { name: "is read whole, its lines with it", refusal: undefined, to: "2026-07-18T14:59:59.634Z" },
  {
    name: "stays in the range when a read of it whole is refused",
    refusal: "too_large",
    to: "2026-07-18T14:09:56.643Z",
  },
])("a trace running over an edge of the range $name", async ({ refusal, to }) => {
  const answer = (tool: string, req: unknown) =>
    refusal && anytime(tool, req)
      ? Promise.reject(new ToolError({ code: refusal, message: "refused", retryable: false }))
      : undefined;
  render("field=user_id/traceId=user-02576", incident, { answer });
  await find(".tr-trace .tr-nm", "");
  expect(requests("get_trace").at(-1)).toEqual({
    value: "user-02576",
    field: "user_id",
    limit: 10_000,
  });
  expect(JSON.stringify(requests("search_logs").at(-1))).toContain(`"to":"${to}"`);
});

// The session, named with its field, lies inside the day: it is read once, over the range,
// and nothing looks for its lines past the range's edges.
test("a named trace inside the range is read once, over the range", async () => {
  openSession();
  await find(".tr-trace .tr-nm", "");
  expect(requests("get_trace")).toEqual([
    { value: session, time: range, field: "sessionId", limit: 10_000 },
  ]);
  expect(requests("aggregate").filter((r) => JSON.stringify(r).includes(session))).toEqual([]);
});

test("in Services no text box intersects another", async () => {
  const fill = vi.spyOn(CanvasRenderingContext2D.prototype, "fillText");
  const clear = vi.spyOn(CanvasRenderingContext2D.prototype, "clearRect");
  // The session's lanes have bars wide enough to label; the incident's traces, read whole,
  // span an hour of short calls.
  openSession();
  (await find('[aria-label="Mode"] button', "Services"))?.click();
  const lanes = await find(".tr-trace canvas");
  const on = (ctx: unknown): ctx is CanvasRenderingContext2D =>
    ctx instanceof CanvasRenderingContext2D && ctx.canvas === lanes;
  // drawn are the boxes of the text the lanes' last frame wrote.
  const drawn = () => {
    const last = Math.max(
      ...clear.mock.invocationCallOrder.filter((_, i) => on(clear.mock.contexts[i])),
    );
    return fill.mock.calls.flatMap(([text, x, y], i) => {
      const ctx = fill.mock.contexts[i];
      const box = lanes?.getBoundingClientRect() ?? new DOMRect();
      return on(ctx) && (fill.mock.invocationCallOrder[i] ?? 0) > last
        ? [new DOMRect(box.left + x, box.top + y - 6, ctx.measureText(text).width, 12)]
        : [];
    });
  };
  await expect.poll(() => drawn().length).toBeGreaterThan(0);
  const boxes = [
    ...drawn(),
    ...[...document.querySelectorAll(".tr-trace .tr-nm")].map((n) => n.getBoundingClientRect()),
  ];
  const overlap = (a: DOMRect, b: DOMRect) =>
    a.left < b.right && b.left < a.right && a.top < b.bottom && b.top < a.bottom;
  expect(boxes.flatMap((a, i) => boxes.slice(i + 1).filter((b) => overlap(a, b)))).toEqual([]);
  vi.restoreAllMocks();
});

// A trace of calls runs some at once at one depth; Flame stacks them rather than overlap.
test("in Flame no two bars overlap", async () => {
  const rect = vi.spyOn(CanvasRenderingContext2D.prototype, "roundRect");
  const clear = vi.spyOn(CanvasRenderingContext2D.prototype, "clearRect");
  render("", incident);
  await find(".tr-trace .tr-nm", "");
  (await find('[aria-label="Mode"] button', "Flame"))?.click();
  const canvas = await find(".tr-trace canvas");
  const on = (ctx: unknown) => ctx instanceof CanvasRenderingContext2D && ctx.canvas === canvas;
  // bars are the last frame's rects a bar's height, not a tick's or an underline's.
  const bars = () => {
    const last = Math.max(
      ...clear.mock.invocationCallOrder.filter((_, i) => on(clear.mock.contexts[i])),
    );
    return rect.mock.calls.flatMap(([x, y, w, h], i) =>
      on(rect.mock.contexts[i]) && (rect.mock.invocationCallOrder[i] ?? 0) > last && h >= 10
        ? [{ x, y, w, h }]
        : [],
    );
  };
  await expect.poll(() => bars().length).toBeGreaterThan(1);
  const all = bars();
  // A bar is snapped to whole buckets of time and drawn at least 3 px wide, so bars meeting
  // in time may share a few pixels; one over another shares more.
  const overlap = (a: (typeof all)[number], b: (typeof all)[number]) =>
    a.x + 3 < b.x + b.w && b.x + 3 < a.x + a.w && a.y + 0.5 < b.y + b.h && b.y + 0.5 < a.y + a.h;
  expect(all.flatMap((a, i) => all.slice(i + 1).filter((b) => overlap(a, b)))).toEqual([]);
  vi.restoreAllMocks();
});

// Shape opens on what this trace does: its operations lead, each marked.
test("Compare lists this trace's operations first, each marked", async () => {
  render("", incident);
  await find(".tr-trace .tr-nm", "");
  (await find('[aria-label="Mode"] button', "Compare"))?.click();
  const marks = () =>
    [...document.querySelectorAll(".tr-shape tbody tr")].map(
      (tr) => tr.lastElementChild?.textContent,
    );
  await expect.poll(() => marks().length).toBeGreaterThan(0);
  expect(marks()[0]).not.toBe("–");
});
