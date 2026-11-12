import { expect, inject, test, vi } from "vitest";
import { createHttpClient, ToolError } from "./client";

declare module "vitest" {
  // baseUrl is the Go tool surface vitest.setup.ts starts.
  export interface ProvidedContext {
    baseUrl: string;
  }
}

const client = createHttpClient({ baseUrl: inject("baseUrl") });
const incident = { from: "2026-07-18T14:00:00Z", to: "2026-07-18T14:10:00Z" };

test("search_logs round-trips through the tool surface", async () => {
  const env = await client.call("search_logs", { selector: { time: incident }, limit: 2 });
  expect(env.data.mode).toBe("scan");
  expect(env.data.lines).toHaveLength(2);
  expect(env.meta.coverage).toBe(1);
});

test("a refusal becomes a ToolError carrying its code and hint", async () => {
  const invalid = client.call("search_logs", { selector: { time: { from: "bogus" } } });
  await expect(invalid).rejects.toBeInstanceOf(ToolError);
  await expect(invalid).rejects.toMatchObject({ code: "invalid_request" });

  const unknown = client.call("get_trace", { field: "no_such_id", value: "x", time: incident });
  await expect(unknown).rejects.toMatchObject({
    code: "unknown_field",
    hint: "call rank_fields to see which columns this tenant emits; any of them can group a trace",
  });
});

test("follow hands over each streamed batch", async () => {
  const abort = new AbortController();
  const batches: unknown[] = [];
  const onBatch = (b: unknown) => {
    batches.push(b);
    abort.abort();
  };
  const req = { selector: { time: incident }, limit: 2 };
  await expect(client.follow(req, onBatch, abort.signal)).rejects.toThrow(/abort/i);
  expect(batches).toHaveLength(1);
});

test("a frame's calls go in one batch stream, and each is asked once", async () => {
  const fetches = vi.spyOn(globalThis, "fetch");
  const batched = createHttpClient({ baseUrl: inject("baseUrl") });
  const req = { selector: { time: incident }, limit: 2 };
  const [a, b, c] = await Promise.all([
    batched.call("search_logs", req),
    batched.call("search_logs", { ...req, limit: 1 }),
    batched.call("search_logs", req),
  ]);
  expect(
    fetches.mock.calls.map(([url]) =>
      (typeof url === "string" ? url : "").replace(/^.*\/v1\//, ""),
    ),
  ).toEqual(["batch/stream"]);
  expect(a?.data.lines).toHaveLength(2);
  expect(b?.data.lines).toHaveLength(1);
  expect(c).toBe(a);
});
