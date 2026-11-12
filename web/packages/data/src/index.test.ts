import { createFakeClient, type Envelope, type LogLine, type SearchResponse } from "@datafocal/api";
import { expect, test, vi } from "vitest";
import { createTail } from "./index";

const line = (row: number, service = "db"): LogLine => ({
  timestamp: new Date(Date.UTC(2026, 9, 20, 14, 0, 0, row)).toISOString(),
  level: "info",
  message: `line ${row}`,
  pattern_id: `p${row % 3}`,
  service,
  segment: "s1",
  row,
});
const batch = (lines: LogLine[]): Envelope<SearchResponse> => ({
  data: { mode: "tail", lines },
  meta: {
    snapshot: "s",
    coverage: 1,
    approx: false,
    cost: { gets: 0, bytes_read: 0 },
    token_estimate: 0,
  },
});
const rows = (n: number, from = 0) => Array.from({ length: n }, (_, i) => line(from + i));

test("the ring buffer never holds more than its capacity", async () => {
  const client = createFakeClient({ followFn: async (_req, onBatch) => onBatch(batch(rows(100))) });
  const tail = createTail(client, { capacity: 10 });
  tail.start({});
  await vi.waitFor(async () => expect(await tail.window(0, 1000)).toHaveLength(10));
  tail.stop();
  expect((await tail.window(0, 1000)).map((l) => l.row)).toEqual(rows(10, 90).map((l) => l.row));
});

test("a stream that ends is followed again, keeping every line each sent", async () => {
  const streams = [rows(5), rows(6, 5)];
  const client = createFakeClient({
    followFn: async (_req, onBatch) => {
      const lines = streams.shift();
      if (lines) {
        onBatch(batch(lines));
      }
    },
  });
  const tail = createTail(client, { capacity: 100 });
  tail.start({});
  await vi.waitFor(async () => expect(await tail.window(0, 100)).toHaveLength(11), {
    timeout: 3000,
  });
  tail.stop();
  expect((await tail.window(0, 100)).map((l) => l.row)).toEqual(rows(11).map((l) => l.row));
});

test("a failed follow is the tail's error until the next batch", async () => {
  const refusal = new Error("unbounded_scan: search_logs: give it a time range");
  const results = [() => Promise.reject(refusal)];
  const client = createFakeClient({
    followFn: async (_req, onBatch) => (results.shift() ?? (async () => onBatch(batch(rows(1)))))(),
  });
  const tail = createTail(client, { capacity: 10 });
  tail.start({});
  await vi.waitFor(() => expect(tail.stats().error).toBe(refusal));
  await vi.waitFor(() => expect(tail.stats().error).toBeUndefined(), { timeout: 2000 });
  tail.stop();
});
