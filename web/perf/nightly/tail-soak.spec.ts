import { test } from "@playwright/test";
import { budget } from "../harness";
import { serveStream, tail } from "../pr/tail-stream";

test.describe.configure({ timeout: 15 * 60_000 });
test.beforeEach(serveStream);

budget({ ...tail(10 * 60_000), name: "live tail soak", max: { heapBytes: 150e6 } });
