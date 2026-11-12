import { test } from "@playwright/test";
import { budget } from "../harness";
import { fling, serveRows } from "../pr/logs-fling";

test.beforeEach(serveRows);

// The 17 ms p95 ceiling allows for requestAnimationFrame's jitter, as in shell.spec.ts.
budget({ ...fling(10_000), name: "logs fling timing", max: { frameP95: 17, frameP99: 33 } });
