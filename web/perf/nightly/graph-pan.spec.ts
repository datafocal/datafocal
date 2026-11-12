import { test } from "@playwright/test";
import { budget } from "../harness";
import { pan, serveRenderer } from "../pr/graph-pan";

test.beforeEach(serveRenderer);

// The nightly runner has a GPU; on software GL the graph's p95 would be nearer 33 ms.
budget({ ...pan, name: "graph pan and zoom timing", max: { frameP95: 17 } });
