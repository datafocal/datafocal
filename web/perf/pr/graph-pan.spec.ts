import { test } from "@playwright/test";
import { budget } from "../harness";
import { pan, serveRenderer } from "./graph-pan";

test.beforeEach(serveRenderer);

// The draw-call and node budgets are per frame, so pan checks them itself.
budget({ ...pan, name: "graph pan and zoom" });
