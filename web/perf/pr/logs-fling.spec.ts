import { test } from "@playwright/test";
import { budget } from "../harness";
import { fling, serveRows } from "./logs-fling";

test.beforeEach(serveRows);

// A 3 s fling, inside the per-PR cap; the nightly run flings for 10 s. At most 720 px of
// 24 px rows are visible, with 10 rows of overscan on each side.
budget({ ...fling(3000), name: "logs fling", max: { domRows: 31 + 2 * 10, longFrames: 0 } });
