import { test } from "@playwright/test";
import { budget } from "../harness";
import { serveStream, tail } from "./tail-stream";

test.beforeEach(serveStream);

budget({ ...tail(60_000), name: "live tail", max: { longFrames: 0 } });
