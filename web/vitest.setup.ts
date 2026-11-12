import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import type { TestProject } from "vitest/node";

// setup starts the Go tool surface over the seeded corpus for the tests that call it,
// providing its URL as baseUrl. The server exits when its stdin closes.
export default async function setup(project: TestProject): Promise<() => void> {
  const server = spawn("go", ["run", "./web/testserver"], {
    cwd: "..",
    stdio: ["pipe", "pipe", "inherit"],
  });
  const exited = once(server, "exit").then(([code]) => {
    throw new Error(`web/testserver exited with ${String(code)}`);
  });
  const [url] = await Promise.race([once(createInterface(server.stdout), "line"), exited]);
  project.provide("baseUrl", String(url));
  return () => server.stdin.end();
}
