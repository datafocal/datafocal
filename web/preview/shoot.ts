// make preview: each page in PREVIEW_PAGES, or every view, served from dist and written as
// PREVIEW_OUT/<name>.png, to attach to a pull request.
import { glob, readFile } from "node:fs/promises";
import { extname, join } from "node:path";

import { chromium } from "playwright-core";
import type { DrawStats } from "../packages/render-core/src";

// PREVIEW_CHROME names a browser already installed, so Playwright never has to download
// one; unset, as in CI's Playwright image, Playwright's own.
const chrome = process.env["PREVIEW_CHROME"];
const origin = "http://datafocal.preview";
const viewport = { width: 1280, height: 800 };
const tallest = 6000;
const types: Record<string, string> = {
  ".html": "text/html",
  ".js": "text/javascript",
  ".css": "text/css",
};

// A scenario opens its view's route at path, the corpus window its view's engine answers
// read: the factory corpus's last day for AI, trace and views, the incident's for logs, graph
// and resources. A page naming no time= reads that window too, or a tab's own where the corpus
// lacks what the tab shows: the factory logs no quota, so Quota reads every agent source's.
// It is shot once ready matches an element and, for a canvas view, a draw put marks on it.
interface Scenario {
  path: string;
  ready: string;
  canvas?: boolean;
  tabs?: Record<string, string>;
}
const factory = `time=${encodeURIComponent("2026-07-27T00:00:00Z,2026-07-28T00:00:00Z")}`;
const incident = `time=${encodeURIComponent("2026-07-18T14:00:00Z,2026-07-18T14:10:00Z")}`;
const sources = `time=${encodeURIComponent("2025-09-01T00:00:00Z,2026-11-01T00:00:00Z")}`;
const scenarios: Record<string, Scenario> = {
  "/ui/ai": {
    path: `/ui/ai/${factory}`,
    ready: ".ai-tab",
    tabs: { quota: sources, memory: sources },
  },
  "/ui/logs": {
    path: `/ui/logs/${incident}/query=${encodeURIComponent('{user_id="user-00042"}')}`,
    ready: '[role="row"]',
  },
  "/ui/graph": {
    path: `/ui/graph/${incident}`,
    ready: '[aria-label="Service graph"] circle',
  },
  "/ui/resources": {
    path: `/ui/resources/${incident}`,
    ready: 'table[aria-label="Services"] tbody tr',
  },
  "/ui/slos": {
    path: `/ui/slos/${incident}`,
    ready: ".slo-card canvas",
  },
  "/ui/views": {
    path: `/ui/views/${factory}`,
    ready: 'table[aria-label="Views"] tbody tr, .vw-cell, .vw-builder form',
  },
  "/ui/trace": {
    path: `/ui/trace/${factory}/field=sessionId/traceId=842e33af-9917-f20e-e46e-5bec950ddc23/where=${encodeURIComponent('service="workspace"')}`,
    ready: ".tr-row",
    canvas: true,
  },
};

// canonical is v's JSON with every object's keys sorted, so equal requests are equal strings.
const canonical = (v: unknown) =>
  JSON.stringify(v, (_, x: unknown) =>
    x && typeof x === "object" && !Array.isArray(x)
      ? Object.fromEntries(Object.entries(x).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : x,
  );

// answers are the engine's to each request a view lists in its requests files (make
// view-fixtures), keyed by tool and request; each corpus's requests read its own window.
const answers = new Map<string, unknown>();
const packages = new URL("../packages/", import.meta.url).pathname;
for await (const file of glob("view-*/fixtures/*.json", { cwd: packages })) {
  const engine: { tool: string; req: unknown; answer: unknown }[] = JSON.parse(
    await readFile(join(packages, file), "utf8"),
  );
  for (const { tool, req, answer } of engine) {
    answers.set(`${tool} ${canonical(req)}`, answer);
  }
}

// Page is one picture: its path and, for a state only a click reaches, the element to click
// once ready, then the one its answer shows. ready replaces the scenario's for a path that
// shows something else first, as Patterns does in place of the lines.
interface Page {
  name: string;
  path: string;
  ready?: string;
  click?: string;
  shows?: string;
}
// Unset, PREVIEW_PAGES is every scenario's route, named for its view.
const pages: Page[] = process.env["PREVIEW_PAGES"]
  ? JSON.parse(process.env["PREVIEW_PAGES"])
  : Object.keys(scenarios).map((path) => ({ name: path.slice("/ui/".length), path }));
const out = process.env["PREVIEW_OUT"] ?? ".";
const dist = new URL("../dist/", import.meta.url).pathname;

const browser = await chromium.launch(chrome ? { executablePath: chrome } : {});
// Every page is shot that can be, so one that cannot leaves the others' pictures.
const failed: string[] = [];
try {
  for (const p of pages) {
    await shoot(p).catch((e: unknown) => failed.push(String(e)));
  }
} finally {
  await browser.close();
}
if (failed.length > 0) {
  throw new Error(failed.join("\n"));
}

// urlOf is the scenario's own path for its bare route, else path in the window its answers
// were recorded over when it names none.
function urlOf(path: string, route: string, s: Scenario): string {
  if (path === route) {
    return s.path;
  }
  if (/\/time=/.test(path)) {
    return path;
  }
  const tab = /\/tab=([^/]+)/.exec(path)?.[1] ?? "";
  const time = s.tabs?.[tab] ?? s.path.slice(route.length + 1).split("/")[0];
  return `${route}/${time}${path.slice(route.length)}`;
}

// shoot writes name.png of path, the scenario for its route or, given more, path itself,
// failing on anything that would make the picture something other than that scenario.
async function shoot({ name, path, ready, click, shows }: Page) {
  const route = path.split("/").slice(0, 3).join("/");
  const scenario = scenarios[route];
  if (!scenario) {
    throw new Error(`${name}: no preview scenario for ${route}; have ${Object.keys(scenarios)}`);
  }
  const s = { ...scenario, ...(ready && { ready }) };
  // Dark, as the UI follows the system and its users' is; headless Chrome's is light.
  const page = await browser.newPage({ viewport, colorScheme: "dark" });
  const failures: string[] = [];
  page.on("pageerror", (err) => failures.push(`uncaught: ${err.message}`));
  page.on("console", (msg) => msg.type() === "error" && failures.push(`console: ${msg.text()}`));
  page.on("response", (res) => res.ok() || failures.push(`${res.status()} ${res.url()}`));
  await page.route("**/*", async (r) => {
    const { pathname } = new URL(r.request().url());
    if (pathname === "/v1/batch/stream") {
      // A batch is answered as the querier streams it: open, then each call's own answer.
      const { calls } = r.request().postDataJSON() as { calls: { tool: string; args: unknown }[] };
      const events = calls.map((c, index) => {
        const key = `${c.tool} ${canonical(c.args)}`;
        const answer = answers.get(key);
        if (answer === undefined) {
          failures.push(`no answer for ${key}`);
        }
        return `event: result\ndata: ${JSON.stringify({ index, envelope: answer })}\n\n`;
      });
      return r.fulfill({
        contentType: "text/event-stream",
        body: `event: open\ndata: {}\n\n${events.join("")}`,
      });
    }
    if (pathname.startsWith("/v1/")) {
      const req = r.request().postDataJSON() as { selector?: { time?: { from?: string } } };
      // A tail's from moves to its newest line on each follow, so a follow is answered as one
      // event by its selector's search up to now.
      const follow = pathname === "/v1/search_logs/follow";
      if (follow) {
        delete req.selector?.time?.from;
      }
      const key = `${follow ? "search_logs" : pathname.slice(4)} ${canonical(req)}`;
      const answer = answers.get(key);
      if (answer === undefined) {
        failures.push(`no answer for ${key}`);
        return r.fulfill({ status: 404 });
      }
      return follow
        ? r.fulfill({
            contentType: "text/event-stream",
            body: `data: ${JSON.stringify(answer)}\n\n`,
          })
        : r.fulfill({ json: answer });
    }
    // As in web.Handler, a path with no file behind it is a route inside the app.
    const file = extname(pathname) ? pathname.replace(/^\/ui\//, "") : "index.html";
    const body = await readFile(join(dist, file)).catch(() => undefined);
    return body
      ? r.fulfill({ body, contentType: types[extname(file)] ?? "" })
      : r.fulfill({ status: 404 });
  });
  const url = urlOf(path, route, s);
  await page.goto(origin + url, { waitUntil: "networkidle" });
  try {
    await page.waitForFunction(
      // Every panel's answer is in: one still loading would be shot as "Loading…".
      ({ ready, canvas }) =>
        document.querySelector(ready) &&
        !document.body.innerText.includes("Loading…") &&
        (!canvas ||
          performance
            .getEntriesByName("draw")
            .some((e) => ((e as PerformanceMark).detail as DrawStats).primitives > 0)),
      s,
      { timeout: 10_000 },
    );
    // The frame the last draw went into is on screen once the next one starts.
    await page.evaluate(() => new Promise(requestAnimationFrame));
  } catch {
    failures.push(`never showed ${s.ready}${s.canvas ? " with a canvas drawn" : ""}`);
  }
  if (click && failures.length === 0) {
    await page.click(click);
    await page.waitForSelector(shows ?? click, { timeout: 10_000 }).catch(() => {
      failures.push(`clicking ${click} never showed ${shows ?? click}`);
    });
    await page.waitForLoadState("networkidle");
  }
  if (failures.length > 0) {
    throw new Error(`${name}: ${url}\n  ${failures.join("\n  ")}`);
  }
  // The view scrolls inside the shell's main, not the document, so the page is made as tall
  // as main's content, up to tallest, for a panel below the fold to be in the picture too.
  const below = await page.evaluate(() => {
    const main = document.querySelector(".shell > main");
    return main ? main.scrollHeight - main.clientHeight : 0;
  });
  if (below > 0) {
    await page.setViewportSize({ ...viewport, height: Math.min(viewport.height + below, tallest) });
    await page.evaluate(() => new Promise(requestAnimationFrame));
  }
  await page.screenshot({ path: join(out, `${name}.png`) });
  await page.close();
}
