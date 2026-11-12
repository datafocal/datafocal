---
name: instrument-logs
description: Find where a codebase is missing the log events that would explain its own failures, slowness and resource use, add them, and configure its logger to emit a format DataFocal parses into fields. Works for any language and logger, with or without the obs observability context. Use when asked to add, improve or audit logging or observability in a repository.
argument-hint: "[path or area to focus on]"
---

# Instrument a codebase's logs

**Goal:** someone looking only at this program's logs in DataFocal can answer:
- Why is it slow?
- Why did it fail?
- What did one request, job or task do, end to end?
- What was it doing when it ran out of memory?

DataFocal derives everything from the lines themselves (fields, patterns, traces, the service graph). Two things decide how much it can derive:

1. **Where the events are.** This is most of the work: §2.
2. **Whether each line arrives in a format DataFocal parses into fields,** with keys it recognizes. This is usually a configuration change in one place: §4.

`$ARGUMENTS` narrows the scope to a path or area. With none, cover the whole repo.

**Language and producer specifics** are in `references/`, one file each: `go.md`, `python.md`, `node.md` (JavaScript and TypeScript), `java.md`, `rust.md`, `dotnet.md`, and `proxies-and-wrappers.md` (proxies, sidecars, process managers). Read the file for every language and producer in the repo before §2. Each one marks which advice was validated on a real repo and which wasn't.

## Ground rules

- **Use the logger the repo already uses.**
  - Change its configuration (format, key names, time encoding) in one place, at the entry point. Don't swap the library.
  - Remove logging configuration anywhere else, such as a `basicConfig()` in a helper module: whichever import runs first wins.
  - **Keep the stream it already writes to.** Never move logs to stdout when stdout carries a protocol (MCP or LSP over stdio, a CLI's output that gets piped).
- If the repo uses [obs](https://github.com/jcopi/obs), instrument through its event context (§5). The placement rules in §2 apply either way.
- **Smallest diff that fills the gaps.** Follow the repo's contribution rules (AGENTS.md, CLAUDE.md, CONTRIBUTING, STYLE): its diff-size caps, its lint (for example, rules against unobserved goroutines), and its error/warn conventions. Under a size cap, implement in the priority order in §6 and list the rest as a follow-up.
- **Constant messages are frozen:** their pattern ids are already in someone's dashboards. A message that interpolates data, or that is an error's own text, is not a stable pattern: replace it with a constant and move the data into keys, and list it as a change.
- **Key names may change only where DataFocal depends on them:**
  - a correlation id becomes `<thing>_id` (`task` → `task_id`);
  - `err` becomes `error`;
  - durations become `duration_ms`.
  
  List every rename in the report as a breaking change for existing queries.
- **Never log** secrets, tokens, credentials, request or response bodies, or personal data.
- **Never log per item inside a hot loop.** Log one event per unit of work, at its boundary.
- Work on a branch and commit.

## 1. Baseline

1. **Collect samples, one file per producer:** each binary, sidecar, proxy or script that writes to a stream. Pods often mix several producers. Use these sources:
   - The deployment's own logs, when reachable (`kubectl logs`, `--previous` for a crashed container, files).
   - **Run the start command the deployment runs** (the Dockerfile `CMD`, compose `command`, `npm start`, a process manager's config), not a shortcut like `node main.js`: process managers and wrappers rewrite every line (`references/proxies-and-wrappers.md`).
   - **List every producer that shares a stream:** a server's supervisor and its workers, a CLI wrapper's banner, migration and seed scripts, and in-process libraries that print to the console (database drivers, ORMs) instead of using the app's logger.
   - A local run the repo documents, using disposable dependencies: a throwaway database container, fake credentials, an unreachable API endpoint.
     - Check that the defaults reach real data, for example that the tenant you seeded is the one being served.
     - Drive a few requests, including failing ones.
   - The repo's own tests with logs on.
     - If tests inject a discard logger, swap in a real one temporarily and revert before committing.
     - Watch for log lines a test harness bridges or captures (a default logger, jest's console capture, pytest's capture): they are an artifact of the test, not product output.
2. **Score each sample:**
   ```
   python3 -I <this skill's dir>/scripts/check_logs.py producer-a.log producer-b.log
   ```
   - The checker parses each line the way DataFocal's ingest does: JSON, then logfmt, then text with a leading time or level.
   - It scores each file alone, then all of them together.
   - Exit status: 0 with no warnings, 1 with warnings, 2 on bad usage.
   - Keep the output.
3. **Note what the baseline can't show:**
   - **An empty sample is a category 1 failure.** Loggers default to `silent`/`off` or a null handler, or read their level from an env file that is loaded after the logger is built. Run without any dev-only env file, and fix the default.
   - **A sparse baseline is a finding.** If a deployment wrote five lines in an hour of real work, most of §2 is missing.
   - **Events at `debug` are invisible at the default level.** Note which work is logged only there.
   - **Keep a like-for-like pair.** If part of the baseline comes from a deployment you can't redeploy, also take a local baseline that you will rerun after the change with the same driver.

## 2. Map where events belong (the main work)

**Build the placement map before editing anything.** For each site, record:
- `file:line`
- which category below it falls under
- the event it should emit (its constant message, plus keys)
- whether the event exists already, exists but is incomplete, or is missing

Find sites by reading the entry points and following the work. Use the search hints as a starting point, not as the list.

| # | Category | Look for | The event it needs |
|---|---|---|---|
| 1 | **Process lifecycle** | `main`, server start, signal handling, `os.Exit`, `sys.exit`, `process.exit`, `System.exit`, top-level `panic`/`raise`/`throw`, config loading | `starting` (version, non-secret config summary). `listening`/`ready` (addr), logged **after the bind succeeds**, never before it. `stopping` (reason). Every fatal exit **through the logger**, at the highest level the logger has, with `error`. A `fmt.Println`, `print` or `console.error` on the way out produces a line with no level or time. If the build carries no version, use what the runtime exposes; if there's none, say so in the report rather than inventing one. See the notes below the table. |
| 2 | **Inbound units of work** | HTTP/gRPC handlers and middleware, router setup, queue/stream consumers, CLI subcommands, cron/scheduled jobs, webhook receivers, generic worker pools | One completion event per unit from **one wrapper per boundary type**: route template, `status`, `duration_ms`, the unit's id, its **work size** (rows, bytes, objects read, items), and `error`. See the notes below the table. |
| 3 | **Outbound calls** | HTTP clients, DB queries, caches, object stores, message publishes, subprocess/exec, LLM/API SDK calls | One event per call: the peer, the operation, `duration_ms`, the outcome, and the **id propagated from the inbound unit**. On a hot path, log only failures and calls over a latency threshold. The same id logged by caller and callee is how DataFocal draws the edge between two services. |
| 4 | **Entity lifecycles and state machines** | status/state fields, enum transitions, `switch` on a state, job/task/session/order models | One event per transition (`task queued`, `task started`, `task failed`) carrying `<entity>_id`, the old and new state, `attempt`. These are what a trace or timeline view is built from. |
| 5 | **Retries, timeouts, fallbacks, limits** | retry loops, backoff, timeouts and deadlines, circuit breakers, rate limiters, "fallback"/"degraded" branches, **failure caps and dead-letter conditions** (a `failures < cap` in a claim query, a max-attempts check) | A `warn` with `attempt`, the cause in `error`, and what happens next. When a cap is reached and the work **stops being retried**, log an `error`: that is the most silent failure there is. A degraded mode that is never logged looks like a healthy one. |
| 6 | **Error handling sites** | swallowed errors (`_ = err`, empty `catch {}`, `.catch(() => {})`, `except: pass`, ignored results), errors only persisted to a database, and the place an error is finally handled | Log **once, where the error is handled**, never at every `return err` on the way up. Every swallowed error that is a real failure gets an event. These are where silent failures live. |
| 7 | **Memory and resource pressure** | memory-heavy steps (loading a whole file, batch, hour or result set), caches, pools, queues/channels, one-shot jobs (CronJobs, batch runs) | See the notes below the table. |
| 8 | **Background workers** | goroutines, threads, async tasks, worker pools, `setInterval`, tickers, schedulers | Start and stop with the worker's name, and recovered panics/exceptions at `error`. A worker that dies silently stops work without a trace. |
| 9 | **Decisions that change behavior** | feature flags, config branches, "skip"/"ignore"/"dedupe" paths, cache-hit-vs-miss branches that change cost | One event at `debug` or `info` naming the branch taken, so a behavior change shows up as a new pattern. |

**Category 1: process lifecycle.**
- **Failures before the logger exists.** Configuration is often validated while modules load, before logging is set up, so a bad setting exits with a raw multi-line trace. Configure a minimal logger first (its level read straight from the environment), or catch at the entry point and write one structured line. Register uncaught-error handlers before loading anything that can throw.
- **Exits that bypass the logger:** the runtime's own report of an error returned from `main`, the default panic or uncaught-exception printer, `.unwrap()`/`.expect()` at startup. Log the error through the logger first, and install a panic or uncaught-exception hook that logs at `error` and then defers to the previous one.
- **A `stopping` line with no exit after it** means a handle (a database pool, a timer) keeps the process alive until it is killed. Check that a `stopped` line follows within the grace period. Report a hang rather than changing shutdown behavior yourself.

**Category 2: inbound units of work.** The language files give each framework's route template, outcome slot and wrapper placement.
- **Route template:** log the template (`/tasks/{id}`), not the raw path. A request that matched no route gets no `route`, not its raw path.
- **Getting the error to the wrapper.** Handlers and framework error handlers usually write the error response themselves, so the wrapper never sees the error. Use the framework's outcome slot if it has one; otherwise give the request context a small mutable object, fill it where the error response is rendered, and read it in the wrapper. Mutate the object: in several runtimes, a value *set* below the wrapper doesn't flow back up.
- **An exception that escapes into the wrapper** usually arrives before the status is set. Report it as 500 yourself, and put panic or exception recovery inside the logging wrapper.
- **Wrapping the response** must not break streaming or upgrades: use the framework's own completion hook, or a writer that forwards flush and hijack.
- **Requests that never complete** (client aborts) may never fire the completion hook. Listen for the close as well if they matter.
- **Routes outside the wrapper.** Check every route the wrapper doesn't cover: routes mounted on an outer mux, static files, and other transports that share the handlers (MCP vs HTTP, gRPC vs REST).
- **The server's own access log:** once the wrapper logs completions, turn it off. It logs the raw path and query string, which carry emails and tokens, and it duplicates the event without a route or duration.
- **The container may log an escaped exception itself,** after the wrapper has returned and its context is gone (servlet containers, ASGI servers). Lower that logger, or accept the duplicate and say so.
- **Health and readiness probes** log at `debug` whatever their status: a probe failing during startup is normal.
- **Level of a completion:** follow the repo's style guide. Without one, use `warn` for a 4xx (the client's failure) and for work that will be retried, and `error` for a 5xx and for work that fails for good.
- **Form apps hide failures in a 200:** a rejected submit re-renders the form while success redirects. Add an outcome key (`validation_errors`) where that matters.
- **Generic wrappers that serve several entity kinds** may name the id `<kind>_id` when the kind comes from a fixed set in the code (`task`, `session`). That is bounded, unlike a key built from data.

**Category 7: memory and resource pressure.**
- **Before each memory-heavy step,** log its input size: `merging segments inputs=361 rows=…`. An OOM kill means the completion event never prints, so this start event is the only line that names what killed the process. It matters most in one-shot jobs, where a periodic gauge never fires.
- **In long-running processes, add a periodic gauge,** from one place, every 15 to 60 s:
  - the memory figure the kill is based on: in a container, cgroup `memory.current`; the language's heap figure is lower than what the kernel counts. Outside a container, the cgroup figure covers a whole session or service, so log it only when the process has its own cgroup, and log the process RSS otherwise;
  - threads or goroutines;
  - queue depths;
  - cache entries and hit/miss counts;
  - pool use against the pool's maximum.
- **When a limit bites, log one event:** cache over budget, queue full, slow consumer dropped, backpressure applied, batch truncated.

**Don't add an event when:**
- a completion event at the boundary already says the same thing;
- it would fire per item in a loop;
- it only restates the line above it.

Prefer completing an existing event (adding `duration_ms`, an id, `error`) over adding a new one.

**In a library rather than an application:**
- Emit events only through a logger the caller injects or configures (an injected `slog.Logger`, `logging.getLogger(__name__)`, an injected pino instance, SLF4J, the `tracing` macros, an `obs` context taken from `context.Context`).
- Never configure output format, and never write to stdout or stderr directly.
- Categories 3 to 7 apply; categories 1 and 2 belong to the application.
- An application's own internal packages are not a library: follow the repo's convention, for example a process-wide default logger.

## 3. The event contract

These are the keys DataFocal recognizes. Every rule below comes from how DataFocal ingests a line.

| Key | Rule |
|---|---|
| `ts` (or `time`, `timestamp`) | An RFC 3339 **string** with a zone (UTC preferred), a colon in the offset (`+00:00`, not `+0000`) and sub-second precision. A time without a zone is ambiguous across hosts. An epoch number is not read as time, and neither is `@timestamp`: the line falls back to the collector's read time. **Write one time key:** when several parse, DataFocal uses the last in sorted order (`time`, `timestamp`, `ts`), so a zone-less `ts` beats a correct `timestamp`. |
| `level` (or `lvl`, `severity`) | A **string**: `debug`, `info`, `warn`, `error`, `fatal`. These normalize, in any case: `trace`, `dbg`, `notice`, `warning`, `err`, `critical`, `panic`. Numeric levels (pino's `30`, syslog's `6`) and custom ones (slog's `ERROR+4`) are not levels to datafocal. A logger with no fatal level logs fatal exits at `error`. |
| `msg` (or `message`) | **A constant string naming the event**: `request done`, `task failed`, `cache evicted`. Put every variable in a key, never `key=value` inside the message. DataFocal groups lines into patterns by masking only tokens that contain a digit, so `user alice failed` and `user bob failed` become two patterns, while `user failed` with `user=alice` is one. It must be a top-level string, never an object or a nested field. |
| `<entity>_id` | Anything someone will look up or join on: `request_id`, `trace_id`, `task_id`, `session_id`, `user_id`. DataFocal treats any `*_id` key as a correlation id: it builds lookup filters for it, groups traces by it, and links services that share its values. **Only the `_id` suffix is looked up:** `requestId`, `reqId`, `trace.id` and `traceId` are not. Use **the same top-level key for the same id everywhere**: an id nested under a span object (`span.request_id`) is a different column from a peer's `request_id`. Propagate it across process boundaries (headers, message metadata) so both sides log it. **A correlation id is a string, unique across processes and restarts:** a per-process counter (`req-1`) collides between replicas, and DataFocal links services only through string ids. |
| `trace_id`, `span_id`, `parent_span_id`, `span.name` | If the repo already has tracing (OpenTelemetry or similar), copy these onto log lines from the active span. If it doesn't, don't add a tracer; `request_id` plus `duration_ms` is enough for DataFocal to derive spans. |
| `duration_ms` | A **number** of milliseconds, with a fractional part, on every completion event: in Go, `float64(d.Microseconds())/1000`. Whole milliseconds turn fast calls into 0. DataFocal treats only this key as latency (`p95`, slow-call ranking). Don't use `elapsed: "1.2s"`, `took`, `latency_ns`, or a duration in seconds. |
| `error` | Spelled `error`, not `err`: the error's text as a string, or an error object DataFocal flattens to `error.message`. It goes on every `error`/`fatal` event, and on `warn` events that have a cause. Check the output: most runtimes serialize an error object's own fields as `{}` unless the logger has an error serializer for that key. Put a stack trace in a field (`stack`, or the logger's own stack key), never on its own lines: continuation lines become separate records with no level. A text producer that can't change can declare the source's `record_start` pattern, so DataFocal folds continuation lines into the previous record's `detail`. |
| `service` (or `service.name`) | Only when one process hosts several logical services, or when it runs outside Kubernetes and its file name doesn't name it. Otherwise DataFocal takes it from the pod or container. |
| counts and sizes | Numbers, not strings: `bytes`, `rows`, `attempt`, `queue_depth`, `status`. |

- **Bounded keys.** Never build key names from data: a map keyed by ids, paths or user input becomes one column per distinct value. Keep nesting shallow, since nested objects become dotted columns. Never log a large payload object as fields.
- **LLM calls:** if the repo calls a model, use the OpenTelemetry `gen_ai.*` names on the call's event (`gen_ai.request.model`, `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens`, `gen_ai.conversation.id`), plus `duration_ms`.

## 4. Match a format DataFocal knows

DataFocal parses three line shapes into fields:
- **JSON**, one object per line;
- **logfmt** (`key=value key="quoted value"`);
- **klog** (`I1020 12:00:00.123456 1 file.go:12] msg`).

Text with a leading time or level word parses too, but only `key=value` pairs at the end become fields, and the level word stays inside the message.

**Keep logfmt or JSON if the repo already emits one; otherwise choose JSON.** Most loggers' defaults are close, but several miss on exactly the keys in §3. **Fix this in the logger's configuration, in one place,** using the language file's table. Check the version the repo uses: option names move between major versions.

What to check in every producer's output, whatever the language:
- one line per event, with no color codes (many loggers color even when not writing to a terminal);
- a string time under `ts`, `time` or `timestamp`, and only one of them;
- a string level under `level`;
- a constant message under `msg` or `message`, at the top level;
- ids with the `_id` suffix at the top level, not nested under a span or context object;
- an `error` that holds text, not `{}`;
- the stack trace in a field.

**Producers besides the app's logger** (`references/proxies-and-wrappers.md`):
- **Proxies and sidecars** the repo configures (Envoy, agentgateway, nginx). Their access logs usually have no message and a duration written as a string.
- **Process managers and wrappers** (pm2, supervisord, gunicorn, uvicorn's supervisor, `npm run`, systemd). Several prefix every line, which turns valid JSON into unparsed text.
- **In-process libraries with their own output** (database clients, ORMs, HTTP clients). Route them into the app's logger through their logging hook.

**Validate any proxy config with the pinned binary** (`--validate-only`, `nginx -t`, `envoy --mode validate`) before committing. A rejected config stops the proxy from starting.

**If a producer can't be configured to fit** (shell scripts, subprocess output, a third-party binary with fixed output), leave it alone. List it as a **format gap** in the report, with three sample lines and the producer's name, so DataFocal can add it as a known format.

## 5. Using the obs observability context (Go)

`obs` (`github.com/jcopi/obs/fnchain`) carries an event context through `context.Context`. Child events copy the parent's fields, so a boundary attaches its ids once and every event below it carries them.

**The pattern:**
- At each boundary from §2, derive the unit's event from the incoming context:
  ```go
  ev := fnchain.FromContextOrRoot(ctx).Event(fnchain.InfoLevel, fnchain.MethodEvent)
  ```
- Attach the unit's ids: `ev.With().Str("request_id", id)`.
- Pass `fnchain.ContextWithEvent(ctx, ev)` down.
- Below it, emit each event as:
  ```go
  fnchain.FromContextOrRoot(ctx).Event(lvl, typ).With().Msg("constant message")….Evt().End()
  ```

**Keys, against §3:**
- `Msg` writes `msg`, and `Err` writes `error`. Both already match.
- `obs` writes no time. Add `Time("ts", time.Now().UTC())` on each event, or rely on the collector's read time where the container runtime stamps lines (Kubernetes does).
- `Dur(key, d)` writes **seconds**. For latency, write `Float64("duration_ms", float64(d.Microseconds())/1000)` instead.
- The event's own id and its parent's are written as `@id` and `@pid`. DataFocal reads neither as a span link. Until `obs` names them `span_id` and `parent_span_id`, also attach the unit's id as `request_id` (or `trace_id`) at the boundary, so every event in the unit shares it.
- Leave `NoLevel` off events that are emitted: it writes `"level": "none"`.

**Validity:** run the checker on real `obs` output.
- If it reports lines that don't parse, `obs` itself is writing invalid JSON. Report that as a format gap for `obs`, with sample lines.
- Don't work around it in the repo.

## 6. Implement

**Priority, highest value per line first:**
1. Category 2 (one wrapper each).
2. Category 6 (swallowed errors).
3. Category 7's start events.
4. Category 4.
5. Category 5.
6. Category 1.
7. The rest of category 7 (the gauge).
8. Category 3, which often needs logger plumbing.

**How:**
- **Attach ids once, at the boundary.** Put a logger that carries the ids into the request/job context, and log through it downstream. Each language file says how, and where the context is lost: across threads, spawned tasks, stream callbacks, and after the wrapper returns. **Verify it:** log from a handler on a request with a body, and from work the request spawns, and check that both lines carry the id.
- **The periodic gauge** is one goroutine, thread or timer, or a ticker case in an existing main loop where the repo's lint forbids unobserved goroutines. It starts at the entry point and stops on shutdown.
- **A package with no logger:** add one through the constructor or dependencies struct it already takes. If tests build it in many places, give it a discard default or a test-helper default so call sites don't change.
- **Tests:**
  - Keep existing tests green.
  - Where the repo tests its middleware or wrappers, add a test that the completion event carries `duration_ms`, the id, and `error` on failure.
  - Don't make tests assert on message wording.
  - Existing tests that spy on the logger or assert on its keys break on renames. Update them to assert fields, and list them in the report.

## 7. Verify

1. Build, lint and test with the repo's own commands. Report what they actually returned.
2. Rerun the like-for-like baseline driver against the new build, and **exercise every site on the placement map** you can: a request, a failing request, a 5xx, a background tick, a retry, a fatal start (bad flag, port in use).
   - Sites that need privileges or infrastructure you don't have can be covered by unit tests instead.
   - List every site verified only by tests, or not at all.
3. Run `check_logs.py` on the new samples, per producer.
   - Warnings should go down, and none should be new.
   - Explain any warning that remains. A warning that comes only from a third-party library's lines (the checker shows the logger name when the line has one) is not yours to fix: say so.
   - Use its list of frequent templates with no `*_id` to check the placement map for gaps.

## 8. Report

- **Placement map:** each site's `file:line`, its category, its event, and its status: added, completed, already fine, or deferred (with why).
- **Key renames:** each one, as a breaking change for existing queries.
- **Format changes:** the one configuration change per producer, and what it fixed.
- **Checker output** before and after, per producer.
- **Format gaps:** producers (including `obs`) whose output DataFocal can't parse into fields, with sample lines.
- **Sites deferred,** and why: hot path, already covered, size cap, or needing a decision from the owner. Under a size cap, present them as the proposed follow-up change.
