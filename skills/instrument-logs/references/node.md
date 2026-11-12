# Node: JavaScript and TypeScript

Validated on two running services: an Express API in JavaScript (winston, morgan, mongoose, pm2) and a Fastify 5 API in TypeScript (pino, knex). TypeScript changes nothing about the logger configuration; only module and type plumbing differs.

## Format

| Logger | Default that misses | Configure |
|---|---|---|
| `pino` | `level` is a number. `time` is epoch ms. Errors are serialized only under `errorKey` (default `err`); anything else holding an Error comes out as `{}`. | `formatters: { level: (label) => ({ level: label }) }`, `timestamp: pino.stdTimeFunctions.isoTime`, `errorKey: 'error'`. Where pino comes only through a framework and isn't a direct dependency, use `` timestamp: () => `,"time":"${new Date().toISOString()}"` `` instead of importing it. Log errors as `log.error(err, 'constant message')`: with no message, pino uses `err.message` as `msg`. A custom `formatters.level` is rejected with multi-target `transport.targets`. |
| Fastify (5.10+) | `reqId: "req-1"`, a counter that restarts at 1 every boot. `responseTime` rather than `duration_ms`. `res.statusCode` nested, and no route or method on `request completed`. | The pino options above, plus `genReqId: () => randomUUID()` and `requestIdHeader: 'x-request-id'`, plus a `logController: new (class extends LogController { … })()` that sets `requestIdLogLabel: 'request_id'` and whose `requestCompleted(error, req, reply)` logs `method`, `route: req.routeOptions.url`, `status: reply.statusCode`, `duration_ms: reply.elapsedTime` (already fractional ms) and `error`. Before 5.10: `requestIdLogLabel` plus `disableRequestLogging: true` and an `onResponse` hook. |
| `winston` | `printf`/`simple`/`cli` text, often with `colorize()`, which puts ANSI codes into `level`. `logger.error(err)` puts the error text in `message`; `logger.error('msg', err)` appends it to the message; `{ error: err }` serializes to `{}`. Levels `http`, `verbose`, `silly` aren't levels DataFocal knows. | `format.combine(format.errors({ stack: true }), format.timestamp(), format.json())`, no `colorize()`. Log errors as `logger.error('constant msg', { error: err.message, stack: err.stack })`, never the Error object itself. Keep to error, warn, info, debug. The default `timestamp` key and its ISO value are already read. |
| `morgan` | A text access line with method, raw URL, status and IP in the message, and no request id. A morgan format function can't see the route template or the error. | Replace it with a ~20-line completion middleware (below) and remove the dependency. |
| `console.*` | No level, no time, no fields. | Route through the app's logger. |
| bunyan | Numeric `level`, `time` is ISO, message under `msg`. Not validated. | Map levels with a serializer or move to pino. |

## Request wrapper

- **Express 4:** a first middleware that records `process.hrtime.bigint()` and listens for `res.on('finish')`; `duration_ms = Number(process.hrtime.bigint() - start) / 1e6`. Capture what the completion needs in a closure at entry: `finish` runs outside the request's async context.
  - Route: `req.route.path` is relative to its router, and Express resets `req.baseUrl` once an error leaves the router. Record `req.baseUrl` in a middleware mounted with each router (`router.use(path, record, subRouter)`) and join it with `req.route.path` at completion. An unmatched request has no `req.route`: log no route.
  - Outcome slot: `res.locals`, filled by the error-handling middleware.
  - `finish` doesn't fire for an aborted request; listen to `close` and check `res.writableFinished` if those matter.
- **Fastify:** the `LogController` above. `requestCompleted` never sees errors that `setErrorHandler` rendered: add a `decorateRequest` slot (with `declare module 'fastify'` in TypeScript) and set it in the error handler. Route: `request.routeOptions.url`.
- **Koa:** not validated. `ctx._matchedRoute` holds the template.
- Log 4xx at `warn`; frameworks and boilerplates often log every 4xx at `error`.

## Ids and context

- **Pino child loggers on `req.log`** (Fastify does this) carry `request_id` into handlers with no other plumbing, but only for code that receives `req`.
- **`AsyncLocalStorage`:** `als.run(store, next)` in the first middleware, **and again after every middleware that resumes from a stream or event callback** (body parsers, multer, some auth libraries): body-parser calls `next` from the socket's context, so every later line on a request with a body loses its ids. Verify with a POST that has a JSON body.

## Libraries that print on their own

- knex: the `log: { warn, error, deprecate, debug }` option. Sequelize: `logging`. Prisma: `log` events. mysql2/pg: their event emitters. Otherwise they write multi-line text into the JSON stream.

## Lifecycle

- `process.on('uncaughtException')` and `unhandledRejection` are registered only once the entry module runs. In CommonJS, a config module that throws during `require` exits with a raw stack before them: require the logger and register the handlers first, or build a minimal logger that reads its level straight from `process.env`.
- Fastify with `@fastify/env`: `.env` is loaded after `Fastify()` has built the logger, so a level set there never applies. A default level of `silent` means production writes nothing.
- Version: `process.env.npm_package_version` exists only under `npm`/`yarn` scripts; otherwise read `package.json` (in ESM it needs `with { type: 'json' }`, and it may sit outside `rootDir`) or report it missing.
- Memory: `process.memoryUsage().rss`, and cgroup `memory.current` via `fs.readFileSync`. `unref()` the gauge's timer and clear it on close.

## Tests

- jest captures console output and wraps it with stack frames; that is not a sample. Score a run of the service instead.
- Tests that spy on `app.log.error` don't see calls made through `request.log`, which is a child with its own methods.
