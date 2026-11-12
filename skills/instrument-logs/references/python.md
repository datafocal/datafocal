# Python

Validated by reading code only, not by running it, on a FastAPI service (uvicorn, stdlib `logging`, SQLModel, alembic). The facts below come from the source of uvicorn 0.49, FastAPI 0.141, Starlette 1.3, python-json-logger 4.2 and structlog.

## Format

| Logger | Default that misses | Configure |
|---|---|---|
| stdlib `logging` | `basicConfig()`'s default `INFO:app.utils:msg` doesn't parse at all: the level is glued to the logger name. A `basicConfig()` in any imported module silently wins if it runs first. | Without a new dependency: a ~25-line `logging.Formatter` subclass that `json.dumps` `ts` = `datetime.fromtimestamp(record.created, UTC).isoformat(timespec="microseconds")`, `level` = `levelname.lower()`, `msg` = `record.getMessage()`, the record's `extra=` attributes, and for `exc_info` an `error` (`"Type: text"`) plus the traceback in `stack`. Install it with `basicConfig(handlers=[h], force=True)` at the entry point, and delete every other `basicConfig()`. |
| `python-json-logger` (≥ 3.1) | With no `fmt`, only `message` is written: no level. `%(asctime)s` is local time with no zone. Tracebacks land in `exc_info`. | `pythonjsonlogger.json.JsonFormatter("%(levelname)s %(name)s %(message)s", rename_fields={"levelname": "level", "message": "msg", "exc_info": "stack"}, timestamp="ts")`. `fmt` must name `levelname` or no level is written; never put `%(asctime)s` in it. `pythonjsonlogger.jsonlogger` is a deprecated alias. |
| `structlog` | Tracebacks go to `exception`. | `TimeStamper(fmt="iso", utc=True, key="ts")`, `add_log_level`, `EventRenamer("msg")`, `format_exc_info` (then rename `exception` to `stack` and add `error`), `JSONRenderer()`. |
| `loguru` | Text with color. | Not validated. `logger.add(sys.stderr, serialize=True)` writes a nested `record` object; prefer a custom sink that writes the §3 keys. |
| `uvicorn` | Text `INFO:     msg` with no time. The access log goes to stdout with the raw path and query (emails, tokens) and no duration. | See below. |
| `alembic` | `fileConfig(alembic.ini)` configures its own handlers: `INFO  [alembic.runtime.migration] …`. | `[formatter_generic] class = <your JsonFormatter>` in `alembic.ini`. |

- Call `logging.captureWarnings(True)`, or `warnings.warn` prints bare text.
- uvicorn's `TRACE` level normalizes to debug; `WARNING` and `CRITICAL` normalize.

## uvicorn, gunicorn and FastAPI

- uvicorn configures its loggers in each worker before importing the app, so configure root **at app import**: clear the `uvicorn` logger's handlers and set `propagate=True`.
- Once a request wrapper logs completions, set `uvicorn.access` handlers to `[]` and `propagate=False`: uvicorn then skips the access log entirely.
- With `--workers N`, the supervisor process never imports the app. Only `uvicorn --log-config <dictConfig file>` reaches its lines; `fastapi run` has no such flag. List the supervisor as its own producer.
- fastapi-cli prints a rich banner to stdout; it stays bare text.

## Request wrapper (ASGI)

- A pure ASGI middleware that wraps `send` and reads `http.response.start` for the status. It doesn't break streaming. Avoid `@app.middleware("http")` (BaseHTTPMiddleware): context changes inside it don't flow back.
- Route template: `scope["route"].path` after the call returns. In FastAPI ≥ 0.14x it is relative to `include_router` prefixes; add the prefix yourself if you need the full template.
- `HTTPException` becomes a response inside `ExceptionMiddleware`, before the wrapper sees it. Register an `HTTPException` handler that writes `detail` into the outcome slot and delegates to FastAPI's `http_exception_handler`.
- Starlette's `ServerErrorMiddleware` is always outermost, so the wrapper can't sit outside error recovery. When an exception propagates into it, log `status=500` and re-raise.
- Django: not validated. A middleware with `request.resolver_match.route` is the equivalent.

## Ids and context

- One `ContextVar` holding a small mutable object, set at the start of the request; the formatter reads it.
- Fill outcome fields by mutating the object, never by `var.set()` below the middleware: sync endpoints run in a copied context (anyio), so the middleware never sees the new value.
- Don't reset the var in `finally`: uvicorn logs "Exception in ASGI application" in the same task after the app returns, and that line, with the stack, would lose its `request_id`. Each request is its own task, so not resetting doesn't leak.

## Lifecycle

- `pydantic-settings` and similar validate configuration at import, before logging exists. Configure logging before importing the settings module.
- Version: `importlib.metadata.version("<dist>")`; for an app that isn't installed, report it missing.
- Memory: `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss` (KiB on Linux) is a peak; read `/proc/self/statm` or cgroup `memory.current` for the current figure.
