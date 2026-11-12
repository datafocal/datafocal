# Rust

Validated by reading code only, not by running it, on agentgateway (a tracing-based proxy with its own formatters). The `tracing-subscriber` facts come from its 0.3.23 source; the `env_logger` row is from memory.

## Format

| Logger | Default that misses | Configure |
|---|---|---|
| `tracing-subscriber` JSON | `.json()` nests the message under `fields`, puts the current span under `span` (its `name` becomes `span.name`, which DataFocal reads as a span's name) and every span under `spans`. Ids recorded on spans arrive as `span.request_id`, not `request_id`. | `fmt().json().flatten_event(true).with_span_list(false)`, and **record correlation ids as fields on the boundary events** rather than only on spans. `with_current_span(false)` drops span-carried ids entirely. No built-in option lifts span fields to the top level; a custom `FormatEvent` can. |
| `tracing-subscriber` text | ANSI color is on by default even off a terminal, which leaves every line unparsed. `%` (Display) fields are unquoted, so `error=connection refused` breaks the trailing pairs. | `.with_ansi(false)`. Prefer JSON. |
| `log` + `env_logger` | `[ts LEVEL target] msg`: DataFocal can't read the bracketed header. Whole-second times, no fields. `pretty_env_logger` adds color and drops the time. | Route `log` into `tracing` (`tracing_subscriber::fmt().init()` installs the `tracing-log` bridge) and configure tracing as above. Otherwise `env_logger::Builder::format(…)` writing one JSON object with `ts`, `level`, `msg`. |
| A custom `FormatEvent` | Projects derived from ztunnel or agentgateway ship their own formatter; none of the options above apply. | Read the formatter and change its keys there. |

- Records bridged from `log` carry `log.target`, `log.module_path`, `log.file`, `log.line`. They're harmless but add four columns.
- Errors: `error = %format!("{err:#}")` for `anyhow` (one line with the cause chain) or `error = %err`. Never `?err` in a text format: `anyhow`'s Debug prints `Caused by:` on separate lines.
- Durations: `duration_ms = d.as_secs_f64() * 1000.0`. `?d` writes a string like `1.2ms`.

## Request wrapper

- axum: `MatchedPath` for the route template. tower-http `TraceLayer::on_response` gives the status and a `Duration`.
- hyper without a router: the wrapper sits in the service function.

## Ids and context

- `.instrument(span)` or `.in_current_span()` on futures, including the ones you `tokio::spawn`. Never hold `span.enter()` across `.await`.
- `tokio::task_local!` values don't cross `tokio::spawn`; re-scope them inside the spawned future.
- Per-process counters (`connection.id = 1, 2, …`) collide across replicas and restarts. DataFocal links services only through string ids with the `_id` suffix.

## Lifecycle

- `fn main() -> anyhow::Result<()>` prints `Error: {err:?}` and `Caused by:` lines to stderr, outside the logger. Log the error with `{:#}` before returning it.
- The default panic hook prints `thread 'x' panicked at …` outside the logger. Install `std::panic::set_hook` that logs at `error` with the location, and calls the previous hook.
- Version: `env!("CARGO_PKG_VERSION")`.
