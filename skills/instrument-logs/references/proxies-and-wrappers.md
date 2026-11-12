# Proxies, sidecars and process managers

## Proxies and sidecars

Their access logs usually have no message and a duration written as a string. Configure, in the repo's config file:
- JSON output;
- a constant `message` such as `"request"`;
- a numeric `duration_ms`;
- request and trace ids as top-level `_id` keys (`trace_id`, not `trace.id`).

| Proxy | Notes |
|---|---|
| nginx | `log_format … escape=json`. `$request_time` is seconds with millisecond precision, and nginx can't convert it: log it as `request_time` and report the missing `duration_ms` as a format gap. |
| Envoy | An access log with `json_format`. `%DURATION%` is whole milliseconds. |
| agentgateway | `logging.format: json` (the default is a tab-separated text format). Its JSON access lines carry no message, `duration` is a whole-millisecond string (`"0ms"`), and `request.id`/`connection.id` are per-process integers emitted only at debug. Use `frontendPolicies.accessLog.add` for a numeric `duration_ms` and a message, validated with `--validate-only` on the pinned version. |

**Validate the config with the pinned binary** before committing. A rejected config stops the proxy from starting.

## Process managers and wrappers

Take the baseline from the command the deployment actually runs.

| Wrapper | What it does to lines | Fix |
|---|---|---|
| pm2 | `time: true` or `log_date_format` prefixes every line with a local time and no zone (`2026-10-28T09:56:07: {…}`), so none of them parse. `log_type: json` nests the app's line as a string under `message`. | Drop `time` and `log_date_format`; don't use `log_type: json`. |
| uvicorn `--workers`, gunicorn | The supervisor writes its own text lines and never imports the app. | Pass the server a logging config file (`--log-config`, gunicorn's `logconfig_dict`). See `python.md`. |
| `npm run`, `yarn` | Print a script banner (`> app@1.0.0 start`) on stdout. | Run `node` directly in the container, or accept the banner as bare text. |
| supervisord, systemd | Can redirect to files or prefix lines (`StandardOutput`, `stdout_logfile`). | Write the app's stream unchanged to the collector's input. |
| Shell entrypoints | `set -x` writes `+ command` lines. | Leave them; they're a separate producer. |

## Formats DataFocal can't parse

If a producer can't be configured to fit (shell scripts, subprocess output, a third-party binary with fixed output), leave it alone. List it as a **format gap** in the report, with three sample lines and the producer's name, so DataFocal can add it as a known format. For multi-line text output, DataFocal can fold continuation lines when the source declares a `record_start` pattern.
