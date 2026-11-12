# Go

Validated on two repos: DataFocal (slog, net/http, MCP over stdio) and a controller service (slog, net/http, agentgateway sidecar).

## Format

| Logger | Default that misses | Configure |
|---|---|---|
| `log/slog` | Using `slog.Default()` without `SetDefault` writes through the `log` package: `2006/01/02 15:04:05 INFO msg k=v`, with no zone, second precision, and fields after free text. | `slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))` for logfmt, or `NewJSONHandler`, at `main`, **on the stream the process already uses**: never stdout when stdout carries MCP, LSP or piped CLI output. For UTC, add a `ReplaceAttr` that converts `slog.TimeKey` to `.UTC()`. slog has no fatal level: use `Error`. |
| `zap` | `NewProduction` writes `ts` as epoch seconds. | `cfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder`. Keys `ts`, `level`, `msg` are already right. |
| `zerolog` | Time has second precision. | `zerolog.TimeFieldFormat = time.RFC3339Nano`. |
| `logrus` | Text formatter. | `log.SetFormatter(&logrus.JSONFormatter{TimestampFormat: time.RFC3339Nano})`. |
| `log` (stdlib) | No level and no fields. | Move to `log/slog` at `main`, as above. |
| `obs` (`fnchain`) | See SKILL.md §5. | See SKILL.md §5. |

- The `flag` package prints usage text on a bad flag; it stays bare text. Explain it rather than fixing it.
- `log.Fatal` and `os.Exit` after `fmt.Fprintln(os.Stderr, …)` bypass the logger: log with `slog.Error(…, "error", err)` and then `os.Exit(1)`.

## Request wrapper (net/http)

- Route template: `r.Pattern` (Go 1.22+ `ServeMux`), read after the handler returns. chi: `chi.RouteContext(r.Context()).RoutePattern()`. gin: `c.FullPath()`.
- Status: a `ResponseWriter` wrapper that implements `Unwrap() http.ResponseWriter`, so `http.ResponseController` still reaches the original `Flusher` and `Hijacker`.
- Outcome slot: a pointer to a small struct in the request context, set where the handler writes its error response.
- `recover()` inside the logging wrapper, so a panicking request gets its completion line with status 500.

## Ids and context

- `slog.With("request_id", id)` stored in `context.Context`, or an `obs` event (SKILL.md §5).
- Goroutines that outlive the request need the values copied, not the request context, which is cancelled when the request ends.

## Lifecycle, version, memory

- Version: `debug.ReadBuildInfo()` (`Main.Version`, `vcs.revision` in `Settings`).
- Memory: `runtime/metrics` `/memory/classes/total:bytes` or `runtime.ReadMemStats`; in a container, also cgroup `/sys/fs/cgroup/memory.current`.
- The gauge: one goroutine started at `main` and stopped by the root context, or a ticker case in an existing main `select` where the repo's lint forbids unobserved goroutines.
- A package with no logger: add a `*slog.Logger` to the constructor or options struct it already takes, defaulting to `slog.New(slog.DiscardHandler)` so tests and call sites don't change.
