# .NET

Not validated on a repo yet. Treat this as a starting point and check the output with the checker.

| Logger | Default that misses | Configure |
|---|---|---|
| Serilog compact JSON | `@t`, `@l` and `@m` are not keys DataFocal reads. | `ExpressionTemplate("{ {ts: @t, level: @l, msg: @m, ..@p} }\n")`. |
| `Microsoft.Extensions.Logging` JSON console | PascalCase `Timestamp`, `LogLevel`, `Message`. | Use Serilog, or a custom formatter that writes `ts`, `level` and `msg`. |

- Ids: `ILogger.BeginScope` or Serilog `LogContext.PushProperty`, with `_id` keys.
- ASP.NET Core: middleware early in the pipeline; the route template is `(context.GetEndpoint() as RouteEndpoint)?.RoutePattern.RawText`.
