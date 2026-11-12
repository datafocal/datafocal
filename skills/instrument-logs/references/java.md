# Java (and the JVM)

Validated on a running Spring Boot 4.1 service (Logback, Tomcat 11, H2). The Log4j2 and logstash-logback-encoder rows were not validated.

## Format

| Logger | Default that misses | Configure |
|---|---|---|
| Spring Boot default console | `2026-10-28T09:52:41.924-04:00  INFO 1183712 --- [main] o.s.s.p.App : msg`: text, with the PID, thread and logger inside the message, and stack traces as bare lines. | Spring Boot's built-in structured logging, below. |
| Spring Boot built-in structured logging (3.4+; `logging.structured.json.*` from 3.5) | `logstash` writes `@timestamp`. `ecs` nests `log.level` and `error.*`, and `rename` changes only a leaf inside its parent, so it can't be made to fit. `gelf` writes an epoch time, syslog levels and `short_message`. | `logging.structured.format.console=logstash` and `logging.structured.json.rename[@timestamp]=ts`. `level`, `message` and `stack_trace` already match, and MDC and SLF4J `addKeyValue` pairs land as top-level fields. The time carries the JVM's offset; set `TZ=UTC` or `-Duser.timezone=UTC` for UTC. No dependency. |
| Logback + `logstash-logback-encoder` | `@timestamp` is not a time key DataFocal reads. A different implementation from Spring's `logstash` format, configured in XML. | `<fieldNames><timestamp>ts</timestamp></fieldNames>`. |
| Log4j2 `JsonTemplateLayout` (ECS) | `@timestamp` and `log.level` are not keys DataFocal reads. | Use a template whose keys are `ts`, `level` and `msg`. |

- **No `error` key.** `log.error("msg", ex)` writes only the stack trace. With Spring's structured logging, add a `StructuredLoggingJsonMembersCustomizer<ILoggingEvent>` doing `members.add("error", ILoggingEvent::getThrowableProxy).whenNotNull().as(p -> p.getClassName() + ": " + p.getMessage())`, set by `logging.structured.json.customizer`. It covers framework lines too.
- Stack traces are long: bound them with `logging.structured.json.stacktrace.*` (`root=first`, `max-length`) if they dominate.
- Spring's fatal start report ("APPLICATION FAILED TO START … Port 8080 was already in use") is logged at `error` with no throwable attached; it can't carry `error`. Note it rather than fighting it.
- Third-party messages that interpolate data (Hikari's `Added connection conn0: url=… user=…`, "Started App in 4.2 seconds") can't be changed.

## Request wrapper (Spring MVC)

- A `OncePerRequestFilter` at `Ordered.HIGHEST_PRECEDENCE`. A `HandlerInterceptor` misses requests that fail before handler mapping. `server.tomcat.accesslog` writes Common Log Format text to a file, with no route, MDC or JSON.
- Route: `request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE)`. An unmatched path reports the static resource handler's `/**`.
- Outcome slot: Spring has one. Read `DispatcherServlet.EXCEPTION_ATTRIBUTE` (set when an `@ExceptionHandler` or `DefaultHandlerExceptionResolver` handled it) and `WebUtils.ERROR_EXCEPTION_ATTRIBUTE` (set when an `@ControllerAdvice` returned a view).
- An exception that escapes `chain.doFilter` leaves `response.getStatus() == 200` inside the filter; the container sets 500 later. Report 500 yourself.
- Tomcat logs every escaped exception again at `error` after the filter has returned and cleared the MDC, so that line has no `request_id`. Either log the throwable on the completion event and lower `org.apache.catalina.core.ContainerBase.[Tomcat].[localhost].[/].[dispatcherServlet]`, or accept the pair.
- Server-rendered forms: a rejected submit is a 200 that re-renders the form; success is a 302.
- WebFlux, Micronaut, Quarkus: not validated.

## Ids and context

- MDC, set in the filter and cleared in `finally`. MDC is thread-local: copy it into executors (`TaskDecorator`) and reactive pipelines.
- Micrometer Tracing writes `traceId`/`spanId` to the MDC, which DataFocal doesn't look up. Rename them (`logging.structured.json.rename[traceId]=trace_id`). The OpenTelemetry Java agent already writes `trace_id`/`span_id`.

## Lifecycle and memory

- Spring already logs the start (with version), "Tomcat started on port" after the bind, and graceful shutdown.
- Gauge: `@Scheduled(fixedRate = 30000)` with `@EnableScheduling`; `MemoryMXBean` for heap, cgroup `memory.current` for the container, Hikari's `HikariPoolMXBean` for pool use. Take optional beans through `ObjectProvider`, or tests whose context lacks them fail.
