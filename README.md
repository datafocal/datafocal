# DataFocal

Open-source observability that starts with AI. DataFocal reads the logs your AI agents,
services and machines already write, learns what normal looks like, and shows you what
changed, why, and the runs and events behind it.

- **Collectors index at the edge.** Each collector writes self-indexed, immutable segments
  straight to object storage (S3, GCS, Azure Blob, R2, MinIO). There is no central ingest
  tier and no database to run.
- **Queries prune instead of scanning.** Every segment carries a footer of summaries, so most
  questions are answered from footers without reading rows.
- **Built for agents and people.** The same typed tools power the web UI and an MCP server,
  so Claude Code, Codex or any MCP client can ask what is running, what changed and why.
- **AI view.** Spend, context, cache, tools and outcomes for Claude Code, Codex, Gemini CLI,
  Copilot CLI, LLM gateways and OpenTelemetry GenAI traces.

[How it works](docs/architecture.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md)

## Run on kind

With docker, kind and kubectl installed, `make deploy-kind` creates (or reuses) the kind
cluster `datafocal`, builds and loads the images, and runs MinIO, a collector on every node
shipping its containers' logs, the querier, and the compactor, which every minute merges
each complete stream-hour into one segment, and each settled five-minute window of an hour
still open, and folds the catalog into snapshots, in the `datafocal` namespace. `make undeploy-kind` removes them.

DataFocal deletes nothing from the bucket; set lifecycle rules instead:
`<tenant>/raw/` and `<tenant>/window/` after a few days (an hour the compactor has not merged by then is lost),
`<tenant>/hour/` after the tenant's retention, the same as the querier's and compactor's
`retention` setting, `<tenant>/catalog/snapshot/` after a day, and never `<tenant>/catalog/v1/`.

Reach the querier through a port-forward:

```
kubectl --context kind-datafocal -n datafocal port-forward svc/querier 8080:8080
```

An MCP client then connects to `http://localhost:8080/mcp`; Claude Code connects with:

```
claude mcp add --transport http DataFocal http://localhost:8080/mcp
```

Any querier replica serves any call, and a result is cut to `mcp_result_tokens` (20,000 by
default) so it fits the client's output limit. Ask it what is running first.

## Run locally

On Linux with systemd and docker, `make install-local` builds the collector, querier and
compactor into `~/.local/bin` and runs them as systemd user services over a MinIO container
on localhost:9000. The collector follows this machine's journal and every Claude Code and
Codex transcript under `~/.claude` and `~/.codex`, and a cAdvisor container samples the host
and each docker container into the journal for the Resources view. With `CLAUDE_QUOTA=1` it also
sets Claude Code's status line, unless you set one, to one that logs the account's quota for
the AI view's Quota tab. The UI is at http://localhost:8080/ui/ and MCP at http://localhost:8080/mcp.

To collect this machine's logs alongside a kind cluster's instead, run
`make install-local-kind`. This deploys the checkout to the cluster as `make deploy-kind`
does, then runs only the collector here, writing to the cluster's MinIO, so the cluster's
one compactor and querier serve both. Run it again after pulling to update both. Reach the querier
through the port-forward above. Running either install switches from the other. Here the
kind nodes are docker containers, which the Resources view leaves out, as the cluster's own
cAdvisor counts their pods.

`make uninstall-local` removes the services and containers. It keeps the `datafocal-minio`
volume and the collector's offsets in `~/.local/state/datafocal`. User services stop when you
log out unless `loginctl enable-linger` is set. Reading the system journal needs membership in
`systemd-journal`, `wheel` or `adm`.

## Develop

`make check` is the pre-merge gate (format, vet, lint, semgrep, race tests),
`make test` runs the tests, and `make web-check` the frontend gate. `make seed` generates a
demo corpus under `./.localdata` and `make serve` runs the querier over it. See
[CONTRIBUTING.md](CONTRIBUTING.md) and [docs/STYLE.md](docs/STYLE.md).

## License

[Apache License 2.0](LICENSE). Test fixtures under `internal/e2e/testdata/ai` carry their
own upstream licences; see the README there.
