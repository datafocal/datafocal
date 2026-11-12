# Contributing to DataFocal

Thanks for helping. Bug reports, fixes and new log formats are all welcome.

## Before you start

- Open an issue for anything bigger than a small fix, so we can agree on the approach first.
- Read [docs/architecture.md](docs/architecture.md) for how the engine fits together and
  [docs/STYLE.md](docs/STYLE.md) for the code style.
- A new external dependency is a design decision: raise it in an issue first.

## Building and testing

You need Go (the version in `go.mod`), Node 22 with corepack, and docker for the end-to-end
suites.

```
make build        # go build ./...
make test         # unit tests, including the allocation and cost-model budgets
make web-check    # the frontend gate
make check        # the full pre-merge gate: fmt, vet, lint, semgrep, race
make seed serve   # a demo corpus and a querier over it, UI at http://localhost:8080/ui/
```

`make check` must pass before a pull request is merged. On a pull request CI runs the same
gate without `-race`, plus `make web-check`, the browser tests and the screenshots
(`.github/workflows/pr.yml`); the race suite, the benchmarks and the images run after the
merge (`main.yml`).

## Pull requests

- Keep each pull request to one change, with tests that drive the package's public API.
- Results are deterministic: the same call over the same snapshot returns identical bytes.
  If your change alters a golden file, regenerate it (`go test ./internal/e2e -update`) and
  say why in the description.
- By contributing you agree your work is licensed under the
  [Apache License 2.0](LICENSE).

## Conduct

Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md).
