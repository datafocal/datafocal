# AGENTS.md — DataFocal

Orientation lives elsewhere: [README.md](README.md) for what this is and
[docs/architecture.md](docs/architecture.md) for how it works.

**[docs/STYLE.md](docs/STYLE.md) is the style guide**: abstraction, Go conventions, naming
and tests. Read it before writing code. Where it is silent, match the code around the
change.

---

## Commands

```
make check    # the pre-merge gate: fmt, vet, lint, semgrep, race
make test     # go test ./...  (includes the alloc-budget and cost-model gates)
make race     # go test -race ./...
make bench    # hot-path micro-benchmarks
make seed     # generate the corpus under ./.localdata
make serve    # run the querier against the seeded corpus
```

`golangci-lint` must be built with a Go toolchain no older than the one compiling the
module, or it panics. If `make lint` fails that way it is a local tool problem, not your
change — say so rather than working around it.

Run `make check` before saying a change is done.

---

## Contracts the gate enforces

- **Determinism.** Same call plus same snapshot gives a byte-identical result;
  golden tests enforce it.
- **Performance budgets.** Allocation budgets and cost-model regression tests assert the
  hot paths; an optimization that isn't in a budget test isn't one.
- **Dependencies.** `depguard` blocks new external dependencies; adding one is a design
  decision, not an implementation detail.

The linter also enforces function length, cyclomatic and cognitive complexity, nesting
depth and unused unexported symbols. Run `make lint` rather than hand-policing them.

---

## Change discipline

**Default to the smallest change that fits the request.** Do not refactor, rename, or
reformat code you were not asked to touch.

**Prefer editing an existing file over adding one.**

**Deleting is progress.** Remove what your change makes obsolete. A refactor that grows the
diff needs a reason stated up front.

**Write the boring version first.** Optimize where a benchmark proves a hotspot, not where
one seems likely.

**Add error handling, fallbacks, retries, and guards where the input is genuinely untrusted
or a caller can actually hit the path.** Defensive code for conditions that cannot occur is
slop, and it reads as correctness work in review.

**Packages are self-contained with one responsibility.** A package's API is the exported
interface its constructor returns ([STYLE.md](docs/STYLE.md#abstraction)). Put new behavior
in the package that owns the concept, behind that interface. Don't export a helper just
so another package or a test can reach it.

**Tests check a package's API: its inputs and its outputs.** Drive the exported interface
and compare what it returns as whole objects. Don't test unexported helpers directly.
Write cases as one table, and fold a new case into an existing table instead of adding a
test that overlaps it.

---

## Working practice

**Read the evidence instead of guessing.** Check the logs, the test output, the actual
error. A plausible theory is not a diagnosis.

**Never fabricate.** Quote real code with `file:line`. Mark inference as inference. If
something isn't findable, say so explicitly — that is a complete answer.

**Push back with evidence.** If a requested change or a review finding turns out to be wrong
on closer inspection, say so and show why. Do not implement something you believe is
incorrect to close a comment.

**Make the fix; don't demo it.** No dev servers, no browser sessions, no walkthrough
scripts unless asked. The diff is what gets reviewed.

**Never imply green.** Report what the checks actually returned. If you can't finish
cleanly, leave your package buildable and say what is blocked and why.

**Work on a branch and commit your work.** Pull requests target `main`.
