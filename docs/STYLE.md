# Style guide — DataFocal

How code is written here. [AGENTS.md](../AGENTS.md) is the front page; where this guide is
silent, match the code around the change.

---

## Abstraction

**A package's API is an exported interface; an unexported struct implements it; a
constructor returns the interface.** The interface is the boundary consumers and their
tests program against, so any package's dependency can be faked from another package.

```go
type Catalog interface {
	Append(ctx context.Context, d segment.Descriptor) error
	Snapshot(ctx context.Context, tenant string) (Snapshot, error)
}

type fsCatalog struct{ root string }

func NewFS(root string) Catalog { return &fsCatalog{root: root} }
```

**Domain values stay plain structs.** Descriptors, footers, records, and request/response
types are data, not behavior — the interface covers what a package *does*, not what it
passes.

**Each package ships its own fake in `fake.go`.** `Fake` is a struct of `Fn`-suffixed
function fields, one per interface method; a nil field returns zero values. Consumers'
tests import it — no mocking frameworks, and no private re-implementations of another
package's fake.

```go
type Fake struct {
	AppendFn   func(d segment.Descriptor) error
	SnapshotFn func(tenant string) (Snapshot, error)
}

func (f Fake) Append(_ context.Context, d segment.Descriptor) error {
	if f.AppendFn == nil {
		return nil
	}
	return f.AppendFn(d)
}
```

**Inject dependencies at construction, as the interface the dependency's package
exports.** Do not declare your own to consume it.

```go
// Avoid — a local re-declaration of what the package already provides
type footerReader interface { ReadFooter(ctx context.Context, key string) (Footer, error) }

// Prefer
store storage.Store
```

**Extract a helper at the third call site.** Two similar blocks are fine; three means the
shape is real. Before writing a helper, search for it — a private copy of something that
already exists in a sibling package is the most common finding in review here.

**Reach for the stdlib before writing your own.** `slices.Sorted(maps.Keys(m))`,
`slices.SortFunc`, `cmp.Or`, `errors.Join`. New external dependencies are blocked by
`depguard` and are a design decision, not an implementation detail.

---

## Go conventions

**Sort map keys before iterating when the result is output, ordering, or a name.** Same call
plus same snapshot must produce a byte-identical result — that is a contract here
([querier.md §5a](docs/design/querier.md)), not a preference, and golden tests enforce it.

**Propagate the caller's `context.Context`.** Never `context.Background()` below an entry
point. Every object-storage call and every network round trip takes a context and honors
cancellation.

**Return errors; do not exit.** `log.Fatal` and `os.Exit` belong in `cmd/` main functions
and nowhere else. Wrap with `%w` and enough context to locate the call:
`fmt.Errorf("read footer %s: %w", key, err)`.

**Return early on error.** No `else` branch after a `return`.

**Log with `log/slog`, in structured attributes.** The message stays static; the values go
in attrs.

```go
// Avoid
logger.Info(fmt.Sprintf("pruned %d segments in %s", n, d))

// Prefer
logger.Info("pruned segments", "count", n, "duration", d)
```

Log an event once — emitting at the call site and again at the summary double-counts.
Sibling code paths use the same level for the same condition.

**Log enough to see the change working from DataFocal's own logs.** A change that alters how
the querier decides, caches, admits, retries, falls back or spends must be readable from its
logs once an instance has ingested them, in the existing views, with no debugger or code
open. Put the decision on the line that already exists for the work: a tool call's outcome
goes on its `request` line (`outcome.attrs`, `internal/api/registry.go`), as a field naming
which way it went and why. Totals that change over time (bytes held, hits, evictions) go on
one summary line per refresh, not on a line per operation. A test asserts the fields, by
capturing the `slog` handler, as it asserts any other output. A change whose behavior can't
be told from its logs isn't done.

**Do not add struct fields that nothing reads.** The `unused` linter misses fields that are
written but never read, and exported symbols with no callers. If a field is genuinely
reserved, the comment says so.

**Comment the why.** Delete comments that restate the code. A comment that disagrees with
the code is a bug, not a nit — fix it in the same change.

**Two sentences, maximum.** Every comment, including doc comments and multi-paragraph
block comments. If the why needs a third sentence, the design belongs in
`docs/design/` and the comment links to it.

---

## Naming

**[docs/design/glossary.md](docs/design/glossary.md) is the vocabulary.** Use its word for a
concept in code, docs, and the API; add a term there in the change that introduces it.

**Name a value for what it means, not how it's computed.** `HLL`, `CountMin`, `DDSketch`,
and `XorFilter` are implementation vocabulary and belong inside `internal/sketch`.
Elsewhere the same value is a distinct-trace count, a template frequency, a latency
distribution, a membership filter. A reader must not have to know which paper a field
implements to know what it holds.

```go
// Avoid — names the algorithm, so the meaning is one lookup away
srcHLL := footer.Sketches.HLL[PrimaryHLLColumn]

// Prefer — the footer says what it carries
traces := footer.DistinctTraces()
```

**A local's name states the quantity, including its scope.** `count` is not a name when the
value is *this template's lines across the whole segment* and the next line adds it to one
time bin. Scope belongs in the name; that omission is what hid a per-bin overcount in
`summary.MergeFooter` all the way through review.

**One spelling per concept.** Before naming a column, constant, or helper, grep for it. Four
private spellings of the `template_id` column is four chances for two of them to drift.

---

## Tests

`testing` plus `github.com/stretchr/testify` for assertions. testify is allowed in
`_test.go` only — `depguard` blocks it in production code, along with any other new
dependency.

**Compare whole objects, not field by field.** Per-field assertions silently miss fields
added later and produce noisier failures.

```go
// Good — one comparison, catches every field including future additions
want := Footer{Rows: 128, Min: 3, Max: 91}
assert.Equal(t, want, got)

// Avoid — misses fields added later
assert.Equal(t, 128, got.Rows)
assert.Equal(t, 3, got.Min)
```

`assert` continues after a failure; `require` stops the test. Use `require` when a later
line would panic on the failure (a nil result, an unexpected error), `assert` otherwise.

When a field is genuinely non-deterministic (timestamps, generated IDs), zero or normalize
just that field before the whole-object compare.

**Collapse near-identical test functions into one table.** A run of tests varying a single
input is a table with a `name` field and `t.Run`.

**Do not assert on your own test inputs**, and do not test the standard library or a fake.
Checking `len()` of a slice the test just constructed, or that `json.Marshal` works, adds
nothing.

**Fakes stay dumb.** No conditional logic inside a test double beyond the nil-`Fn`
zero-value default — if it branches, callers can't assert on what passed through it.

**Assert against an oracle where one exists.** Property tests for the sketch error bounds
and merge associativity, zero-false-negatives for the membership filter, golden files for
wire format, allocation budgets for hot paths. Write the failing test first so it isn't
shaped to pass buggy code.

**A `break` inside an assertion loop silently under-verifies.** Accumulate across all cases.
