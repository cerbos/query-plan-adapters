# Ent adapter example application

A runnable program that uses the adapter the way a consumer would — against a **generated ent
client** — over the shared [demo domain](../../demo/README.md).

## Run it

```bash
# from the repository root
demo/scripts/run-example.sh ent
```

Needs `docker` (with compose), `jq`, and a Go toolchain satisfying this directory's `go` directive.
The runner starts the pinned Cerbos PDP and diffs the output against `demo/expected.json`; this
directory's `run.sh` builds this module and runs the program. The store is a SQLite file that
`main.go` deletes and recreates on every run, so a failed run leaves its rows behind to inspect.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), across all three plan kinds:

1. **Filtered list** — `Result.Predicate` handed to a generated `*ent.DocumentQuery` through
   `Where(func(s *entsql.Selector) { s.Where(result.Predicate) })`.
2. **`KIND_ALWAYS_ALLOWED`** — the query runs with no filter.
3. **`KIND_ALWAYS_DENIED`** — no query runs.
4. **Pagination** — ent's own `Limit`/`Offset` on top of the adapter's predicate.
5. **Composed** — the predicate ANDed with generated predicates the application owns (`region`,
   `archived`).

No suite in [`../`](../) builds a generated ent client (`../adversarial_test.go` uses a hand-built
`entsql.Selector` over `database/sql`), so the line every consumer writes runs here and nowhere else.

### The attribute map

```go
var mapper = cerbosent.MapperMap{
    "request.resource.attr.ownerId": {Column: document.FieldOwnerID},
    "request.resource.attr.public":  {Column: document.FieldIsPublic},
}
```

The columns come from the **generated** field constants, so renaming a schema field is a compile
error rather than an unmapped-reference error at run time. The columns are `owner_id` and
`is_public`: a Cerbos attribute name is neither the column name nor the model's field name, which is
why the mapper is required. `region` and `archived` are absent on purpose: they are application
columns no policy reads, and composing them is shape 5.

### `KindAlwaysDenied` carries no predicate, and shape 5 depends on that

`Translate` returns a `Predicate` for `KindConditional` only; for a denial the caller runs no query
(the switch in the [adapter's README](../README.md#quick-start), and `applyPlan` in `main.go`). So
there is no `WHERE false` for the application's predicate to be ANDed with, and it cannot resurrect a
denied row. Shape 5 asserts that the composed shape reports `KIND_ALWAYS_DENIED` with no ids while
the application predicate is in place. The application's predicate is applied **before** the plan,
because an application should not need to know which plan kind the PDP will return.

## This example proves usage shapes, not packaging

Every other example installs its adapter as the artifact a registry would serve (`npm pack`,
`pdm build`, `publishToMavenLocal`;
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). Go has no packaging step,
so this one resolves the adapter through a `replace` directive:

```
replace github.com/cerbos/query-plan-adapters/ent => ../
```

It therefore says nothing about what `go get` gives a consumer. Two gaps follow:

- **Resolution is never exercised.** A consumer resolves the module at an `ent/vX.Y.Z` tag through
  the module proxy, which checks that the path `../go.mod` declares matches the path the repository
  serves and that the tag prefix resolves. A `replace` answers both from the filesystem.
- **A directory read sees files the zip omits:** files in nested modules and `vendor/`, symlinks and
  other irregular files, and `.hg_archival.txt` (per `golang.org/x/mod/zip`; `testdata/`, dotfiles
  and `.gitignore` *are* in the zip). Adapter source reached through a symlink would build here and
  be missing for a consumer.

The packaging half of [#349](https://github.com/cerbos/query-plan-adapters/issues/349) stays open for
Go; this example covers the usage half.

## The example is excluded from the module a consumer downloads

A directory containing a `go.mod` is excluded from its parent's module zip, so this example's
generator and driver dependencies — `entgo.io/ent/cmd/ent` and the `ariga.io/atlas`, `hcl` and
`cobra` trees behind it, and `modernc.org/sqlite` — never reach a consumer of
`github.com/cerbos/query-plan-adapters/ent`. That is why ADR 0002 does not require Go to check the
exclusion the way Python and Java must.

Verified by packing the parent module with `golang.org/x/mod/zip` (what `cmd/go` uses) at the commit
that added this directory:

| `ent/example/go.mod` | entries in the zip | of those, under `example/` |
| -------------------- | ------------------ | -------------------------- |
| present              | 15                 | 0                          |
| renamed away         | 40                 | 25                         |

The second row is the control. The totals move as either directory grows; the first row's
right-hand cell must stay 0. `run.sh` re-checks the cause on every run: `go list -m` here must name
this module, not the adapter.

## Layout

| Path               | What it is                                                                     |
| ------------------ | ------------------------------------------------------------------------------ |
| `run.sh`           | check → resolve → build → run. Prints the JSON document on stdout.             |
| `main.go`          | The example: one function per usage shape.                                     |
| `ent/schema/`      | The hand-written ent schema — the demo domain's one entity.                    |
| `ent/`             | The generated client, committed.                                               |
| `go.mod`, `go.sum` | This example's pins. Committed; Renovate manages them.                         |

### The generated client is committed

That is ent's convention, and it means this directory builds, vets and lints from a checkout with
no generation step. After editing `ent/schema/document.go`, regenerate:

```bash
go generate ./ent
```

The generator is pinned as a `tool` dependency in `go.mod`, which is where most of its indirect
requires come from; none of them are in the built program or in the adapter.

## `go.mod` is the committed lockfile, and Go leaves no `>=` hole

`renovate.json` automerges non-major bumps, so an ent bump arrives as one PR touching both
[`../go.mod`](../go.mod) and this directory, and the `example` job on that PR blocks the automerge if
real usage breaks. That only works while the job stays in
[`.github/workflows/ent.yaml`](../../.github/workflows/ent.yaml).

- A Go `require` names one exact version, so unlike `sqlalchemy>=2.0` a new ent release cannot enter
  this build without a commit that edits this file.
- `run.sh` exports `GOFLAGS=-mod=readonly`, so a build that would change `go.mod` fails instead.
- Through the `replace`, this module selects the higher of its own ent version and the adapter's. A
  bump in `../go.mod` alone changes the version here too, and this `go.sum` lacks the new hashes — a
  build failure, not a silent pass.

## Scope

A JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/) (see
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)). It runs SQLite under
one Go toolchain; [`../adversarial_test.go`](../adversarial_test.go) already replays the whole corpus
on SQLite, PostgreSQL and MySQL, so a second dialect here would re-run the same plumbing.
