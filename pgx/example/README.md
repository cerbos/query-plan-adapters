# pgx adapter example application

A runnable program that uses the adapter the way a consumer would — splicing the `WHERE` fragment it
returns into statements the application owns — over the shared
[demo domain](../../demo/README.md), against a real PostgreSQL server.

## Run it

```bash
# from the repository root
demo/scripts/run-example.sh pgx
```

Needs `docker` (with compose), `jq`, and a Go toolchain satisfying this directory's `go` directive.
The runner starts the pinned Cerbos PDP and diffs the output against `demo/expected.json`; this
directory's `run.sh` starts PostgreSQL, builds this module and runs the program.

PostgreSQL is the image [`../POSTGRES_IMAGE`](../POSTGRES_IMAGE) names — the same file
[`../adversarial_test.go`](../adversarial_test.go) reads — published on port **15432** (not 5432, so
it cannot collide with the adversarial suite's own containers; `main.go`'s DSN uses the same
number). `run.sh` creates and removes the container, so no database state lives in this directory.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), across all three plan kinds:

1. **Filtered list** — `Result.Where` and `Result.Args` in a `SELECT` the application owns.
2. **`KIND_ALWAYS_ALLOWED`** — the query runs with no filter.
3. **`KIND_ALWAYS_DENIED`** — no query runs.
4. **Pagination** — the fragment first, with the application's `LIMIT`/`OFFSET` numbered after it.
5. **Composed** — the application's predicate first, with the fragment shifted by
   `WithPlaceholderOffset`.

No suite in [`../`](../) composes the fragment — they all hand `Result.Where` straight to a `SELECT`
of their own — so shapes 4 and 5 run here and nowhere else.

### Composition: PostgreSQL parameters are ordinal

`$1` is the first argument sent with the statement, not the first in the fragment. Both directions
appear here:

| Direction | Shape | What the application must do |
| --- | --- | --- |
| Application predicate **first** | 5, composed | `Translate(…, WithPlaceholderOffset(len(appArgs)))`, then send `append(appArgs, result.Args...)` |
| Fragment **first**, application parameters after | 4, paginated | no option: number `LIMIT`/`OFFSET` from `len(result.Args)+1` and send `append(result.Args, pageSize, offset)` |

The statements the program runs, printed on stderr:

```sql
-- shape 5, alice/view: the application's two parameters take $1 and $2, so the fragment starts at $3
SELECT id FROM document WHERE (archived = $1 AND region = $2)
  AND (("document"."is_public" OR ("document"."owner_id" = $3::text)))
  -- args: [false, emea, alice]

-- shape 4, alice/view: the fragment keeps $1, so the application's page parameters start at $2
SELECT id FROM document WHERE ("document"."is_public" OR ("document"."owner_id" = $1::text))
  ORDER BY id LIMIT $2 OFFSET $3
  -- args: [alice, 2, 0]
```

The adapter's parameters carry explicit casts (`$3::text`) because a plan operand can land where
PostgreSQL has nothing to infer a type from (see `pgTypeSuffix` in [`../render.go`](../render.go)).
The application's parameters sit next to its own columns and can use the plain form.

### Misnumbering fails loudly

- **`checkPlaceholders`** runs on every statement a usage shape builds: the statement must reference
  exactly `$1..$n` for its `n` arguments, with no gaps or duplicates. A gap (`$1, $2, $4`) means the
  fragment was shifted too far.
- **`assertOffsetIsLoadBearing`** is the negative control that keeps shape 5 from passing vacuously.
  It rebuilds the composed statement without the offset and requires that `checkPlaceholders`
  rejects it and that executing it anyway does not return the correct rows.
- **`assertDenialCannotBeSpliced`** splices a denied plan's empty fragment into the composed
  statement (`… WHERE (archived = $1 AND region = $2) AND `) and requires PostgreSQL to reject it
  with `syntax error at end of input` and return no rows. A caller who ignores `Kind` gets a hard
  failure, not the application's own rows.

These two controls are the only code that bypasses `checkPlaceholders`.

In this domain a misnumbering is always loud anyway: pgx refuses an argument count that disagrees
with the statement (`expected 2 arguments, got 3`) and the server refuses a mistyped value.
`checkPlaceholders` adds a failure that names the composition, before a round trip. What neither
can see is arguments in the right number but the wrong order: two swapped same-typed parameters
return a silently wrong answer. The demo domain cannot construct that case (every permutation is a
type error), so it is documented, here and in
[the adapter's README](../README.md#composing-the-fragment-with-your-own-predicates), rather than
asserted.

### `KindAlwaysDenied` carries no fragment, and shape 5 depends on that

`Translate` returns `Where` and `Args` for `KindConditional` only; for a denial the caller runs no
query (the switch in the [adapter's README](../README.md#quick-start)). As in
[`ent/example/`](../../ent/example/README.md), the application's predicate has nothing to be ANDed
with and cannot resurrect a denied row, so shape 5 asserts `KIND_ALWAYS_DENIED` with no ids while the
application predicate is in place. The adapter never renders `WHERE false` for a denial, so the
composed-over-denied statement is not executed; `assertDenialCannotBeSpliced` covers the mistake
instead.

### The attribute map

```go
var mapper = cerbospgx.MapperMap{
    "request.resource.attr.ownerId": {Column: colOwnerID},
    "request.resource.attr.public":  {Column: colIsPublic},
}
```

The columns are `owner_id` and `is_public`: a Cerbos attribute name is not a column name, which is
why the mapper is required. Without an entry the adapter returns an error rather than guessing.
`region` and `archived` are absent on purpose: they are application columns no policy reads, and
composing them is shape 5.

## This example proves usage shapes, not packaging

Every other example installs its adapter as the artifact a registry would serve (`npm pack`,
`pdm build`, `publishToMavenLocal`;
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). Go has no packaging step,
so this one resolves the adapter through a `replace` directive:

```
replace github.com/cerbos/query-plan-adapters/pgx => ../
```

It therefore says nothing about what `go get` gives a consumer. Two gaps follow:

- **Resolution is never exercised.** A consumer resolves the module at a `pgx/vX.Y.Z` tag through
  the module proxy, which checks that the path `../go.mod` declares matches the path the repository
  serves and that the tag prefix resolves. A `replace` answers both from the filesystem.
- **A directory read sees files the zip omits:** files in nested modules and `vendor/`, symlinks and
  other irregular files, and `.hg_archival.txt` (per `golang.org/x/mod/zip`). Adapter source reached
  through a symlink would build here and be missing for a consumer.

The packaging half of [#349](https://github.com/cerbos/query-plan-adapters/issues/349) stays open for
Go; this example covers the usage half, which matters most here because the adapter returns SQL
text.

## The example is excluded from the module a consumer downloads

A directory containing a `go.mod` is excluded from its parent's module zip, so this example's code
and version pins are not part of `github.com/cerbos/query-plan-adapters/pgx`. That is why ADR 0002
does not require Go to check the exclusion the way Python and Java must.

Verified by packing the parent module with `golang.org/x/mod/zip` (what `cmd/go` uses) at the commit
that added this directory:

| `pgx/example/go.mod` | entries in the zip | of those, under `example/` |
| -------------------- | ------------------ | -------------------------- |
| present              | 16                 | 0                          |
| renamed away         | 20                 | 4                          |

The second row is the control. The totals move as either directory grows; the first row's
right-hand cell must stay 0. `run.sh` re-checks the cause on every run: `go list -m` here must name
this module, not the adapter.

The dependency benefit is smaller than for `ent/example/`: this example requires only
`github.com/jackc/pgx/v5` and the Cerbos SDK, which the adapter already requires. What the boundary
buys is that the example's source is not published and its `require` versions never enter a
consumer's version selection, so it cannot raise a consumer's pgx floor.

## Layout

| Path               | What it is                                                                     |
| ------------------ | ------------------------------------------------------------------------------ |
| `run.sh`           | check → start PostgreSQL → resolve → build → run. Prints the JSON document on stdout. |
| `main.go`          | The example: one function per usage shape, the DDL (five columns), and the composition guards. |
| `go.mod`, `go.sum` | This example's pins. Committed; Renovate manages them.                          |
| `.gitignore`       | The binary a bare `go build` leaves here. `run.sh` builds into a scratch directory. |

## `go.mod` is the committed lockfile, and Go leaves no `>=` hole

`renovate.json` automerges non-major bumps, so a pgx bump arrives as one PR touching both
[`../go.mod`](../go.mod) and this directory, and the `example` job on that PR blocks the automerge if
real usage breaks. That only works while the job stays in
[`.github/workflows/pgx.yaml`](../../.github/workflows/pgx.yaml).

- A Go `require` names one exact version, so unlike `sqlalchemy>=2.0` a new pgx release cannot enter
  this build without a commit that edits this file.
- `run.sh` exports `GOFLAGS=-mod=readonly`, so a build that would change `go.mod` fails instead.
- Through the `replace`, this module selects the higher of its own pgx version and the adapter's. A
  bump in `../go.mod` alone changes the version here too, and this `go.sum` lacks the new hashes — a
  build failure, not a silent pass.

## Scope

A JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/) (see
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)). It runs one
PostgreSQL major under one Go toolchain; the adapter has no dialect option, because the SQL it emits
is PostgreSQL's.
