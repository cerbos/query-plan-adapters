# pgx adapter example application

A runnable program that uses the adapter the way a consumer would — splicing the `WHERE` fragment it
returns into statements the application owns — over the shared
[demo domain](../../demo/README.md), against a real PostgreSQL server.

```bash
# from the repository root
demo/scripts/run-example.sh pgx
```

Needs `docker` (with compose), `jq`, and a Go toolchain satisfying this directory's `go` directive.
The runner starts the pinned Cerbos PDP; this directory's `run.sh` starts PostgreSQL, builds this
module and runs the program.

## This example proves usage shapes, not packaging

Every other example in this repository builds its adapter into the artifact a registry would serve —
`npm pack`, `pdm build`, `publishToMavenLocal` — and installs **that**, so that the published surface
is executed:
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md). **Go cannot participate,
and this example does not.** There is no packaging step to run: a Go example either resolves the
adapter through a `replace` directive, which is not what a consumer does, or resolves a published tag,
which would never test the change under review. This one uses `replace`:

```
replace github.com/cerbos/query-plan-adapters/pgx => ../
```

So nothing here says anything about what a `go get` of this module would give a consumer. That is
stated rather than glossed because the ADR's Consequences section asks for exactly that — "Their
READMEs should say so rather than implying packaging coverage they do not have" — and because the gap
is not empty:

- **Resolution is never exercised.** A consumer resolves `github.com/cerbos/query-plan-adapters/pgx`
  at a `pgx/vX.Y.Z` tag through the module proxy, which checks that the path `../go.mod` declares is
  the path the repository serves it under, and that the tag prefix resolves at all. A `replace`
  answers both questions from the filesystem instead, so neither is asked here.
- **A directory read sees files the zip omits.** Not many — reading `golang.org/x/mod/zip`, the
  omissions are files in nested modules and in `vendor/`, symbolic links and other irregular files,
  and `.hg_archival.txt`. So adapter source reached through a symlink would build here and be missing
  for a consumer.

What is left is the other half of
[cerbos/query-plan-adapters#349](https://github.com/cerbos/query-plan-adapters/issues/349), and for
this adapter it is the larger half by some distance, because this adapter hands back **SQL text**.

## Composition is the point here: PostgreSQL parameters are ordinal

`Translate` returns a bare boolean expression in `Result.Where` and its bound values in
`Result.Args`. The expression carries no `WHERE` keyword, so the application decides where it goes —
and the moment it goes anywhere other than immediately after `WHERE`, the numbering matters. `$1`
means "the first argument I send with this statement", not "the first argument in this fragment", so a
fragment numbered from `$1` spliced into a statement that already binds two arguments is wrong in a
way nothing in Go's type system can see.

`WithPlaceholderOffset(n)` is the adapter's answer, and it is the whole answer for one of the two
directions. Both appear here:

| Direction | Shape | What the application must do |
| --- | --- | --- |
| Application predicate **first** | 5, composed | `Translate(…, WithPlaceholderOffset(len(appArgs)))`, then send `append(appArgs, result.Args...)` |
| Fragment **first**, application parameters after | 4, paginated | no option: number `LIMIT`/`OFFSET` from `len(result.Args)+1` and send `append(result.Args, pageSize, offset)` |

The statements this program actually runs, on stderr where a CI log shows them:

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

The two halves are visibly different in one more way worth noticing: the adapter's parameters carry
explicit casts (`$3::text`) and the application's do not. PostgreSQL infers an untyped `$n` from the
context it appears in, and a plan operand can land somewhere with nothing to infer from —
[`../render.go`](../render.go)'s `pgTypeSuffix` is where that is explained. An application comparing a
parameter against its own column has context, and writes the plain form.

### The misnumbering is made to fail loudly

Every statement a usage shape builds goes through `checkPlaceholders` before it is executed: the
statement must reference exactly `$1..$n` for the `n` arguments it is about to be sent, with no gaps
and no duplicates. A gap matters as much as a duplicate — `$1, $2, $4` with three arguments means the
fragment was shifted too far and every value past the gap is bound to the wrong placeholder.

The two negative controls below deliberately go around it — the first executes a statement
`checkPlaceholders` has just been required to reject, and the second is about syntax rather than
numbering. Nothing else does.

`assertOffsetIsLoadBearing` is the negative control for that check, and it is why shape 5 cannot pass
vacuously. It rebuilds the composed statement the way a consumer who forgot the option would —
fragment numbered from `$1`, arguments appended after the application's — and requires two things:
that `checkPlaceholders` **reports** it, and that executing it does not produce the same rows as the
correct composition. Without it, an offset that was wrong or unnecessary would leave every shape
green.

**What is honestly not at risk here, and what is.** PostgreSQL is not defenceless: a misnumbered
composition changes how many distinct placeholders the statement has, and pgx refuses to bind an
argument count that disagrees with it (`expected 2 arguments, got 3`), while the server refuses a
value whose type does not fit the placeholder's inferred type. In this domain the mistake is therefore
always loud, and the run records which way it failed. What `checkPlaceholders` adds is a failure that
**names the composition**, before a round trip, rather than a driver message about argument counts
that reads as a bug in the application's own SQL.

The class neither the driver nor the check can see is arguments in the right *number* and the wrong
*order*: the offset renumbers the fragment's placeholders, and nothing verifies that `Result.Args`
ended up at the position the offset promised. Two same-typed parameters swapped is a silently wrong
answer. The demo domain cannot construct one — its application predicate binds a boolean and a text
where the plan binds a text, so every permutation is a type error — so that one is documented rather
than asserted, both here and in [the adapter's README](../README.md#composing-the-fragment-with-your-own-predicates).

## `KindAlwaysDenied` carries no fragment, and shape 5 depends on that

`Translate` returns `Where` and `Args` for `KindConditional` only. For the two unconditional kinds the
caller applies no filter and — for a denial — runs no query at all, which is the switch the
[adapter's README](../README.md#usage) shows.

That is the same structural situation as [`ent/example/`](../../ent/example/README.md): there is
nothing for the application's predicate to be ANDed *with*, so it cannot resurrect a denied row, and
what shape 5 asserts is that the composed shape reports `KIND_ALWAYS_DENIED` with no ids while the
application predicate is in place. **This adapter cannot render `WHERE false` for a denial** — the
`FALSE` its renderer can emit belongs to a translated expression, and a denied plan produces no
expression to render — so the composed-over-denied case is not executed here either.

What this adapter *can* answer, and ent cannot, is what the mistake does — because a string can be
concatenated whatever it contains. `assertDenialCannotBeSpliced` pins it: splicing the empty fragment
into the composed statement yields `… WHERE (archived = $1 AND region = $2) AND `, PostgreSQL rejects
it with `syntax error at end of input`, and no rows come back. So a caller who ignores `Kind` gets a
hard failure rather than the application's own rows. An adapter that rendered `TRUE` for a denial
would instead answer with them, which is why this is checked rather than assumed.

## The example is excluded from the module a consumer downloads

A directory containing a `go.mod` is excluded from its parent's module zip, so this example's code and
its version pins are not part of `github.com/cerbos/query-plan-adapters/pgx` at all. That is what
[`CLAUDE.md`](../../CLAUDE.md) means by both Go modules being standalone, and it is the reason
ADR 0002 exempts Go from checking the exclusion deliberately the way Python and Java must.

It was verified rather than assumed, by packing the parent module with `golang.org/x/mod/zip` — the
package `cmd/go` itself builds module zips with — and counting the entries, twice. At the commit that
added this directory:

| `pgx/example/go.mod` | entries in the zip | of those, under `example/` |
| -------------------- | ------------------ | -------------------------- |
| present              | 16                 | 0                          |
| renamed away         | 20                 | 4                          |

The second row is the control: without it the first proves only that the packer produced *something*.
Every number here moves as either directory grows. The one that must not is the first row's right-hand
cell.

The dependency argument is **thinner here than for `ent/example/`**, and worth stating so nobody reads
across from that README and over-claims. Ent's example pulls in a code generator — `entgo.io/ent/cmd/ent`
and the `ariga.io/atlas`, `hcl` and `cobra` trees behind it — none of which a consumer should ever
resolve. This example requires only `github.com/jackc/pgx/v5` and the Cerbos SDK, which the adapter
already requires itself, so nothing large is being kept out. What the boundary buys here is the rest of
it: the example's source is not part of the published module, and its `require` versions never
participate in a consumer's version selection, so an example that needed a newer pgx could not raise a
consumer's floor.

`run.sh` re-checks the cause rather than the consequence on every run — that `go list -m` here names
this module and not the adapter — because the cause is the half a stray edit could regress and it
costs nothing to ask.

## Layout

| Path               | What it is                                                                     |
| ------------------ | ------------------------------------------------------------------------------ |
| `run.sh`           | check → start PostgreSQL → resolve → build → run. Prints the JSON document on stdout. |
| `main.go`          | The example itself: one function per usage shape, plus the composition guards.  |
| `go.mod`, `go.sum` | This example's pins. Committed, and Renovate manages them. See below.           |
| `.gitignore`       | The binary a bare `go build` leaves here. `run.sh` builds into a scratch directory. |

There is no schema file and no generated client: the DDL is five columns in `main.go`, which is the
whole of what this adapter needs to be handed. `owner_id` and `is_public` are deliberately not spelled
the way the Cerbos attributes they carry are — a Cerbos attribute name is not a column name, which is
what makes the mapper necessary rather than decorative:

```go
var mapper = cerbospgx.MapperMap{
    "request.resource.attr.ownerId": {Column: colOwnerID},
    "request.resource.attr.public":  {Column: colIsPublic},
}
```

Resolution is fail-closed, so without it the adapter has nothing to resolve
`request.resource.attr.ownerId` to and returns an error — which is itself worth seeing in an example.
`region` and `archived` are deliberately absent: they are the application's columns, never referenced
by policy, and composing them with the adapter's fragment is shape 5.

## The store

`run.sh` starts the PostgreSQL image [`../POSTGRES_IMAGE`](../POSTGRES_IMAGE) names — the same file
[`../adversarial_test.go`](../adversarial_test.go) reads, so the server this example composes SQL
against is the one the conformance corpus is replayed against, and bumping it is one edit. It is
published on **15432** rather than 5432, because the adversarial suite starts PostgreSQL containers of
its own and a demo server holding the default port would leave one of the two reading the other's
rows. `main.go`'s DSN is the other half of that number.

The container is created and removed by `run.sh`, so unlike `ent/example/` there is no database file
in this directory — the rows live in the container and go with it. The one thing `.gitignore`d is the
binary a bare `go build` in here leaves behind, which `run.sh` avoids by building into a scratch
directory.

## `go.mod` is the committed lockfile, and Go leaves no `>=` hole

`go.mod` and `go.sum` are committed for the same reason every other example commits its lockfile: it
is what makes the CI job load-bearing. `renovate.json` automerges every non-major bump, so a pgx bump
arrives as one PR touching both [`../go.mod`](../go.mod) and this directory — and the `example` job on
that PR is what blocks the automerge when the new version breaks real usage. That only works while the
job stays in [`.github/workflows/pgx.yaml`](../../.github/workflows/pgx.yaml); there is a comment on
the job saying so.

A Python or npm manifest can defeat that by declaring a floor — `sqlalchemy>=2.0` absorbs every
future 2.x silently, so nothing ever touches the example directory and the job has nothing to gate.
**Go has no such spelling.** A `require` directive names one exact version, and version selection
never reaches past the versions the module graph names, so a new pgx release cannot enter this build
without a commit that edits this file. `run.sh` exports `-mod=readonly` on top of that, so an attempt
fails loudly rather than quietly rewriting `go.mod`.

The `replace` directive tightens it further, in a way worth knowing before it surprises someone: this
module's pgx version is the maximum of what it requires and what the replaced adapter requires. So a
bump landing in `../go.mod` alone changes the version selected here too, and this module's `go.sum`
would not carry the new hashes — a build failure, not a silent pass. The gate is fail-closed from
either direction.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It runs one PostgreSQL major under one Go toolchain, which is what this adapter targets: unlike the
ent adapter there is no dialect option to cover, because the SQL this one emits is PostgreSQL's.
