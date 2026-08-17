# Ent adapter example application

A runnable program that uses the adapter the way a consumer would — against a **generated ent
client** — over the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh ent
```

Needs `docker` (with compose), `jq`, and a Go toolchain satisfying this directory's `go` directive.
The runner starts the pinned Cerbos PDP; this directory's `run.sh` builds this module and runs the
program.

## This example proves usage shapes, not packaging

Every other example in this repository builds its adapter into the artifact a registry would serve —
`npm pack`, `pdm build`, `publishToMavenLocal` — and installs **that**, so that the published surface
is executed:
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md). **Go cannot participate,
and this example does not.** There is no packaging step to run: a Go example either resolves the
adapter through a `replace` directive, which is not what a consumer does, or resolves a published tag,
which would never test the change under review. This one uses `replace`:

```
replace github.com/cerbos/query-plan-adapters/ent => ../
```

So nothing here says anything about what a `go get` of this module would give a consumer. That is
stated rather than glossed because the ADR's Consequences section asks for exactly that — "Their
READMEs should say so rather than implying packaging coverage they do not have" — and because the gap
is not empty:

- **Resolution is never exercised.** A consumer resolves `github.com/cerbos/query-plan-adapters/ent`
  at an `ent/vX.Y.Z` tag through the module proxy, which checks that the path `../go.mod` declares is
  the path the repository serves it under, and that the tag prefix resolves at all. A `replace`
  answers both questions from the filesystem instead, so neither is asked here.
- **A directory read sees files the zip omits.** Not many — reading `golang.org/x/mod/zip`, the
  omissions are files in nested modules and in `vendor/`, symbolic links and other irregular files,
  and `.hg_archival.txt`. (Not `testdata/`, and not dot- or underscore-prefixed paths: those are
  rules about what the Go *build* ignores, and the zip carries them. `.gitignore` is in it.) So
  adapter source reached through a symlink would build here and be missing for a consumer.

What is left is the other half of
[cerbos/query-plan-adapters#349](https://github.com/cerbos/query-plan-adapters/issues/349), and here
it is the larger half:

**Usage shape.** A conformance harness runs one flat filtered query, and
[`../adversarial_test.go`](../adversarial_test.go) runs it through a hand-built `entsql.Selector`
against `database/sql` — no generated ent client appears anywhere in `../`. So the line every
consumer of this adapter actually writes is executed here and nowhere else in this repository:

```go
query.Where(func(s *entsql.Selector) { s.Where(result.Predicate) })
```

…on a `*ent.DocumentQuery`, paged with ent's own `Limit`/`Offset`, and ANDed with generated
predicates the application owns. All [five shapes](../../demo/README.md#the-five-usage-shapes) run,
across all three plan kinds.

## The example is excluded from the module a consumer downloads

A directory containing a `go.mod` is excluded from its parent's module zip, so this example's
generator and driver dependencies — `entgo.io/ent/cmd/ent` and the `ariga.io/atlas`, `hcl` and
`cobra` trees behind it, `modernc.org/sqlite` — never reach a consumer of
`github.com/cerbos/query-plan-adapters/ent`. That is what
[`CLAUDE.md`](../../CLAUDE.md) means by both Go modules being standalone, and it is the reason
ADR 0002 exempts Go from checking the exclusion deliberately the way Python and Java must.

It was verified rather than assumed, by packing the parent module with `golang.org/x/mod/zip` — the
package `cmd/go` itself builds module zips with — and counting the entries, twice. At the commit that
added this directory:

| `ent/example/go.mod` | entries in the zip | of those, under `example/` |
| -------------------- | ------------------ | -------------------------- |
| present              | 15                 | 0                          |
| renamed away         | 40                 | 25                         |

The second row is the control: without it the first proves only that the packer produced *something*.
Every number here moves as either directory grows. The one that must not is the first row's right-hand
cell.

`run.sh` re-checks the cause rather than the consequence on every run — that `go list -m` here names
this module and not the adapter — because the cause is the half a stray edit could regress and it
costs nothing to ask.

## Layout

| Path               | What it is                                                                     |
| ------------------ | ------------------------------------------------------------------------------ |
| `run.sh`           | check → resolve → build → run. Prints the JSON document on stdout.             |
| `main.go`          | The example itself: one function per usage shape.                              |
| `ent/schema/`      | The hand-written ent schema — the demo domain's one entity.                    |
| `ent/`             | The generated client, committed. See below.                                    |
| `go.mod`, `go.sum` | This example's pins. Committed, and Renovate manages them. See below.          |

The SQLite file is scratch state `main.go` deletes and recreates on every run — a dedicated file
rather than an in-memory database, so a failing run leaves the seeded rows behind to inspect.

## The generated client is committed

Which is ent's own convention, and it means this directory builds, vets and lints straight from a
checkout with no generation step — the same thing a consumer's repository gets. Regenerate after
editing `ent/schema/document.go`:

```bash
go generate ./ent
```

The generator is pinned as a `tool` dependency in `go.mod`, so the version that runs is the version
this module declares rather than whatever is on the machine. That is also where most of the indirect
requires in `go.mod` come from; none of them are in the built program, and by the section above none
of them are in the adapter either.

## `go.mod` is the committed lockfile, and Go leaves no `>=` hole

`go.mod` and `go.sum` are committed for the same reason every other example commits its lockfile: it
is what makes the CI job load-bearing. `renovate.json` automerges every non-major bump, so an ent
bump arrives as one PR touching both [`../go.mod`](../go.mod) and this directory — and the `example`
job on that PR is what blocks the automerge when the new version breaks real usage. That only works
while the job stays in [`.github/workflows/ent.yaml`](../../.github/workflows/ent.yaml); there is a
comment on the job saying so.

A Python or npm manifest can defeat that by declaring a floor — `sqlalchemy>=2.0` absorbs every
future 2.x silently, so nothing ever touches the example directory and the job has nothing to gate.
**Go has no such spelling.** A `require` directive names one exact version, and version selection
never reaches past the versions the module graph names, so a new ent release cannot enter this build
without a commit that edits this file. `run.sh` exports `-mod=readonly` on top of that, so an attempt
fails loudly rather than quietly rewriting `go.mod`.

The `replace` directive tightens it further, in a way worth knowing before it surprises someone: this
module's ent version is the maximum of what it requires and what the replaced adapter requires. So a
bump landing in `../go.mod` alone changes the version selected here too, and this module's `go.sum`
would not carry the new hashes — a build failure, not a silent pass. The gate is fail-closed from
either direction.

## The attribute map

Cerbos attribute names are not column names, so a consumer always writes one of these:

```go
var mapper = cerbosent.MapperMap{
    "request.resource.attr.ownerId": {Column: document.FieldOwnerID},
    "request.resource.attr.public":  {Column: document.FieldIsPublic},
}
```

Resolution is fail-closed, so without it the adapter has nothing to resolve
`request.resource.attr.ownerId` to and returns an error — which is itself worth seeing in an example.
The columns come from the **generated** field constants, so renaming a field in the schema is a
compile error here rather than an unmapped-reference failure at run time. The schema calls them
`owner_id` and `is_public`, which is ordinary ent naming and makes the point that a Cerbos attribute
name is neither the column name nor the name on the model.

`region` and `archived` are deliberately absent: they are the application's columns, never referenced
by policy, and composing them with the adapter's predicate is shape 5.

## `KindAlwaysDenied` carries no predicate, and shape 5 depends on that

`Translate` returns a `Predicate` for `KindConditional` only. For the two unconditional kinds the
caller applies no filter and — for a denial — runs no query at all, which is the switch the
[adapter's README](../README.md#usage) shows and the one `applyPlan` in `main.go` writes.

That makes shape 5 read differently here than in an adapter whose translation of a denial is a
`WHERE false` query it can hand back. There the composed shape executes
`WHERE false AND <application predicate>` and demonstrates that the application's own filter cannot
resurrect a denied row. Here there is nothing for the application predicate to be ANDed *with*: the
property holds structurally, and what this example asserts is that the composed shape reports
`KIND_ALWAYS_DENIED` with no ids while the application predicate is in place.

The application's half is applied **before** the plan is, because that is the honest order: an
application composing its own filters does not know, and must not have to know, which plan kind the
PDP is about to return.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It runs SQLite under one Go toolchain. The adapter's `WithDialect` also covers PostgreSQL and MySQL,
and [`../adversarial_test.go`](../adversarial_test.go) replays the whole conformance corpus against
all three; a second dialect here would re-run the same plumbing.
