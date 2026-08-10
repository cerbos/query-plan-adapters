# `@cerbos/orm-drizzle` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh drizzle
```

Needs `docker` (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP; this
directory's `run.sh` packs the adapter, installs the tarball, builds, and runs.

It follows [`prisma/example/`](../../prisma/example/), which is the reference implementation.

## What it proves

Not what the adapter translates — [`../src/adversarial.test.ts`](../src/adversarial.test.ts)
proves that against a hostile corpus with a live PDP as the oracle, on SQLite and on PostgreSQL.
This proves the two things that harness structurally cannot:

**Packaging.** `run.sh` builds the artifact `npm publish` would upload and installs *that*, so the
import in [`src/main.ts`](src/main.ts) resolves through the published surface — the `exports` map,
`types`, the `files` allowlist, and the peer range against this example's own `drizzle-orm`. The
harness imports from `"."` and touches none of it. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

Both halves are load-bearing and both have been checked by breaking them:

| Break                                   | Example                  | `npm test`  | `npm run test:adversarial` |
| --------------------------------------- | ------------------------ | ----------- | -------------------------- |
| `exports["."]` points at a missing file  | fails (TS2307)           | 135 passing | 159 passing                |
| `lib/**/*.js` dropped from `files`       | fails (MODULE_NOT_FOUND) | 135 passing | 159 passing                |

`tsconfig.json` sets `moduleResolution: "nodenext"` for the first row specifically: the legacy
`node10` resolver ignores `exports` entirely and falls back to `main`/`types`, so a broken
`exports` map would compile clean here and only fail for a consumer.

`run.sh` deletes `lib/` and `tsconfig.tsbuildinfo` before compiling for the same row. `tsc --build`
is incremental and keys its state on this example's own sources, which do not change when the
adapter's packaging does — so on a warm tree that first break compiles clean and reaches the
second row's runtime error instead. CI is always cold; the tree where someone checks the break by
hand is not.

**Usage shape.** A harness runs one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), including pagination and — the one that
earns the exercise — the adapter's filter ANDed with the application's own predicate, across all
three plan kinds.

## Layout

| Path                | What it is                                                                     |
| ------------------- | ------------------------------------------------------------------------------ |
| `run.sh`            | pack → install → compile cold → run. Prints the JSON document on stdout.        |
| `src/main.ts`       | The example itself: one function per usage shape.                              |
| `src/schema.ts`     | The demo domain's one table, as a consumer would write it: flat scalar columns, no relations. Carries the `CREATE TABLE` beside the schema it has to agree with. |
| `package.json`      | The ORM and SDK pins Renovate manages.                                         |
| `package-lock.json` | Committed. See below.                                                          |

There is no generation step and no `drizzle-kit`: a Drizzle schema is ordinary TypeScript, and one
table's DDL is a string next to it. The SQLite file is scratch state `main.ts` deletes and
recreates on every run — a dedicated file rather than `:memory:`, so a failing run leaves the
seeded rows behind to inspect.

## Two things that look odd and are not

**`@cerbos/orm-drizzle` is not in `package.json`.** `npm pack`'s tarball gets a fresh integrity
hash on every build, so a committed lockfile naming it would break `npm ci` the moment the adapter
changed. `run.sh` runs `npm ci` for the pinned tree and then installs the tarball on top with
`--no-save --no-package-lock`, which leaves both manifests exactly as committed while still
resolving the adapter and its peers the way a consumer's install does.

**The lockfile is committed anyway,** because it is what makes the CI job load-bearing.
`renovate.json` automerges every non-major bump, so a Drizzle bump arrives as one PR touching both
`drizzle/package.json` and this file — and the `example` job on that PR is what blocks the
automerge when the new ORM breaks real usage. That only works while the job stays in
[`.github/workflows/drizzle.yaml`](../../.github/workflows/drizzle.yaml); there is a comment on the
job saying so.

## The mapper

Cerbos attribute names are not column names, so a consumer always writes one of these:

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": documents.ownerId,
  "request.resource.attr.public": documents.public,
};
```

Without it the adapter has nothing to resolve `request.resource.attr.ownerId` to and throws —
which is itself worth seeing in an example. `src/schema.ts` names the columns `owner_id` and
`public` while the TypeScript properties are `ownerId` and `public`, which is ordinary Drizzle and
makes the point that a Cerbos attribute name is neither of those two things.

`archived` and `region` are deliberately absent from the mapper: they are the application's
columns, never referenced by policy, and composing them with the adapter's filter is shape 5.

## `undefined` is Drizzle's word for "no predicate"

The three plan kinds land on Drizzle unusually cleanly, which is worth naming because it is what
makes shape 5 a one-liner here:

```ts
type Where = SQL | undefined | "denied";
```

`ALWAYS_ALLOWED` becomes `undefined`, which both `.where()` and `and()` already treat as "no
predicate", so `and(where, APPLICATION_FILTER)` collapses to the application's predicate alone
without a branch. That is also why `ALWAYS_DENIED` is a string sentinel rather than a second
`undefined`: a denial that reached `and()` would come back out as the application's filter and
return rows the PDP denied.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It runs SQLite only. The PostgreSQL leg in
[`../src/adversarial.test.ts`](../src/adversarial.test.ts) exists to discriminate collation, LIKE
escaping and parameter typing — semantics, already covered there. It also does **not** prove the
declared peer range: `@cerbos/orm-drizzle` claims `^0.44.0 || ^0.45.0` and this example installs
0.45. Widening the harness matrix is the fix for that, and it is out of scope here.
