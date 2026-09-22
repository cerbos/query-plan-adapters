# `@cerbos/orm-drizzle` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

## Run it

```bash
(cd drizzle && npm ci)            # the adapter's build dependencies; run.sh builds it before packing
demo/scripts/run-example.sh drizzle   # from the repository root
```

Needs `docker` (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP, sets
`CERBOS_HOST`, runs this directory's `run.sh`, and diffs its stdout against
[`demo/expected.json`](../../demo/expected.json). `run.sh` builds and packs the adapter, runs
`npm ci`, installs the tarball on top, compiles from cold and runs `node lib/main.js`.

[`prisma/example/`](../../prisma/example/) is the reference implementation this one follows.

## What it demonstrates

The [five usage shapes](../../demo/README.md#the-five-usage-shapes), one function each in
[`src/main.ts`](src/main.ts):

1. A plain filtered list — the adapter's filter is the whole query.
2. `ALWAYS_ALLOWED` — mapped to `undefined`, which `.where()` treats as "no predicate".
3. `ALWAYS_DENIED` — short-circuits to an empty result without querying.
4. Pagination — `orderBy` + `limit` / `offset` on top of the filter.
5. Composition — `and(adapterFilter, APPLICATION_FILTER)` across all three plan kinds.

The call site maps plan kinds to a `where` value:

```ts
type Where = SQL | undefined | "denied";
```

`ALWAYS_ALLOWED` becomes `undefined`, so `and(where, APPLICATION_FILTER)` collapses to the
application's predicate with no branch. `ALWAYS_DENIED` is a string sentinel, not a second
`undefined`: a denial that reached `and()` would come back as the application's filter alone and
return rows the PDP denied.

The mapper covers only the attributes policy reads:

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": documents.ownerId,
  "request.resource.attr.public": documents.public,
};
```

Cerbos attribute names are neither column names (`owner_id`) nor TypeScript properties, so a
consumer always writes one; without it the adapter throws. `archived` and `region` are left out on
purpose: they belong to the application filter in shape 5, not to policy.

## Packaging proof

[`../src/adversarial.test.ts`](../src/adversarial.test.ts) proves what the adapter translates. It
imports from `"."`, so it never touches the published surface. This example does:
`run.sh` installs the `npm pack` tarball, so the import resolves through the `exports` map, `types`,
the `files` allowlist, and the peer range against this example's own `drizzle-orm`
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)).

Both breaks were checked by hand:

| Break                                   | Example                  | `npm test`  | `npm run test:adversarial` |
| --------------------------------------- | ------------------------ | ----------- | -------------------------- |
| `exports["."]` points at a missing file  | fails (TS2307)           | passes | passes                |
| `lib/**/*.js` dropped from `files`       | fails (MODULE_NOT_FOUND) | passes | passes                |

- `tsconfig.json` sets `moduleResolution: "nodenext"`, because the legacy `node10` resolver ignores
  `exports` and would compile a broken map clean.
- `run.sh` deletes `lib/` and `tsconfig.tsbuildinfo` first. `tsc --build` is incremental and keyed
  on this example's sources, so on a warm tree the first break would compile clean.

## Layout

| Path                | What it is                                                                     |
| ------------------- | ------------------------------------------------------------------------------ |
| `run.sh`            | pack → install → compile cold → run. Prints the JSON document on stdout.        |
| `src/main.ts`       | The example: one function per usage shape.                                     |
| `src/schema.ts`     | The demo's one table as flat scalar columns, with its `CREATE TABLE` beside it. |
| `package.json`      | The ORM and SDK pins Renovate manages.                                         |
| `package-lock.json` | Committed (see below).                                                         |

There is no `drizzle-kit` or generation step. The SQLite database is a scratch file (`demo.db`)
that `main.ts` deletes and recreates on every run; a file rather than `:memory:` so a failed run
leaves the seeded rows to inspect.

## Things that look odd

- **`@cerbos/orm-drizzle` is not in `package.json`.** The packed tarball's integrity hash changes on
  every build, which would break `npm ci` against a committed lockfile. `run.sh` runs `npm ci`, then
  installs the tarball with `--no-save --no-package-lock`.
- **The lockfile is committed anyway.** Renovate automerges non-major bumps, so a Drizzle bump is
  one PR touching `drizzle/package.json` and this lockfile, and the `example` job on that PR blocks
  the automerge if real usage breaks. That only works while the job stays in
  [`.github/workflows/drizzle.yaml`](../../.github/workflows/drizzle.yaml).

## Scope

- A JSON-printing CLI, not an onboarding artifact — that is
  [`spring-data/example/`](../../spring-data/example/)
  ([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).
- SQLite only. Collation, LIKE escaping and parameter typing on PostgreSQL and MySQL are covered by
  the adversarial suite.
- It does **not** prove the full peer range: the adapter declares `^0.44.0 || ^0.45.0` and this
  example installs 0.45.
