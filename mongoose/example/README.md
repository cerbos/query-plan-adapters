# `@cerbos/orm-mongoose` example application

A runnable program that installs the adapter as a published package and uses it the way a consumer
would, against the shared [demo domain](../../demo/README.md).

## Run it

```bash
# from the repository root
(cd mongoose && npm install)          # run.sh builds the adapter before packing it
demo/scripts/run-example.sh mongoose
```

Needs Docker (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP; this
directory's `run.sh` starts MongoDB, packs the adapter, installs the tarball, compiles cold, runs,
and prints one JSON document that the runner diffs against `demo/expected.json`. It follows
[`prisma/example/`](../../prisma/example/), the reference implementation.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), across all three plan kinds:

1. A plain filtered list.
2. `KIND_ALWAYS_ALLOWED` — no predicate.
3. `KIND_ALWAYS_DENIED` — no query at all.
4. Pagination on top of the filter.
5. The adapter's filter ANDed with an application-owned filter (`archived`, `region`).

The mapper is the one piece of configuration a consumer cannot skip; the adapter throws
`No mapper entry for <reference>` for any attribute without one
([#492](https://github.com/cerbos/query-plan-adapters/issues/492)):

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "isPublic" },
};
```

`isPublic` differs from the policy's `public` on purpose: a Cerbos attribute name is not a document
path. `archived` and `region` are absent because policy never references them.

The plan kinds map onto Mongoose as `type Where = MongooseFilter | "denied"`:

- `ALWAYS_ALLOWED` becomes `{}`, which `find()` and `$and` both treat as "no predicate", so
  `{ $and: [where, APPLICATION_FILTER] }` needs no branch.
- `ALWAYS_DENIED` is a string sentinel, not a second `{}`: a denial that reached `$and` would return
  the application's filter alone, i.e. documents the PDP denied.
- A `KIND_CONDITIONAL` result with no `filters` throws rather than falling back to `{}`.

## Packaging checks

The adapter's test suites import from source. This example installs `npm pack`'s tarball, so the
import in [`src/main.ts`](src/main.ts) resolves through the published `exports` map, `types` and
`files` allowlist ([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). Both
breaks below were checked by hand:

| Break                                   | Example                  | `npm test`  | `npm run test:adversarial` |
| --------------------------------------- | ------------------------ | ----------- | -------------------------- |
| `exports["."]` points at a missing file  | fails (TS2307)           | passes | passes                |
| `lib/**/*.js` dropped from `files`       | fails (MODULE_NOT_FOUND) | passes | passes                |

- `tsconfig.json` uses `moduleResolution: "nodenext"`; the legacy `node10` resolver ignores
  `exports`, so a broken map would compile clean here and fail only for a consumer.
- `run.sh` deletes `lib/` and `tsconfig.tsbuildinfo` before compiling, because incremental
  `tsc --build` would otherwise skip the first break on a warm tree.
- `@cerbos/orm-mongoose` is not in `package.json`: the tarball's integrity hash changes on every
  build, which would break `npm ci`. `run.sh` runs `npm ci`, then installs the tarball with
  `--no-save --no-package-lock`.
- The lockfile is committed so a Renovate Mongoose bump touches both `mongoose/package.json` and
  this lockfile, and the `example` job in
  [`.github/workflows/mongoose.yaml`](../../.github/workflows/mongoose.yaml) blocks the automerge if
  real usage breaks. Keep the job in that workflow.
- There is no Mongoose peer range to prove: the adapter never imports `mongoose`. The `mongoose` the
  filter is handed to is the application's, from this directory's manifest.

## Layout

| Path                | What it is                                                                     |
| ------------------- | ------------------------------------------------------------------------------ |
| `run.sh`            | Start MongoDB → pack → install → compile cold → run. Prints the JSON document on stdout. |
| `src/main.ts`       | The example: one function per usage shape.                                     |
| `src/schema.ts`     | The demo's one collection: a flat document, `_id` set to the demo id (`"d1"`, …) rather than an ObjectId. |
| `package.json`      | The ORM and SDK pins Renovate manages.                                         |
| `package-lock.json` | Committed (see above).                                                         |

## The store

- `run.sh` reads the image from [`../MONGO_IMAGE`](../MONGO_IMAGE), the same file `npm run mongo`
  and the baseline CI leg read, so there is one pin to bump. It runs only that baseline server;
  [`../MONGO_NEXT_IMAGE`](../MONGO_NEXT_IMAGE) is covered by the adapter's own suites.
- MongoDB is published on host port **27117**, not 27017, so it cannot collide with `npm run mongo`
  or the adapter's CI (the same reason the demo PDP uses 13592/13593).

This is a JSON-printing CLI, not an onboarding artifact; that is
[`spring-data/example/`](../../spring-data/example/)
([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).
