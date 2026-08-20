# `@cerbos/orm-mongoose` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh mongoose
```

Needs `docker` (with compose), `jq` and Node 22+, plus `npm install` in [`../`](..) — `run.sh`
packs the adapter by building it, so the adapter's own `node_modules` has to exist. The runner
starts the pinned Cerbos PDP; this directory's `run.sh` starts the pinned MongoDB server, packs
the adapter, installs the tarball, builds, and runs.

It consumes the shared runtime contract in [`demo/cases.json`](../../demo/cases.json).

## What it proves

Not what the adapter translates — [`../src/adversarial.test.ts`](../src/adversarial.test.ts)
proves that against a hostile corpus with a live PDP as the oracle, on two MongoDB server
versions. This proves the two things that harness structurally cannot:

**Packaging.** `run.sh` builds the artifact `npm publish` would upload and installs *that*, so the
import in [`src/main.ts`](src/main.ts) resolves through the published surface — the `exports` map,
`types`, and the `files` allowlist. The harness imports from `"."` and touches none of it. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

Both halves are load-bearing and both have been checked by breaking them:

| Break                                   | Example                  | `npm test`  | `npm run test:adversarial` |
| --------------------------------------- | ------------------------ | ----------- | -------------------------- |
| `exports["."]` points at a missing file  | fails (TS2307)           | 104 passing | 158 passing                |
| `lib/**/*.js` dropped from `files`       | fails (MODULE_NOT_FOUND) | 104 passing | 158 passing                |

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
| `run.sh`            | start MongoDB → pack → install → compile cold → run. Prints the JSON document on stdout. |
| `src/main.ts`       | The example itself: one function per usage shape.                              |
| `src/schema.ts`     | The demo domain's one collection, as a consumer would model it: one flat document, no sub-documents. |
| `package.json`      | The ORM and SDK pins Renovate manages.                                         |
| `package-lock.json` | Committed. See below.                                                          |

`_id` carries the demo domain's own id (`"d1"`, `"d2"`, …) rather than an ObjectId, which is
ordinary Mongoose and means the id this example reports is the id the store keys on, with nothing
projected between the two.

## The store

`run.sh` starts MongoDB itself rather than leaving it to the shared runner.
`demo/docker-compose.yml` holds the one service every example genuinely shares — the PDP — and
nearly every store belongs to somebody else; putting them there would grow it into the language
switch that [split](../../demo/README.md#running-an-example) exists to avoid.

Two things in `run.sh` are deliberate:

- **The image is not named here.** `run.sh` reads [`../MONGO_IMAGE`](../MONGO_IMAGE), the same
  file `../package.json`'s `mongo` script and the baseline leg of
  [`.github/workflows/mongoose.yaml`](../../.github/workflows/mongoose.yaml) read, so the server
  this example proves the packaging against is the one the adapter is developed and tested
  against. A literal here would be a third copy, and
  `conformance/scripts/validate-corpus.sh` can only hold copies to one digest per *tag* — nothing
  holds two tags equal, so moving the adapter off 7.0 would leave the example behind, still
  pinned and still green.
- **The host port is 27117, not 27017.** 27017 is what `npm run mongo` and the adapter's own CI
  bind, and a demo server holding it would leave one of the two silently reading the other's data
  — the same failure mode that puts the demo PDP on 13592/13593 instead of 3592/3593.

The example runs one server version: the baseline, whatever `MONGO_IMAGE` currently names. The
adapter's workflow also runs [`../MONGO_NEXT_IMAGE`](../MONGO_NEXT_IMAGE) because the corpus
discriminates server *behaviour* — three-valued logic, BSON ordering, regex handling — which is
semantics, already covered there.

## Two things that look odd and are not

**`@cerbos/orm-mongoose` is not in `package.json`.** `npm pack`'s tarball gets a fresh integrity
hash on every build, so a committed lockfile naming it would break `npm ci` the moment the adapter
changed. `run.sh` runs `npm ci` for the pinned tree and then installs the tarball on top with
`--no-save --no-package-lock`, which leaves both manifests exactly as committed while still
resolving the adapter the way a consumer's install does.

**The lockfile is committed anyway,** because it is what makes the CI job load-bearing.
`renovate.json` automerges every non-major bump, so a Mongoose bump arrives as one PR touching
both `mongoose/package.json` and this file — and the `example` job on that PR is what blocks the
automerge when the new ORM breaks real usage. That only works while the job stays in
[`.github/workflows/mongoose.yaml`](../../.github/workflows/mongoose.yaml); there is a comment on
the job saying so.

## The mapper

Cerbos attribute names are not document paths, so a consumer always writes one of these:

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "isPublic" },
};
```

This is the one piece of configuration a Mongoose consumer must not skip. An unmapped reference
resolves to *itself*, so the adapter would filter on a document path literally named
`request.resource.attr.ownerId` — which matches nothing and returns an empty list rather than an
error. `src/schema.ts` names the second path `isPublic` while the policy calls the attribute
`public`, which is ordinary Mongoose and makes the point that a Cerbos attribute name is neither
of those two things.

`region` and `archived` are deliberately absent from the mapper: they are the application's
paths, never referenced by policy, and composing them with the adapter's filter is shape 5.

## `{}` is MongoDB's word for "no predicate"

The three plan kinds land on Mongoose the same way they land on Drizzle, which is worth naming
because it is what makes shape 5 a one-liner here:

```ts
type Where = MongooseFilter | "denied";
```

`ALWAYS_ALLOWED` becomes `{}`, which both `find()` and `$and` already treat as "no predicate", so
`{ $and: [where, APPLICATION_FILTER] }` needs no branch for it. That is also why `ALWAYS_DENIED`
is a string sentinel rather than a second `{}`: a denial that reached `$and` would come back out
as the application's filter and return documents the PDP denied.

A `KIND_CONDITIONAL` result whose `filters` is missing throws instead of falling back to `{}`, for
the same reason — quietly widening an absent predicate to match-all is the one mistranslation that
hands back rows the PDP refused.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

There is no declared peer range for it to prove, unlike `prisma/example/` and `drizzle/example/`.
`@cerbos/orm-mongoose` never imports `mongoose` — it returns a plain filter object, and `mongoose`
is a dev dependency of the adapter only — so nothing in the published package constrains, or
needs to constrain, which Mongoose a consumer pairs it with. What the example does add is that
the `mongoose` the filter is handed to is the *application's*, installed from this directory's own
manifest rather than the adapter's.
