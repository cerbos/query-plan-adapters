# `@cerbos/orm-prisma` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh prisma
```

Needs `docker` (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP; this
directory's `run.sh` packs the adapter, installs the tarball, builds, and runs.

This example consumes the shared runtime contract in `demo/cases.json`.

## What it proves

Not what the adapter translates — [`../src/adversarial.test.ts`](../src/adversarial.test.ts)
proves that against a hostile corpus with a live PDP as the oracle. This proves the two things
that harness structurally cannot:

**Packaging.** `run.sh` builds the artifact `npm publish` would upload and installs *that*, so the
import in [`src/main.ts`](src/main.ts) resolves through the published surface — the `exports` map,
`types`, the `files` allowlist, and the peer range against this example's own `@prisma/client`.
The harness imports from `"."` and touches none of it. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

Both halves are load-bearing and both have been checked by breaking them:

| Break                                     | Example        | `npm test`  | `npm run test:adversarial` |
| ----------------------------------------- | -------------- | ----------- | -------------------------- |
| `exports["."]` points at a missing file    | fails (TS2307) | 223 passing | 162 passing                |
| `lib/**/*.js` dropped from `files`         | fails (MODULE_NOT_FOUND) | 223 passing | 162 passing      |

`tsconfig.json` sets `moduleResolution: "nodenext"` for the first row specifically: the legacy
`node10` resolver ignores `exports` entirely and falls back to `main`/`types`, so a broken
`exports` map would compile clean here and only fail for a consumer.

**Usage shape.** A harness runs one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), including pagination and — the one that
earns the exercise — the adapter's filter ANDed with the application's own predicate, across all
three plan kinds.

## Layout

| Path                   | What it is                                                          |
| ---------------------- | -------------------------------------------------------------------- |
| `run.sh`               | pack → install → generate → compile → run. Prints the JSON document on stdout. |
| `src/main.ts`          | The example itself: one function per usage shape.                    |
| `prisma/schema.prisma` | The demo domain's one model, as a consumer would write it: flat scalar columns, no relations. |
| `package.json`         | The ORM and SDK pins Renovate manages.                               |
| `package-lock.json`    | Committed. See below.                                                |

## Two things that look odd and are not

**`@cerbos/orm-prisma` is not in `package.json`.** `npm pack`'s tarball gets a fresh integrity
hash on every build, so a committed lockfile naming it would break `npm ci` the moment the adapter
changed. `run.sh` runs `npm ci` for the pinned tree and then installs the tarball on top with
`--no-save --no-package-lock`, which leaves both manifests exactly as committed while still
resolving the adapter and its peers the way a consumer's install does.

**The lockfile is committed anyway,** because it is what makes the CI job load-bearing.
`renovate.json` automerges every non-major bump, so a Prisma bump arrives as one PR touching both
`prisma/package.json` and this file — and the `example` job on that PR is what blocks the
automerge when the new ORM breaks real usage. That only works while the job stays in
`.github/workflows/prisma.yaml`; there is a comment on the job saying so.

## The mapper

Cerbos attribute names are not column names, so a consumer always writes one of these:

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "public" },
};
```

Without it the adapter emits `request.resource.attr.ownerId` as a literal Prisma field and the
query fails — which is itself worth seeing in an example.

`archived` and `region` are deliberately absent: they are the application's columns, never
referenced by policy, and composing them with the adapter's filter is shape 5.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It also does **not** prove the declared peer range. `@cerbos/orm-prisma` claims
`^5 || ^6 || ^7`; this example runs one of those. Widening the harness matrix is the fix for that,
and it is out of scope here.
