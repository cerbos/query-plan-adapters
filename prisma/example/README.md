# `@cerbos/orm-prisma` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

## Run it

```bash
(cd prisma && npm ci)                  # once: run.sh builds the adapter before packing it
demo/scripts/run-example.sh prisma     # from the repository root
```

Needs `docker` (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP and sets
`CERBOS_HOST`; this directory's `run.sh` packs the adapter, installs the tarball, generates the
Prisma client, creates a scratch SQLite database, compiles and runs. The program prints one JSON
document, which the runner diffs against `demo/expected.json`.

This is the reference implementation every other adapter's example copies.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), across all three plan kinds,
one function each in [`src/main.ts`](src/main.ts), including:

- pagination over the adapter's filter;
- the adapter's filter ANDed with the application's own predicate (`archived`, `region`) — the
  shape that breaks first, because `ALWAYS_ALLOWED` contributes no predicate.

The mapper is what every consumer writes, since Cerbos attribute names are not column names:

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "public" },
};
```

Without it the adapter emits `request.resource.attr.ownerId` as a literal Prisma field and the query
fails. `archived` and `region` are absent on purpose: policy never references them.

## Packaging and verification

Translation is proved elsewhere ([`../src/adversarial.test.ts`](../src/adversarial.test.ts), against
the hostile corpus). This example proves what that harness cannot: that the import resolves through
the **published surface** — the `exports` map, `types`, the `files` allowlist, and the peer range
against this example's own `@prisma/client`. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

Both checks were verified by breaking them:

| Break | Example | `npm test` | `npm run test:adversarial` |
| --- | --- | --- | --- |
| `exports["."]` points at a missing file | fails (TS2307) | passes | passes |
| `lib/**/*.js` dropped from `files` | fails (MODULE_NOT_FOUND) | passes | passes |

`tsconfig.json` uses `moduleResolution: "nodenext"` for the first row: the legacy `node10`
resolver ignores `exports`, so a broken map would compile here and fail only for a consumer.

| Path | What it is |
| --- | --- |
| `run.sh` | pack → install → generate → compile → run. JSON on stdout, everything else on stderr. |
| `src/main.ts` | The example: one function per usage shape. |
| `prisma/schema.prisma` | The demo domain's one model: flat scalar columns, no relations. |
| `package.json` | The ORM and SDK pins Renovate manages. |
| `package-lock.json` | Committed (see below). |

**`@cerbos/orm-prisma` is not in `package.json`.** The packed tarball's integrity hash changes on
every build, so a lockfile naming it would break `npm ci`. `run.sh` runs `npm ci`, then installs the
tarball with `--no-save --no-package-lock`, leaving both manifests as committed.

**The lockfile is committed anyway.** `renovate.json` automerges non-major bumps, so a Prisma bump
is one PR touching `prisma/package.json` and this lockfile, and the `example` job on that PR blocks
the automerge if real usage breaks. That only works while the job stays in
`.github/workflows/prisma.yaml`.

## Scope

- A JSON-printing CLI, not an onboarding artifact; that is
  [`spring-data/example/`](../../spring-data/example/). See
  [ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md).
- It does **not** prove the declared peer range: the adapter claims `^5 || ^6 || ^7` and this
  example runs Prisma 7 only.
