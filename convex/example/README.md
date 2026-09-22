# `@cerbos/orm-convex` example application

Installs the adapter **as a published package** and uses it the way a consumer would, against the
shared [demo domain](../../demo/README.md).

## Run it

```bash
(cd convex && npm ci)                 # the adapter's build dependencies; run.sh builds it before packing
demo/scripts/run-example.sh convex    # from the repository root
```

Needs Docker (with compose), `jq`, `curl` and Node 22+. The runner starts the pinned Cerbos PDP;
[`run.sh`](run.sh) then starts a Convex backend, packs the adapter, installs the tarball, deploys the
functions, compiles the client from cold, and runs it. stdout is one JSON document, which the runner
diffs against `demo/expected.json`.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes) of the demo domain:

- **Shapes 1–3** — a plain filtered list, `KIND_ALWAYS_ALLOWED` and `KIND_ALWAYS_DENIED` (`list` in [`convex/documents.ts`](convex/documents.ts)).
- **Shape 4** — `.paginate()` over the adapter's filter (`page`).
- **Shape 5** — the adapter's filter ANDed with the application's own predicate in one `.filter()` call.

It does **not** exercise `allowPostFilter`: every demo shape is a flat comparison Convex's engine
evaluates itself, so the adapter is called at its default of `false` and a post-filter would fail
the run. Post-filter semantics are covered by the conformance corpus.

### Two halves: client and backend

`queryPlanToConvex` returns a function of Convex's `FilterBuilder`, which only exists inside a
Convex query. So [`src/main.ts`](src/main.ts) plans, seeds and reports, and
[`convex/documents.ts`](convex/documents.ts) translates and queries. What that means for a consumer:

- **The plan is serialized on the way in.** `@cerbos/core` builds plans from `PlanExpression` class
  instances, which Convex's argument encoder rejects
  (`PlanExpression {…} is not a supported Convex type`). The client round-trips the plan through
  JSON; the adapter handles the prototype-less tree because it classifies operands by shape, not
  `instanceof` ([#419](https://github.com/cerbos/query-plan-adapters/issues/419)).
- **The plan kind comes back as a bare string.** The client re-narrows it against the adapter's
  re-exported `PlanKind`.
- **These are public `query` functions; yours should be `internalQuery`.** The example's client is
  an outside process. A real application plans in trusted code and passes the plan to an
  `internalQuery` — anyone who can hand you a plan can hand you an `ALWAYS_ALLOWED` one. See the
  adapter README's "Trusted usage pattern".

### The mapper

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "public" },
};
```

Without it the adapter throws `No mapper entry for …`
([#492](https://github.com/cerbos/query-plan-adapters/issues/492)). `region` and `archived` are
not mapped: they are the application's own fields, never referenced by
[`demo/policies/document.yaml`](../../demo/policies/document.yaml), and used only in shape 5.

### Shape 4: `.paginate()`

Convex has no filtered count, so pagination stands in for it. `.filter()` runs before
`.paginate()`, so a page holds `numItems` *allowed* documents. The client asserts page sizes and
the sorted union of ids, never per-page order. `isDone` is the only end condition: a filtered
`.paginate()` works under a read budget, so a short or empty page is not the end. The loop is
bounded by the seed-row count so a cursor that never finishes fails rather than hangs.

### Shape 5: one `.filter()` call

```ts
return (q) => q.and(
  adapterFilter(q),                                // the adapter's filter (KIND_CONDITIONAL)
  q.eq(q.field("archived"), application.archived), // the application's own predicate,
  q.eq(q.field("region"), application.region),     // declared in demo/seeds.json
);
```

`KIND_ALWAYS_ALLOWED` has no adapter filter, so the application predicate stands alone.
`KIND_ALWAYS_DENIED` returns before any predicate is built, so no application predicate can turn a
denial into rows.

## Packaging checks

`run.sh` installs the tarball `npm publish` would upload, so the adapter resolves through its
published surface — `exports`, `types`, the `files` allowlist, and the `@cerbos/core` peer range
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). The conformance harness
imports from `"."` and never touches it. Two resolvers read that surface:

| Resolver | Reads | Where |
| --- | --- | --- |
| Convex's bundler and `tsc`, `moduleResolution: "Bundler"` | `exports`, `types`, `files` | [`convex/documents.ts`](convex/documents.ts), at `npx convex deploy` |
| `tsc`, `moduleResolution: "nodenext"` | `exports`, `types` | [`src/main.ts`](src/main.ts), at `npm run build` |

Both were verified by breaking them:

| Break | `npx convex deploy` | `npm run build` (client) | `npm test` |
| --- | --- | --- | --- |
| `exports["."]` points at a missing file | fails — `The module "./lib/missing.js" was not found on the file system` | fails (TS2307) | passes |
| `lib/**/*.js` dropped from `files` | fails — `The module "./lib/index.js" was not found on the file system` | **passes** — the `.d.ts` files still ship | passes |

The second row is why the deploy is needed: only something that executes the package notices a
`files` allowlist that ships types but no implementation.

- `tsconfig.json` uses `nodenext` because the legacy `node10` resolver ignores `exports` and would
  compile a broken map cleanly.
- `run.sh` deletes `lib/` and `tsconfig.tsbuildinfo` before compiling: `tsc --build` is incremental
  and keyed on this example's sources, so a warm tree would hide a packaging break.
- `@cerbos/orm-convex` is **not** in `package.json`: the tarball's integrity hash changes every
  build, which would break `npm ci`. `run.sh` runs `npm ci` and then
  `npm install --no-save --no-package-lock` on the tarball.
- `@cerbos/core` **is** in `package.json`: it is the adapter's peer, and declaring it is what a
  pnpm or Yarn consumer has to do.
- `package-lock.json` is committed so Renovate's automerged Convex bumps are blocked by this example
  when they break real usage. That only works while the job stays in
  [`.github/workflows/convex.yaml`](../../.github/workflows/convex.yaml).
- The client uses `makeFunctionReference` rather than the generated `api` object: `convex/_generated/`
  is written by a deploy, is not committed, and is ESM while this client compiles to CommonJS. An
  application that ships its own `_generated` would use `api` — the reference is the same value.

## Ports

The Convex backend runs on **13210/13211** and the PDP on **13592/13593**, not the defaults
(3210/3211 are what `npm run convex:up` binds; 3592/3593 are Cerbos's). Sharing them would not fail
— it would deploy over a conformance run's functions or plan against another policy suite. Both are
passed through `$CONVEX_URL` and `$CERBOS_HOST` with no fallback. The backend is started from the
adapter's own [`../docker-compose.yml`](../docker-compose.yml) with the ports overridden, so the
image pin lives in one place.

## Layout

| Path | What it is |
| --- | --- |
| `run.sh` | backend up → pack → install → deploy → compile cold → run |
| `src/main.ts` | The client: plans against the PDP, seeds, walks the shapes, prints the JSON document |
| `convex/documents.ts` | The backend: where the adapter runs, beside `ctx.db` |
| `convex/schema.ts` | The demo domain's one table: flat scalar fields, no relations |
| `package.json` / `package-lock.json` | Convex, the Cerbos SDK and the `@cerbos/core` peer, pinned and managed by Renovate |

## Scope

A JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/); [ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)
explains why both exist. It follows [`prisma/example/`](../../prisma/example/), the reference
implementation.
