# `@cerbos/orm-convex` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh convex
```

Needs `docker` (with compose), `jq`, `curl` and Node 22+. The runner starts the pinned Cerbos PDP;
this directory's `run.sh` starts a Convex backend, packs the adapter, installs the tarball, deploys
the functions, builds the client, and runs it.

It follows [`prisma/example/`](../../prisma/example/), which is the reference implementation.

## What it proves

Not what the adapter translates — [`../src/adversarial.test.ts`](../src/adversarial.test.ts)
proves that against a hostile corpus with a live PDP as the oracle, inside a real Convex backend.
This proves the two things that harness structurally cannot:

**Packaging.** `run.sh` builds the artifact `npm publish` would upload and installs *that*, so the
adapter resolves through the published surface — the `exports` map, `types`, the `files` allowlist,
and the `@cerbos/core` peer range against the copy this example declares. The harness imports from
`"."` and touches none of it. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

Two independent resolvers read that surface here, which is a Convex property rather than a
belt-and-braces choice — the adapter runs in the backend and the client only talks to it:

| Resolver | Reads | Where |
| --- | --- | --- |
| Convex's bundler and its `tsc`, under `moduleResolution: "Bundler"` | `exports`, `types`, `files` | [`convex/documents.ts`](convex/documents.ts), at `npx convex deploy` |
| `tsc` under `moduleResolution: "nodenext"` | `exports`, `types` | [`src/main.ts`](src/main.ts), at `npm run build` |

Both halves are load-bearing and both have been checked by breaking them. The bundler runs first,
so it is what reports either break; the third column is what the client compile would have said,
and it is the row where the two resolvers differ:

| Break                                   | `npx convex deploy`                                | `npm run build` (client) | `npm test`  |
| --------------------------------------- | -------------------------------------------------- | ------------------------ | ----------- |
| `exports["."]` points at a missing file  | fails — `The module "./lib/missing.js" was not found on the file system` | fails (TS2307) | 457 passing |
| `lib/**/*.js` dropped from `files`       | fails — `The module "./lib/index.js" was not found on the file system`   | **passes** — the `.d.ts` files are still shipped, so the types resolve and only the bundle does not | 457 passing |

That second row is why the deploy is not redundant with the client compile: a `files` allowlist can
ship every type declaration and no implementation, and only something that has to *execute* the
package notices.

`tsconfig.json` sets `moduleResolution: "nodenext"` for the first row specifically: the legacy
`node10` resolver ignores `exports` entirely and falls back to `main`/`types`, so a broken
`exports` map would compile clean here and only fail for a consumer.

`run.sh` deletes `lib/` and `tsconfig.tsbuildinfo` before compiling for the same row. `tsc --build`
is incremental and keys its state on this example's own sources, which do not change when the
adapter's packaging does. CI is always cold; the tree where someone checks the break by hand is
not.

**Usage shape.** A harness runs one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), including pagination and — the one that
earns the exercise — the adapter's filter ANDed with the application's own predicate, across all
three plan kinds.

## Layout

| Path                     | What it is                                                          |
| ------------------------ | ------------------------------------------------------------------- |
| `run.sh`                 | backend up → pack → install → deploy → compile cold → run. Prints the JSON document on stdout. |
| `src/main.ts`            | The client: plans against the PDP, seeds, walks the shapes, prints the document. |
| `convex/documents.ts`    | The backend: where the adapter runs, beside `ctx.db`.               |
| `convex/schema.ts`       | The demo domain's one table, as a consumer would model it: flat scalar fields, no relations. |
| `package.json`           | The ORM, the SDK and the `@cerbos/core` peer, all pins Renovate manages. |
| `package-lock.json`      | Committed. See below.                                               |

## Why this example has two halves

Every other TypeScript example calls its adapter in the process it runs in. This one cannot, and
not by preference: `queryPlanToConvex` returns a **function of Convex's `FilterBuilder`**, and the
only place that builder exists is inside a Convex query. So the plan travels to the backend and the
adapter runs there — `src/main.ts` plans, seeds and reports; `convex/documents.ts` translates and
queries.

That shape has three consequences a consumer meets on day one, and this example meets all three.

**The plan is serialized on the way in.** `@cerbos/core` builds a plan out of `PlanExpression`
class instances, and Convex's argument encoder rejects a class instance outright:

```
Error: PlanExpression {"operator":"or",…} is not a supported Convex type
```

So `src/main.ts` round-trips the plan through JSON, and the backend receives the same tree with the
prototypes gone. Which is exactly why the adapter classifies operands by their shape rather than
with `instanceof`: an `instanceof` check could not survive this crossing on *any* consumer's
machine ([#419](https://github.com/cerbos/query-plan-adapters/issues/419)).

**The plan kind comes back as a bare string.** A Convex function's return value is data, so the
`PlanKind` the adapter reported arrives with nothing left of its type. `src/main.ts` re-narrows it
against the adapter's own re-exported `PlanKind` — which is also what makes that reported kind the
adapter's answer rather than the plan kind the client already had in hand.

**These queries are `query`, and yours should be `internalQuery`.** The example's client is an
outside process, so it needs public functions to call. A real application calls Cerbos from trusted
code and passes the plan to an `internalQuery`: anyone who can hand you a plan can hand you an
`ALWAYS_ALLOWED` one. See the adapter README's "Trusted usage pattern".

## Three things that look odd and are not

**`@cerbos/orm-convex` is not in `package.json`.** `npm pack`'s tarball gets a fresh integrity hash
on every build, so a committed lockfile naming it would break `npm ci` the moment the adapter
changed. `run.sh` runs `npm ci` for the pinned tree and then installs the tarball on top with
`--no-save --no-package-lock`, which leaves both manifests exactly as committed while still
resolving the adapter and its peers the way a consumer's install does.

**`@cerbos/core` *is* in `package.json`,** even though neither half imports much from it. It is the
adapter's peer dependency — the copy of the query-plan types the Cerbos client and the adapter have
to share — and `npm install @cerbos/orm-convex @cerbos/core` is what the adapter's README tells a
consumer to run. npm 7+ would install the missing peer on its own; pnpm and Yarn would not, so
declaring it is what a real consumer does.

**The lockfile is committed anyway,** because it is what makes the CI job load-bearing.
`renovate.json` automerges every non-major bump, so a Convex bump arrives as one PR touching both
`convex/package.json` and this file — and the `example` job on that PR is what blocks the automerge
when the new Convex breaks real usage. That only works while the job stays in
[`.github/workflows/convex.yaml`](../../.github/workflows/convex.yaml); there is a comment on the
job saying so.

## The mapper

Cerbos attribute paths are not document field names, so a consumer always writes one of these:

```ts
const MAPPER: Mapper = {
  "request.resource.attr.ownerId": { field: "ownerId" },
  "request.resource.attr.public": { field: "public" },
};
```

Leaving it out does not throw here — it silently returns nothing. Convex reads a dotted field name
as a path *into* the document, so an unmapped `request.resource.attr.ownerId` becomes a lookup for
a `request` field that no document has, and an absent path is not an error to the filter engine.
That is worth seeing in an example.

`region` and `archived` are deliberately absent from the mapper: they are the application's own
fields, never referenced by [`demo/policies/document.yaml`](../../demo/policies/document.yaml), and
composing them with the adapter's filter is shape 5.

## Shape 4 is `.paginate()`

Convex has no filtered count, which is why count is not one of the five shapes. `convex/documents.ts`
applies `.filter()` and then `.paginate()`, so a page holds `numItems` documents the adapter
**allowed** rather than `numItems` documents of which some are then dropped, and the client walks
the cursor. Pages are asserted by their sizes and by the sorted union of their ids, never by
per-page order — `demo/expected.json` is shared by every store and several of them have no total
order to paginate by.

`isDone` is the only end condition the client accepts. A filtered `.paginate()` walks the table
under a read budget, so a page shorter than `numItems` is not the end and an empty one is not
either; treating either as terminal would truncate the union quietly. The loop is bounded by the
seed-row count instead, so a cursor that never reports itself done fails the run rather than
hanging it.

## Shape 5 is one `.filter()` call

Convex takes a single predicate, so composing means building both halves inside it:

```ts
return (q) => q.and(
  adapterFilter(q),                               // KIND_CONDITIONAL: the adapter's filter
  q.eq(q.field("archived"), application.archived), // the application's own predicate,
  q.eq(q.field("region"), application.region),     // declared in demo/seeds.json
);
```

Both halves are optional and the two absences mean different things. `KIND_ALWAYS_ALLOWED` has no
adapter filter, so the application's predicate stands alone. `KIND_ALWAYS_DENIED` never reaches the
composition at all — the query returns before a predicate is built, because a denial ANDed with
anything is still a denial and it must not be reachable through that path.

## Ports

The Convex backend runs on **13210/13211**, not Convex's default 3210/3211, and the PDP on
13592/13593. Those defaults are what `npm run convex:up` and every adapter's `cerbos run` test
sidecar bind, and a demo container holding one of them would not fail — it would let this example
deploy over the functions a conformance run is using, or plan against the conformance corpus while
diffing against the demo expectations. Both are reached through the environment (`$CONVEX_URL`,
`$CERBOS_HOST`) with no fallback, so neither number is written down twice and neither can be
defaulted into.

The backend is started from [`../docker-compose.yml`](../docker-compose.yml) — the adapter's own
file, with the ports overridden — rather than a copy, so the image pin stays in one place.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It uses `makeFunctionReference` rather than the generated `api` object: `convex/_generated/` is
written by a deploy and is not committed, and it is ESM while this client compiles to CommonJS. A
Convex application that ships its own `_generated` would use `api` instead — the reference is the
same value either way.

It does **not** prove `allowPostFilter`. Every shape in the demo domain is a flat comparison
Convex's filter engine evaluates itself, so the adapter is called at its default of `false` and a
post-filter appearing at all would fail the run. Post-filter semantics are the conformance corpus's
job, and it carries them.
