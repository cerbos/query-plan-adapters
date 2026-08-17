# `@cerbos/langchain-chromadb` example application

A runnable program that installs the adapter **as a published package** and uses it the way a
consumer would, against the shared [demo domain](../../demo/README.md).

```bash
# from the repository root
demo/scripts/run-example.sh langchain-chromadb
```

Needs `docker` (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP; this
directory's `run.sh` starts ChromaDB, packs the adapter, installs the tarball, builds, and runs.

It consumes the shared runtime contract in [`demo/cases.json`](../../demo/cases.json).

## What it proves

Not what the adapter translates. [`../src/translator.test.ts`](../src/translator.test.ts) pins the
`Where` document every corpus shape emits, and
[`../src/adversarial.test.ts`](../src/adversarial.test.ts) proves those documents return what a
live PDP's `check()` allows. This proves the two things neither of them structurally can:

**Packaging.** `run.sh` builds the artifact `npm publish` would upload and installs *that*, so the
import in [`src/main.ts`](src/main.ts) resolves through the published surface — the `exports` map,
`types`, the `files` allowlist, and the `@cerbos/core` peer range against the copy this example
declares. Both suites import from `"."` and touch none of it. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

`tsconfig.json` sets `moduleResolution: "nodenext"` for the `exports` half specifically: the legacy
`node10` resolver ignores `exports` entirely and falls back to `main`/`types`, so a broken
`exports` map would compile clean here and only fail for a consumer. `run.sh` deletes `lib/` and
`tsconfig.tsbuildinfo` before compiling for the same reason — `tsc --build` keys its incremental
state on this example's own sources, which do not change when the adapter's packaging does, so on a
warm tree a broken `exports` map compiles clean and the break surfaces only at runtime. CI is
always cold; the tree where someone checks the break by hand is not.

**Usage shape.** Both suites run one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), including a limit walked to the end of
the result set and — the one that earns the exercise — the adapter's filter ANDed with the
application's own predicate, across all three plan kinds.

## Layout

| Path                | What it is                                                              |
| ------------------- | ----------------------------------------------------------------------- |
| `run.sh`            | start ChromaDB → pack → install → compile cold → run. Prints the JSON document on stdout. |
| `src/main.ts`       | The example itself: one function per usage shape.                       |
| `package.json`      | The client and SDK pins Renovate manages.                               |
| `package-lock.json` | Committed. See below.                                                   |

There is no schema file: a Chroma collection has none. The four flat attributes the demo domain
carries become four metadata keys on each record, and the collection is scratch state `main.ts`
deletes and recreates on every run.

The ChromaDB container is started by this directory's `run.sh` rather than by the shared runner,
on **18234** rather than the 8234 `npm run chroma` and this adapter's CI bind. A demo server
holding that port would let this example create and delete collections inside the server a
conformance run is using — the same collision `demo/docker-compose.yml` avoids by publishing the
PDP on 13592/13593. The image is read from [`../CHROMA_IMAGE`](../CHROMA_IMAGE), the one constant
this adapter's suites and workflow already share, so there is no second pin to bump.

## Two things that look odd and are not

**`@cerbos/langchain-chromadb` is not in `package.json`.** `npm pack`'s tarball gets a fresh
integrity hash on every build, so a committed lockfile naming it would break `npm ci` the moment
the adapter changed. `run.sh` runs `npm ci` for the pinned tree and then installs the tarball on
top with `--no-save --no-package-lock`, which leaves both manifests exactly as committed while
still resolving the adapter and its peers the way a consumer's install does.

**The lockfile is committed anyway,** because it is what makes the CI job load-bearing.
`renovate.json` automerges every non-major bump, so a `chromadb` bump arrives as one PR touching
both `langchain-chromadb/package.json` and this file — and the `example` job on that PR is what
blocks the automerge when the new client breaks real usage. That only works while the job stays in
[`.github/workflows/chromadb.yaml`](../../.github/workflows/chromadb.yaml); there is a comment on
the job saying so.

## The mapper

Cerbos attribute names are not Chroma metadata keys, so a consumer always writes one of these:

```ts
const FIELD_NAME_MAPPER: FieldMapper = {
  "request.resource.attr.ownerId": "ownerId",
  "request.resource.attr.public": "public",
};
```

Without an entry the adapter falls back to the attribute path verbatim, and
`request.resource.attr.ownerId` is not a key any record in this collection carries — so the filter
would be well-formed, accepted by Chroma, and select nothing.

`archived` and `region` are deliberately absent: they are the application's metadata keys, never
referenced by [`demo/policies/document.yaml`](../../demo/policies/document.yaml), and composing
them with the adapter's filter is shape 5. Nothing declares `required: true` either — that
assertion exists to permit `$ne` and `$nin`, and the demo policy produces neither.

## `{}` is not Chroma's word for "no constraint"

The one place the three plan kinds do not land cleanly, and the reason shape 2 and shape 5 are
worth running here rather than assumed:

```ts
type Filter = Where | undefined | "denied";
```

`ALWAYS_ALLOWED` comes back from the adapter as `filters: {}` — an empty clause, a faithful
spelling of "there is nothing to filter on". Chroma's own validator rejects it:

```
ChromaValueError: Expected 'where' to have exactly one operator, but got 0
```

So `undefined` — omitting `where` altogether — is what "no constraint" is spelled as on this
store, and the caller drops the empty clause rather than forwarding it. The same applies one level
in: `{ $and: [{}, applicationFilter] }` is rejected for the same reason, which is why `conjoin`
returns the application's predicate alone instead of wrapping both. This is a loud failure rather
than a silent over-grant — the first unconditional plan an application meets raises it — but it is
the kind of thing only a program that actually calls the store finds.

`ALWAYS_DENIED` is a string sentinel rather than a second `undefined` precisely because the two
must not collapse: a denial that reached a query as "no constraint" would return the whole
collection.

## Two query methods, on purpose

Shapes 1, 2, 3 and 5 use `collection.query` — the similarity search LangChain's Chroma vector
store calls underneath `similaritySearch`, with the adapter's clause passed as its `where`
argument. "No limit" has to be spelled as `nResults: <collection size>`, because a vector search
always caps its neighbours.

Shape 4 leaves it behind. `nResults` is a limit but there is no offset to go with it, so
`collection.query` can return the first page and no other; the second page needs `collection.get`,
Chroma's metadata-only retrieval path, which takes the same `where` clause plus `limit` and
`offset`. Exercising both is the point: the clause this adapter emits has to be accepted by
whichever of the two a consumer reaches for.

Pages are asserted by their sizes and by the sorted union of their ids, never by per-page order —
`demo/cases.json` is shared by every store and several have no total order to paginate by.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It talks to `chromadb` directly rather than through `@langchain/community`'s Chroma vector store.
That store is a wrapper over the same client and the same `where` argument, and reaching for it
would add an embedding provider — a network call and an API key — to a program whose subject is
the packaging of this adapter. The dependency this example has to pin is the one the adapter
declares, and that is `chromadb`.
