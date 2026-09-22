# `@cerbos/langchain-chromadb` example application

A runnable program that installs the adapter as a published package and uses it the way a consumer
would, against the shared [demo domain](../../demo/README.md).

## Run it

```bash
# from the repository root
(cd langchain-chromadb && npm ci)     # run.sh builds the adapter before packing it
demo/scripts/run-example.sh langchain-chromadb
```

Needs Docker (with compose), `jq` and Node 22+. The runner starts the pinned Cerbos PDP; this
directory's `run.sh` starts ChromaDB, packs the adapter, installs the tarball, compiles cold, runs,
and prints one JSON document that the runner diffs against `demo/expected.json`. It follows
[`prisma/example/`](../../prisma/example/), the reference implementation.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), across all three plan kinds:

1. A plain filtered list (`collection.query`).
2. `KIND_ALWAYS_ALLOWED` — `where` omitted.
3. `KIND_ALWAYS_DENIED` — no query at all.
4. A limit walked to the end of the result set (`collection.get` with `limit`/`offset`).
5. The adapter's clause ANDed with an application-owned one (`archived`, `region`).

The mapper every consumer writes, since attribute names are not metadata keys:

```ts
const FIELD_NAME_MAPPER: FieldMapper = {
  "request.resource.attr.ownerId": "ownerId",
  "request.resource.attr.public": "public",
};
```

Without an entry the adapter uses the attribute path verbatim as the key, which no record carries,
so the query silently returns nothing. `archived` and `region` are absent because policy never
references them; nothing is `required: true` because the demo policy emits no `$ne`/`$nin`.

### `{}` is not Chroma's "no constraint"

The plan kinds map onto Chroma as `type Filter = Where | undefined | "denied"`:

- `ALWAYS_ALLOWED` comes back as `filters: {}`, which Chroma's validator rejects
  (`ChromaValueError: Expected 'where' to have exactly one operator, but got 0`). The example maps
  it to `undefined` and omits `where`; `conjoin` likewise returns the application's clause alone
  rather than `{ $and: [{}, applicationFilter] }`, which is rejected for the same reason.
- `ALWAYS_DENIED` is a string sentinel, not a second `undefined`: a denial read as "no constraint"
  would return the whole collection.

### Two query methods

Shapes 1, 2, 3 and 5 use `collection.query`, the similarity search LangChain's Chroma store calls
under `similaritySearch`; "no limit" is spelled `nResults: <collection size>`. Shape 4 needs an
offset, which `query` lacks, so it uses `collection.get` with the same `where` plus `limit` and
`offset`. Pages are asserted by size and the sorted union of their ids, never per-page order.

The example calls `chromadb` directly rather than `@langchain/community`'s Chroma store: that store
wraps the same client and `where` argument, and would add an embedding provider (a network call and
an API key). `chromadb` is the dependency the adapter declares.

## Packaging checks

The adapter's test suites import from source. This example installs `npm pack`'s tarball, so the
import in [`src/main.ts`](src/main.ts) resolves through the published `exports` map, `types`, `files`
allowlist and the `@cerbos/core` peer range
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)).

- `tsconfig.json` uses `moduleResolution: "nodenext"`; the legacy `node10` resolver ignores
  `exports`, so a broken map would compile clean here and fail only for a consumer.
- `run.sh` deletes `lib/` and `tsconfig.tsbuildinfo` before compiling, because incremental
  `tsc --build` would otherwise hide a broken `exports` map on a warm tree.
- `@cerbos/langchain-chromadb` is not in `package.json`: the tarball's integrity hash changes on
  every build, which would break `npm ci`. `run.sh` runs `npm ci`, then installs the tarball with
  `--no-save --no-package-lock`.
- `@cerbos/core` is declared because the adapter lists it as a peer, as a consumer on pnpm or Yarn
  would have to.
- The lockfile is committed so a Renovate `chromadb` bump touches both
  `langchain-chromadb/package.json` and this lockfile, and the `example` job in
  [`.github/workflows/chromadb.yaml`](../../.github/workflows/chromadb.yaml) blocks the automerge if
  real usage breaks. Keep the job in that workflow.

## Layout

| Path                | What it is                                                              |
| ------------------- | ----------------------------------------------------------------------- |
| `run.sh`            | Start ChromaDB → pack → install → compile cold → run. Prints the JSON document on stdout. |
| `src/main.ts`       | The example: one function per usage shape.                              |
| `package.json`      | The client and SDK pins Renovate manages.                               |
| `package-lock.json` | Committed (see above).                                                  |

There is no schema file: the demo's four flat attributes become four metadata keys per record, and
`main.ts` deletes and recreates the collection on every run.

## The store

- `run.sh` reads the image from [`../CHROMA_IMAGE`](../CHROMA_IMAGE), the constant the adapter's
  suites and workflow already share, so there is one pin to bump.
- ChromaDB is published on host port **18234**, not the 8234 that `npm run chroma` and the
  adapter's CI bind, so the example cannot create or delete collections in a server a conformance
  run is using (the same reason the demo PDP uses 13592/13593).

This is a JSON-printing CLI, not an onboarding artifact; that is
[`spring-data/example/`](../../spring-data/example/)
([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).
