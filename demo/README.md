# The demo domain

The realistic policy suite, seed rows and expected id sets that every **example application**
shares: one resource kind, four flat scalar attributes, three actions. Each adapter's `example/`
installs the packed adapter, runs five usage shapes against this domain, and is diffed against one
shared `expected.json`.

It is one of the repository's two shared policy directories, and they prove different things:

| Directory       | Proves     | Shapes     | Per-adapter exceptions        |
| --------------- | ---------- | ---------- | ----------------------------- |
| `conformance/`  | semantics  | hostile    | five classification buckets   |
| `demo/`         | plumbing   | realistic  | **none**, by construction     |

**Semantics** is whether a translated filter returns exactly the rows the PDP allows. **Plumbing**
is whether the adapter can be installed, imported and handed to the ORM's real query methods at all.
A third, unnamed policy suite at the repository root was absorbed into the conformance corpus
([ADR 0008](../docs/adr/0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md)).
Glossary: [`CONTEXT.md`](../CONTEXT.md).

## Running an example

```bash
demo/scripts/run-example.sh prisma     # any adapter directory name
demo/scripts/validate-demo.sh          # integrity checks; no PDP, database or network
```

`run-example.sh` needs `docker` (with compose) and `jq`, plus the adapter's own toolchain. It starts
the pinned PDP, invokes `<adapter>/example/run.sh`, and diffs its stdout against `expected.json`.
Everything language-independent (PDP lifecycle, output capture, canonicalisation, the diff) lives in
the runner; everything language-specific, including packaging, lives in each `run.sh`.

### What an example must do

An example is a program taking **no arguments** — not an HTTP service, which would add a web
framework to most examples for no reason related to the adapter. Its `run.sh` must:

- pack the adapter into a real distributable and install **that**
  ([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)). Go has no packaging step:
  ent and pgx use a `replace` directive and prove usage shapes only.
- print exactly one JSON document to stdout, with everything else on stderr.
- reach the PDP at `$CERBOS_HOST`, which the runner sets — never a hardcoded address;
  `validate-demo.sh` fails on one. The demo PDP listens on `13592`/`13593`, not Cerbos's default
  `3592`/`3593`, because another local PDP may hold the defaults, and a client assuming them would
  silently plan against the wrong policies. (The first two examples shipped
  `?? "localhost:3593"`, and the mismatch read as an adapter bug — #367.)
- take its principal from `seeds.json`: look the id up in `principals` and plan with what comes
  back. Never write out an `{ id, roles }` of its own; `validate-demo.sh` fails on a restated one.
  Naming the id is fine; its **roles** exist nowhere else.

The document:

```jsonc
{
  "adapter": "prisma",
  "shapes": {
    "filtered":      { "alice/view": { "kind": "KIND_CONDITIONAL", "ids": ["d1", …] } },
    "alwaysAllowed": { … },
    "alwaysDenied":  { … },
    "paginated":     { "alice/view": { "kind": …, "pageSize": 2, "pageSizes": [2,2,1], "ids": […] } },
    "composed":      { … }
  }
}
```

- `shapes` is diffed exactly against `expected.json`'s `shapes`, with each shape's `description`
  stripped.
- Every entry pins the plan `kind` beside the ids, so an example cannot return all eight rows for
  `admin-view` without reaching the PDP.
- **Ids are always sorted, and pagination is asserted by page sizes plus the sorted union — never by
  per-page order.** Several stores have no total order, and an order-dependent assertion would need
  the per-adapter carve-out ADR 0001 rules out. Overlapping pages would shrink the union below the
  sum of the sizes, so disjointness is still checked.

## Changing the demo domain

A change here re-runs every adapter's example job, as `conformance/` re-runs every adapter's test
workflow.

- **Adding a seed row or attribute**: update `expected.json` and every example's schema in the same
  commit. `validate-demo.sh` catches the first; the runner's exact diff catches the second.
- **Adding a usage shape**: implement it in every example. There is no per-adapter classification to
  opt out with, and adding one is what
  [ADR 0001](../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) rules out.
- **Adding an adapter**: add its example in the same change that registers it in
  `conformance/actions.json`; check 4 reads that roster, so the two land together or CI stays red.
  Step by step: [conformance/README.md](../conformance/README.md#adding-a-new-adapter).
- **A shape that needs a carve-out for one adapter is wrong for this directory.** Argue it in
  `conformance/`, where the classification buckets exist.

The domain is roughly the intersection of every adapter's query language, one of which is a vector
store. It is a **floor, not a ceiling**: every example implements the shared shapes and may add
richer local scenarios nothing shared asserts. `spring-data/example/` keeps its
photo/album/workspace domain on that basis.

## What an example covers that a conformance harness cannot

Every harness already plans against a live PDP, translates, runs a real ORM call against a real
store and compares ids with `check()`. Two gaps remain, because of how harnesses are built:

1. **Packaging.** Every harness imports its adapter from source (`from "."`), so the published
   surface — `exports` maps, type declarations, `files` allowlists, peer ranges, POM scopes — runs
   nowhere. Examples install the packed artifact (Go examples excepted, as above).
2. **Usage shape.** A harness runs one flat filtered query. Consumers also paginate and compose the
   adapter's filter with their own predicates, which is where a "returns a filter object" API most
   often fails.

## The five usage shapes

Every example exercises all five:

1. Plain filtered list
2. `KIND_ALWAYS_ALLOWED`
3. `KIND_ALWAYS_DENIED`
4. Pagination or a limit applied on top of the filter
5. The adapter's filter combined with an **application-owned** filter

Shape 5 matters most. Count, sort and relation traversal are absent: they have no filtered form in
ChromaDB or Convex, and anything needing a per-adapter carve-out does not belong here
([ADR 0001](../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).

## Contents

| File                     | What it is                                                          |
| ------------------------ | ------------------------------------------------------------------- |
| `policies/document.yaml` | One resource kind. `view` is conditional, `admin-view` unconditional, and `publish` is absent so the planner denies it. |
| `seeds.json`             | Eight rows across three owners, the three principals, and the application's own predicate. |
| `expected.json`          | The **one shared** expectations file. Every example asserts against it. |
| `cerbos-config.yaml`     | PDP configuration.                                                  |
| `docker-compose.yml`     | The PDP itself, pinned to `conformance/CERBOS_VERSION` **and** `conformance/CERBOS_IMAGE_DIGEST`. |
| `scripts/run-example.sh` | Runs one adapter's example and diffs it against `expected.json`.    |
| `scripts/validate-demo.sh` | Integrity checks. Needs no PDP, database or network.              |

`seeds.json` also declares `applicationFilter` (`archived == false AND region == 'emea'`), the
predicate the **application** owns. It never appears in policy; ANDing it with the adapter's filter
is shape 5. It lives here rather than in each example so `validate-demo.sh` can recompute it.

## Why the expectations are hardcoded

`conformance/` bans hand-written expectations, because there a wrong expectation hides an
authorization bug. Here the id lists are frozen on purpose: for plumbing (did the package import,
did the ORM accept the filter, did rows come back) a frozen list is the better tripwire and doubles
as documentation.

`validate-demo.sh` first reads the adapter roster (`adapters` in `conformance/actions.json`) once,
requiring it to be non-empty with non-empty, single-line names, and reuses it for every adapter
check, so a missing or malformed roster cannot skip validation. It then checks:

1. **Structural.** `expected.json` declares exactly the five shapes and every entry is well-formed
   for its shape (an `alwaysAllowed` entry with a conditional kind would leave that kind untested).
   Together with the runner's exact diff, this makes a pass mean "all five shapes".
2. **Non-degeneracy.** Shape 5 is recomputed from shape 1 and `seeds.json`, and must differ from
   *both* filters it composes. Equal to the adapter's filter, and the example could drop the
   application predicate; equal to the application predicate's result, and it could drop **the
   adapter** — an authorization hole that reads as a green build.
3. **Pin reuse and reachability.** The demo has no `CERBOS_VERSION` of its own: one PDP pin in the
   repository, reused. No example may name a PDP client address; it must use `$CERBOS_HOST`. The
   scan targets client addresses only — `docker-compose.yml`'s `"13592:3592"` names the container's
   own listen port and is correct.
4. **Example coverage.** Every adapter on the `adapters` roster has a runnable `example/run.sh`.
   There is no second list, no opt-out and no environment variable to disable it: registering an
   adapter demands an example. An adapter that cannot implement the five shapes has a packaging or
   ergonomics problem worth finding before release (#349) — the same reason ADR 0001 gives this
   directory no classification buckets.
5. **Principal provenance.** An example looks its principal up in `seeds.json` rather than writing
   one out. A restated principal does not fail quietly like a hardcoded address; it fails later,
   when someone edits `seeds.json`, as an apparent adapter bug. The check stops it at write time
   (#349).

   The signal is an id **next to a role**. Naming `alice` alone is unavoidable (lookup key,
   `expected.json` key, printed output); `alice` beside `user` restates the record. Comments are
   skipped and literals matched whole, so an id in prose, a Javadoc block or a printed message is
   fine — `spring-data/example/` documents `?user=alice&role=user` and passes. An example's **own
   Cerbos policies** (identified by `apiVersion`) are skipped, since writing roles out is their
   point.

   Known limits:
   - A role that is also a principal id is dropped from the role side. `admin` is both here, so
     restating *only* the admin principal is not caught (though `{ id: "admin", roles: ["user"] }`
     still pairs).
   - A diagnostic passing an id and a role as two separate literals reads as a restatement; put both
     in one message, or interpolate the id.
   - Pairing is **windowed**, so an id and role bound to variables far apart are not seen. Widening
     the window is not the fix: every example's shapes block puts `alice` and `admin` within a few
     lines of each other.

   This catches a principal *written out*, the mistake copying a literal makes; it does not prove
   one was not assembled piecewise.
