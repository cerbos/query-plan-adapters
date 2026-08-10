# The demo domain

The single realistic policy suite, seed rows and expected id sets that every **example
application** shares. One resource kind, four flat scalar attributes, three actions.

This is the second of the repository's three policy directories, and they have distinct jobs:

| Directory       | Proves     | Shapes     | Per-adapter exceptions        |
| --------------- | ---------- | ---------- | ----------------------------- |
| `policies/`     | —          | assorted   | per-adapter unit-test policy  |
| `conformance/`  | semantics  | hostile    | five classification buckets   |
| `demo/`         | plumbing   | realistic  | **none**, by construction     |

**Semantics** is whether a translated filter returns exactly the rows the PDP allows.
**Plumbing** is whether the adapter can be installed, imported and handed to the ORM's real query
methods at all. A filter can be semantically perfect and still unusable, which is what this
directory is for. See [`CONTEXT.md`](../CONTEXT.md) for the full glossary.

## What an example covers that a conformance harness cannot

Every harness already plans against a live PDP over gRPC, translates, runs a real ORM call against
a real store, and compares ids against per-row `check()`. That chain is covered for all ten
adapters and nothing here adds to it. Two gaps remain, and they are gaps *because of how the
harnesses are built*:

1. **Packaging.** Every harness imports its adapter from source (`from "."`). The published
   surface — `exports` maps, type declarations, `files` allowlists, peer ranges, POM scopes — is
   executed nowhere. Examples install the packed artifact instead; see
   [ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md).
2. **Usage shape.** A harness runs one flat filtered query. Consumers also paginate, and compose
   the adapter's filter with predicates of their own. That second category is where a "returns a
   filter object" API most often fails in practice.

## The five usage shapes

Every example exercises all five:

1. Plain filtered list
2. `KIND_ALWAYS_ALLOWED`
3. `KIND_ALWAYS_DENIED`
4. Pagination or a limit applied on top of the filter
5. The adapter's filter combined with an **application-owned** filter

Shape 5 is the one that earns the exercise. Count, sort and relation traversal are deliberately
absent — they have no filtered form in ChromaDB or Convex, and anything needing a per-adapter
carve-out does not belong here
([ADR 0001](../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).

## Contents

| File                     | What it is                                                          |
| ------------------------ | ------------------------------------------------------------------- |
| `policies/document.yaml` | One resource kind. `view` is conditional, `admin-view` unconditional, and `publish` is absent so the planner denies it. |
| `seeds.json`             | Eight rows across three owners, the three principals, and the application's own predicate. |
| `expected.json`          | The **one shared** expectations file. All ten examples assert against it. |
| `cerbos-config.yaml`     | PDP configuration.                                                  |
| `docker-compose.yml`     | The PDP itself, pinned to `conformance/CERBOS_VERSION` **and** `conformance/CERBOS_IMAGE_DIGEST`. |
| `scripts/run-example.sh` | Runs one adapter's example and diffs it against `expected.json`.    |
| `scripts/validate-demo.sh` | Integrity checks. Needs no PDP, database or network.              |

`seeds.json` also declares `applicationFilter` — the predicate the **application** owns
(`archived == false AND region == 'emea'`). It is never expressed in policy; ANDing it with the
adapter's filter is shape 5. It lives in the corpus rather than in ten copies inside ten examples
so `validate-demo.sh` can recompute it.

## Running an example

```bash
demo/scripts/run-example.sh prisma
```

Needs `docker` (with compose) and `jq`. The runner starts the pinned PDP, invokes
`<adapter>/example/run.sh`, and diffs its stdout against `expected.json`.

The split between the two scripts is deliberate. Everything language-independent — PDP lifecycle,
output capture, canonicalisation, the diff — lives in the runner. Everything language-specific
lives in `run.sh`: ten languages means ten packaging stories, and putting all ten in the shared
runner would make it a language switch.

### What an example must do

Each example is a program taking **no arguments** — not an HTTP service, which does not generalise
past Java and Node and would add a web framework to nine examples for reasons unrelated to the
adapter. Its `run.sh` must:

- pack the adapter into a real distributable and install **that** (ADR 0002; Go uses `replace` and
  proves usage shapes only)
- print exactly one JSON document to stdout, with everything else on stderr
- reach the PDP at `$CERBOS_HOST`, which the runner sets — never a hardcoded address. The demo PDP
  is published on `13592`/`13593` rather than the default `3592`/`3593` on purpose: those are the
  ports every adapter's `cerbos run` test sidecar binds, and a demo PDP still holding them makes
  that sidecar fail to bind while the suite silently talks to the demo policies instead.

The document is:

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

`shapes` is diffed against `expected.json`'s `shapes` with each shape's inline `description`
stripped, so the expectations can carry their own prose without ten examples reproducing it.

Every entry pins the plan `kind` alongside the ids. That is what stops an example returning all
eight rows for `admin-view` without ever having reached the PDP.

**Ids are always sorted, and pagination is asserted by page sizes plus the sorted union — never
by per-page order.** Several of the ten stores have no total order to paginate by, and an
order-dependent assertion would need exactly the per-adapter carve-out ADR 0001 rules out.
Disjointness still falls out: overlapping pages would shrink the union below the sum of the sizes.

## Why the expectations are hardcoded

`conformance/` bans hand-written expectations, because there a wrong expectation hides an
authorization bug. Here the id lists are frozen on purpose: this proves plumbing — did the package
import, did the ORM accept the filter, did rows come back — where a frozen list is the better
tripwire and reads as documentation.

The rot risk is real, and `validate-demo.sh` is what answers it:

1. **Structural.** `expected.json` declares exactly the five shapes and every entry is well-formed
   for its shape — an `alwaysAllowed` entry carrying a conditional kind would leave that kind
   untested while still looking covered. The runner diffs the whole document exactly, so this is
   what makes that diff mean "all five shapes".
2. **Non-degeneracy.** The lists still discriminate. Shape 5 is recomputed from shape 1 and
   `seeds.json`, and must differ from *both* filters it composes: equal to the adapter's filter
   and the example could drop the application predicate; equal to the application predicate's own
   result and the example could drop **the adapter** — an authorization hole that reads as a green
   build.
3. **Pin reuse.** The demo domain has no `CERBOS_VERSION` of its own. One PDP pin in the
   repository, reused, and every example reaches it.
4. **Example coverage.** Every adapter has an `example/`. **Currently disabled** — it cannot pass
   until the last child of cerbos/query-plan-adapters#349 merges, and enabling it is
   cerbos/query-plan-adapters#360's job. Run with `DEMO_REQUIRE_ALL_EXAMPLES=1` to see where it
   stands. The roster it reads is `adapters` in `conformance/actions.json`; there is deliberately
   no second list.

## Changing the demo domain

A change here re-runs every adapter's example job, the same way `conformance/` re-runs every
adapter's test workflow.

- **Adding a seed row or attribute** means updating `expected.json` in the same commit, and every
  example's schema. `validate-demo.sh` catches the first; the exact diff in the runner catches the
  second.
- **Adding a usage shape** means implementing it in all ten examples. There is no per-adapter
  classification to opt out with, and adding one is what ADR 0001 rules out.
- **A shape that needs a carve-out for one adapter is wrong for this directory.** The argument
  belongs in `conformance/`, where the classification buckets exist.

The domain is thin by construction — roughly the intersection of ten query languages, one of which
is a vector store. It is a **floor, not a ceiling**: every example must implement the shared
shapes, and each is then free to add richer adapter-local scenarios that nothing shared asserts.
`spring-data/example/` keeps its photo/album/workspace domain on exactly that basis.
