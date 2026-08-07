# Azure AI Search Adapter

This repository does not ship an Azure AI Search (OData `$filter`) query plan adapter.

## Why this is out of scope

The feasibility analysis in #141 is the reason. OData `$filter` maps the comparison and boolean
core cleanly — `eq/ne/lt/le/gt/ge`, `and/or/not`, `in` via `search.in()`, and the collection
macros via `any()`/`all()` — but three families have no faithful translation:

| Cerbos shape | Problem |
| --- | --- |
| `contains`, `startsWith`, `endsWith` | No substring predicate in `$filter`. `search.ismatch()` is full-text, not substring — different semantics, not a workaround |
| `exists_one` | OData has no "exactly one" quantifier. Approximating it as `any()` **admits rows the policy denies** |
| `map`, `hasIntersection` + `map` | `$filter` has no projection |

Under this repository's central invariant — **a shape an adapter cannot express must throw,
never emit a filter** — every one of those has to fail closed. The `exists_one` case is the
decisive one: approximating it as `any()` is exactly the class of silent over-grant the corpus
exists to catch, so the only correct behaviour is to reject it.

What remains after fail-closing all of them is an adapter that handles comparisons, booleans,
membership and the collection macros, and throws on the whole string-predicate family. String
predicates are among the most common shapes in real resource policies, so the adapter would
throw on a large fraction of the policies people actually write — while still carrying the full
onboarding cost: 140 corpus actions classified, a differential harness against a live Azure AI
Search index, a CI workflow, and a publish path.

The value/cost ratio is what fails here, not the engineering. Callers who want Cerbos-derived
filters against Azure AI Search are better served calling `PlanResources` and translating the
subset they need against their own index schema, where they can see exactly which shapes they
are choosing to support and reject the rest explicitly.

Worth revisiting if Azure AI Search adds substring predicates to `$filter` — that would move
the largest blocked family into full support and change the calculation.

## Prior requests

- #141 — "Azure AI Search" (includes the full operator-coverage analysis)
