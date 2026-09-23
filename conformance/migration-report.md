# Migration report: live-PDP harness → recorded goldens

One-off review aid for this change; delete after merge together with `migration-renames.json`.

For every adapter, each case whose classification differs between the old `conformance/actions.json`
(at the branch point, 6b2dddd) and the new `<adapter>/conformance-ledger.json`, as observed by running the new harness
against PDP 0.55.0. `null-eq-missing` used to be asserted as a throw under a per-call option; it is now
classified through the mapping's per-attribute null convention, so it appears for every adapter.

## activerecord

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is omitted when NULL, so the mapping declares null_representation: :omitted, and the adapter refuses a null constant under it: the wire node is  |
| `rel-ne-null-hop` | `relation/not-equals/one-hop-null-literal` | passes | unsupported | parent.aOptionalString is omitted when NULL or when the parent row is absent, so the mapping declares null_representation: :omitted, and the adapter refuses eve |

Passing on 0.55.0: core 26/26, extended 61/80, adversarial 171/227.

## convex

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | passes |  |

Passing on 0.55.0: core 26/26, extended 70/80, adversarial 206/227.

## drizzle

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is declared nullAttributeRepresentation "omitted": a NULL column sends no attribute, so CEL denies `== null` with a missing-attribute error whil |

Passing on 0.55.0: core 26/26, extended 59/80, adversarial 183/227.

## elasticsearch-java

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | Elasticsearch does not index a JSON null, so `== null` cannot tell a NULL column from a missing field and is refused. |

Passing on 0.55.0: core 25/26, extended 31/80, adversarial 85/227.

## ent

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is mapped with NullConventionOmitted (a NULL column is a missing attribute), so a null operand against it is refused: the plan cannot say whethe |

Passing on 0.55.0: core 26/26, extended 59/80, adversarial 183/227.

## langchain-chromadb

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | Chroma metadata holds only finite numbers, strings and booleans, so a null comparison operand is refused: an absent key cannot be told apart from a stored null. |

Passing on 0.55.0: core 19/26, extended 13/80, adversarial 30/227.

## mongoose

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | passes |  |

Passing on 0.55.0: core 26/26, extended 52/80, adversarial 148/227.

## pgx

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is mapped with NullConventionOmitted (a NULL column is a missing attribute), so a null operand against it is refused: the plan cannot say whethe |

Passing on 0.55.0: core 26/26, extended 59/80, adversarial 183/227.

## prisma

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is declared nullAttributeRepresentation "omitted": a NULL column sends no attribute, so CEL denies `== null` with a missing-attribute error whil |

Passing on 0.55.0: core 26/26, extended 49/80, adversarial 109/227.

## spring-data

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is declared NullAttributeRepresentation.OMITTED in the mapping (resources.json omits it when NULL), so `== null` is a CEL missing-attribute erro |
| `rel-ne-null-hop` | `relation/not-equals/one-hop-null-literal` | passes | unsupported | parent.aOptionalString is declared NullAttributeRepresentation.OMITTED in the mapping (resources.json omits it when NULL), and the adapter refuses every null op |

Passing on 0.55.0: core 26/26, extended 58/80, adversarial 180/227.

## sqlalchemy

| Old action | Case | Before | After | Ledger reason |
|---|---|---|---|---|
| `cr-div-neg-zero` | `arithmetic/divide/negative-zero-divisor` | unsupported | passes |  |
| `null-eq-missing` | `null/equals/null-literal-on-missing-attribute` | throws under per-call omitted option | unsupported | aOptionalString is declared `omitted` in attribute_null_representation (a NULL column sends no attribute), so `== null` is a CEL missing-attribute error that de |

Passing on 0.55.0: core 26/26, extended 61/80, adversarial 185/227.
