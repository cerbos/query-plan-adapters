# TypeORM Adapter

This repository does not ship a TypeORM query plan adapter.

## Why this is out of scope

TypeScript SQL is already covered twice, by adapters that are proved rather than asserted:

- **Prisma** (`@cerbos/orm-prisma`) — emits a Prisma `where` object
- **Drizzle** (`@cerbos/orm-drizzle`) — emits a Drizzle `SQL` builder value

Both run the full adversarial corpus against a real store with `check()` as the oracle. A
TypeORM adapter would be a third translator over the same semantic ground: the same SQL
three-valued logic, the same LIKE-metacharacter escaping problem, the same absent-to-one-parent
over-grant, the same collation questions. It would not exercise a query language this
repository has not already had to reason about.

That matters because the corpus is not free. Adding an adapter means classifying all 140
actions in `conformance/actions.json` for it, standing up a differential harness and CI
workflow, and thereafter re-running it on every corpus change. The repository's history is that
the *same* semantic bug — value-first operand inversion, LIKE metacharacter leaks, three-valued
logic under negation — has shipped identically to several adapters at once. Each additional
adapter over ground already covered multiplies that triage surface without adding coverage of a
new failure mode.

If you need this with TypeORM, the pragmatic path is to use the Drizzle adapter's output — it
produces SQL — or call `PlanResources` and translate against your own entity metadata, owning
the fail-closed decision explicitly.

This is a judgement about marginal coverage, not about TypeORM. If a TypeORM-specific hazard
turned up that neither Prisma nor Drizzle can express — something in its relation or subquery
model that changes which rows a subquery sees — that would be a reason to revisit.

## Prior requests

- #14 — "TypeORM Adapter"
