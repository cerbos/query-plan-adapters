# Issue 430: audit implementation

The [audit](https://github.com/cerbos/query-plan-adapters/issues/430) was written
against `78c459a`. This records its disposition after reconciling the findings with
the intervening fixes and implementing the remaining code changes.

| Finding | Disposition |
| --- | --- |
| F1, F2 | Mongoose uses per-call translation context and one leaf emitter. Collection-scope assertions now apply uniformly. Reentrancy tests fail against the original global state. |
| F3 | Convex table validation, insert arguments and stored document types share one validator. The PDP resource builder remains independent. |
| F4 | Convex conditional results require `path: db`, `post` or `split` and the corresponding functions. Version 0.3.0 documents the public API change. |
| F5, F6 | Chroma results discriminate their payload by plan kind; one polarity-aware walk preserves existing output and refusal order. Version 0.2.0 documents the narrower type. |
| F7, F8 | Prisma uses per-call context, scopes the defensive map fallback, and resolves projection leaf names at the mapper boundary. Tests cover reentrancy, exact and prefix-resolved projection mappings, and nested scope isolation. |
| F9, F10 | Drizzle guards scalar relation leaves through their required parent hops and passes the null representation through translation options. Reentrancy tests fail against the original global state. |
| F11, F12 | Ent and pgx reject unknown renderer enum values and unlowered symbolic operands. The narrower `isSymbolic` predicate retains its original behavior. |
| F13 | Both Go translators retain `Column.Type` instead of projecting it into boolean flags. The existing exclusion semantics for arithmetic remain intact. |
| F14 | Ent rejects unknown dialect configuration, including for unconditional plans. This is documented as a breaking change. |
| F15 | The Elasticsearch polarity walkers already landed in #454. The remaining unused `ne` default-operator entry is removed. |
| F16 | Elasticsearch operand and size-result variants have explicit types instead of boolean/null tags. |
| F17 | SQLAlchemy parses wire operands once into immutable value, variable and expression nodes. Traversal and substitution use those nodes. |
| F18 | Already fixed by #458: disabled operator overrides are normalized without collapsing the `None` versus empty-map contract. |
| F19, F20 | Spring Data delegates numeric comparisons to the existing implementation and inspects relation-chain length without constructing discarded subqueries. |
| F21 | The per-test corpus-gap labels already landed in #455/#458. |
| F22, F25, F26 | Already fixed by #458: shared oracle-query execution, closed ledger schemas, and derived demo call expectations. |
| F23 | Already fixed by #459: npm publishing requires the full adapter workflow at the tagged commit. |
| CG-a, CG-b | Five new shared actions cover projection lambda bodies and negated scalar predicates through a parent. Every adapter executes or explicitly rejects them, with non-degeneracy checks and updated golden expectations. |

## Changes exposed by the new corpus actions

The corpus grows from 274 to 279 actions without changing seed rows or existing
planner fixtures. Drizzle previously admitted parentless rows under the new
negated leaf predicates. Prisma's negated projection comparison dropped explicit
null list elements that CEL allows. Mongoose's hierarchy guard unnecessarily
rejected scalar paths through to-one relations. The new cases also establish
Elasticsearch support for positive flat-array equality and guarded negated
hierarchy overlap.

Chroma rejects all five new shapes with pinned messages. Elasticsearch rejects
the negated projection body: negating a term query over a flat array excludes
mixed arrays, whereas CEL's existential test can accept one nonmatching element.
The other outcomes are checked against the live PDP and real stores.

## Preserved boundaries

F24's optional publish-workflow consolidation is not applied. The current
repository contract and #459 require stable publisher filenames because npm
trusted publishing binds each package to its configured workflow. The release
gate is fixed; consolidation remains an operational migration requiring the
corresponding npm settings changes and release verification.

F4 retains the pre-existing assertion between the internal Convex query builder
and the public caller-selected `Q`/`R` generic parameters. Removing that assertion
requires a separate builder-interface change; the result-state union no longer
needs an incomplete-object assembly assertion.

The CI instructions now refer to each workflow for its runtime matrix, removing
the stale claim that Convex uses the same Node versions as every other TypeScript
adapter.

F27 remains rejected as the audit specifies. No shared corpus loader, published
dependency, service-image pin, packaging rule or release workflow is replaced.

## Validation

Verification covers native builds and type checks, translator contracts, live
PDP comparisons against the adapters' datastore variants, and the changed
Convex and Chroma public types through packed examples. The Go translator trees
remain byte-identical. Golden changes are reviewed separately from planner
fixtures; the latter add only the five new actions.

Independent review found and corrected a Drizzle optional-options dereference
and a Prisma prefix-mapping regression before final validation.

| Surface | Verified |
| --- | --- |
| Prisma | Both supported client majors on SQLite, PostgreSQL and MySQL; build, type checks and translator tests |
| Mongoose | Build, type checks, translator tests and baseline MongoDB conformance |
| Drizzle | Build, type checks, translator tests, SQLite/PostgreSQL/MySQL conformance and packed example |
| Convex | Build, type checks, translator tests, deployed-backend conformance and packed example |
| ChromaDB | Build, type checks, translator tests, real-server conformance and packed example |
| Ent / pgx | Builds, lint and full native suites; Ent includes SQLite/PostgreSQL/MySQL |
| SQLAlchemy | Configured Pyright, package build, full 2.x and 1.4 suites |
| ActiveRecord | Full 8.0 and 7.1 suites |
| Spring Data | Baseline H2/PostgreSQL/MySQL and next-Hibernate H2 builds and suites |
| Elasticsearch | Full suites against both pinned server versions |
| Shared | Fixture regeneration, corpus integrity, documentation and demo validators, whitespace checks |

The MongoDB 8 forward leg could not execute locally: the pinned server exits on
this Docker host with the upstream Linux-kernel incompatibility
`SERVER-121912`. The MongoDB baseline suite passes; the forward image pin and CI
coverage are unchanged. SQLAlchemy's configured type checks pass; direct checking
of the whole source file still reports pre-existing diagnostics, with no new
messages introduced by the typed-node refactor.
