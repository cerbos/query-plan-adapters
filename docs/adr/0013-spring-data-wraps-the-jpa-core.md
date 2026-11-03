# spring-data wraps the jpa core

Accepted.

## Context

`spring-data` translated a plan into a Spring Data JPA `Specification`, but only its entry point,
`SpringDataQueryPlanAdapter`, imported Spring. The other 32 source files are a Jakarta Persistence
criteria translator: they take a `CriteriaBuilder`, a `Root` and a `CriteriaQuery` and build a
`Predicate`. Quarkus, Jakarta EE, Micronaut Data JPA and plain Hibernate applications have all
three and no `Specification`, so the only way to reuse the translator was to put Spring Data on the
classpath to call one method.

[ADR 0007](0007-adapters-share-data-not-code.md) says adapters share data, not code, and that every
adapter is an independently publishable package that depends on nothing else in this repository.
Splitting the translator out means one adapter depends on another.

## Considered options

### Keep one adapter, and make Spring Data an optional dependency

The plain-JPA entry point would sit beside `SpringDataQueryPlanAdapter` in one artifact, with
spring-data-jpa `compileOnly`. Rejected: the artifact would still be named and documented for
Spring, a JPA user would have to know the Spring class is safe to leave unloaded, and the published
POM could not say which half a consumer uses.

### Two adapters, each with its own copy of the translator

The Go precedent: ent and pgx vendor one translator tree, held byte-identical by
`validate-corpus.sh`. Rejected: Go vendors because two modules cannot share a package without one
importing the other's module path. A Maven artifact has no such cost, and a 7 000-line copy held
identical by a diff is a second place for every translator fix to land.

### A jpa core, and spring-data as a wrapper over it

Accepted.

## Decision

- **`jpa/` is the translator**, published as `dev.cerbos:cerbos-jpa`, package
  `dev.cerbos.queryplan.jpa`. `JpaQueryPlanAdapter.toFilter(plan, options)` returns a
  `QueryPlanFilter`, whose `toPredicate(root, query, cb)` builds a fresh `Predicate` per query. An
  always-allowed plan is `cb.conjunction()` and an always-denied one `cb.disjunction()`, so the
  predicate is never null ([ADR 0011](0011-denial-fails-closed-in-the-result.md)).
- **`spring-data/` is a wrapper.** `SpringDataQueryPlanAdapter.toSpecification` keeps its overloads
  and returns the filter's method reference, or `Specification.unrestricted()` for an
  always-allowed plan ([ADR 0003](0003-spring-data-returns-specification-directly.md)). It depends
  on `cerbos-jpa` as an `api` dependency, since `AttributeMapping`, `Options` and the refusal types
  are its API too.
- **The dependency is a published coordinate**, not a path import. spring-data's POM names
  `dev.cerbos:cerbos-jpa` at its own version, and its build substitutes this checkout's `jpa/`
  through `includeBuild("../jpa")`, so a change to the core runs spring-data's suites in the same
  pull request. Its example resolves both from mavenLocal
  ([ADR 0002](0002-examples-install-the-packed-artifact.md)).
- **Both stay on the roster**, each with a ledger, a harness and an example. jpa's harness runs
  every store leg (H2, PostgreSQL, MySQL, MySQL with server-side prepared statements) under both
  ORM sets. spring-data's runs on H2, through `toSpecification`: the SQL is the core's, and what
  the wrapper can get wrong (the always-allowed `Specification`, the predicate rebuilt per query)
  shows on any store.

## Consequences

- ADR 0007 keeps its rule for adapters over different stores or query layers. This is the one case
  where an adapter is a thin adapter of another's output type, and the coupling is the point: a
  translator fix lands once, for both.
- **Breaking, for spring-data callers.** `AttributeMapping`, `NullAttributeRepresentation`,
  `OperatorFunction` and the refusal types move to `dev.cerbos.queryplan.jpa`; `Options`, with
  `MAX_MACRO_DEPTH_PROPERTY` and `DEFAULT_MAX_MACRO_DEPTH`, moves to `JpaQueryPlanAdapter`; and the
  system property is `dev.cerbos.queryplan.jpa.maxMacroDepth`. Both adapters are
  `0.1.0-alpha.1` and unpublished, so the cost is an import change.
- The two ledgers hold the same entries, since the translation is the same. A ledger change in
  one is a ledger change in the other.
- A release publishes `jpa` before `spring-data`.
