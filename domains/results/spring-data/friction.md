# spring-data: domain stress friction

A thin run over H2 only: spring-data wraps jpa (ADR 0013) and takes jpa's `AttributeMapping`/`Options` unchanged.
Harness: `spring-data/src/domainTest/java/dev/cerbos/queryplan/jpa/domains/SpringDataDomainHarnessTest.java` (`./gradlew domainTest`).
Its source set compiles jpa's domain files (`../jpa/src/domainTest/java`, minus jpa's own harness), so both adapters run
exactly the same mapping. Each plan becomes a `Specification` via `SpringDataQueryPlanAdapter.toSpecification(PlanResourcesResponse,
Options)`, and runs through `SimpleJpaRepository.findAll(spec)` on the same Hibernate 6.6 / H2 setup as jpa.

| store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| h2 | 331 | 20 | 15 | 2 | 0 |

Every one of the 368 verdicts and returned id sets is identical to `jpa/h2.json`. Mapping sizes, scores, the hardest problem
and every non-pass cause are therefore those in [../jpa/friction.md](../jpa/friction.md). The wrapper adds nothing to configure
beyond the jpa mapping. The 2 over-grants are the PDP 0.56.0 `has()` plans (healthcare `patient_p3 view`, hr
`mgr_e02 view_compensation`), not the adapter.
