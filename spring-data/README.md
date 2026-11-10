# cerbos-spring-data

> **Alpha release — `0.1.0-alpha.1`.** Operator coverage is stable; the API and the field/relation
> mapping shapes may still change before `1.0`. Feedback welcome.

[Cerbos](https://cerbos.dev) query plan adapter for [Spring Data JPA](https://spring.io/projects/spring-data-jpa).
Converts a Cerbos `PlanResources` response into a `org.springframework.data.jpa.domain.Specification<T>`
you can pass straight to a `JpaSpecificationExecutor`.

The translation is the [jpa](../jpa/) adapter's, which works on any Jakarta Persistence
`CriteriaQuery` without Spring; this adapter hands it to Spring Data as a `Specification`. The
mapping, the options, the refusals, the supported operators and every gotcha are the same, and
[jpa/README.md](../jpa/README.md) documents them.

## Install

The adapter is **not published to Maven Central yet**. Build it and the [jpa](../jpa/) core it
wraps, and install both into your local Maven repository, from a checkout of this repository:

```bash
(cd jpa && ./gradlew publishToMavenLocal)          # needs JDK 17+
(cd spring-data && ./gradlew publishToMavenLocal)
```

Then depend on it, with `mavenLocal()` among your Gradle repositories.

Gradle:

```kotlin
repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation("dev.cerbos:cerbos-spring-data:0.1.0-alpha.1")
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1") // to call the PDP
}
```

Maven:

```xml
<dependency>
    <groupId>dev.cerbos</groupId>
    <artifactId>cerbos-spring-data</artifactId>
    <version>0.1.0-alpha.1</version>
</dependency>
```

Requirements:

- JDK 17+.
- Spring Data JPA (`org.springframework.data:spring-data-jpa`) **3.5.2 or later**, supplied by your
  application — an always-allowed plan becomes `Specification.unrestricted()`, added in 3.5.2. From
  the Spring Boot BOM that means **Boot 3.5.4 or later** (3.5.0–3.5.3 manage 3.5.0/3.5.1).
- Developed against Spring Data JPA 3.5 / Hibernate 6.6; CI also runs every suite under Spring Data
  JPA 4 / Hibernate 7 (the Spring Boot 4 pair) — see [Development](#development).
- `dev.cerbos:cerbos-jpa`, which the adapter's POM brings at the same version.
- String columns the mapping references need a byte-exact collation — see
  [Database collation requirements](../jpa/README.md#database-collation-requirements).

## Quick start

```java
import dev.cerbos.queryplan.jpa.AttributeMapping;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.List;
import java.util.Map;

public interface ContactRepository
        extends JpaRepository<Contact, Long>, JpaSpecificationExecutor<Contact> {}

// Cerbos resource attributes -> JPA paths / relations on your entity
static final Map<String, AttributeMapping> MAPPING = Map.of(
    "request.resource.attr.ownerId",    AttributeMapping.field("owner.id"),
    "request.resource.attr.isPublic",   AttributeMapping.field("isPublic"),
    "request.resource.attr.department", AttributeMapping.field("department"),
    "request.resource.attr.tags",       AttributeMapping.relation("tags", Map.of(
        "name", AttributeMapping.field("name"))));

List<Contact> viewableContacts(CerbosBlockingClient cerbos, ContactRepository contacts) {
    PlanResourcesResult plan = cerbos.plan(
        Principal.newInstance("alice", "USER"),
        Resource.newInstance("contact"),
        List.of("view"));

    // Optional: skip the database round trip when nothing is allowed.
    if (plan.isAlwaysDenied()) {
        return List.of();
    }

    Specification<Contact> allowed = SpringDataQueryPlanAdapter.toSpecification(plan, MAPPING);
    return contacts.findAll(allowed);
}
```

The returned `Specification` covers every plan kind, so you never switch on it:

| Plan kind | Specification |
|---|---|
| `KIND_ALWAYS_ALLOWED` | `Specification.unrestricted()` — no `WHERE` clause |
| `KIND_ALWAYS_DENIED` | always-false predicate (`1=0`) |
| `KIND_CONDITIONAL` | the translated predicate tree |

Compose it with your own filters and hand it to a repository method; don't call
`Specification.toPredicate` yourself.

```java
Specification<Contact> own = (root, query, cb) -> cb.like(root.get("name"), "Smith%");
Page<Contact> page = contacts.findAll(own.and(allowed), pageable);
```

`toSpecification` also accepts the raw `PlanResourcesResponse` protobuf.

> [!WARNING]
> **The Specification is SELECT-only.** Never pass it to `repository.delete(Specification)` or any
> criteria bulk operation. Hibernate's multi-table bulk delete first clears `@ElementCollection` /
> join tables using the same predicate, which removes the rows the correlated subquery reads — the
> delete removes 0 entities while destroying their collection rows, and under a blocklist policy the
> now-ownerless rows become visible to everyone. The adapter detects a `CriteriaDelete` context and
> throws `UnsupportedOperationException` before anything is deleted. Select ids, then delete by id:
>
> ```java
> List<Long> ids = contacts.findAll(allowed).stream().map(Contact::getId).toList();
> contacts.deleteAllById(ids);
> ```

## Mapping, options and refusals

Everything you pass besides the plan is the jpa adapter's, in package `dev.cerbos.queryplan.jpa`:

| Topic | Where |
|---|---|
| `AttributeMapping.field(...)` / `relation(...)` | [Field mapping](../jpa/README.md#field-mapping) |
| `JpaQueryPlanAdapter.Options` | [Declaring the translation](../jpa/README.md#declaring-the-translation-options) |
| `UnsupportedPlanShapeException`, `UnmappedAttributeException`, `MalformedPlanException` | [Handling refusals](../jpa/README.md#handling-refusals) |
| `OperatorFunction` | [Operator overrides](../jpa/README.md#operator-overrides) |
| `NullAttributeRepresentation` | [NULL attribute representation](../jpa/README.md#null-attribute-representation) |
| Collations, operators, mapping hazards, gotchas | [jpa/README.md](../jpa/README.md#database-collation-requirements) onwards |

```java
import dev.cerbos.queryplan.jpa.JpaQueryPlanAdapter.Options;
import dev.cerbos.queryplan.jpa.NullAttributeRepresentation;

Options options = Options.of(MAPPING)
    .withNullAttributeRepresentation(NullAttributeRepresentation.EXPLICIT)
    .withMaxMacroDepth(8);

Specification<Contact> allowed = SpringDataQueryPlanAdapter.toSpecification(plan, options);
```

The positional overloads, `toSpecification(plan, mapper, overrides, nullAttributeRepresentation)`,
are shorthand for `Options`. A conditional plan is translated when Spring Data first evaluates the
Specification, so that is where most refusals are raised; the `NullAttributeRepresentation.OMITTED`
check and a missing condition are raised by `toSpecification`.

## Conformance contract

The adapter replays the shared [conformance corpus](../conformance/README.md) through
`toSpecification`: for each recorded plan of Cerbos PDP 0.57.0 and 0.56.0, it translates the plan,
runs the query against 43 of the corpus's 44 seed rows on H2, and compares the returned ids with the
`check()` decisions the PDP recorded. The jpa adapter replays the same corpus on H2, PostgreSQL and
MySQL; the SQL is its, so this harness proves the Specification selects what the jpa filter does.
The ledger lacks `null-list`, so j4 is not loaded. No PDP runs in the test. Results for the current
PDP (0.56.0), where the total is every golden case of that tier; a case marked as a planner
divergence is skipped, and counts toward the total but not as passed:

| Tier | Passed / total |
| --- | --- |
| core | 30 / 30 |
| extended | 98 / 102 |
| adversarial | 423 / 462 |

Every case that does not pass is listed with its reason in
[`conformance-ledger.json`](conformance-ledger.json), which holds the jpa adapter's entries: the
translation is the same. [jpa's contract](../jpa/README.md#conformance-contract) says what the
planner divergences are.

## Gotchas

The [jpa adapter's gotchas](../jpa/README.md#gotchas) all apply. Two are Spring's own.

### Don't call `Specification.toPredicate` yourself

Hand the Specification to a repository method. A paged `findAll` runs a separate `COUNT` query with
its own `Root`, and the Specification builds a fresh predicate for each, as Hibernate requires. An
always-allowed plan is `Specification.unrestricted()`, whose predicate is `null`, which Spring Data
reads as "no `WHERE`" and a hand-built query would not.

### `@ElementCollection` / `@OneToMany` + `spring.jpa.open-in-view=false`

A `relation(...)` mapping filters through `EXISTS`, but the entity's collection is still lazy.
Serializing the entity after the transaction closes fails with
`failed to lazily initialize a collection of role: …Photo.tags: could not initialize proxy - no Session`.
Fix it the usual JPA way: eager-fetch small collections, map to DTOs inside
`@Transactional(readOnly = true)`, or return DTO projections.

## Behaviour changes

- Fixed an over-grant, in the jpa translator: an attribute whose Java type is not `Boolean` read where CEL needs a boolean (`!R.attr.aString`) is CEL's type error, UNKNOWN under both polarities, where it was compared with `true`.
- **Breaking:** the translator moved to the [jpa](../jpa/) adapter, which this adapter now depends
  on ([ADR 0013](../docs/adr/0013-spring-data-wraps-the-jpa-core.md)). `toSpecification` and its
  overloads are unchanged, and the Specifications they return select the same rows. Change imports:

  | Before | After |
  |---|---|
  | `dev.cerbos.queryplan.springdata.AttributeMapping` (and `NullAttributeRepresentation`, `OperatorFunction`, the three refusal exceptions) | `dev.cerbos.queryplan.jpa.…` |
  | `SpringDataQueryPlanAdapter.Options` | `JpaQueryPlanAdapter.Options` |
  | `SpringDataQueryPlanAdapter.MAX_MACRO_DEPTH_PROPERTY`, `DEFAULT_MAX_MACRO_DEPTH` | `JpaQueryPlanAdapter.…` |
  | `-Ddev.cerbos.queryplan.springdata.maxMacroDepth` | `-Ddev.cerbos.queryplan.jpa.maxMacroDepth` |

- **Breaking** — `toSpecification(...)` returns `Specification<T>` directly; the `Result<T>` wrapper
  is gone. Drop the second `.toSpecification()` call, and use `planResult.isAlwaysDenied()` to skip
  the database. Raises the Spring Data JPA floor to 3.5.2
  ([ADR 0003](../docs/adr/0003-spring-data-returns-specification-directly.md)).

  ```java
  // before
  Result<Contact> result = SpringDataQueryPlanAdapter.toSpecification(planResult, MAPPING);
  repository.findAll(tenantBoundary.and(result.toSpecification()));
  // after
  Specification<Contact> allowed = SpringDataQueryPlanAdapter.toSpecification(planResult, MAPPING);
  repository.findAll(tenantBoundary.and(allowed));
  ```

Translation changes, from before and after the move, are in the
[jpa adapter's behaviour changes](../jpa/README.md#behaviour-changes).

## Example application

[`example/`](example) holds two runnable Spring Boot programs that resolve this adapter and the jpa
core from mavenLocal as real Maven coordinates
([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)): a photo-sharing app with
three resource kinds, and a program implementing the [shared demo domain](../demo). Run the latter
from the repository root with `demo/scripts/run-example.sh spring-data`.

## Development

JDK 17+. Gradle comes from the committed wrapper. The build substitutes `../jpa` for the
`cerbos-jpa` dependency (`includeBuild` in [`settings.gradle.kts`](settings.gradle.kts)), and the
suites read `../conformance/`, so build in a checkout of the **whole repository**. No suite needs
Docker.

```bash
# From spring-data/:
./gradlew build
ADAPTER_TEST_ORM=next ./gradlew test      # every suite under Hibernate 7 / Spring Data JPA 4
```

`ADAPTER_TEST_ORM` is `baseline` (default: Hibernate 6.6 / Spring Data JPA 3.5) or `next`
(Hibernate 7 / Spring Data JPA 4), declared in [`build.gradle.kts`](build.gradle.kts); the included
jpa build reads the same variable. Spring Data JPA 4 removed
`JpaSpecificationExecutor.delete(Specification)`, so the bulk-delete hazard can't be reached through
that overload there; the guard still fires on any `CriteriaDelete`.

| Suite | Role |
|---|---|
| `SpringDataQueryPlanAdapterTest` | What the wrapper adds: `unrestricted()` for an always-allowed plan, refusals raised by `toSpecification`, the `delete(Specification)` guard |
| `RepositorySurfaceTest` | Spring Data glue: `findAll` / `count` / paging, de-duplication, composition |
| `AdversarialConformanceTest` | Conformance through `toSpecification` on H2; exceptions in [`conformance-ledger.json`](conformance-ledger.json) |

The translator's own suites, and the PostgreSQL and MySQL legs, are in [jpa](../jpa/README.md#testing).
