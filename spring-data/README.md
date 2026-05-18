# cerbos-spring-data

> **Alpha release — `0.1.0-alpha.1`.** API and operator coverage are stable, but field/relation
> mapping shapes may still change before `1.0`. We'd love feedback while it's still alpha.

[Cerbos](https://cerbos.dev) query plan adapter for [Spring Data JPA](https://spring.io/projects/spring-data-jpa). Converts a Cerbos `PlanResources` response into a `org.springframework.data.jpa.domain.Specification<T>` you can pass straight to a `JpaSpecificationExecutor`.

## Install

Gradle:

```kotlin
dependencies {
    implementation("dev.cerbos:cerbos-spring-data:0.1.0-alpha.1")
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

You'll also need the Cerbos Java SDK (`dev.cerbos:cerbos-sdk-java`) to call the PDP and Spring Data JPA (`org.springframework.data:spring-data-jpa`).

## Quick start

```java
import dev.cerbos.queryplan.springdata.AttributeMapping;
import dev.cerbos.queryplan.springdata.Result;
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

import java.util.Map;

public interface ContactRepository
        extends JpaRepository<Contact, Long>, JpaSpecificationExecutor<Contact> {}

// Map Cerbos resource attributes to JPA paths or relations on your entity:
Map<String, AttributeMapping> MAPPING = Map.of(
    "request.resource.attr.ownerId",    AttributeMapping.field("owner.id"),
    "request.resource.attr.isPublic",   AttributeMapping.field("isPublic"),
    "request.resource.attr.department", AttributeMapping.field("department"),
    "request.resource.attr.tags",       AttributeMapping.relation("tags", Map.of(
        "name", AttributeMapping.field("name")
    ))
);

// 1) Call the PDP for a query plan
var planResult = cerbosClient.plan(
    Principal.newInstance("alice", "USER"),
    Resource.newInstance("contact"),
    "view");

// 2) Translate to a Specification
Result<Contact> result =
    SpringDataQueryPlanAdapter.toSpecification(planResult, MAPPING);

// 3) Execute via your repository
List<Contact> contacts = contactRepository.findAll(result.toSpecification());
```

`Result.toSpecification()` returns a Specification that captures all three plan kinds, so you don't need to switch on the result kind unless you want to short-circuit the DB hit:

| Kind                   | Specification                          |
|------------------------|----------------------------------------|
| `Result.AlwaysAllowed` | always-true predicate (`1=1`)          |
| `Result.AlwaysDenied`  | always-false predicate (`1=0`)         |
| `Result.Conditional`   | the translated predicate tree          |

Compose it with your own filters:

```java
Specification<Contact> own =
    (root, query, cb) -> cb.like(root.get("name"), "Smith%");

List<Contact> results = contactRepository.findAll(
    own.and(result.toSpecification()), pageable);
```

## Field mapping

Map each `request.resource.attr.<name>` to a JPA path or a relation:

| Helper                                                       | Use for                                                 |
|---------------------------------------------------------------|---------------------------------------------------------|
| `AttributeMapping.field("aPath")`                             | Simple column or `@Embedded` dotted path                |
| `AttributeMapping.relation("tags")`                           | `@ElementCollection<String>` (bare values)              |
| `AttributeMapping.relation("tags", "name")`                   | `@OneToMany` collection where the default member field is `name` |
| `AttributeMapping.relation("tags", Map.of("name", field("name")))` | `@OneToMany<Tag>` with explicit nested field mapping |

`Field("nested.aBool")` traverses embeddables via JPA `Path.get(...)`. Use it for both simple columns and `@Embedded` paths.

## Supported operators

| Cerbos operator                  | JPA Criteria translation                                            |
|----------------------------------|---------------------------------------------------------------------|
| `and` / `or` / `not`             | `cb.and` / `cb.or` / `cb.not`                                       |
| `eq` / `ne`                      | `cb.equal` / `cb.notEqual` (auto `isNull`/`isNotNull` for `null` RHS) |
| `lt` / `gt` / `le` / `ge`        | `cb.lessThan` / `greaterThan` / `lessThanOrEqualTo` / `greaterThanOrEqualTo` |
| `in`                             | `path.in(values)` or correlated `EXISTS` for collections            |
| `contains` / `startsWith` / `endsWith` | `cb.like(...)` with proper `_`/`%`/`\` escaping              |
| `isSet(field, true/false)`       | `cb.isNotNull` / `cb.isNull`                                        |
| `hasIntersection(coll, [values])` | Correlated `EXISTS` with `IN`                                      |
| `hasIntersection(coll.map(x, x.f), [values])` | Correlated `EXISTS` with projected `IN`             |
| `size(coll) > 0` / `>= 1`        | Correlated `EXISTS`                                                 |
| `size(coll) == 0` / `<= 0` / `< 1`| `NOT EXISTS`                                                       |
| `exists(coll, lambda)`           | Correlated `EXISTS` with lambda body                                |
| `exists_one(coll, lambda)`       | Correlated `(SELECT COUNT...) = 1`                                  |
| `all(coll, lambda)`              | `NOT EXISTS (... AND NOT(body))`                                    |
| `except(coll, lambda)`           | Correlated `EXISTS (... AND NOT(body))`                             |
| `filter(coll, lambda)`           | Same as `exists` (filter returns a list — treated as "exists matching") |
| Bare boolean variable            | `cb.equal(path, true)`                                              |
| `eq(field, add(const1, const2))` | Constant fold then compare: `cb.equal(field, const1 ⊕ const2)`     |
| `eq(value, add(const, field))`   | Solve for `field` (string prefix/suffix strip; numeric subtract); unsolvable cases become `1=0` / `1=1` |

Unsupported operators raise `IllegalArgumentException` — override them with `OperatorFunction`:

```java
Map<String, OperatorFunction> overrides = Map.of(
    "contains", (cb, field, value) ->
        cb.equal(cb.lower(field.as(String.class)), value.toString().toLowerCase())
);

Result<Contact> result =
    SpringDataQueryPlanAdapter.toSpecification(planResult, MAPPING, overrides);
```

## Not yet supported

The Criteria-based predicate builder has no shape for these CEL constructs; they
throw `IllegalArgumentException` with a message naming the operator. Override
via `OperatorFunction` when the runtime can express them (e.g. database-specific
SQL fragments), or wait for adapter support.

| Construct                                       | Example CEL                                       | Notes |
|-------------------------------------------------|---------------------------------------------------|-------|
| Arithmetic (`add`/`sub`/`mult`/`div`/`mod`)     | `R.attr.aNumber + 1 > 2`                          | `add` is supported only as constant folding inside `eq`/`ne`; other arithmetic on document fields requires a column-expression engine the Criteria API doesn't expose. |
| Regex match                                     | `R.attr.aString.matches("^foo.*")`                | JPA has no portable regex predicate; override per-dialect (`regexp_like`, `~`, `REGEXP`). |
| List indexing                                   | `R.attr.tags[0] == "x"`                           | JPA collections are unordered sets — no positional access. |
| Type casts (`int(...)` / `double(...)` / `string(...)`) | `int(R.attr.aString) > 0`                 | No portable `CAST` in Criteria; override per-dialect. |
| Ternary (`cond ? a : b`)                        | `(R.attr.aBool ? R.attr.aNumber : 0) > 0`         | The CEL planner emits this as `if(cond, then, else)`; JPA Criteria has no `CASE WHEN` value-expression builder. |
| `size(string)`                                  | `size(R.attr.aString) > 0`                        | Only `size(collection)` (`Relation` mapping) is supported; for strings use `cb.length` via an override. |
| Field-to-field comparison                       | `R.attr.aString == R.attr.id`                     | The leaf operator handler requires one variable + one value operand; throws explicitly. |
| `eq(map(...), [...])`                           | `R.attr.tags.map(t, t.id) == ["tag1", "tag2"]`    | Use `hasIntersection(map(...), [...])` instead. |
| `size(filter(...)) <op> N`                      | `size(R.attr.tags.filter(t, t.name == "x")) > 0`  | Use `exists(coll, lambda)` for emptiness; `size()` only accepts a Variable operand. |
| `size(coll) <op> N` for `N > 0`                 | `size(R.attr.tags) > 5`                           | Only emptiness checks are supported. |
| Hierarchy operators (`hierarchy-*`)             | `hierarchy.overlaps(...)`                         | Not yet ported from the Prisma adapter; ~250 LoC follow-up. |

## Gotchas

Things you're likely to hit when integrating the adapter into a Spring Boot app — see the
[`example/`](example) photo-sharing application for a runnable end-to-end reference.

### Pin `protobuf-java` to the cerbos-sdk-java's gencode version

`cerbos-sdk-java` 0.18.0 ships protobuf message classes generated against
`protobuf-java` 4.33.5. If your application classpath ends up with an **older** runtime
— either because you pin it explicitly, or a transitive dependency wins resolution — the
SDK throws on first message decode:

```text
com.google.protobuf.RuntimeVersion$ProtobufRuntimeVersionException:
  Detected incompatible Protobuf Gencode/Runtime versions when loading Principal:
  gencode 4.33.5, runtime 4.31.1. Runtime version cannot be older than the linked gencode version.
```

Fix — add a direct dependency matching the SDK's gencode:

```kotlin
implementation("com.google.protobuf:protobuf-java:4.33.5")
```

Spring Boot's BOM does not manage `protobuf-java`, so without an explicit pin Gradle's
default conflict resolver picks the highest version on the graph. Pinning makes the
contract explicit and survives BOM upgrades.

### `@ElementCollection` / `@OneToMany` + `spring.jpa.open-in-view=false`

Mapping a Cerbos attribute via `AttributeMapping.relation(...)` translates `"x" in tags`
to a correlated `EXISTS` subquery — but the entity collection itself is still lazy by
default. If your controller serializes the entity (or any field traversal happens after
the transaction closes), you'll see:

```text
HttpMessageNotWritableException: Could not write JSON:
  failed to lazily initialize a collection of role: …Photo.tags: could not initialize proxy - no Session
```

Pick one:

- **Eager-fetch** the collection if it's small (`@ElementCollection(fetch = FetchType.EAGER)`).
- **Do the entity-to-DTO mapping inside `@Transactional(readOnly = true)`** so the Hibernate
  session is still open while you walk relations.
- **Don't serialize entities** — return a DTO projection instead.

The adapter itself has no opinion here — this is the same `open-in-view=false` footgun any
JPA app hits — but it's worth flagging because Cerbos plans frequently *do* reference
collection attributes (`tags`, `members`, `categories`), and those are the ones developers
typically forget to fetch.

### Don't cache the produced `Predicate`

`Result.Conditional.toSpecification()` returns a Specification whose lambda **rebuilds the
predicate tree against each invocation's `Root`/`CriteriaQuery`**. Spring Data's
`findAll(spec, Pageable)` fires a separate `COUNT` query with its own root, and Hibernate 6
rejects a `Predicate` produced against a stale root with
`SqlTreeCreationException: Could not locate TableGroup`. Pass the Specification to
repository methods; don't cache the `Predicate` it returns.

## Build

From the `spring-data/` directory:

```bash
# With Docker (recommended — matches CI):
docker run --rm -v "$(pwd)/..":/app -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_RYUK_DISABLED=true --network host -w /app/spring-data gradle:8.12-jdk17 \
  gradle build --no-daemon

# Or with a local Gradle 8.x + JDK 17+:
gradle build --no-daemon
```

## End-to-end testing

Every test runs against a **real Cerbos PDP container** — there is no stubbing of policy
evaluation. Two run modes are supported:

### 1. Self-managed (default)

[Testcontainers](https://testcontainers.com) pulls and starts `ghcr.io/cerbos/cerbos:latest`,
mounts `../policies/resource.yaml`, and runs the suite against the gRPC endpoint. The container's
**audit + decision logs are streamed to the test JVM logger** so you can see every
`PlanResources` call the test issued.

```bash
gradle test
```

### 2. Externally-managed (Prisma-style sidecar)

Matches what the Prisma adapter does with `cerbos run -- jest`: a long-lived PDP container
started separately, tests connect to it via `CERBOS_HOST` / `CERBOS_PORT` env vars. Useful for
debugging the live PDP between test runs.

```bash
./scripts/run-e2e.sh        # docker compose up -d  →  gradle test  →  audit log summary  →  down
```

At the end of `run-e2e.sh` you'll see something like:

```
==> Cerbos PDP audit summary
    PlanResources calls served: 122
    CheckResources calls served: 0
    Audit log archived at: /tmp/cerbos-audit-XXXX.log

==> Sample decision log entries:
{"log.logger":"cerbos.audit","log.kind":"decision","callId":"01KRX3A0DFF9F00ZC1F0M8Z1MD",
 "planResources":{"input":{"actions":["equal-nested"], ...},
                  "output":{"filter":{"condition":{...},"kind":"KIND_CONDITIONAL"}, ...}}, ...}
```

— this is the PDP's own decision log, proving every assertion in the suite came from a real
policy evaluation against the shared `../policies/resource.yaml`.

You can also run the compose stack by hand:

```bash
docker compose up -d
CERBOS_HOST=localhost CERBOS_PORT=3593 gradle test
docker compose down
```

When `CERBOS_HOST` is unset, the suite falls back to mode (1) automatically.
