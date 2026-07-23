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

| Kind                   | Specification                                                |
|------------------------|--------------------------------------------------------------|
| `Result.AlwaysAllowed` | `null` predicate — Spring Data omits the `WHERE` clause      |
| `Result.AlwaysDenied`  | always-false predicate (`1=0`)                               |
| `Result.Conditional`   | the translated predicate tree                                |

Compose it with your own filters:

```java
Specification<Contact> own =
    (root, query, cb) -> cb.like(root.get("name"), "Smith%");

Page<Contact> results = contactRepository.findAll(
    own.and(result.toSpecification()), pageable);
```

> [!WARNING]
> **The Specification is SELECT-only.** Never pass it to
> `repository.delete(Specification)` or any other criteria bulk operation. Relation
> mappings translate to correlated subqueries over collection/join tables, and
> Hibernate's multi-table bulk delete first clears those `@ElementCollection`/join
> tables using the same predicate — the pre-clear removes exactly the rows the
> correlated subquery references, so the delete removes **0 entity rows while
> silently destroying their collection rows** (e.g. all ownership entries). Under a
> blocklist policy like `!(P.id in R.attr.ownedBy)`, the now-ownerless survivors
> become visible to every principal. The adapter detects the bulk-delete invocation
> context and throws `UnsupportedOperationException` before anything is deleted.
> To delete policy-permitted rows, select ids first, then delete by id:
>
> ```java
> List<Long> ids = contactRepository.findAll(result.toSpecification())
>         .stream().map(Contact::getId).toList();
> contactRepository.deleteAllById(ids);
> ```

## Database collation requirements

> **⚠️ Hard requirement: every string column referenced by an `AttributeMapping` MUST use a
> binary or case-sensitive collation.** On MySQL use `utf8mb4_bin` or `utf8mb4_0900_as_cs`;
> on SQL Server use a `*_CS_AS` collation (e.g. `Latin1_General_100_CS_AS`). PostgreSQL,
> H2, and Oracle are case-sensitive by default and are safe unless you opt into
> case-insensitive behavior (PostgreSQL nondeterministic `ICU` collations, `citext`).

**Why.** CEL string comparison at the PDP is exact and case-sensitive: with
`R.attr.department == "finance"`, a `check()` call for a resource holding
`department = "Finance"` returns **DENY**. But the adapter builds every string predicate
with no collation control, so the database's column collation decides what matches. MySQL
8's default collation, `utf8mb4_0900_ai_ci`, is case- **and** accent-insensitive, and SQL
Server defaults to CI collations — on those defaults `WHERE department = 'finance'`
matches the `'Finance'` row the PDP just denied. The plan-based filter silently returns
rows the policy denies: **an authorization over-grant**, with no error or log line to
notice. The same divergence applies to accent folding (`'résumé'` vs `'resume'`).

**Every string predicate the adapter emits is affected:**

- `eq` / `ne` (`cb.equal` / `cb.notEqual`)
- string ordering: `lt` / `gt` / `le` / `ge`
- the LIKE family: `contains` / `startsWith` / `endsWith`, including the constant-receiver
  forms and the field-to-field variants built from `REPLACE`-escaped column patterns
- `in` list membership (`path.in(...)`)
- `hasIntersection` (both the direct-collection and `map(...)`-projection translations)
- `hierarchy(...)` ancestor/descendant checks (prefix `LIKE` and ancestor-prefix `IN` lists)

**`OperatorFunction` overrides are not a workaround for all of these.** In particular, the
`hasIntersection` translation over a plain field (`path.in(values)`) is built before any
override consultation, so a user-supplied `OperatorFunction` cannot intercept it. Fix the
collation in the schema — that is the only route that covers every predicate site.

Role and tenancy checks are the highest-risk shapes: `'admin'` vs `'Admin'` under
`eq`/`in`/`hasIntersection`, and hierarchy descendant checks where `LIKE 'a:b:%'` matches
`'A:B:x'`.

**How this is enforced in CI.** The differential oracle suite
(`AdversarialConformanceTest`) runs against real PostgreSQL and MySQL databases via
Testcontainers, with mixed-case seed rows whose `check()` decisions differ from what a
case-insensitive collation would match. The MySQL leg creates its schema with
`utf8mb4_0900_as_cs`; running it against MySQL's default collation makes the suite fail,
demonstrating the over-grant. Run the legs locally:

```bash
# PostgreSQL (case-sensitive by default — passes)
ADAPTER_TEST_DB=postgres gradle test --tests AdversarialConformanceTest

# MySQL with the required case-sensitive collation — passes
ADAPTER_TEST_DB=mysql gradle test --tests AdversarialConformanceTest

# MySQL with its DEFAULT collation — FAILS, reproducing the over-grant
ADAPTER_TEST_DB=mysql ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_ai_ci \
  gradle test --tests AdversarialConformanceTest
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
| `contains` / `startsWith` / `endsWith` | `cb.like(...)` with proper `_`/`%`/`\` escaping — incl. the constant-receiver form (`"a,b".contains(R.attr.x)`: the constant is the haystack, the column the needle) |
| `isSet(field, true/false)`       | `cb.isNotNull` / `cb.isNull`                                        |
| `hasIntersection(coll, [values])` | Correlated `EXISTS` with `IN`                                      |
| `hasIntersection(coll.map(x, x.f), [values])` | Correlated `EXISTS` with projected `IN`             |
| `size(coll) > 0` / `>= 1`        | Correlated `EXISTS`                                                 |
| `size(coll) == 0` / `<= 0` / `< 1`| `NOT EXISTS`                                                       |
| `size(coll) <op> N`              | Correlated `(SELECT COUNT...) <op> N`                               |
| `size(coll.filter(x, pred)) <op> N` | Correlated `(SELECT COUNT... WHERE pred) <op> N`                 |
| `size(string)`                   | `cb.length(column)` (see Gotchas for astral-character caveat)       |
| Field-to-field (`R.attr.a == R.attr.b`) | `cb.equal(pathA, pathB)` and friends for `eq`/`ne`/`lt`/`gt`/`le`/`ge`, incl. inside lambdas |
| Ternary (`cond ? a : b`)         | Predicate rewrite: `(cond AND cmp(a, v)) OR (NOT cond AND cmp(b, v))` — nested, boolean-position, and value-first forms compose; constant residues fold (see Gotchas for `NULL` semantics) |
| Arithmetic in comparisons (`add`/`sub`/`mult`/`div`) | `cb.sum`/`diff`/`prod`/`quot` in double space, compared via `eq`/`ne`/`lt`/`gt`/`le`/`ge`; nested and both-sides shapes compose (see Gotchas — CEL attribute arithmetic is double arithmetic) |
| Timestamp comparisons (`timestamp(R.attr.createdAt) < now() - duration("24h")`) | Temporal column comparison for all of `eq`/`ne`/`lt`/`gt`/`le`/`ge`, both operand orders (value-first forms mirror). The planner folds `now()`/`now() - duration(...)` to a constant RFC-3339 instant at **plan time**, so the window is fixed per query — re-plan to refresh it. The mapped column must be `java.time.Instant` or `java.time.OffsetDateTime` (both denote an absolute instant; Hibernate 6 stores them UTC-normalized). `LocalDateTime`, `java.util.Date`, and `String` columns throw a named error — they are ambiguous about the absolute instant they store (see Gotchas). A `NULL` column value is excluded, matching `check()` denying on the missing attribute. |
| Field-to-field `contains`/`startsWith`/`endsWith` | `cb.like` over a `REPLACE`-escaped column-derived pattern (`\`, `%`, `_`), with an `IS NOT NULL` needle guard |
| Multi-hop relation chains (`R.attr.categories.subCategories`) | Correlated subquery joining through every hop; `exists`/`in`/`hasIntersection`/`size` all treat the chain as the flattened union of tail elements |
| `exists(coll, lambda)`           | Correlated `EXISTS` with lambda body                                |
| `exists_one(coll, lambda)`       | Correlated `(SELECT COUNT...) = 1`                                  |
| `all(coll, lambda)`              | `NOT EXISTS (... AND NOT(body))`                                    |
| `except(coll, lambda)`           | Correlated `EXISTS (... AND NOT(body))`                             |
| `filter(coll, lambda)`           | Same as `exists` (filter returns a list — treated as "exists matching") |
| Bare boolean variable            | `cb.equal(path, true)`                                              |
| `eq(field, add(const1, const2))` | Constant fold then compare: `cb.equal(field, const1 ⊕ const2)`     |
| `eq(value, add(const, field))`   | Solve for `field` (string prefix/suffix strip; numeric subtract); unsolvable cases become `1=0` / `1=1` |
| `hierarchy(...).overlaps / ancestorOf / descendentOf` | Segment/prefix predicates (`IN` over ancestor prefixes, `LIKE 'a:b:%'` for descendants), mirroring the Prisma adapter |
| Value-first comparisons (`5 < R.attr.x`) | Normalized field-first with the operator mirrored (`x > 5`) |

Unsupported constructs raise `IllegalArgumentException`. Some — but not all — can be
overridden with an `OperatorFunction`:

```java
Map<String, OperatorFunction> overrides = Map.of(
    "contains", (cb, field, value) ->
        cb.equal(cb.lower(field.as(String.class)), value.toString().toLowerCase())
);

Result<Contact> result =
    SpringDataQueryPlanAdapter.toSpecification(planResult, MAPPING, overrides);
```

An override is consulted only where the adapter has already resolved a `(field, value)`
pair for a top-level operator — the plain comparisons (`eq`/`ne`/`lt`/`gt`/`le`/`ge`,
consulted under the mirrored name for value-first forms), the LIKE family, unknown
top-level leaf operators such as `matches`, and timestamp comparisons (the override
receives the parsed `java.time.Instant` as the value, including for column types the
default translation rejects). Constructs rejected **while resolving an operand** — `mod`,
`int()`/type casts, list indexing — throw before any override lookup and **cannot be
intercepted**; the "Not yet supported" table below marks each row.

## Not yet supported

The Criteria-based predicate builder has no shape for these CEL constructs; they
throw `IllegalArgumentException` with a message naming the operator. The
"Overridable" column says whether a registered `OperatorFunction` can intercept
the construct: rows marked **no** are rejected while resolving an operand,
*before* any override consultation, so an override genuinely cannot fire for them.

| Construct                                       | Example CEL                                       | Overridable | Notes |
|-------------------------------------------------|---------------------------------------------------|-------------|-------|
| `mod`                                           | `R.attr.aNumber % 2 == 0`                         | no          | CEL `%` is int-only and Cerbos attribute numbers are doubles, so `%` on an attribute always errors → the check API denies every row; translating to SQL `MOD` would fabricate matches. |
| Arithmetic on non-numeric operands              | `R.attr.aString + "x" < "y"`                      | no          | Ordering through string concatenation is not translated; `add` string folding remains `eq`/`ne`-only. |
| Regex match                                     | `R.attr.aString.matches("^foo.*")`                | yes (`matches`) | JPA has no portable regex predicate; override per-dialect (`regexp_like`, `~`, `REGEXP`). |
| List indexing                                   | `R.attr.tags[0] == "x"`                           | no          | JPA collections are unordered sets — no positional access. |
| Type casts (`int(...)` / `double(...)` / `string(...)`) | `int(R.attr.aString) > 0`                 | no          | No portable `CAST` in Criteria. |
| `eq(map(...), [...])`                           | `R.attr.tags.map(t, t.id) == ["tag1", "tag2"]`    | no          | Use `hasIntersection(map(...), [...])` instead. |
| Timestamp comparison on an ambiguous column type | `timestamp(R.attr.createdAt) < now() - duration("24h")` with `createdAt` mapped to `LocalDateTime`/`java.util.Date`/`String` | yes (the comparison operator) | The supported shape (see table above) requires an `Instant` or `OffsetDateTime` column. Other types don't pin the absolute instant they store — a wrong zone/format assumption would silently diverge from `check()`. The override receives the parsed `Instant` and can apply schema-specific knowledge. |
| Timestamp shapes beyond `timestamp(field) vs constant` | `timestamp(R.attr.a) < timestamp(R.attr.b)`, `timestamp(...)` inside arithmetic | no | Only the leaf comparison shape the planner emits for time-window policies is translated; nested/derived shapes keep their named errors. |
| `eq`/`ne` against a list constant               | `R.attr.tags == ["a", "b"]`                       | no          | Whole-list equality has no scalar-column translation (the plan arrives as `eq(variable, value-list)` verbatim); rejected before path resolution. Map the attribute as a Relation and use `in`/`hasIntersection`, or compare elements individually. |

## Gotchas

Things you're likely to hit when integrating the adapter into a Spring Boot app — see the
[`example/`](example) photo-sharing application for a runnable end-to-end reference.

### `size(string)` counts differently for astral characters

CEL's `size(string)` counts Unicode code points; the adapter translates it to SQL
`LENGTH()`, whose unit varies by database (UTF-16 units on H2, characters on PostgreSQL,
bytes on some MySQL collations). The two only diverge for characters outside the Basic
Multilingual Plane (emoji, some CJK extensions): `size("héllo🚀")` is 6 in CEL but
`LENGTH` may report 7. If your data contains astral characters and a policy compares
lengths near those values, rows can be filtered differently than a `check` call would
decide. Keep length thresholds away from values that straddle the difference, or avoid
`size(string)` in policies over data that contains astral characters.

### Attribute arithmetic is double arithmetic — use double literals in policies

Cerbos attribute values arrive as protobuf numbers, which CEL treats as doubles.
CEL arithmetic has no int/double cross-type overloads, so `R.attr.aNumber + 1`
(int literal) is an evaluation error at `check` time — every row is denied — while
`R.attr.aNumber + 1.0` evaluates normally. The planner erases the distinction (both
forms produce an identical wire plan), so the adapter translates the double reading,
the only satisfiable one: `/` is true double division (`5 / 2.0 == 2.5`), never
integer truncation. Write double literals (`1.0`, `2.0`) in policy arithmetic over
attributes, or the plan-based filter and per-resource `check` calls will disagree.

### Timestamp comparisons: plan-time `now()`, and only unambiguous column types

`timestamp(R.attr.createdAt) < now() - duration("24h")` reaches the adapter as a
comparison against a **constant** instant: the planner evaluates `now()` and folds the
duration arithmetic when the plan is produced, wrapping the result back in
`timestamp("<RFC-3339>")`. Two consequences:

- The cutoff is frozen at plan time. A cached `Specification` keeps filtering against the
  old instant — call `plan` (and re-translate) per request if the window must track wall
  clock.
- The mapped column must be `java.time.Instant` or `java.time.OffsetDateTime`. Both
  unambiguously denote an absolute instant, and Hibernate 6 binds them UTC-normalized
  (`TIMESTAMP_UTC`), so the database comparison is an instant comparison on H2,
  PostgreSQL, and MySQL alike (the differential oracle runs all three). `LocalDateTime`
  has no zone, `java.util.Date` binding routes through zone conversions, and `String`
  ordering depends on format and offset — the adapter throws a named error for those
  rather than guessing an assumption that could silently diverge from `check()`. If you
  know your schema's zone semantics, register an `OperatorFunction` override for the
  comparison operator: it receives the parsed `Instant` as the value.

  On MySQL, Hibernate maps these columns to `TIMESTAMP`, whose range ends at
  2038-01-19 — instants beyond that need a schema-controlled `DATETIME(6)` column
  (still UTC-normalized by Hibernate).

### Field-to-field string matching builds its pattern with `REPLACE`

`R.attr.a.contains(R.attr.b)` becomes `a LIKE CONCAT('%', <escaped b>, '%') ESCAPE '\'`
where `b` is escaped via nested `REPLACE` calls (`\`, `%`, `_`) — chosen because
`REPLACE` is available on H2, PostgreSQL, MySQL, Oracle, and SQL Server. A `NULL`
needle column excludes the row (`IS NOT NULL` guard), which matches CEL
missing-attribute → deny and also defends against dialects whose `CONCAT` treats
`NULL` as `''` (which would otherwise turn the pattern into match-everything `'%%'`).

### NULL columns follow CEL error semantics — even under negation

Cerbos denies a check when the condition hits a CEL evaluation error (typically a
null/missing attribute where no null overload exists). The adapter mirrors this with SQL
three-valued logic, including the places where naive translations leak under `NOT`:

- Collection macros are tri-state: `R.attr.items.all(t, t.qty > 0)` with an item whose
  `qty` is NULL yields UNKNOWN (row excluded under both `all(...)` and `!all(...)`),
  matching CEL's error-absorption rules — `exists` is still true if *any* element
  matches, `all` is still false if *any* element fails, `exists_one` errors on any
  unknown element. This costs two extra correlated `COUNT` subqueries per macro.
- Ternaries carry a third arm that is UNKNOWN exactly when the condition column is NULL,
  so `!(ternary...)` cannot flip a null-condition row to included.
- `ne` against an unsolvable string concatenation reduces to `IS NOT NULL`, not `TRUE`.

### Division by a column is guarded with `NULLIF` — zero divisors deny

CEL double division by zero yields ±Infinity (a defined result that a comparison could
turn into ALLOW); SQL raises an error that would abort the whole query. The adapter
divides by `NULLIF(divisor, 0)`, so zero-divisor rows become UNKNOWN and are excluded.
This is deliberately under-inclusive: a policy relying on `x / 0 == Infinity` semantics
will deny those rows here while a per-resource `check` would allow them. Constant
arithmetic (including `0/0 → NaN`) is folded in Java with full IEEE fidelity.

### Ternary with a `NULL` condition column excludes the row

A comparison wrapping a ternary is rewritten as
`(cond AND cmp(then, v)) OR (NOT cond AND cmp(else, v))`. Under SQL three-valued
logic a `NULL` condition column makes both arms unknown, so the row matches
neither branch. This is deliberate: in CEL a null/missing ternary condition is
an evaluation error, and Cerbos denies the check — the SQL filter and a
per-resource `check` call agree. It differs from what a SQL `CASE WHEN` would
do (fall through to the `ELSE` branch).

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

### MySQL / MariaDB `LIKE` backslash escaping

`contains` / `startsWith` / `endsWith` translate to `cb.like(path, pattern, '\\')` — the
adapter escapes `%`, `_`, and `\` in the user value and declares `\` as the SQL escape
character (the three-arg `LIKE … ESCAPE '\'` form). On most databases this is exact and
unambiguous.

MySQL and MariaDB are the exception: by default they **also** treat `\` as an escape
character *inside the string literal itself*, so the escape is effectively applied twice and
a literal backslash in the attribute value can match incorrectly. If your data contains
backslashes and you target MySQL/MariaDB, either:

- run the server with [`NO_BACKSLASH_ESCAPES`](https://dev.mysql.com/doc/refman/en/sql-mode.html#sqlmode_no_backslash_escapes)
  enabled (Hibernate 6.4+ emits standard-conforming escaping in that mode), or
- register an `OperatorFunction` override for `contains`/`startsWith`/`endsWith` that builds
  the `LIKE` predicate with an escape character your dialect handles cleanly.

Values without backslashes are unaffected.

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
evaluation. Three suites with distinct roles:

| Suite | Role |
|---|---|
| `SpringDataQueryPlanAdapterTest` | Unit: protobuf operands built by hand, executed against H2 to catch translation/mapping errors and pin error messages |
| `SpringDataIntegrationTest` | Integration: the shared `/policies/resource.yaml` conformance actions planned by a live PDP, results asserted against seeded rows |
| `AdversarialConformanceTest` | Differential: hostile policy shapes + hostile seed data (LIKE metacharacters, unicode, empty collections, value-first operand order); the adapter's filtered rows are compared per action against an oracle computed from the PDP's own `check` API — no hand-computed expectations, so any semantic divergence between the generated SQL and Cerbos's evaluation fails mechanically |

Two run modes are supported:

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
