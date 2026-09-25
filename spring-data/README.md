# cerbos-spring-data

> **Alpha release — `0.1.0-alpha.1`.** Operator coverage is stable; the API and the field/relation
> mapping shapes may still change before `1.0`. Feedback welcome.

[Cerbos](https://cerbos.dev) query plan adapter for [Spring Data JPA](https://spring.io/projects/spring-data-jpa).
Converts a Cerbos `PlanResources` response into a `org.springframework.data.jpa.domain.Specification<T>`
you can pass straight to a `JpaSpecificationExecutor`.

## Install

The adapter is **not published to Maven Central yet**. Build it and install it into your local
Maven repository first, from a checkout of this repository:

```bash
cd spring-data
./gradlew publishToMavenLocal   # needs JDK 17+
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
  JPA 4 / Hibernate 7 (the Spring Boot 4 pair) — see [Build](#build).
- String columns the mapping references need a byte-exact collation — see
  [Database collation requirements](#database-collation-requirements).

## Quick start

```java
import dev.cerbos.queryplan.springdata.AttributeMapping;
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

## Field mapping

Map each `request.resource.attr.<name>` to a JPA path or an association:

| Helper | Use for |
|---|---|
| `AttributeMapping.field("aPath")` | A column, an `@Embedded` dotted path (`"details.width"`), or a to-one path (`"owner.id"`) |
| `AttributeMapping.field("aPath", NullAttributeRepresentation.EXPLICIT)` | Same, declaring the attribute's NULL convention (see [below](#declare-the-convention-per-attribute)) |
| `AttributeMapping.relation("tags")` | `@ElementCollection<String>` — the elements are the values |
| `AttributeMapping.relation("tags", "name")` | `@OneToMany<Tag>` where `name` stands in for the element in `in` / `hasIntersection` |
| `AttributeMapping.relation("tags", Map.of("name", field("name")))` | `@OneToMany<Tag>` with member fields for lambda bodies (`t.name`); nested values may be `relation(...)` for multi-hop chains |
| `AttributeMapping.relation("tags", "name", Map.of(...))` | Both a default member field and nested member fields |
| `AttributeMapping.relation(...).withPositionField("position")` | Any relation, declaring the member field that holds each element's zero-based index in the list sent to `check()`, so positional reads (`R.attr.tags[0]`) translate |

`relation(...)` names a JPA association, so the correlated subquery is a criteria association join
and Hibernate applies that association's own `@SQLRestriction` and discriminator (see
[Mapping hazards](#mapping-hazards)). A dotted `field` path through a to-one association is a LEFT
join.

A plan variable the mapping does not name, or names the wrong way round for its operator (a
`relation` compared as a scalar, a `field` walked by a macro), throws `UnmappedAttributeException`
rather than guessing a column.

### Declaring the translation: `Options`

Every setting lives in one immutable record, `SpringDataQueryPlanAdapter.Options`. The positional
overloads (`mapper`, `overrides`, `nullAttributeRepresentation`) are shorthand for it.

```java
import dev.cerbos.queryplan.springdata.SpringDataQueryPlanAdapter.Options;

Options options = Options.of(MAPPING)
    .withOperatorOverrides(overrides)                                     // Map<String, OperatorFunction>
    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED) // default EXPLICIT
    .withMaxMacroDepth(8);                                                // default 5

Specification<Contact> allowed = SpringDataQueryPlanAdapter.toSpecification(plan, options);
```

| Option | Default | See |
|---|---|---|
| mapping | — (required) | [Field mapping](#field-mapping) |
| `withOperatorOverrides` | none | [Operator overrides](#operator-overrides) |
| `withNullAttributeRepresentation` | `EXPLICIT` | [NULL attribute representation](#null-attribute-representation) |
| `withMaxMacroDepth` | 5, or the `dev.cerbos.queryplan.springdata.maxMacroDepth` system property | [Nested collection macros](#nested-collection-macros-multiply-correlated-subqueries--depth-is-bounded) |

Collections are copied on construction and each `with…` returns a new instance, so build an
`Options` once and share it across threads.

### Handling refusals

A shape the adapter cannot translate throws instead of emitting a best-effort filter. There are
three exception types, all extending `IllegalArgumentException`:

| Exception | Meaning | What to do |
|---|---|---|
| `UnsupportedPlanShapeException` | Well-formed plan the Criteria API cannot express faithfully — a regex `LIKE` cannot spell, casts, list index without a declared position field, `mod` outside `int()` of an `Integer` column, `except()` outside `size()` and short list equality, macros nested past the depth bound | Rewrite the policy, register an `OperatorFunction` where [Not yet supported](#not-yet-supported) says one reaches, or use per-row `check()` |
| `UnmappedAttributeException` | The mapping doesn't cover the plan — unmapped variable, `Relation` where a scalar is needed (or vice versa), a temporal column type that doesn't pin an instant | Change the mapping |
| `MalformedPlanException` | The plan breaks the planner's wire contract — wrong arity, lambda without a variable, conditional plan without a condition | Hand-built plan, or an upstream bug to report |

A conditional plan is translated when the Specification is first evaluated, so that is where these
are raised — except the `NullAttributeRepresentation.OMITTED` check, which runs in
`toSpecification`. The bulk-delete guard is a separate `UnsupportedOperationException`.

### Operator overrides

An `OperatorFunction` replaces the translation of one operator:

```java
Map<String, OperatorFunction> overrides = Map.of(
    "matches", (cb, field, value) ->
        cb.isTrue(cb.function("regexp_like", Boolean.class, field, cb.literal(value.toString()))));

Specification<Contact> allowed =
    SpringDataQueryPlanAdapter.toSpecification(plan, MAPPING, overrides);
```

An override replaces the adapter's semantics with yours. Database regex dialects are not RE2, so
this one is only correct if every pattern your policies use means the same thing in both. The
conformance corpus does not test overrides.

The signature is `Predicate apply(CriteriaBuilder cb, Expression<?> field, Object value)`. An override
is consulted only where the adapter has resolved a `(field, value)` pair for a top-level operator:

- **Reached:** `eq`/`ne`/`lt`/`gt`/`le`/`ge` (value-first forms under the mirrored name; `add`-folded,
  null-RHS and arithmetic-vs-constant forms included), `string()` over a boolean column (receives the
  column and a `Boolean`) or a string column (the column and the `String`), `contains`/`startsWith`/`endsWith` with a **column** receiver, scalar
  `in`, a bare boolean attribute (as `eq`), unknown leaf operators such as `matches`, and timestamp
  comparisons (value is the parsed `java.time.Instant`, including for column types the default
  rejects). `not` wraps the built predicate, so an override applies under both polarities.
- **Not reached:** operand-level refusals (`mod`, casts, list indexing), every correlated-subquery
  shape (macros, `size(...)`, `in`/`hasIntersection` over a `Relation`, `in(R.attr.x, R.attr.coll)`),
  field-to-field comparisons, constant-receiver string matches (`"a,b".contains(R.attr.x)`), and
  `hasIntersection` over a plain `Field` (built as `path IN (values)` directly).

The `OperatorFunction` Javadoc is the authoritative list.

## NULL attribute representation

`R.attr.x == null` plans the same way however your application represents a NULL column in the
attributes it sends to `check()`, so tell the adapter which convention you use:

| Attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
|---|---|---|
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (missing attribute) | selects it — **over-grants** |

The default, `EXPLICIT`, translates to `IS NULL`. If you omit attributes for NULL columns, pass
`OMITTED`: every null comparison operand against an undeclared attribute then throws
`UnsupportedPlanShapeException` instead of emitting a filter that returns denied rows. This check is
eager (in `toSpecification`), and it rejects every such null operand, including aligned ones like
`x != null`, because a leaf can't tell whether an enclosing `not` will flip it
([#302](https://github.com/cerbos/query-plan-adapters/issues/302)). Declare `OMITTED` on the
attribute itself (below) and `eq`/`ne` against a bare null translate instead.

```java
SpringDataQueryPlanAdapter.toSpecification(plan,
    Options.of(mapping).withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED));
// or positionally:
SpringDataQueryPlanAdapter.toSpecification(plan, mapping, Map.of(), NullAttributeRepresentation.OMITTED);
```

### Declare the convention per attribute

One policy suite can use both conventions, so you can declare it per attribute; the call-level
option covers only undeclared attributes:

```java
Map<String, AttributeMapping> mapping = Map.of(
    // sent as an explicit null when the column is NULL
    "request.resource.attr.owner",
    AttributeMapping.field("ownerId", NullAttributeRepresentation.EXPLICIT),
    // undeclared: the call-level default applies
    "request.resource.attr.department", AttributeMapping.field("department"));
```

Declaring `EXPLICIT` asserts the column can be NULL **and** a NULL reaches `check()` as an explicit
null. The equality family (`eq`, `ne`, `in`) over it then never renders as SQL UNKNOWN, so
`null != "x"` includes the row as CEL does. Ordering and string operators are unchanged (a null
receiver is a CEL error, which denies like UNKNOWN). Undeclared attributes keep the old rendering,
where `!=` against a constant under-grants NULL rows.

Declaring `OMITTED` asserts a NULL reaches `check()` as a missing attribute. `x == null` and
`x != null` against it then translate three-valued: a NULL column is UNKNOWN under both polarities,
as CEL's missing-attribute error denies under both, so `x == null` selects no row and `x != null`
selects exactly the non-NULL ones. Other null operands (a null in an `in` list, say) still throw, as
does a registered override for `eq`/`ne`, which would receive the null.

A field-to-field `==`/`!=` between an `EXPLICIT` attribute and an undeclared or `OMITTED` one is
definite for a NULL on the explicit side and UNKNOWN for a NULL on the other. See [#308](https://github.com/cerbos/query-plan-adapters/issues/308)
and [ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

## Database collation requirements

> **⚠️ Hard requirement: every string column referenced by an `AttributeMapping` MUST use a
> byte-exact collation.**
>
> - MySQL: `utf8mb4_0900_bin` (MySQL 8.0.17+). Case-sensitive is not enough.
> - SQL Server: a `*_CS_AS` collation (e.g. `Latin1_General_100_CS_AS`).
> - PostgreSQL: equality is exact by default, but string ordering (`<`, `>`, `<=`, `>=`) follows
>   the column collation, and a linguistic default such as `en_US.utf8` does not order by code
>   point (`'OneSet' < 'b'` is false, `'One' > 'a'` is true). Use `"C"` for columns compared by ordering, and avoid
>   case-insensitive behaviour (nondeterministic ICU collations, `citext`).
> - H2, Oracle: safe by default, unless you opt into case-insensitive behaviour. H2 orders strings
>   by UTF-16 code unit (Java's `String.compareTo`), not by code point as CEL does, so on every
>   store the adapter refuses an ordering against a literal holding a character at or above
>   U+D800 (an astral character, or U+E000–U+FFFF): only such a literal can be ordered
>   differently by the two.

CEL string comparison is exact: `R.attr.department == "finance"` denies a row holding `"Finance"`.
The adapter emits string predicates without collation control, so the column collation decides.
MySQL's default `utf8mb4_0900_ai_ci` and SQL Server's CI defaults are case- and accent-insensitive,
so `WHERE department = 'finance'` returns the `'Finance'` row the PDP denied — **a silent
authorization over-grant**. Role and tenancy checks (`'admin'` vs `'Admin'`, hierarchy prefixes
`LIKE 'a:b:%'` vs `'A:B:x'`) are the highest-risk shapes.

**Case-sensitive is not byte-exact.** `utf8mb4_0900_as_cs` still gives default-ignorable code points
like SOFT HYPHEN (U+00AD) no weight, so `'o­ne' = 'one'` is TRUE: `==`/`in` over-grant and `!=`
under-grants ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)). `utf8mb4_bin` is
byte-exact but PAD SPACE (`'a' = 'a '` is TRUE). `utf8mb4_0900_bin` is both byte-exact and NO PAD.

Affected predicates: `eq`/`ne`, string `lt`/`gt`/`le`/`ge`, `contains`/`startsWith`/`endsWith`
(including constant-receiver and field-to-field forms), `in`, `hasIntersection` (direct and
`map(...)`), `hierarchy(...)`, `matches(...)`, and `string()` and `+` comparisons.
`OperatorFunction` overrides can't cover all of them (for example `hasIntersection` over a plain
field never consults one), so fix the collation in the schema.

`string()` over a boolean or numeric column has no string predicate in SQL — the constant is
inverted in Java and the column compared with the result — because a literal-vs-literal comparison
would use the **connection** collation, which MySQL Connector/J sets to `utf8mb4_0900_ai_ci` by
default.

CI runs the conformance suite on PostgreSQL and MySQL with mixed-case and soft-hyphen (`h6`) seeds;
the PostgreSQL database is initialised with `--lc-collate=C` and the MySQL schema uses
`utf8mb4_0900_bin`. Reproduce locally:

```bash
ADAPTER_TEST_DB=postgres ./gradlew test --tests AdversarialConformanceTest   # passes
ADAPTER_TEST_DB=mysql    ./gradlew test --tests AdversarialConformanceTest   # passes (utf8mb4_0900_bin)

# MySQL's DEFAULT collation — FAILS, reproducing the over-grant
ADAPTER_TEST_DB=mysql ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_ai_ci \
  ./gradlew test --tests AdversarialConformanceTest
# Case-sensitive but not byte-exact — FAILS on seed h6
ADAPTER_TEST_DB=mysql ADAPTER_TEST_MYSQL_COLLATION=utf8mb4_0900_as_cs \
  ./gradlew test --tests AdversarialConformanceTest
# PostgreSQL's linguistic glibc default — FAILS both comparison/*/string-code-point-order cases (#489)
ADAPTER_TEST_DB=postgres ADAPTER_TEST_POSTGRES_INITDB_ARGS=--lc-collate=en_US.utf8 \
  ./gradlew test --tests AdversarialConformanceTest
```

## Supported operators

| Cerbos operator | JPA Criteria translation |
|---|---|
| `and` / `or` / `not` | `cb.and` / `cb.or` / `cb.not` |
| `eq` / `ne` | `cb.equal` / `cb.notEqual`; `null` RHS becomes `isNull` / `isNotNull` (`R.attr.x != null` arrives as `ne` against null) |
| `lt` / `gt` / `le` / `ge` | `cb.lessThan` / `greaterThan` / `lessThanOrEqualTo` / `greaterThanOrEqualTo` |
| Value-first (`5 < R.attr.x`) | Normalized field-first with the operator mirrored |
| `in` | `path.in(values)`, or correlated `EXISTS` over a relation |
| `R.attr.x in list.map(t, body)` over a literal list | `size(list.filter(t, R.attr.x == body)) > 0` as a strict count, UNKNOWN when any element's body errors, as CEL's `map` does |
| `in(R.attr.x, R.attr.coll)` | Correlated `EXISTS` comparing member to scalar; a `NULL` scalar matches a `NULL` member (CEL `null in [..., null]` is true) |
| `contains` / `startsWith` / `endsWith` | `cb.like` with `\`, `%`, `_`, `[` escaped; also the constant-receiver form (`"a,b".contains(R.attr.x)`) |
| Field-to-field `contains` / `startsWith` / `endsWith` | `LIKE` over a `REPLACE`-escaped column pattern with a NULL-needle guard |
| Field-to-field comparisons | `cb.equal(pathA, pathB)` and friends, including inside lambdas |
| `R.attr.coll == ["x"]`, `== []`, and `!=`, over a relation or its `map(t, t.f)` projection | `size(coll) == 1 && coll.exists(e, e == "x")` (`size(coll) == 0`): a list of at most one element has no order to compare. Longer lists need `withPositionField`, and are `size(coll) == n && coll[0] == v0 && ...` |
| `size(a.except(b))`, `a.except(b) == []` / `== [v]` | `size(a.filter(x, !(x in b)))`, as Cerbos keeps each element of `a` that `b` does not contain; equality with `[v]` is a difference of one element that equals `v` |
| `R.attr.list[i] <op> v`, `R.attr.list[i].f <op> v` (relation with `withPositionField`) | `list.exists(e, e.position = i && e <op> v)` as a three-valued score subquery; UNKNOWN when no element sits at `i` or `i` is not a non-negative integer, as CEL errors |
| `hasIntersection(coll, [...])`, `hasIntersection(coll.map(x, x.f), [...])` | Correlated `EXISTS` with `IN` (projected for `map`) |
| `size(coll) > 0` / `>= 1`; `== 0` / `<= 0` / `< 1`; `<op> N` | `EXISTS`; `NOT EXISTS`; correlated `COUNT` |
| `size(coll.filter(x, pred)) <op> N` | Correlated strict count, NULL-poisoned when any element body is undetermined; over a literal list, the same per-element `CASE` sum as `exists_one` |
| `size(string)` | `cb.length(column)` (see [Gotchas](#sizestring-counts-differently-for-astral-characters)) |
| `exists` / `all` / `filter` | One correlated aggregate scoring subquery with CEL's three-valued truth table |
| `exists_one` | Correlated strict count `= 1`, NULL-poisoned; over a literal list (a principal attribute the planner cannot unroll), a sum of per-element `CASE` terms, NULL when any element's body is UNKNOWN |
| Multi-hop relation chains (`R.attr.categories.subCategories`) | Correlated subquery through every hop; the chain is the flattened union of tail elements |
| Ternary (`cond ? a : b`) | `(cond AND cmp(a, v)) OR (NOT cond AND cmp(b, v))`, UNKNOWN when `cond` is NULL |
| `int(R.attr.d) <op> c` (`d` a `Double` or `Integer` column) | Solved for the column, since CEL truncates toward zero where SQL `CAST` rounds: `int(d) == 0` is `-1 < d < 1`; UNKNOWN for a NULL or out-of-int64-range column, as CEL errors |
| `int(R.attr.s) <op> c` (`s` a `String` column) | No `CAST`: `s` must be Go's base-10 integer (`[+-]?[0-9]+`, checked by `REPLACE`-ing the digits away), and its value is compared by sign, then by the length and lexical order of its digits with leading zeros trimmed; UNKNOWN for a malformed, out-of-int64-range or NULL string, as CEL errors |
| `int(R.attr.n) % k` (`n` an `Integer` column) | `MOD(n, k)`, which truncates toward zero as CEL does (`-5 % 2` is `-1`); a zero divisor is UNKNOWN |
| Arithmetic (`add`/`sub`/`mult`/`div`) in comparisons | `cb.sum`/`diff`/`prod`/`quot` in double space; a zero column divisor split out and compared as CEL's NaN / ±Infinity (see [Gotchas](#division-by-a-column-zero-divisors-compare-as-cels-nan-and-infinities)) |
| `eq(field, add(c1, c2))`, `eq(value, add(c, field))` | Constant fold; solve for `field` (string prefix/suffix strip, numeric subtract), unsolvable → `1=0` / `1=1` |
| String `+` in comparisons (`R.attr.a == "p:" + R.id`, `R.attr.a + R.attr.b == "x"`) | `cb.concat` when a string constant or `String` column sits under the `add`, any other leaf refused; UNKNOWN when a concatenated column is NULL |
| `timestamp(R.attr.t) <op> now() - duration(...)` | Temporal comparison for all six operators, both operand orders; column must be `Instant` or `OffsetDateTime`; NULL excluded (see [Gotchas](#timestamp-comparisons-plan-time-now-and-only-unambiguous-column-types)) |
| `string(R.attr.x) == "text"` / `!=` | By column type: a `String` column is compared as it stands; a `Boolean` column is `col = true`, `col = false`, or no row for any other constant; a `Double`/`Integer`/`Long` column is compared with the one double CEL renders as `text` (Go's shortest `%g`: `"-0.6"`, `"1e+06"`), or no row when none does. NULL excluded under both polarities |
| `hierarchy(...).overlaps / ancestorOf / descendentOf` | `IN` over ancestor prefixes; `LIKE 'a:b:%'` for descendants. With an empty delimiter (one segment per character) between a column and a constant: `IN` over character prefixes, `''` included, and `LIKE 'ab_%'` for strict descendants |
| A scalar against a list or map literal (`R.attr.s == {"a": 1}`, `R.attr.s in [["x"]]`) | Decided: CEL equality across types is false, so `==` matches no row and `!=` every present one; the planner's `list(...)` / `struct(...)` literal expressions are folded to constants first |
| `R.attr.s.matches("re")` (and `== true` / `== false`) | Where the pattern's language is exact in `LIKE`: each top-level alternative a finite set of strings (literals, escapes, classes, `\d \w \s`, POSIX classes, groups, `|`, bounded repetition, a leading `(?i)` over ASCII) anchored or not, as `=` / `IN` / `LIKE 's%'` / `'%s'` / `'%s%'`; `^...$` with `.*` / `.+` between finite parts, as `LIKE` plus `NOT LIKE '%\n%'` (RE2's `.` excludes a newline); `^[set]*$` / `^[set]+$` as `REPLACE` of every member leaving `''`. A pattern RE2 rejects (lookaround, backreference) is UNKNOWN, as CEL errors. A registered `matches` override takes precedence |
| Bare boolean variable | `cb.equal(path, true)` |

## Not yet supported

These throw `UnsupportedPlanShapeException` naming the operator — except the ambiguous-column
timestamp row, which is `UnmappedAttributeException` because a different mapping fixes it, and the
map-valued attribute row, which names an attribute no mapping can declare.
**Overridable: no** means the refusal happens while resolving an operand, before any override is
consulted.

| Construct | Example CEL | Overridable | Notes |
|---|---|---|---|
| `mod` other than over `int()` of an `Integer` column | `int(R.attr.aDouble) % 2 == 0` | no | CEL `%` is int-only and attribute numbers are doubles, so a bare `R.attr.x % 2` denies every row, and `int()` over a double truncates where SQL `CAST` rounds |
| Regex match `LIKE` cannot spell exactly | `R.attr.aString.matches("^[^x]+")`, `matches("a.b")` | yes (`matches`) | No portable RE2 predicate; override per dialect (`regexp_like`, `~`, `REGEXP`) if its regex means the same as RE2 for your patterns |
| Ordering a string against a literal holding a character at or above U+D800 | `R.attr.s < "h\u00e9llo\uFFFD"` | yes | CEL orders strings by code point; H2 compares UTF-16 code units, which put a surrogate pair before U+E000–U+FFFF |
| A macro over a map-valued attribute (a to-one relation or embedded object as a whole) | `R.attr.parent.exists(k, k == "inner")` | no | CEL ranges over the map's keys, and a JPA row has no key set; the whole object is not a mappable attribute, so this is `UnmappedAttributeException` |
| List indexing without a declared order | `R.attr.tags[0] == "x"` | no | JPA collections are unordered; declare `withPositionField(...)` on the relation |
| Type casts (`double()`, `timestamp()` over a string, `int()` other than over a `Double`/`Integer`/`String` column compared with a number, `string()` other than `==`/`!=` a string constant over a string, boolean or numeric column) | `int(R.attr.aString) > 0` | no | No portable `CAST` in Criteria; `string(x) == "0"`, `"-0"`, `"NaN"` and `"±Inf"` are refused too, since SQL cannot tell the value CEL renders that way from its neighbours |
| `eq(map(...), [...])` | `R.attr.tags.map(t, t.id) == ["a", "b"]` | no | Use `hasIntersection(map(...), [...])` |
| Timestamp on an ambiguous column type | `timestamp(R.attr.createdAt) < now() - duration("24h")`, `createdAt` a `LocalDateTime`/`Date`/`String` | yes (the comparison operator) | These types don't pin an absolute instant; the override receives the parsed `Instant` |
| Other timestamp shapes | `timestamp(R.attr.a) < timestamp(R.attr.b)`, `timestamp()` in arithmetic | no | Only `timestamp(field)` vs constant is translated |
| `eq`/`ne` between a relation without a declared position field and a list constant of two or more elements | `R.attr.tags == ["a", "b"]` | no | CEL list equality is ordered and a JPA collection has none; declare `withPositionField(...)`, or use `in`/`hasIntersection` |
| `except` in boolean position, compared with a list of two or more elements, or removing a computed list of two or more | `R.attr.tags.except(["a"]) == ["b", "c"]` | no | A list difference in SQL has no order to compare; `size(...)` of one and equality with at most one element translate |

## Conformance contract

The adapter replays the shared [conformance corpus](../conformance/README.md): for each recorded
plan of Cerbos PDP 0.55.0 and 0.54.0, it translates the plan, runs the query against 42 seed rows on
H2, PostgreSQL and MySQL, and compares the returned ids with the `check()` decisions the PDP
recorded. No PDP runs in the test. Results for the current PDP (0.55.0), where the total is every
golden case of that tier; a case marked as a planner divergence is skipped, and counts toward the
total but not as passed:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 76 / 80 |
| adversarial | 275 / 314 |

Every case that does not pass is listed with its reason in
[`conformance-ledger.json`](conformance-ledger.json): 36 are `unsupported`, where the adapter
throws one of its refusal types (`UnsupportedPlanShapeException`, or `UnmappedAttributeException`
when the plan reads an attribute the mapping does not declare) rather than emit a filter. Four
extended cases and three adversarial cases are planner divergences the corpus skips: `null/has/missing-attribute` and
`null/has/composed-with-comparison`, where the planner folds `has()` to true by design while
`check()` receives the omitted attribute as absent (see
[Gotchas](#has-does-not-filter-a-null-column--write--null-instead)),
`arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated`, where the
planner drops the int type of the literal in `R.attr.x + 1` while `check()` has no double + int
overload and denies every row (write `1.0`), and
three `composition/*` cases whose DENY condition reads `aNumber`, which j2 lacks: the plan's
`not(...)` of it denies j2, while `check()` receives `aNumber` as absent and treats the erroring
DENY as not matching ([#530](https://github.com/cerbos/query-plan-adapters/issues/530)).

Other guarantees:

- CEL type errors and missing-attribute errors survive negation. Numeric fields are never coerced to
  strings, and NaN ordering stays unknown rather than becoming a negatable false.
- Constant NaN ordering follows Cerbos 0.55: an unordered comparison is false, so its negation is
  true (in 0.54 it was an error and stayed denied under negation).
- Bare comparisons between temporal columns throw: the database compares instants while CEL compares
  the attribute strings. Use `timestamp()` for instant comparison.
- An `eq`/`ne` null comparison against an attribute declared
  `AttributeMapping.field(path, NullAttributeRepresentation.OMITTED)` is UNKNOWN for a NULL column
  under both polarities; declared `EXPLICIT`, `eq`, `ne` and `in` include NULL rows where CEL's null
  value says they should (cerbos/query-plan-adapters#302, #308).

## Mapping hazards

The conformance contract proves the *plan* side. The other half is the *mapping*: **the rows the
subquery reads must be the rows the application put into the resource attributes.** The shared
corpus catalogues six ways that breaks, and every adapter records a position on each.

This adapter builds an **ORM-association subquery**: `AttributeMapping.relation("tags")` is reached
with `correlate(root).join("tags")`, so everything Hibernate applies to that association applies
here too.

**There is no option to declare a store-side predicate on the mapping** (unlike drizzle, ent, pgx and
prisma). Hibernate already applies your association filters; declaring one again would silently
drop rows the PDP permits. **Do not re-declare them.**

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Reproduced by Hibernate** | `@SQLRestriction` (`@Where` before 6.3) on the collection applies however it is reached, explicit join included. `@Filter` applies only while *enabled on the session* — enable it on the session that runs the `Specification`; note `EntityManager.find()` ignores filters |
| Default scope on the target model | **Reproduced by Hibernate** | `@SQLRestriction` on the target entity applies to this subquery too. A soft-delete done as a repository convention is not — move it to `@SQLRestriction` |
| Subtype discrimination | **Reproduced by Hibernate** | An association typed to a `@DiscriminatorValue` subclass is restricted to it; one typed to the base type sees siblings, as the application does |
| To-one relation used as a collection | **Caller-owned** | A `@OneToOne(mappedBy = …)` whose foreign key has no unique constraint. Add the constraint |
| Composite association key | **Reproduced by JPA** | The mapping names the association, never its columns; Hibernate resolves `@JoinColumns` |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain`, `relation/or/two-hops-or-collection-exists` and siblings) | None — a missing parent is UNKNOWN under both polarities ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)); a dotted to-one `jpaPath` is a LEFT join so a disjunction's other branch still holds |

One more is this adapter's own. `withPositionField` asserts that the member field holds exactly the
element's index in the list sent to `check()`: `0` first, no gaps or duplicates. A column that
drifts from that order silently reads the wrong element, so derive the attribute list and the column
from one ordering.

The "Reproduced by Hibernate" rows are claims about Hibernate — see the
[Hibernate user guide](https://docs.jboss.org/hibernate/orm/current/userguide/html_single/Hibernate_User_Guide.html#pc-where)
and Hibernate's `OneToManySQLRestrictionTests`, and verify against the version you run. The adapter
is only as correct as the mapping is honest: keep one definition of what an association contains
for both the query side and the code that builds resource attributes.

## Gotchas

### `size(string)` counts differently for astral characters

CEL counts code points; SQL `LENGTH()` counts UTF-16 units (H2), characters (PostgreSQL) or bytes
(some MySQL collations). They differ only outside the BMP: `size("héllo🚀")` is 6 in CEL but
`LENGTH` may say 7. Keep length thresholds away from such values, or avoid `size(string)` over
data with emoji.

### Attribute arithmetic is double arithmetic — use double literals in policies

Attribute numbers are doubles in CEL, and there is no int/double cross overload: `R.attr.n + 1` is
an error in `check()` (every row denied) while `R.attr.n + 1.0` works. Both plan identically, so the
adapter translates the double reading (`/` is true division). Write `1.0`, `2.0` in policy
arithmetic over attributes.

### MySQL: keeping arithmetic IEEE-faithful

Applies only to **arithmetic inside a comparison** (`R.attr.n * 0.1 == 0.3`). Two MySQL defaults
evaluate it as exact decimal, where `3 * 0.1 == 0.3` is TRUE but CEL says FALSE — an over-grant:

- Hibernate's `MySQLDialect` casts to `decimal(53,20)`.
- Connector/J's default client-side prepared statements inline doubles as `DECIMAL` literals.

The adapter ships `MySqlDoubleCastFunctionContributor` (auto-discovered via `META-INF/services`),
which on MySQL 8.0.17+ renders `cast(col as double)` for every arithmetic column. That keeps both
prepared-statement modes correct with **no JDBC settings**. H2 and PostgreSQL are unaffected.

Set `useServerPrepStmts=true` in the JDBC URL if any of these apply:

- MySQL older than 8.0.17.
- MariaDB.
- A non-Hibernate JPA provider (the adapter falls back to `cast(col as float(53))`).
- `hibernate.boot.allow_jdbc_metadata_access=false` with only `hibernate.dialect` set (the
  version-gated registration is skipped).

CI's MySQL leg runs client-side mode so `arithmetic/multiply/inexact-fraction-equals` catches a regression;
`ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS=true` runs it server-side. Both must pass.

### Timestamp comparisons: plan-time `now()`, and only unambiguous column types

The planner folds `now() - duration("24h")` into a constant instant when it plans, so:

- The cutoff is frozen per plan. Re-plan per request if the window must track the clock; don't cache
  the Specification.
- The column must be `java.time.Instant` or `java.time.OffsetDateTime` (Hibernate 6 stores both
  UTC-normalized). `LocalDateTime`, `java.util.Date` and `String` throw; if you know their zone
  semantics, register an `OperatorFunction` for the comparison operator (it receives an `Instant`).
- On MySQL these map to `TIMESTAMP`, which ends at 2038-01-19; use a `DATETIME(6)` column for later
  instants.

### Field-to-field string matching builds its pattern with `REPLACE`

`R.attr.a.contains(R.attr.b)` becomes `a LIKE CONCAT('%', <b escaped by nested REPLACE>, '%') ESCAPE '\'`
(`REPLACE` is portable across H2, PostgreSQL, MySQL, Oracle and SQL Server). A NULL needle excludes
the row under both polarities, matching CEL, and prevents `CONCAT` dialects that treat NULL as `''`
from matching everything.

### NULL columns follow CEL error semantics — even under negation

A CEL evaluation error denies, and the adapter reproduces this with SQL three-valued logic:

- Collection macros are tri-state: `items.all(t, t.qty > 0)` with a NULL `qty` is UNKNOWN under both
  `all` and `!all`; `exists` is still true if any element matches, `all` still false if any fails,
  `exists_one` errors on any unknown element.
- A ternary with a NULL condition column is UNKNOWN, so `!(ternary)` can't include the row.
- `ne` against an unsolvable string concatenation reduces to `IS NOT NULL`, not `TRUE`.

### `has(...)` does not filter a NULL column — write `!= null` instead

An attribute the plan request omits is unknown to the planner, which assumes the data layer supplies
it: for a mapped column, the column exists and only its value is open. So the planner reads
`has(R.attr.aOptionalString)` as the guard for the access beside it and folds it to true by design.
Alone it plans as `KIND_ALWAYS_ALLOWED`; composed, as in
`has(R.attr.aOptionalString) && R.attr.aNumber > 0`, only `R.attr.aNumber > 0` reaches the adapter,
so a row whose column is NULL is not excluded. The corpus's per-row `check()` calls send the same
omission as absent, where `has()` is false, so the two calls answer different questions: the corpus
declares both forms, `null/has/missing-attribute` and `null/has/composed-with-comparison`, as
planner divergences, and the conformance suite skips them for the PDP versions they name.

**Workaround (PDP-verified):**

```
R.attr.aOptionalString != null
```

This plans as `ne(variable, null)` → `a_optional_string IS NOT NULL`, and `check()` agrees in every
case (missing → deny, explicit `null` → deny, present → allow). `has(R.attr.x) && R.attr.x != null`
produces the same plan, so it is a safe drop-in edit. The planner cannot make this rewrite for you:
`has()` is true for an attribute explicitly set to `null`, where `!= null` is false.

### Nested collection macros multiply correlated subqueries — depth is bounded

Each macro (`exists`/`exists_one`/`all`/`filter`/`size(filter(...))`) is one correlated subquery, but
its body is translated once per polarity, so a depth-`d` `exists` chain emits `2^d − 1` subqueries
(`3^d` for `exists_one` / `size(filter(...))`). Measured on H2 (`MacroNestingBenchmarkTest`, ~3 000
rows):

| depth | correlated subqueries | translate | execute |
|-------|-----------------------|-----------|---------|
| 1     | 1                     | ~0.2 ms   | ~1.7 ms |
| 2     | 3                     | ~0.3 ms   | ~2.2 ms |
| 3     | 7                     | ~0.6 ms   | ~2.8 ms |
| 4     | 15                    | ~1.3 ms   | ~5.9 ms |

Depth is capped at **5** by default; deeper plans throw `UnsupportedPlanShapeException`. Literal-list
folds count as levels too. The cap counts macro nesting, not expression size. To raise it:

```java
Options.of(MAPPING).withMaxMacroDepth(8)
```

```
-Ddev.cerbos.queryplan.springdata.maxMacroDepth=8
```

`Options` wins over the property, which wins over the default. The property is read per translation
and must be a positive integer (otherwise a plain `IllegalArgumentException`).

### Division by a column: zero divisors compare as CEL's NaN and infinities

CEL `x / 0` is ±Infinity and `0 / 0` is NaN; SQL errors. A comparison against a division by a
column is split on the divisor: where it is zero, the NaN or infinity is compared in Java (so
`a / a != 2.0` allows a zero row, as CEL does), and elsewhere the division runs in SQL over
`NULLIF(divisor, 0)`. Arithmetic around the division is folded the same way when its other leaves
are constants (`a / a + 1.0 > 1.0`); arithmetic that also reads another column, or holds a second
such division, throws `UnsupportedPlanShapeException`, since SQL has no value that carries NaN
through it. Constant arithmetic (including `0/0 → NaN`) is folded in Java with IEEE semantics.

A non-zero dividend over `-0.0` is `-Infinity`, over `0.0` `+Infinity`, and SQL cannot read the
sign of a stored zero (`-0.0 = 0.0` holds). So when the divisor is a `Double` column or an
expression, and the two infinities decide the comparison differently (`a / d > 0.0`), it throws
`UnsupportedPlanShapeException`. An integral column divisor never holds `-0.0`, and `a / a` is NaN
at zero whatever the sign, so both still translate.

### Ternary with a `NULL` condition column excludes the row

The rewrite `(cond AND cmp(then, v)) OR (NOT cond AND cmp(else, v))` matches neither branch when
`cond` is NULL. That matches CEL (a null condition is an error, `check()` denies), unlike SQL
`CASE WHEN`, which would fall through to `ELSE`.

### Pin `protobuf-java` to the cerbos-sdk-java's gencode version

`cerbos-sdk-java` 0.20.1 is generated against `protobuf-java` 4.35.1. An **older** runtime on your
classpath (pinned, or pulled in transitively by gRPC) fails on first decode:

```text
com.google.protobuf.RuntimeVersion$ProtobufRuntimeVersionException:
  Detected incompatible Protobuf Gencode/Runtime versions when loading Principal:
  gencode 4.35.1, runtime 4.31.1. Runtime version cannot be older than the linked gencode version.
```

The adapter publishes a runtime-scope `protobuf-java` pin at the matching version. If your build
still resolves an older one, add it directly (the Spring Boot BOM doesn't manage protobuf):

```kotlin
implementation("com.google.protobuf:protobuf-java:4.35.1")
```

### `@ElementCollection` / `@OneToMany` + `spring.jpa.open-in-view=false`

A `relation(...)` mapping filters through `EXISTS`, but the entity's collection is still lazy.
Serializing the entity after the transaction closes fails with
`failed to lazily initialize a collection of role: …Photo.tags: could not initialize proxy - no Session`.
Fix it the usual JPA way: eager-fetch small collections, map to DTOs inside
`@Transactional(readOnly = true)`, or return DTO projections.

### MySQL / MariaDB `LIKE` backslash escaping

The LIKE family uses `cb.like(path, pattern, '\\')`, escaping `%`, `_`, `\` and `[` and declaring `\`
as the escape character. MySQL and MariaDB **also** treat `\` as an escape inside string literals, so
a literal backslash in a value can match incorrectly. If your data contains backslashes, either:

- enable [`NO_BACKSLASH_ESCAPES`](https://dev.mysql.com/doc/refman/en/sql-mode.html#sqlmode_no_backslash_escapes)
  (Hibernate 6.4+ emits standard escaping in that mode), or
- register an `OperatorFunction` for `contains`/`startsWith`/`endsWith` with an escape your dialect
  handles cleanly.

Values without backslashes are unaffected.

### SQL Server `LIKE` `[` character classes

T-SQL treats `[...]` as a character class even with an `ESCAPE` clause, so `'[SEC]%'` would match
rows starting with S, E or C. The adapter escapes `[` as `\[` in every pattern it generates
(constant LIKE, field-to-field `REPLACE`, hierarchy prefixes); elsewhere the escape is a no-op, which
the H2, PostgreSQL and MySQL legs verify. `]` is left alone — no class can open once `[` is escaped.

## Behaviour changes

- **Breaking:** a comparison against a division by a `Double` column or an expression that may be
  zero (`R.attr.a / R.attr.d > 0.0`) now throws `UnsupportedPlanShapeException` when the sign of
  that zero decides the result, instead of assuming a positive zero and over-granting rows holding
  `-0.0`. See [Division by a column](#division-by-a-column-zero-divisors-compare-as-cels-nan-and-infinities).
- `int(R.attr.x) <op> c` over a `Double`, `Integer` or `String` column now translates instead of
  throwing.
- `matches()` now translates without an override where `LIKE` spells the pattern's RE2 language
  exactly (see [Supported operators](#supported-operators)); a pattern RE2 rejects is UNKNOWN, as
  CEL errors. Other patterns still throw unless a `matches` override is registered, which still
  takes precedence.
- `size(...)` of an `except()` difference, and its equality with a list of at most one element,
  now translate instead of throwing, as does list equality of any length over a relation that
  declares `withPositionField` (and over its `map(t, t.f)` projection).
- The planner's `list(...)` and `struct(...)` literal expressions (lists of lists, maps, lists of
  maps) are folded to constants, and a scalar compared with a list or map literal is decided
  (false for `==`, true for `!=`) instead of throwing. Macros over a literal list of maps
  (`P.attr.items.exists(t, t.name == R.attr.x)`) now translate.
- `AttributeMapping.Relation#withPositionField` declares a relation's element order, and positional
  reads over such a relation (`R.attr.tags[0] == "x"`, `R.attr.tags[0].name == "x"`) now
  translate. `Relation` gains a `positionField` record component; the three-argument constructor
  is kept.
- `exists_one` and `size(filter(...))` over a literal list (a principal attribute longer than the
  planner unrolls), and membership in a `map()` over one, now translate instead of throwing.
- `==`/`!=` between a relation and a list constant of at most one element now translates instead of
  throwing.
- `int(R.attr.n) % k` over an `Integer` column now translates to `MOD` instead of throwing.
- Arithmetic composed on a division by a column (`a / a + 1.0 != 2.0`) now translates when its
  other leaves are constants, instead of throwing: a zero divisor gives CEL's NaN or infinity.
- String concatenation with a column (`R.attr.a == "p:" + R.id`, `R.attr.a + R.attr.b == "x"`,
  under any comparison operator) now translates to `CONCAT` instead of throwing.
- `string()` over a string or numeric column compared with a string constant (`==`/`!=`) now
  translates instead of throwing.
- A field-to-field `==`/`!=` between an `EXPLICIT` attribute and an undeclared or `OMITTED` one now
  translates instead of throwing `UnmappedAttributeException`.
- `==`/`!=` against a bare null on an attribute declared
  `AttributeMapping.field(path, NullAttributeRepresentation.OMITTED)` now translates instead of
  throwing: `x == null` selects no row and `x != null` exactly the non-NULL rows, and a NULL row
  stays out under negation. A call-level `OMITTED` still refuses them on undeclared attributes.
- [#509](https://github.com/cerbos/query-plan-adapters/issues/509): a collection macro nested over
  the relation an enclosing lambda iterates — `tags.exists(t, tags.exists(u, u.name != t.name))` —
  ranges its subquery over a fresh root pinned to the outer row by identity instead of joining off
  the correlated root. On Hibernate 7 the two `tags` joins got the same navigable path, the outer
  `t` rendered as the inner join, and the body compared each tag with itself: an under-grant, and
  an over-grant under negation. Hibernate 6.6 rendered it correctly and now emits one redundant
  self-join. **Breaking:** the same nesting over a relation owned by a collection element (rather
  than by the entity) now throws, since only an entity root can be given a fresh range variable.
- Membership in a **collection** mapping (`x in R.attr.list`, `hasIntersection(R.attr.list, [...])`,
  and `hasIntersection` over a `map()` projection) drops each constant whose type cannot equal the
  element column's before the SQL is built, since CEL's `"2" in [2]` is false. H2 used to coerce
  `'2'` onto a numeric element column and return rows the PDP denies, and Hibernate refused to
  build a Boolean-to-String comparison (`SemanticException`). Both now return what `check()`
  allows.
- Membership against a scalar **field** mapping (`R.attr.aNumber in ["5", 2]`) drops such a constant
  the same way; with none left it is false for a present value and still denied for a missing one.
  H2 used to coerce `'5'` onto the numeric column and return a row the PDP denies — an over-grant
  fix, fewer rows (`type-mismatch/in/number-field-in-mixed-literal-list`).
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
- **Breaking** — macro-depth bound now counts literal-list folds; a plan past the limit throws
  instead of emitting a filter. Default still 5
  ([#457](https://github.com/cerbos/query-plan-adapters/issues/457)).
- `hierarchy(R.attr.scope, "")` (empty delimiter) between a column and a constant is a
  per-character string prefix: `descendentOf` is `LIKE prefix || '_%'`, so the path itself no
  longer matches as the old `LIKE` did (`hierarchy/descendent-of/empty-delimiter` returned a row the
  PDP denies). Any other empty-delimiter shape throws.
- **Breaking** — bare comparisons between temporal columns throw; use `timestamp()`.
- A negated column-needle match (`!R.attr.a.contains(R.attr.b)`) no longer returns rows whose needle
  is NULL — an over-grant fix, fewer rows
  ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).
- A dotted `jpaPath` through a to-one association is a LEFT join rather than an implicit INNER
  join, so disjunctions whose other branch holds now return those rows — an under-grant fix, more
  rows ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)).
- `string()` over a boolean column compared with a string constant now translates instead of
  throwing; `string()` over other types still throws.
- The `mod` refusal message no longer claims the condition "can never be satisfied by the PDP"; it
  names the unlowerable `int()` cast instead.
- Constant NaN ordering follows Cerbos 0.55 (unordered comparison is false, its negation true).

## Example application

[`example/`](example) holds two runnable Spring Boot programs that resolve this adapter from
mavenLocal as a real Maven coordinate
([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)): a photo-sharing app with
three resource kinds, and a program implementing the [shared demo domain](../demo). Run the latter
from the repository root with `demo/scripts/run-example.sh spring-data`.

## Build

JDK 17+. Gradle comes from the committed wrapper. The suites read `../conformance/`, so build in a
checkout of the **whole repository**; the conformance suite needs Docker on PostgreSQL and MySQL:

```bash
# From spring-data/:
./gradlew build
```

| Variable | Values | Selects |
|---|---|---|
| `ADAPTER_TEST_DB` | `h2` (default), `postgres`, `mysql` | Database for the conformance suite |
| `ADAPTER_TEST_ORM` | `baseline` (default), `next` | Hibernate 6.6 / Spring Data JPA 3.5, or Hibernate 7 / Spring Data JPA 4 (declared in [`build.gradle.kts`](build.gradle.kts)); unknown values fail |
| `ADAPTER_TEST_MYSQL_COLLATION` | e.g. `utf8mb4_0900_ai_ci` | Override the MySQL leg's `utf8mb4_0900_bin` |
| `ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS` | `true` | Run the MySQL leg with server-side prepared statements |

Hibernate 7 / Spring Data JPA 4 needs no translation change, and CI runs the conformance suite on
H2, PostgreSQL and MySQL under both ORM sets. Spring Data JPA 4 removed
`JpaSpecificationExecutor.delete(Specification)`, so the bulk-delete hazard can't be reached through
that overload there; the guard still fires on any `CriteriaDelete`.

## Testing

| Suite | Role | Needs |
|---|---|---|
| `SpringDataQueryPlanAdapterTest` | Call contract: malformed operands, overrides, mapping validation, macro depth, bulk-delete guard, plus corpus-gap shapes | H2 in-process |
| `RepositorySurfaceTest` | Spring Data glue: `findAll` / `count` / paging, de-duplication, composition | H2 in-process |
| `RefusalTypesTest` | Which refusal type each refused corpus case and each malformed plan throws | nothing — no database |
| `AdversarialConformanceTest` | Conformance: every recorded golden plan of both PDPs replayed, rows compared with the recorded `check()` decisions; exceptions in [`conformance-ledger.json`](conformance-ledger.json) | H2 in-process, or Docker for PostgreSQL/MySQL |

```bash
./gradlew test                            # every suite
ADAPTER_TEST_DB=postgres ./gradlew test   # conformance suite on PostgreSQL
ADAPTER_TEST_DB=mysql ./gradlew test      # … on MySQL (see "Database collation requirements")
ADAPTER_TEST_ORM=next ./gradlew test      # every suite under Hibernate 7 / Spring Data JPA 4
```

The PostgreSQL and MySQL images are pinned by tag and digest in [`POSTGRES_IMAGE`](POSTGRES_IMAGE)
and [`MYSQL_IMAGE`](MYSQL_IMAGE) (files, so Renovate's custom manager can bump them;
`conformance/scripts/validate-corpus.sh` checks them).

