# cerbos-exposed

> **Alpha release — `0.1.0-alpha.1`.** The API and the mapping shapes may still change before
> `1.0`, and the [Conformance contract](#conformance-contract) below is not filled in yet: every
> figure in it is an output of the differential harness, and it is written
> `TBD-AFTER-CONFORMANCE-RUN` until that run has happened rather than estimated. We'd love feedback
> while it's still alpha.

[Cerbos](https://cerbos.dev) query plan adapter for
[Exposed](https://github.com/JetBrains/Exposed), JetBrains' Kotlin SQL framework. Converts a Cerbos
`PlanResources` response into an `Op<Boolean>` you can hand to `where { }`, `andWhere { }` or a DAO
`find { }`.

The invariant behind everything below: **a shape this adapter cannot express raises, it never emits
a filter.** A wrong filter returns rows the PDP denies, which is an authorization bug that no error
and no log line would tell you about; a refusal is a bug report. See
[Handling refusals](#handling-refusals).

## Install

Gradle:

```kotlin
dependencies {
    implementation("dev.cerbos:cerbos-exposed:0.1.0-alpha.1")
}
```

> [!NOTE]
> **Not on Maven Central yet.** The build configures `publishToMavenLocal` only — a release would
> additionally need POM metadata and signing, which its `publishing` block says in full. Until then,
> build the adapter and run `gradle publishToMavenLocal` to resolve the coordinate above from your
> local repository.

You'll also need:

- **The Cerbos Java SDK** (`dev.cerbos:cerbos-sdk-java`) to call the PDP.
- **Exposed 1.0.0 or later**, which you bring yourself. `exposed-core` and `exposed-jdbc` are
  `compileOnly` here, so installing the adapter cannot move your ORM version. The published jar is
  compiled against **1.0.0** and CI runs every suite — including the differential conformance
  harness — against both **1.0.0** and **1.5.0**. That direction is deliberate: JetBrains promises
  that code built against an older 1.x keeps working on a newer one and promises nothing in the
  other direction, so compiling against the floor is the only arrangement where a green build proves
  the claim for the artifact you actually install
  ([ADR 0009](../docs/adr/0009-the-exposed-adapter-is-jdbc-first-and-returns-a-sealed-result.md)).
  Exposed 0.x is out of scope: 1.0 moved every symbol from `org.jetbrains.exposed.sql.*` to
  `org.jetbrains.exposed.v1.*`, so one artifact cannot serve both.
- **Kotlin 2.2 or later** and **JVM 17 or later**. The adapter's language and API levels are pinned
  to 2.2, the minimum Exposed 1.x itself states, so it never demands a newer compiler than the ORM
  beside it.
- **`exposed-jdbc`.** This release is **JDBC only**. `select` and `selectAll` are defined per
  transport module, and building a correlated subquery from `exposed-core` alone needs an
  `@InternalApi` accessor this adapter will not opt into, so every subquery is built through the
  JDBC `Query`. R2DBC is a follow-up module behind that one seam, not a rewrite. DAO is JDBC-only
  regardless.
- **`exposed-java-time` or `exposed-kotlin-datetime`**, whichever declares your temporal columns.
  Both are probed for on the classpath; you need only the one you already use.

### Pin `protobuf-java` to the SDK's gencode version

`cerbos-sdk-java` ships protobuf message classes generated against a specific `protobuf-java`. If an
**older** runtime wins dependency resolution — because you pinned it, or a transitive dependency
did — the SDK throws on the first message decode:

```text
com.google.protobuf.RuntimeVersion$ProtobufRuntimeVersionException:
  Detected incompatible Protobuf Gencode/Runtime versions when loading Principal:
  gencode 4.35.1, runtime 4.31.1. Runtime version cannot be older than the linked gencode version.
```

The adapter declares both `dev.cerbos:cerbos-sdk-java` and a matching `com.google.protobuf:protobuf-java`
at `implementation` scope, so they reach you transitively; add a direct dependency matching the SDK's
gencode if anything else on your graph drags protobuf backwards.

## Quick start

```kotlin
import dev.cerbos.queryplan.exposed.ExposedQueryPlanAdapter
import dev.cerbos.queryplan.exposed.Options
import dev.cerbos.queryplan.exposed.QueryPlanFilter
import dev.cerbos.queryplan.exposed.cerbosMapping
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction

// Map the plan's attribute references onto your columns:
val mapping = cerbosMapping {
    "request.resource.id" to Documents.id
    "request.resource.attr.ownerId" to Documents.ownerId
    "request.resource.attr.isPublic" to Documents.isPublic
}

// 1) Ask the PDP for a query plan
val plan = cerbosClient.plan(
    Principal.newInstance("alice", "USER"),
    Resource.newInstance("document"),
    "view",
)

// 2) Translate it
val filter = ExposedQueryPlanAdapter.toFilter(plan, Options.of(mapping))

// 3) Execute
val documents = transaction {
    when (filter) {
        QueryPlanFilter.AlwaysDenied -> emptyList()          // no query at all
        QueryPlanFilter.AlwaysAllowed -> Documents.selectAll().toList()
        is QueryPlanFilter.Conditional -> Documents.selectAll().where { filter.op }.toList()
    }
}
```

The result is sealed into three kinds because an `Op` is a **value**, not a builder the framework
invokes later: an always-denied plan lets you skip the database entirely, which a bare `Op.FALSE`
would hide behind a round trip for a guaranteed empty result.

| Plan kind | `QueryPlanFilter` | `toOp()` |
|---|---|---|
| `KIND_ALWAYS_ALLOWED` | `AlwaysAllowed` | `Op.TRUE` |
| `KIND_ALWAYS_DENIED` | `AlwaysDenied` | `Op.FALSE` |
| `KIND_CONDITIONAL` | `Conditional(op)` | the translated predicate |

If you always run the query, collapse the three with `toOp()` and compose as usual:

```kotlin
Documents.selectAll()
    .where { filter.toOp() and (Documents.archived eq false) and (Documents.region eq "emea") }
    .orderBy(Documents.id)
    .limit(20)
```

The predicate is an ordinary Exposed one, so DAO works with no extra step:

```kotlin
val documents = transaction { DocumentEntity.find(filter.toOp()).toList() }
```

Build the filter **once per request** and hand it to the query. It renders inside your transaction,
for that transaction's dialect — the adapter never reads the dialect while translating, precisely so
the same `Op` is correct wherever you run it.

### Declaring the translation: `Options`

Everything the adapter can be told lives in one immutable `Options`. Build it with `Options.of(...)`
and refine it with the `with…` methods, each of which returns a new instance:

```kotlin
val options = Options.of(mapping)
    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
    .withMaxMacroDepth(8)
```

`maxMacroDepth` bounds how deeply collection macros (`exists`, `all`, `exists_one`, …) may nest, and
defaults to 5. Each level multiplies the correlated subqueries the filter carries, so the bound is a
cost guard: a plan nested past it is refused rather than emitted. `Options` is a class with withers
rather than a positional constructor so a setting can be added later without breaking your call.

## Mapping attributes

`cerbosMapping { }` builds the usual static table. Resolution is **fail-closed**: a reference
nothing maps raises `UnmappedAttributeException` rather than guessing a column name.

```kotlin
val mapping = cerbosMapping {
    // A column. `to` takes a bare Column for the common case…
    "request.resource.id" to Documents.id
    "request.resource.attr.aString" to Documents.aString
    // …or `field(...)`, which additionally declares this attribute's NULL convention.
    "request.resource.attr.owner" to field(Documents.ownerId, nulls = NullAttributeRepresentation.EXPLICIT)

    // A to-many relation whose elements are objects.
    "request.resource.attr.tags" to many(Tags, from = Documents.id, to = Tags.documentId) {
        "id" to Tags.tagId
        "name" to Tags.name
        // Optional. See "Mapping hazards".
        visibleWhen { t -> t[Tags.deletedAt].isNull() }
    }

    // A to-many relation whose elements ARE values: `element` is the value each element stands for.
    "request.resource.attr.tagNames" to
        many(Tags, from = Documents.id, to = Tags.documentId, element = Tags.name)

    // A to-one relation, and a chain through it.
    "request.resource.attr.parent" to one(Parents, from = Documents.id, to = Parents.documentId) {
        "aString" to Parents.aString
        "inner" to one(Inners, from = Parents.id, to = Inners.parentId) {
            "aBool" to Inners.aBool
        }
    }
}
```

- **`from` is a column on the scope that declares the relation** — the root table at the top level,
  the enclosing relation's table when nested — and **`to` is the matching column on the related
  table**. The correlation is always `<table>.<to> = <parent>.<from>`.
- **A dotted reference resolves against the longest mapped prefix**, and the remainder is walked
  through that relation's nested entries. So `request.resource.attr.parent.inner.aBool` above is one
  mapping entry reached in three steps, not a third top-level key.
- **Cardinality is declared, not inferred.** `one` is read as a scalar through a correlated scalar
  subquery: no matching row yields NULL, which is already CEL's missing-attribute error, so both
  polarities deny. `many` is a collection.
- **A relation is validated where it is built.** `to` must be a column of `table`, `element` must be
  a column of `table`, and every nested entry must read `table` — each is an
  `IllegalArgumentException` at mapping-construction time, not a wrong join at query time.

### A resolver instead of a table

`AttributeResolver` is a `fun interface`, so a mapping can be a rule rather than a table:

```kotlin
val mapping = AttributeResolver { reference ->
    columnsByAttributeName[reference.removePrefix("request.resource.attr.")]
        ?.let { AttributeMapping.field(it) }
}
```

Returning `null` is the fail-closed answer and raises `UnmappedAttributeException`. Longest-prefix
resolution works the same way here: a function-style resolver is asked for progressively shorter
prefixes of the same reference, so answer for the prefix you own and return `null` for the rest.
`AttributeMappings.of(map)` assembles the static form without the Kotlin DSL.

## NULL attribute representation

`R.attr.x == null` compiles to the same `eq(x, null)` plan node however your application represents
a NULL column in the attributes it sends to `check()`, so the adapter cannot infer the convention
and has to be told which one you use.

| attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

`NullAttributeRepresentation` defaults to `EXPLICIT`. If your application omits attributes for NULL
columns, pass `OMITTED` and the adapter rejects every null comparison operand instead of emitting a
filter that returns rows the PDP denies:

```kotlin
Options.of(mapping).withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
```

The rejection is deliberately wider than the shapes that actually over-grant, because a leaf cannot
tell whether an enclosing `not` will flip `IS NOT NULL` back into a NULL-selecting predicate.
Rejecting every null operand is correct under any nesting. See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

### Declare the convention per attribute

The option above is a whole-call default, and one policy suite can legitimately use both
conventions: the same column mapped twice, sent as an explicit null under one attribute name and
omitted under another. Declare it per attribute and the call-level option only covers what the
mapping does not:

```kotlin
val mapping = cerbosMapping {
    // sent as an explicit null when the column is NULL
    "request.resource.attr.owner" to field(Documents.ownerId, nulls = NullAttributeRepresentation.EXPLICIT)
    // omitted when the column is NULL — the call-level default applies
    "request.resource.attr.department" to Documents.department
}
```

Declaring the explicit convention asserts two things at once: the column can be NULL, **and** a NULL
reaches `check()` as an explicit null. The equality family (`eq`, `ne`, `in`) over that attribute is
then rendered so it can never be SQL UNKNOWN — CEL holds a null *value* under this convention, so
`null != "x"` is TRUE and the row must come back, while UNKNOWN would drop it under *both*
polarities. Ordering and string operators are left alone: a null receiver raises a no-overload error
in CEL, which denies exactly as UNKNOWN does.

Leaving an attribute undeclared keeps the conservative rendering, so `!=` against a constant
under-grants those rows until you declare it. See
[#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

## Handling refusals

A shape the adapter cannot translate raises rather than emitting a best-effort filter, and the raise
is one of three types so you can route on it without matching the message:

| Exception | Meaning | What to do |
|---|---|---|
| `UnsupportedPlanShapeException` | The plan is well-formed, and SQL as this adapter builds it cannot express it faithfully | Rewrite the policy, or answer that request another way (a per-row `check()`, another store) |
| `UnmappedAttributeException` | The mapping came up short — a variable nothing maps, a relation where a scalar is needed, a column where a collection operator needs a relation, a temporal column whose type does not pin an absolute instant | Change the mapping |
| `MalformedPlanException` | The plan violates the planner's wire contract — wrong arity, a lambda whose second operand is not a variable, a conditional plan with no condition, an unknown filter kind | A hand-built plan, or an upstream bug to report |

All three extend `IllegalArgumentException`, so catching that catches every refusal. A branch only an
adapter bug can reach raises `IllegalStateException` instead, deliberately, so it is never mistaken
for a classified refusal.

Every refusal message is pinned in the shared corpus (`conformance/actions.json`) and asserted by
this adapter's conformance run, so a classification proves the raise names the mechanism its reason
declares rather than merely that something raised.

## Aliased root tables

The adapter emits a predicate and nothing else: it never adds a join, and it never rewrites the
query you build. So if you query through an alias, **map the alias's columns**, not the table's — a
predicate over `Documents.ownerId` does not resolve inside a query whose only `FROM` entry is
`Documents.alias("d")`:

```kotlin
val d = Documents.alias("d")

val mapping = cerbosMapping {
    "request.resource.id" to d[Documents.id]
    "request.resource.attr.ownerId" to d[Documents.ownerId]
    // A relation anchored at the root correlates to the alias too.
    "request.resource.attr.tags" to many(Tags, from = d[Documents.id], to = Tags.documentId) {
        "name" to Tags.name
    }
}

d.selectAll().where { ExposedQueryPlanAdapter.toFilter(plan, Options.of(mapping)).toOp() }
```

The aliases the adapter allocates for its own subqueries are prefixed (`cerbos_1`, `cerbos_2`, …),
so they are deterministic and cannot collide with yours.

## Database collation and case sensitivity

> **⚠️ Hard requirement: every string column a mapping references must compare case- and
> accent-sensitively, and `LIKE` must be case-sensitive.**

CEL string comparison at the PDP is exact and byte-sensitive: with `R.attr.department == "finance"`,
a `check()` for a resource holding `"Finance"` returns **DENY**. The adapter emits the comparison
with no collation control, so the store decides what matches. Where the store is more permissive
than CEL, the filter returns rows the policy denies — an over-grant, with nothing to notice it by.
Both of the following are store configuration, not adapter limitations:

- **MySQL.** The default `utf8mb4_0900_ai_ci` is case- *and* accent-insensitive, so `=` itself
  over-grants. Use a case- and accent-sensitive collation — `utf8mb4_0900_as_cs`, or `utf8mb4_bin` —
  on every string column a mapping references. The differential harness creates its MySQL schema
  with `utf8mb4_0900_as_cs` for exactly this reason, and its mixed-case seeds fail the oracle
  comparison under the default.
- **SQLite.** `LIKE` is case-insensitive for ASCII by default, and this adapter lowers string
  matching (`contains` / `startsWith` / `endsWith`, and hierarchy prefix tests) to `LIKE`. Set
  `PRAGMA case_sensitive_like = ON` on the connection, as the harness does.

PostgreSQL and H2 are case-sensitive by default and are safe unless you opt into
case-insensitive behaviour (a nondeterministic ICU collation, `citext`).

Role and tenancy checks are the highest-risk shapes: `'admin'` versus `'Admin'` under `eq` and `in`,
and hierarchy descendant checks. Treat collation as part of your policy contract.

## Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `check()` decisions using 22 hostile
seed rows, on H2, SQLite, PostgreSQL and MySQL. The Spring Data adapter defines the reference
semantics this one follows.

The figures below are an **output** of that harness. They are filled in from the first full
conformance run and are written as a marker until then, rather than estimated — a number nobody
measured is worse than no number.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | `TBD-AFTER-CONFORMANCE-RUN` of the 192 reference conformance actions, on H2, SQLite, PostgreSQL and MySQL |
| Fail-closed corpus shapes | `TBD-AFTER-CONFORMANCE-RUN` — the list of refused shapes, each with the mechanism it names (`TBD-AFTER-CONFORMANCE-RUN` actions) |
| Representation-dependent | `null-eq-missing` — rejected under `NullAttributeRepresentation.OMITTED`; translated as `IS NULL` under the default, which over-grants if the caller omits attributes for NULL columns |
| Attribute NULL convention | The equality family (`eq`, `ne`, `in`) over an attribute the caller sends as an explicit null renders definitely, so a NULL row is included where CEL's null *value* says it should be. Declare it per attribute — `field(column, nulls = EXPLICIT)` — or the conservative rendering applies and `!=` against a constant under-grants those rows (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `check()` denies the missing-attribute rows. Until the planner is fixed, write `R.attr.x != null` rather than `has(R.attr.x)` for database-backed attributes |

Two suites read that classification and they answer different questions.
`AdversarialConformanceTest` plans each action against a real PDP, executes the translated query
against a real store, and compares the returned ids with per-row `check()` decisions — the PDP is
the oracle for both sides, so there are no hand-written expectations.
`ExposedTranslatorTest` translates the same actions offline from `conformance/wire-fixtures/` and
asserts the emitted SQL against [`golden/expectations.json`](golden/expectations.json). What the
second buys over the first is the rows nobody seeded: two queries can agree on all 22 seeds and
disagree on the row you have, so a rewrite that quietly changes the emitted SQL passes the oracle
and shows up there as a diff.

### Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the predicate
select the rows `check()` allows. The other half is the *mapping*: **the rows the subquery reads must
be the rows the application put into the resource attributes.** Six ways that can break are
catalogued in the shared corpus, and every adapter has to record a position on each of them.

This adapter builds a **bare-table subquery.** A relation is a table plus a source and a target
column, and Exposed has no association metadata for the translator to consult, so nothing your own
reads apply reaches the generated subquery. Where your reads narrow that table, declare the same
predicate as `visibleWhen` on the relation and the adapter reproduces it. Declaring nothing emits
exactly what the adapter emitted before the field existed — it cannot detect the omission, so
silence is not a warning.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned**, reproducible with `visibleWhen` | A DAO `EntityClass` overriding `searchQuery` to fold a predicate into every read, an `EntityClass.referrersOn`/`optionalReferrersOn` reached through a helper that always narrows it, or any repository function that appends `.andWhere { … }` on the way out. All of those rewrite a query Exposed builds; none of them reaches inside the correlated subquery this adapter builds, which is given a `Table`, a `from` column and a `to` column and reads the table bare |
| Default scope on the target model | **Caller-owned**, reproducible with `visibleWhen` | A soft-delete predicate (`deleted_at IS NULL`), a tenant column, a `published` flag — anything every application read of that table filters on. Exposed has no default-scope construct at all, so the convention lives in your own query code and only you can see it. A DAO `EntityClass.searchQuery` override is the closest thing to one, and it is still invisible here |
| Subtype discrimination | **Caller-owned**, reproducible with `visibleWhen` | A discriminator column (`kind`, `type`) where one table holds several row kinds, typically with a DAO `EntityClass` per kind narrowing on it. Declare the same predicate: `visibleWhen { t -> t[Tags.kind] eq "label" }` |
| To-one relation used as a collection | **Caller-owned** | A `one(...)` relation whose `to` column carries no unique index. The mapping declares the cardinality; the **database** is what has to enforce it. Add the unique constraint. The failure is not uniform across stores: a scalar subquery returning several rows is a loud runtime error on PostgreSQL, MySQL and H2, but SQLite silently takes the first row — so a schema that passes your SQLite tests can raise in production, or, worse, keep answering from an arbitrary row |
| Composite association key | **Rejected by the type system** | `from` and `to` are each one `Column<*>`, so a two-column association key cannot be expressed. That is a compile error, not a wrong join |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`w1-all-chain` and its siblings) | None — a to-one hop is read as a correlated **scalar subquery**, which is NULL when no row correlates, and NULL is already CEL's missing-attribute error, so the row is denied under both polarities with no separate hop guard ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

`visibleWhen` takes a **resolver lambda**, not a ready predicate, because every subquery instance
reads through a fresh alias and the predicate has to be built against that alias:

```kotlin
"request.resource.attr.tags" to many(Tags, from = Documents.id, to = Tags.documentId) {
    "name" to Tags.name
    visibleWhen { t -> (t[Tags.deletedAt].isNull()) and (t[Tags.kind] eq "label") }
}
```

It is ANDed into the subquery's **correlation**, so it narrows the rows the subquery examines rather
than the rows it returns. That is what keeps it right under negation: `all` lowers to a scoring
subquery, and restricting the scan turns it into "every visible row satisfies the body" instead of
"every row in the table does". It applies to every shape built on the relation — the macros, the
counts, membership and the existence guards alike.

One consequence of a bare-table subquery is worth stating plainly: this adapter is only as correct
as the mapping is honest. A predicate you apply on the read path that builds the resource attributes
but not in `visibleWhen` makes the two disagree, and no conformance action can see it — the oracle
is computed from the attributes and the adapter reads the store. Keep one definition of what the
relation contains.

## Dialects

| Dialect | What CI executes | What is distinctive about it |
| --- | --- | --- |
| H2 | the default (`ADAPTER_TEST_DB=h2`), in process, on both the baseline and the floor Exposed release | Case-sensitive by default; the store the floor leg is proved against |
| SQLite | `ADAPTER_TEST_DB=sqlite`, in process | Needs `PRAGMA case_sensitive_like = ON`; no boolean and no temporal type, so both are stored as text or integers; a multi-row scalar subquery does not raise |
| PostgreSQL | `ADAPTER_TEST_DB=postgres`, Testcontainers | Real `boolean` and `timestamptz`; cannot type a bound `NULL` (`$1 IS NULL` is an error), which is why the adapter renders NULL as a literal |
| MySQL | `ADAPTER_TEST_DB=mysql`, Testcontainers | Needs a case- and accent-sensitive collation; no boolean type; `LIKE` backslash handling and cast spellings differ |

These are not the same test four times, which is the point: collation, LIKE escaping, cast targets
and parameter typing are all translator behaviour, so a dialect the harness does not execute is a
dialect this adapter does not cover. The PostgreSQL and MySQL servers are pinned by tag **and**
digest in [`POSTGRES_IMAGE`](POSTGRES_IMAGE) and [`MYSQL_IMAGE`](MYSQL_IMAGE), read at runtime and
declared as inputs of `gradle test`. Files rather than Kotlin constants for one reason:
`renovate.json`'s custom manager bumps `<SERVICE>_IMAGE` files and nothing else, so a reference held
in source would never get a Renovate PR.

MariaDB is not proved and is therefore not claimed.

## Development

JDK 17 or later and Gradle 8.x — CI pins Gradle 8.12, and so does the container below. There is no
Gradle wrapper and no Dockerfile: run Gradle from this directory, or run the same build in the
official Gradle image with the **repository root** mounted (every suite reads the shared corpus at
`../conformance/`) and the Docker socket passed through (the differential suite starts a pinned PDP
through Testcontainers):

```bash
# From the repository root:
docker run --rm -v "$(pwd)":/repo -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_RYUK_DISABLED=true --network host -w /repo/exposed gradle:8.12-jdk17 \
  gradle build --no-daemon

# Or with a local Gradle 8.x + JDK 17+, from exposed/:
gradle build --no-daemon
gradle goldenUpdate   # rewrite golden/expectations.json from what the translator emits today
```

Two environment variables select what the build runs against, both declared once in
[`build.gradle.kts`](build.gradle.kts), and an unknown value fails rather than falling back to the
default:

- `ADAPTER_TEST_DB` — `h2` (default), `sqlite`, `postgres` or `mysql`: the store the differential
  suite executes against. See [Dialects](#dialects).
- `ADAPTER_TEST_ORM` — `baseline` (default) or `floor`: the Exposed version set. `baseline` is the
  latest release, the one the golden asset was rendered under and the one `example/` pins; `floor`
  is the release the published jar is compiled against. See
  [The golden expectations](#the-golden-expectations).

Only the differential suite needs Docker. `ExposedTranslatorTest` reads its plans from the shared
corpus's wire fixtures and needs no PDP and no database server at all.

## The golden expectations

[`golden/expectations.json`](golden/expectations.json) is this adapter's
[golden expectation](../conformance/README.md#golden-expectations) file: the SQL it is pinned to
emit for each corpus action, regenerated with `gradle goldenUpdate` and reviewed as a diff. An action
the corpus says this adapter must refuse carries no entry — its message is already pinned in
`conformance/actions.json`.

The adapter emits an `Op<Boolean>`, so the recorded value is that predicate **rendered**, per
dialect, with its bind parameters recorded beside it as typed arguments rather than inlined. The
types are the point: a double bound as a decimal evaluates the adapter's deliberately IEEE arithmetic
exactly — `3 * 0.1 == 0.3` is true in decimal and false in CEL — so it admits rows the PDP denies,
and no amount of statement text would show it.

Exposed's own renderer is therefore an input to the recorded bytes, and `exposed-core` is a
`compileOnly` dependency, so a consumer brings their own. The three rules that follow from that
(`conformance/README.md`, "When the generator is an input") all apply: the file declares the Exposed
minor that rendered it, `gradle goldenUpdate` refuses to run under another one, and the `floor` leg
asserts a pinned divergence list in **both** directions instead of the bytes. Where the floor and the
baseline render identically that list is empty, and the assertion is that it stays empty.

`gradle test` never regenerates the asset, so a translator change that moves the emitted SQL fails
CI whatever anyone ran locally.

## License

Apache 2.0
