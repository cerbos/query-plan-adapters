# cerbos-exposed

> **Alpha release — `0.1.0-alpha.1`.** The API and the mapping shapes may still change before
> `1.0`. The [Conformance contract](#conformance-contract) below is filled in from the conformance
> harness, which replays recorded PDP decisions on every store this adapter claims. We'd love
> feedback while it's still alpha.

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
> build the adapter and run `./gradlew publishToMavenLocal` to resolve the coordinate above from your
> local repository.

You'll also need:

- **The Cerbos Java SDK** (`dev.cerbos:cerbos-sdk-java`) to call the PDP.
- **Exposed 1.0.0 or later**, which you bring yourself. `exposed-core` and `exposed-jdbc` are
  `compileOnly` here, so installing the adapter cannot move your ORM version. The published jar is
  compiled against **1.0.0** and CI runs every suite against both **1.0.0** and **1.5.0** — the
  floor legs including the conformance harness on every store, so the floor is proved against the
  PDP's recorded `check()` decisions rather than only against compilation. That direction is
  deliberate: JetBrains promises that code built against an older 1.x keeps working on a newer one
  and promises nothing in the other direction, so compiling against the floor is the only
  arrangement where a green build proves the claim for the artifact you actually install
  ([ADR 0011](../docs/adr/0011-the-exposed-adapter-is-jdbc-first-and-returns-a-sealed-result.md)).
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
- **`exposed-java-time` or `exposed-kotlin-datetime`**, but only if your policies compare
  timestamps, and only whichever one already declares your temporal columns. The adapter declares
  **neither**, not even at `compileOnly`: it matches on `InstantColumnType` and
  `OffsetDateTimeColumnType`, the abstract bases that live in `exposed-core` and that both modules'
  column types extend, so it compiles against neither and loads without either. See
  [Timestamp columns](#timestamp-columns).

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

### Where the predicate works

`ExposedSurfaceTest` executes the emitted `Op` in each of these against H2, and the two write
statements against SQLite as well. None of them is a shape a conformance harness reaches: every
harness in this repository runs one flat `SELECT … WHERE`.

| Shape | Notes |
| --- | --- |
| `where { }`, `andWhere { }` | Including the load-bearing one: the adapter's predicate as a conjunct beside the application's own |
| `count()` | `count()` rewrites the query into a `COUNT` projection, so the predicate survives a rewrite rather than only the `SELECT` it was first attached to |
| `orderBy(...).limit(n).offset(m)` | Pagination over the filtered query |
| DAO `find` | `EntityClass.find(op)` takes the same value `where { }` does |
| A query that joins another table | The adapter adds no join of its own; yours is untouched |
| `deleteWhere { }` and `update({ }) { }` | Including a predicate carrying a correlated `EXISTS` |

**The `deleteWhere` / `update` proof stops at H2 and SQLite.** No leg executes a write statement on
PostgreSQL or MySQL, so treat those two as read-proved and write-untested: the `SELECT` path is
replayed on all four stores, a `DELETE` or `UPDATE` carrying a correlated subquery is not.

### Declaring the translation: `Options`

Everything the adapter can be told lives in one immutable `Options`. Build it with `Options.of(...)`
and refine it with the `with…` methods, each of which returns a new instance:

```kotlin
val options = Options.of(mapping)
    .withNullAttributeRepresentation(NullAttributeRepresentation.OMITTED)
    .withMaxMacroDepth(8)
```

`maxMacroDepth` bounds how deeply collection macros may nest before a plan is refused rather than
translated, and defaults to 5. What counts a level is exactly **one per collection macro the walk
enters** — `exists`, `all`, `exists_one` and `size(filter(…))` — whether it ranges over a mapped
relation or over a literal list. Both kinds multiply:

- A macro over a **mapped relation** emits a correlated subquery, and a nested one emits that
  subquery once per enclosing level.
- A macro over a **literal list** emits no subquery at all — it substitutes each element into the
  lambda body and walks the resulting `or`/`and` chain — but it duplicates everything under it once
  per element. Two three-element folds over one relation macro emit nine correlated subqueries; an
  eleven-element pair emits 121.

> [!IMPORTANT]
> It is **not** a bound on the size of the emitted expression, and must not be read as one. A
> single 100-element fold is one level. A ternary doubles its subtree per level and a zero-capable
> division branches per level, and neither is counted at all. A relation **chain** is one level
> however many hops it has, and so is a `size()`, a membership test or a hierarchy relation over
> one.

`Options` is a class with withers rather than a positional constructor so a setting can be added
later without breaking your call.

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
- **A top-level relation's `from` is the one key nothing can check.** The adapter is never told
  which table you select from: a mapping names columns, and the root scope reads them bare. So
  `from` naming a column of some other table is accepted at construction and fails at *execution*,
  exactly the way [an aliased root table](#aliased-root-tables) does — a rejected statement, never a
  wrong row set. A nested relation's `from` *is* checked, because the enclosing relation declares
  its table.
- **`one(...)` is a promise the database has to keep.** The mapping declares the cardinality; only a
  unique index on `to` enforces it. Without one the scalar subquery can return several rows, which
  PostgreSQL, MySQL and H2 raise on and **SQLite silently answers from the first row** — so a schema
  that passes your SQLite tests can fail, or quietly answer from an arbitrary row, in production.

### The operand's type has to match the column's

> [!IMPORTANT]
> A comparison between a mapped column and something of **another CEL value family** — text,
> numeric or boolean — is answered from the types, never handed to the store. A column type the
> adapter has no CEL reading for raises `UnmappedAttributeException` before any SQL exists.

| What is compared | What the adapter does |
| --- | --- |
| A mapped column against a plan constant of another family | `==` is FALSE for a present value, `!=` TRUE, and anything else (an ordering) UNKNOWN. A NULL column is a missing attribute, which CEL denies, so `==` and `!=` are UNKNOWN for it, unless the attribute declares `EXPLICIT`, whose null is a value and compares FALSE. No constant is bound. A `null` constant is not a type mismatch: it renders `IS NULL` / `IS NOT NULL` |
| Two mapped columns of different families | The same answer, UNKNOWN when either undeclared column is NULL |
| The elements of an `in` or `hasIntersection` list, against a column or a relation's `element` column | An element of another family equals nothing, so it drops out of the disjunction; the rest compare as usual. A `null` element renders `IS NULL` |
| A list or map — a constant, or one the planner builds with `list()` / `struct()` from constants alone — against a scalar column, or as the member of `in` over a relation | Never equal to a scalar, so answered as a type mismatch; `["a"] in tagNames` is FALSE. One built from an attribute is refused, since the attribute may be missing. Against a relation-mapped attribute it is whole-list equality, and refused |
| A member column against a relation's `element` column | Same family, or refused |
| `contains`, `startsWith`, `endsWith` (haystack **or** column needle) and `size()` over a number or boolean column | UNKNOWN: CEL has no overload, so the call raises and denies on every row, under either polarity |
| Every hierarchy operator's path column (or `list()` segment) | A **text** column. A number or boolean one is UNKNOWN, since `hierarchy()` has no overload for it; any other kind is refused |
| A column type the adapter has no CEL reading for — temporal, binary, array, enum, a custom `ColumnType` | Refused against any constant, **against every element of an `in` list**, **against any other column, including one of its own type**, and under a string match or `size()`. Cerbos carries a timestamp attribute as an RFC 3339 **string**, so `R.attr.createdAt == R.attr.updatedAt` compares strings in CEL and instants in SQL, and `"…T00:00:00Z"` and `"…T00:00:00.000Z"` are one instant and two strings. A DAO key lands here too — a `UUIDTable` id is an `EntityIDColumnType(UUIDColumnType)` — so `request.resource.id in [...]` is refused there while a `varchar` key is fine. Compare two temporal columns as `timestamp(R.attr.a) < timestamp(R.attr.b)`, which means the instant on both sides and translates when both columns pin an absolute instant in the same representation |

A DAO id column and a `transform`ed column are read through the type underneath, so a `varchar`
primary key is text; a column reached through a to-one hop is judged by its **declared** type, not
by the subquery that reads it.

**Why the store is never asked.** `R.attr.aString == P.attr.level` is legal CEL and arrives as
`eq(variable, value)` with nothing in it naming a type. CEL answers it from the *values*: equality
is a definite `false`, and every ordering raises a no-overload error, which denies. SQL has to
coerce one side instead, and **MySQL coerces the string** — `'abc' = 0` is TRUE there. Against the
pinned MySQL server, the predicate an earlier version emitted returned *every* seeded row for a
policy the PDP allows none of; H2 raises a conversion error and PostgreSQL aborts the statement. A
plain `Op.FALSE` would be right unnegated and wrong under `not(...)` for a row whose attribute is
missing, which is why the NULL column stays UNKNOWN. The same holds for the string matches and
`size()`: `a_number LIKE '%2%'` is TRUE for `123` on MySQL and SQLite, and `CHAR_LENGTH(1)` is `1`,
where CEL raises.

**When it is refused**, the message says what to do: compare the attribute against a value of the
column's own type, or map it onto one of the kinds this adapter compares: text, integer,
floating-point, decimal or boolean. A temporal column is compared by wrapping both sides in
`timestamp()`. It is an `UnmappedAttributeException` rather than an `UnsupportedPlanShapeException`:
the plan is fine, and the fix is in your mapping.

### Regular expressions

`R.attr.s.matches("re")` is translated only when the RE2 pattern's language can be spelled
**exactly** with `LIKE`, `=` and `REPLACE`, since no SQL engine's regex dialect is RE2:

- each top-level alternative is a finite set of strings (literals, escapes, classes including
  `\d \w \s` and POSIX ones, groups, alternation, bounded repetition), anchored or not: `= s`,
  `LIKE 's%'`, `LIKE '%s'` or `LIKE '%s%'`. RE2's `$` matches only at the end of the text, so an
  anchored alternative is an exact `=`;
- in an alternative anchored at both ends, `.*` and `.+` become `%` and `_%`, with
  `NOT LIKE '%\n%'`, because RE2's `.` excludes a newline;
- `^[set]*$` and `^[set]+$` hold when deleting every member of the set with `REPLACE` leaves `''`;
- a leading `(?i)` expands each ASCII letter to its case-fold orbit, as RE2 does (`k` also matches
  U+212A KELVIN SIGN, `s` also U+017F LONG S).

A pattern RE2 rejects (a lookaround, a backreference, a stacked quantifier) makes `matches()`
raise, so it is UNKNOWN on every row, as is a number or boolean receiver. Anything else is refused.
`matches(...) == true` and its `false` / `!=` spellings translate the same way.

### Positional reads: `position(column)`

A to-many relation is a correlated subquery, and its rows carry no list order, so `R.attr.tags[0]`
needs the mapping to say where each element sits. Declare the column holding each row's zero-based
index in the list the application sends to `check()`:

```kotlin
"request.resource.attr.tags" to many(Tags, from = Documents.id, to = Tags.documentId) {
    "name" to Tags.name
    position(Tags.position)
}
```

It must hold exactly the list index: `0` for the first element, one row per position, no gaps. The
adapter trusts it. `list[k] op constant` and `list[k].member op constant` then read the one row at
position `k`: a read past the end, a negative index or a fractional one is CEL's error and
UNKNOWN; a scalar element is compared as a value (a NULL element is CEL's null element), a member
under its own declared convention. Without a position column, or through a to-many hop, a
positional read is refused.

### A resolver instead of a table

`AttributeResolver` is a `fun interface`, so a mapping can be a rule rather than a table:

```kotlin
val mapping = AttributeResolver { reference ->
    columnsByAttributeName[reference.removePrefix("request.resource.attr.")]
        ?.let { AttributeMapping.field(it) }
}
```

Returning `null` is the fail-closed answer and raises `UnmappedAttributeException`. Longest-prefix
resolution works the same way here: the reference is offered whole first, and then as progressively
**shorter** prefixes, longest first, until something answers.
`AttributeMappings.of(map)` assembles the static form without the Kotlin DSL.

> [!WARNING]
> **A resolver that answers every name short-circuits the walk.** The first prefix that resolves
> wins, and the whole reference is the first prefix tried — so a resolver built on a map with a
> default, or one that strips a prefix and hands back a column for anything left over, never lets
> the chain walk begin. `request.resource.attr.mainCategory.subCategories` then resolves as a
> *scalar column* rather than as a relation reached through `mainCategory`, and `size()` of it
> becomes a string length instead of a count. Nothing is wrong enough to raise. **Return `null` for
> every name you do not map**, including the dotted paths that reach through a relation you do map.

### Timestamp columns

`timestamp(R.attr.createdAt) > timestamp("2024-06-01T00:00:00Z")` translates only when the mapped
column's own Exposed type says which **absolute instant** a stored value denotes:

| Mapped column | Result |
| --- | --- |
| `timestamp(...)` from `exposed-java-time` or `exposed-kotlin-datetime` | Translated |
| `timestampWithTimeZone(...)` from either module | Translated; the literal is normalised to UTC, because CEL timestamp equality is equality of the instant and the offset a literal was written in must not reach the comparison |
| `datetime(...)` (a `LocalDateTime`), a date column, a text column | `UnmappedAttributeException` — remap the column |

A local date-time and a date carry no zone, so the same stored reading could mean any instant; a
text column orders lexicographically, which agrees with chronology only for one fixed-width
zone-normalised layout. Guessing a zone would silently include rows the PDP denies, so each of them
fails closed — and as a **mapping** error, because the plan is fine and the mapping does not say
enough.

The literal is bound through the mapped column's **own column type**, which is the one converter
guaranteed to agree with what your application wrote: binding a `java.time.Instant` against a column
you declared with `exposed-kotlin-datetime` would otherwise go through a different conversion from
the insert's. Neither datetime module is a dependency of the adapter — `InstantColumnType` and
`OffsetDateTimeColumnType` are the abstract bases in `exposed-core`, and both modules' column types
extend them — so this works with whichever one you already use, and with neither on the classpath
the class still loads.

Sub-millisecond thresholds are **not** fail-closed here. Cerbos folds `now()` into a window at
nanosecond precision, and `java.time.Instant` carries nanoseconds, so the corpus's `timestamp/less-than/relative-window` and
`timestamp/greater-than/relative-window-value-first` translate exactly — the precision the Python and TypeScript adapters have to refuse.

## NULL attribute representation

`R.attr.x == null` compiles to the same `eq(x, null)` plan node however your application represents
a NULL column in the attributes it sends to `check()`, so the adapter cannot infer the convention
and has to be told which one you use.

| attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
| --- | --- | --- |
| `{"x": null}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

`NullAttributeRepresentation` defaults to `EXPLICIT`. If your application omits attributes for NULL
columns, pass `OMITTED` and the adapter rejects every null comparison operand against an attribute
that declares no convention of its own, instead of emitting a filter that returns rows the PDP
denies (declaring `OMITTED` per attribute does better; see below):

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
omitted under another. Declare it per attribute, and every part of the walk that asks — `eq`, `ne`,
an `in` against a constant list and the membership subquery alike — reads that one declaration:

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

**Leaving an attribute undeclared is not the same as inheriting the call-level option.** An
undeclared column renders exactly as it always has, **as if it were `NOT NULL`**, and the call-level
`nullAttributeRepresentation` then decides one thing only: whether a null *operand* in the plan is
refused. It never turns the definite rendering on, anywhere in the walk — not in a leaf comparison,
not in the membership subquery. The asymmetry is deliberate — the call-level
default is `EXPLICIT`, so inheriting it would hand definite equality to every undeclared column and
return NULL rows the PDP denies for an attribute the caller in fact omits. The cost is that `!=`
against a constant under-grants those rows until you declare the convention on that column, which
fails closed. See [#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

**Declaring `OMITTED` answers `== null` and `!= null` instead of refusing them.** A NULL column sends
no attribute, so CEL raises a missing-attribute error and `check()` denies the row whichever way the
comparison is written; a present value is never null. `field(column, nulls = OMITTED)` therefore
renders `x == null` as `x IS NULL AND UNKNOWN` — UNKNOWN for the NULL row, FALSE otherwise — and
`x != null` as its negation, which is UNKNOWN and TRUE. The same holds for a field reached through a
to-one relation, where an absent related row is also a missing attribute. A list constant carrying a
null (`x in [null, "a"]`) is still refused, since SQL drops a NULL inside an `IN` list, and an
undeclared attribute under a call-level `OMITTED` is still refused as above.

## Handling refusals

A shape the adapter cannot translate raises rather than emitting a best-effort filter, and the raise
is one of three types so you can route on it without matching the message:

| Exception | Meaning | What to do |
|---|---|---|
| `UnsupportedPlanShapeException` | The plan is well-formed, and SQL as this adapter builds it cannot express it faithfully | Rewrite the policy, or answer that request another way (a per-row `check()`, another store) |
| `UnmappedAttributeException` | The mapping came up short — a variable nothing maps, a relation where a scalar is needed, a column where a collection operator needs a relation, a temporal column whose type does not pin an absolute instant, or [an operand whose type the mapped column does not hold](#the-operands-type-has-to-match-the-columns) | Change the mapping |
| `MalformedPlanException` | The plan violates the planner's wire contract — wrong arity, a lambda whose second operand is not a variable, a conditional plan with no condition, an unknown filter kind | A hand-built plan, or an upstream bug to report |

All three extend `IllegalArgumentException`, so catching that catches every refusal. A branch only an
adapter bug can reach raises `IllegalStateException` instead, deliberately, so it is never mistaken
for a classified refusal.

Every corpus case this adapter refuses is listed in
[`conformance-ledger.json`](conformance-ledger.json) with the mechanism that makes it
inexpressible, and the conformance harness asserts each one raises `UnsupportedPlanShapeException`
or `UnmappedAttributeException` rather than return a filter.

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

Mapping the un-aliased columns instead **fails loudly at execution, never with a wrong row set.**
The predicate names `documents`, the query renamed it to `d`, and the base table is then out of
scope, so the statement is rejected by the database — on H2 an `ExposedSQLException` reading
`Column "DOCUMENTS.OWNER_ID" not found`. That exact wording is one engine's; what the suite pins is
the shape of the failure, which is what matters: the predicate cannot quietly bind to some other
copy of the table and return rows the PDP denies.

The aliases the adapter allocates for **its own** subqueries are `cerbos_1`, `cerbos_2`, … numbered
in walk order, so the emitted SQL is deterministic. Every subquery is aliased, never only on
collision — one relation can be entered twice in one plan, and an unaliased inner table would
capture the outer correlation. **Do not give a table in your own query an alias beginning
`cerbos_`**: that prefix is what keeps the two sets apart.

## Database collation and case sensitivity

> **⚠️ Hard requirement: every string column a mapping references must compare byte-exactly, and
> `LIKE` must be case-sensitive.**

CEL string comparison at the PDP is exact and byte-sensitive: with `R.attr.department == "finance"`,
a `check()` for a resource holding `"Finance"` returns **DENY**. The adapter emits the comparison
with no collation control, so the store decides what matches. Where the store is more permissive
than CEL, the filter returns rows the policy denies — an over-grant, with nothing to notice it by.
Both of the following are store configuration, not adapter limitations:

- **MySQL.** The default `utf8mb4_0900_ai_ci` is case- *and* accent-insensitive, so `=` itself
  over-grants. Use `utf8mb4_0900_bin` (MySQL 8.0.17+) on every string column a mapping references.
  Case-sensitive is not enough: `utf8mb4_0900_as_cs` is still a Unicode collation, which gives a
  default-ignorable code point such as SOFT HYPHEN (U+00AD) no weight, so `'o\u00ADne' = 'one'` is
  TRUE under it and `==` and `in` over-grant while `!=` under-grants
  ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)). `utf8mb4_bin` is byte-exact but
  PAD SPACE, so `'a' = 'a '` is TRUE under it. `utf8mb4_0900_bin` is the one MySQL collation that is
  both byte-exact and NO PAD. The conformance harness creates its MySQL schema with it for exactly
  this reason: its mixed-case seeds return rows the PDP denies under the default, and its
  soft-hyphen seed (`h6`) under `utf8mb4_0900_as_cs`.
- **SQLite.** `LIKE` is case-insensitive for ASCII by default, and this adapter lowers string
  matching (`contains` / `startsWith` / `endsWith`, and hierarchy prefix tests) to `LIKE`. Set
  `PRAGMA case_sensitive_like = ON` on the connection, as the harness does.

- **PostgreSQL.** `=` is byte-exact under any deterministic collation, but `<`, `>` and their
  siblings order strings by the database's collation, and CEL orders them by code point. Under the
  image default `en_US.utf8`, `"One" > "a"` is TRUE, so a string ordering returns rows the PDP denies
  ([#489](https://github.com/cerbos/query-plan-adapters/issues/489)). Use the `C` collation on every
  string column a mapping orders (`--lc-collate=C` at `initdb`, or `COLLATE "C"` on the column). The
  harness initialises its PostgreSQL database with `--lc-collate=C`;
  `ADAPTER_TEST_POSTGRES_INITDB_ARGS` overrides it to reproduce the over-grant.

H2 is case-sensitive and orders by code point by default. PostgreSQL and H2 are safe for `=` and
`LIKE` unless you opt into case-insensitive behaviour (a nondeterministic ICU collation, `citext`).

Role and tenancy checks are the highest-risk shapes: `'admin'` versus `'Admin'` under `eq` and `in`,
and hierarchy descendant checks. Treat collation as part of your policy contract.

## Conformance contract

The adapter replays the shared [conformance corpus](../conformance/README.md): for each recorded
plan of Cerbos PDP 0.55.0 and 0.54.0, it translates the plan, runs the query against 41 seed rows on
H2, SQLite, PostgreSQL and MySQL, and compares the returned ids with the `check()` decisions the PDP
recorded. No PDP runs in the test. Results for the current PDP (0.55.0), where the total is every
golden case of that tier; a case marked as a planner divergence is skipped, and counts toward the
total but not as passed:

| Tier | Passed / total |
| --- | --- |
| core | 26 / 26 |
| extended | 72 / 80 |
| adversarial | 269 / 308 |

The same cases pass on all four stores, and under both MySQL prepared-statement modes. Every case
that does not pass is listed with its reason in [`conformance-ledger.json`](conformance-ledger.json):
40 are `unsupported`, where the adapter throws `UnsupportedPlanShapeException`, or
`UnmappedAttributeException` when the fix is a mapping change, rather than emit a filter. None is
`divergent`. They fall into these families:

- a regex `matches()` pattern `LIKE`, `=` and `REPLACE` cannot spell exactly (an unbounded
  repetition of a literal, a lone `.`, a negated class, an inline flag other than a leading `(?i)`):
  CEL matches with RE2, which no SQL engine implements. See [Regular expressions](#regular-expressions);
- a positional read of a list (`[i]`, `.member` of `[i]`) over a relation that declares no
  `position(column)`, or through a to-many hop: without a position column the rows carry no list
  order to index into;
- `except()`, `filter()` or `map()` used as a value rather than inside `size()` or
  `hasIntersection()`, and whole-list equality against a relation;
- `int()`, `double()` and `timestamp()` over a string, and `%`: SQL `CAST` reads a numeric prefix
  where CEL requires the whole string, and rounds where CEL truncates. `int()` of a numeric column
  compared with a constant translates, solved for the column: `int(x) >= 1` is `x >= 1`,
  `int(x) == 0` is `-1 < x < 1`, UNKNOWN outside `±2^63`, where CEL raises. Arithmetic CEL has no
  overload for on any row — `attr % n` (every attribute number is a double) or `int(...) + attr` —
  is UNKNOWN. Integer arithmetic over `int()` (`int(x) / 2`, `int(x) % 2`) stays refused: whether
  a whole literal beside it is an int or a double is not on the wire;
- `string()` over a computed number (a conversion, a ternary of numbers) rather than a mapped
  column, and over a floating-point column compared with `"0"` or `"-0"`: SQL `CAST` prints numbers
  differently from CEL, and cannot read the sign of a stored `-0.0`. `string()` of a
  boolean-valued expression (a comparison, a connective, a ternary of booleans) compared with a
  literal translates: CEL prints exactly `true` or `false`, so it is the condition or its
  negation;
- a division whose divisor is a floating-point column or computed arithmetic: its zero may be
  `-0.0`, which CEL divides into the opposite infinity, and SQL compares `-0.0` equal to `0.0`;
- a macro or `in` over the to-one `parent` as a map: CEL ranges over its keys, and a related row has
  no key set SQL can read;
- two instant columns compared without `timestamp()`. A comparison between recognised types that differ, or between a scalar column or
  element and a list or map built from constants, is not refused: it is answered from the types.
  See
  [The operand's type has to match the column's](#the-operands-type-has-to-match-the-columns);
- a hierarchy split on the empty delimiter anywhere but between a column and a constant (which
  translates as a code-point prefix test), a division as a divisor, and a macro over a value
  another macro computes. A macro over a literal list — a principal list, or one the planner builds
  with `list()`/`struct()` from constants — folds per element: `exists`, `all`, `exists_one` (up to
  32 elements, as a pairwise exclusion that stays UNKNOWN when any element errors),
  `size(filter(...))` (a strict count, UNKNOWN when any element errors), and `x in list.map(...)`
  (a disjunction of equalities).

`==` and `!=` between two columns under mixed null conventions translate: the definite expansion
over the `EXPLICIT` side, made UNKNOWN when the other side's column is NULL.

The cases the corpus declares a planner divergence (`plannerDivergence`: the plan and `check()`
disagree, so no adapter can pass them) are skipped. Among them is `null/has/missing-attribute`: the
planner folds `has()` to always-allowed while `check()` denies the missing-attribute rows. Until the
planner is fixed, write `R.attr.x != null` rather than `has(R.attr.x)` for database-backed
attributes, with the attribute declared `OMITTED`.

Other guarantees:

- CEL type errors and missing-attribute errors survive negation, and an absent `parent` hop stays
  UNKNOWN under negation rather than becoming false.
- Constant NaN ordering follows Cerbos 0.55: an unordered comparison is false, so its negation is
  true (in 0.54 it was an error and stayed denied under negation).
- `== null` and `!= null` against an attribute declared `field(column, nulls = OMITTED)` are
  UNKNOWN for a NULL column, so the row is denied under both polarities as `check()` denies it;
  declared `EXPLICIT`, `eq`, `ne` and `in` include NULL rows where CEL's null value says they should
  (cerbos/query-plan-adapters#302, #308).

### What a column type buys over a query plan

A query plan names no operand types. That is why the adapters built on a type-blind query builder
need the caller to declare something like `ValueString` or `ValueBool` before they can tell CEL's
two `+` overloads apart, and why the reference fails closed on shapes that need a cast at all. An
Exposed `Column` carries its own type, and the dialect is known at render time, so the same
questions are answered from the mapping you already wrote — with **no per-column type declarations
of any kind**. Seven corpus cases translate here that the reference or most other SQL adapters
refuse:

| Case | What it is, and what settles it |
| --- | --- |
| `cast/string/from-boolean`, `cast/string/from-boolean-case-changed-literal` | `string()` over a boolean column. Deliberately not a `CAST`: SQLite and MySQL store a boolean as 1/0 and would render `"1"` where CEL and PostgreSQL render `"true"`. Against a string literal under `==` or `!=` it is solved for the column (`"true"` is `x = TRUE`, any other spelling such as `"True"` matches no row), which also keeps the comparison out of MySQL's case-insensitive connection collation. Anywhere else it is lowered to a `CASE` with an explicit NULL arm, so a NULL column stays NULL rather than falling through to the `ELSE`, and every engine says the same thing |
| `cast/string/from-double`, `cast/string/from-double-spellings` | `string()` over a floating-point column compared with `==` or `!=`, which CEL renders as Go's shortest `%g` form (`1e+06`, `2`, `-9.5e+18`) and no SQL `CAST` does. Exactly one double prints as a given string, so the comparison is **solved** for the column: `string(x) == "1e+06"` is `x = 1000000.0`, and a literal that is not CEL's spelling of any double matches no row. `"0"` and `"-0"` are refused, since SQL cannot tell `-0.0` from `0.0` (`cast/string/from-negative-zero-double`), and so is an ordering over it. A `DECIMAL` column is refused, because it renders its declared scale (`1.50`) where CEL renders `1.5` |
| `identifier/equals/concatenation`, `string/concatenate/field-to-field` | CEL's `+` over strings — against a constant, and between two columns. One text operand, a string constant or a text column, settles the whole expression, because CEL has no mixed-type `+`. The emitted operator **propagates** NULL: `||`, and `CONCAT()` on MySQL alone, where `||` is logical OR outside `PIPES_AS_CONCAT`. Exposed's own `Concat` and PostgreSQL's `CONCAT()` *skip* a NULL argument, which would compare a partial string and match rows `check()` denies |
| `hierarchy/overlaps/path-built-from-identifier` | A hierarchy path constructed by `list()` rather than read from a column — a shape several SQL adapters refuse |

Guessing arithmetic for `+` is wrong in the dangerous direction: `text + text` is a hard error on
PostgreSQL, `0` on SQLite, and on MySQL an over-grant matching almost every row
([#391](https://github.com/cerbos/query-plan-adapters/issues/391)). The mapped column types are what
make the guess unnecessary.

Two further actions the reference refuses translate here for a different reason. `arithmetic/add/self-division-plus-constant-greater-than`
and `arithmetic/add/self-division-plus-constant-not-equals` compose arithmetic on a division whose denominator may be zero, where CEL
carries a NaN or a signed infinity through the sum and SQL has no value that does. The adapter
carries the division as a symbolic value that branches on a zero denominator, folds each non-finite
arm with IEEE rules at translation time, and sends only the guarded quotient to the database, so no
NaN or infinity is ever bound. The one case that stays refused is a non-finite arm meeting another
**column** under further arithmetic, whose sign no plan can state.

**The second thing the column type buys is not a translation at all — it is a refusal.** Knowing
that `aString` is a `varchar` is what lets this adapter see that `R.attr.aString == P.attr.level`
compares a text column with a number, and refuse it. A type-blind query builder has nothing to see
it with: the plan names no operand types, so the comparison is emitted and the **store** decides
what it means, which on MySQL means coercing the string and matching every row the PDP denies. That
is a whole class of silent over-grant that the declaration-free mapping here closes by construction
and that `ValueString`-style declarations close only for the attributes someone remembered to
declare. [The operand's type has to match the column's](#the-operands-type-has-to-match-the-columns)
is the rule; the over-grant it prevents was reproduced against the pinned MySQL server before it
was, and the corpus's type-mismatch cases now prove the answer on every store.

### `size(string)` counts characters, and astral characters count differently

CEL's `size(string)` counts Unicode code points. The adapter lowers it to `CHAR_LENGTH`, and to
`LENGTH` on SQLite, which has no `CHAR_LENGTH` and whose `LENGTH` already counts characters for a
text value. MySQL's `LENGTH` counts **bytes** and is never emitted: measuring a multibyte string in
bytes would compare it against the wrong threshold and return rows the PDP denies.

String **ordering** follows CEL's code point order on every store. PostgreSQL (`C`), MySQL
(`utf8mb4_0900_bin`) and SQLite compare UTF-8 bytes, which is code point order; H2 compares UTF-16
code units, which puts an astral character before U+E000–U+FFFF. So an ordering against a literal
holding a character at or above U+D800 is rendered on H2 as `STRINGTOUTF8(col) < STRINGTOUTF8(?)`,
and raises when rendered on a dialect outside those four. Two columns ordered against each other
are still compared by the store: on H2 that is code unit order.

What remains is the unit each engine calls a character. H2 counts UTF-16 units, so a character
outside the Basic Multilingual Plane — emoji, some CJK extensions — counts as 2 where CEL counts 1:
`size("héllo🚀")` is 6 in CEL and 7 there. Keep length thresholds away from values that straddle
that difference, or keep `size(string)` out of policies over data that carries astral characters.
The Spring Data adapter documents the same caveat.

Two more rules about `size()`, whatever it counts. Over a **number or boolean column** it is a CEL
no-overload error, which denies, so the comparison is UNKNOWN rather than `CHAR_LENGTH(1)`, which is
`1`; over a column the adapter has no CEL reading for it is refused. And a **NaN threshold** is refused: `size(x) > NaN` is false
under IEEE for every length, and the rounding the threshold arithmetic does has no answer for a NaN
at all. The **infinities** are deliberately not refused — IEEE orders them totally, so
`size(x) > +Infinity` is false and `size(x) < +Infinity` is true for every present value, which is
what a threshold past `Int.MAX_VALUE` already answers correctly.

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
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/all/to-one-chain` and its siblings) | None — a to-one hop is read as a correlated **scalar subquery**, which is NULL when no row correlates, and NULL is already CEL's missing-attribute error, so the row is denied under both polarities with no separate hop guard ([#309](https://github.com/cerbos/query-plan-adapters/issues/309), [#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

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

> [!WARNING]
> **The lambda runs at translation time, outside any transaction** — it is called while the `Op` is
> being built, not while it is rendered. So it must be **pure**: build a predicate out of the alias
> it is handed and nothing else. It must not read `currentDialect` (there is no transaction to read
> one from), must not run a query, and must not depend on request state, because the predicate it
> returns is baked into the `Op` and rendered later, possibly against a different dialect from the
> one you had in mind. It is called once per subquery instance, so a relation entered twice in one
> plan calls it twice, with a different alias each time.

One consequence of a bare-table subquery is worth stating plainly: this adapter is only as correct
as the mapping is honest. A predicate you apply on the read path that builds the resource attributes
but not in `visibleWhen` makes the two disagree, and no conformance case can see it — the PDP
decides from the attributes and the adapter reads the store. Keep one definition of what the
relation contains.

### Known gaps

Real, and not fixed here, because each needs a corpus action first: this repository's rule is that
a translation change starts in the shared corpus, where the question is put to every adapter, not
in one of them. Treat them as constraints on the policies you write.

| Gap | Effect |
| --- | --- |
| A NaN or an infinity **stored** in a floating-point column | The adapter folds the non-finite values it produces itself, from a division, with IEEE rules at translation time. A non-finite value already in a column is compared by the database, whose ordering is not IEEE's: PostgreSQL orders NaN above every number. |

## Dialects

| Dialect | What CI executes | What is distinctive about it |
| --- | --- | --- |
| H2 | the default (`ADAPTER_TEST_DB=h2`), in process | Case-sensitive by default |
| SQLite | `ADAPTER_TEST_DB=sqlite`, in process | Needs `PRAGMA case_sensitive_like = ON`; no boolean and no temporal type, so both are stored as text or integers; a multi-row scalar subquery does not raise |
| PostgreSQL | `ADAPTER_TEST_DB=postgres`, Testcontainers, initialised with `--lc-collate=C` | Needs a code-point collation for string ordering; real `boolean` and `timestamptz`; cannot type a bound `NULL` (`$1 IS NULL` is an error), which is why the adapter renders NULL as a literal |
| MySQL | `ADAPTER_TEST_DB=mysql`, Testcontainers, **twice** — once per Connector/J prepared-statement mode | Needs a case- and accent-sensitive collation; no boolean type; `LIKE` backslash handling and cast spellings differ; coerces a *string* when a comparison mixes types |

These are not the same test four times, which is the point: collation, LIKE escaping, cast targets
and parameter typing are all translator behaviour, so a dialect the harness does not execute is a
dialect this adapter does not cover. The PostgreSQL and MySQL servers are pinned by tag **and**
digest in [`POSTGRES_IMAGE`](POSTGRES_IMAGE) and [`MYSQL_IMAGE`](MYSQL_IMAGE), read at runtime and
declared as inputs of the `test` task. Files rather than Kotlin constants for one reason:
`renovate.json`'s custom manager bumps `<SERVICE>_IMAGE` files and nothing else, so a reference held
in source would never get a Renovate PR.

**MySQL is executed twice**, once per Connector/J prepared-statement mode, as the Spring Data
adapter's MySQL leg is. The driver's default is *client*-side prepared statements, which interpolate
a double bind into the statement as an exact `DECIMAL` literal — and decimal arithmetic is not IEEE
double arithmetic: `3 * 0.1 == 0.3` is true in decimal and false in CEL. An adapter that relied on
the bind's type would over-grant there and nowhere else. This one casts explicitly instead, both
modes return the recorded ids, and running both is what keeps that true, because the mode is chosen in
*your* JDBC URL and the adapter never sees which.

**No corpus case diverges by store.** Every compared case returns the same ids on all four (and
under both MySQL modes), and every refused one is refused on all four, so nothing in the ledger is
conditional on a store. That is a result, not a reason to run fewer legs: it says
the emitted SQL means the same thing on each engine *today*, and each leg is what would notice the
next translator change where it stops doing so. Two of the four are also the only place some of the
choices above are executed at all — the MySQL `CONCAT()` arm and the MySQL `CHAR` cast target are
dead code on every other engine.

It is not a claim that no store-specific divergence exists, only that no *corpus shape* has one. A
code review found one the corpus cannot reach: a comparison mixing a text column with a number,
which MySQL answered by coercing the string and matching every row. The corpus now carries it as
its type-mismatch cases, and the adapter answers it from the types — see
[The operand's type has to match the column's](#the-operands-type-has-to-match-the-columns).

MariaDB is not proved and is therefore not claimed.

## Example application

This repository carries a runnable [`example/`](example/), which installs the adapter as a
**published artifact** (never from source) and uses it against a live PDP over the shared
[demo domain](../demo/README.md):

```bash
# from the repository root
demo/scripts/run-example.sh exposed
```

It is the only place the published surface is executed: the POM's dependency scopes, a consumer
bringing their own Exposed, and the predicate composed with an application-owned filter, paginated,
and handed to a DAO `find`. See [`example/README.md`](example/README.md) for what it proves and what
it does not.

## Development

JDK 17 or later. Gradle comes from the committed wrapper. The suites read `../conformance/`, so
build in a checkout of the **whole repository**; the conformance suite needs Docker on PostgreSQL
and MySQL:

```bash
# From exposed/:
./gradlew build
```

| Variable | Values | Selects |
|---|---|---|
| `ADAPTER_TEST_DB` | `h2` (default), `sqlite`, `postgres`, `mysql` | The store the conformance suite runs on. See [Dialects](#dialects) |
| `ADAPTER_TEST_ORM` | `baseline` (default), `floor` | Exposed 1.5.0, the release `example/` pins, or 1.0.0, the release the published jar compiles against (declared in [`build.gradle.kts`](build.gradle.kts)) |
| `ADAPTER_TEST_CONTAINER_SUITES` | `run` (default), `skip` | Whether the suites that start containers of their own run (below) |
| `ADAPTER_TEST_MYSQL_COLLATION` | e.g. `utf8mb4_0900_ai_ci` | Override the MySQL leg's `utf8mb4_0900_bin` |
| `ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS` | `true` | Run the MySQL leg with server-side prepared statements |
| `ADAPTER_TEST_POSTGRES_INITDB_ARGS` | e.g. `--lc-collate=en_US.utf8` | Override the PostgreSQL leg's `--lc-collate=C` |

An unknown value fails rather than falling back to the default.

## Testing

| Suite | Role | Needs |
|---|---|---|
| `AdversarialConformanceTest` | Conformance: every recorded golden plan of both PDPs replayed, rows compared with the recorded `check()` decisions; exceptions in [`conformance-ledger.json`](conformance-ledger.json) | H2 or SQLite in-process, or Docker for PostgreSQL/MySQL |
| `ContractTest` | Call contract: the null-convention options, a resolver mapping, `maxMacroDepth`, and plans the planner cannot produce | nothing — no database |
| `ExposedSurfaceTest`, `ContractSmokeTest`, `MappingDslTest` | The public surface: `QueryPlanFilter`, the mapping DSL, composition with `where`, `andWhere` and DAO `find` | H2 in-process |
| The translator and review suites | Branches no policy reaches, caller-supplied options, and dialect rendering | H2 and SQLite in-process |

```bash
./gradlew test                              # every suite
ADAPTER_TEST_DB=postgres ./gradlew test     # conformance suite on PostgreSQL
ADAPTER_TEST_DB=mysql ./gradlew test        # … on MySQL (see "Database collation and case sensitivity")
ADAPTER_TEST_ORM=floor ./gradlew test       # every suite on Exposed 1.0.0
```

Two tagged suites start containers of their own, use Docker when it is there, and skip when it is
not: `ReviewOperandTypeTest` (`docker`) shows the coercion behind the operand-type rule on the
pinned MySQL, and `OfflineRendererTest`'s `server-cross-check` cases assert that its stub
PostgreSQL and MySQL connections render byte-identically to the real drivers. Each asks about the
Exposed release or a server image, never about the store or the JDK, so the build excludes both tags
on the non-H2 store legs and wherever `ADAPTER_TEST_CONTAINER_SUITES=skip`, which the workflow sets
on its second JDK leg. `AdversarialConformanceTest` carries neither tag and runs on every leg.

## License

Apache 2.0
