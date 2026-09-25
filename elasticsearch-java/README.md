# Cerbos + Elasticsearch Adapter

Translates a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into an [Elasticsearch](https://www.elastic.co/elasticsearch) Query DSL clause, returned as a
`Map<String, Object>` of plain JDK values, so you can enforce Cerbos decisions inside your searches.

## Install

The adapter is not published to Maven Central. Copy the package into your project:

1. Copy the whole directory [`src/main/java/dev/cerbos/queryplan/elasticsearch/`](src/main/java/dev/cerbos/queryplan/elasticsearch/)
   (16 files; the adapter needs all of them) and adjust each file's `package` declaration.
2. Add the dependencies below.

```kotlin
// Gradle
dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    implementation("com.google.protobuf:protobuf-java:4.35.1")
}
```

```xml
<!-- Maven -->
<dependency>
    <groupId>dev.cerbos</groupId>
    <artifactId>cerbos-sdk-java</artifactId>
    <version>0.20.1</version>
</dependency>
<dependency>
    <groupId>com.google.protobuf</groupId>
    <artifactId>protobuf-java</artifactId>
    <version>4.35.1</version>
</dependency>
```

The public surface is five files: `ElasticsearchQueryPlanAdapter` (the entry point, with its
`Options`, `Result` and `ScalarType` types), `OperatorFunction`, and the three exceptions
`UnsupportedPlanShapeException`, `UnmappedAttributeException` and `MalformedPlanException`. The
other files are package-private collaborators (`PlanWalker`, `Scope`, `Polarity`,
`LeafTranslator`, `CollectionTranslator`, `HierarchyTranslator`, `SizeTranslator`,
`RegexTranslator`, `Queries`, `PlanValues`, `Refusals`).

The build configures `publishToMavenLocal` only so [`example/`](example/) can resolve
`dev.cerbos:cerbos-elasticsearch` as a real Maven coordinate
([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)). That is not a release: no
POM metadata or signing for Maven Central is configured.

### Requirements

- Java 17+.
- [cerbos-sdk-java](https://github.com/cerbos/cerbos-sdk-java) 0.13.0+; built and tested against
  0.20.1, the version [`build.gradle.kts`](build.gradle.kts) pins.
- `protobuf-java`, pinned to the gencode version your cerbos-sdk-java release was built with (its
  POM names it; 4.35.1 for 0.20.1). The SDK declares protobuf at runtime scope only, and an older
  runtime that gRPC pulls in transitively throws `RuntimeVersion$ProtobufRuntimeVersionException`
  at the first message decode, so add it explicitly.
- Elasticsearch 8.x. The baseline server is pinned in [`ELASTICSEARCH_IMAGE`](ELASTICSEARCH_IMAGE);
  CI also runs the container-backed suites against the next major, pinned in
  [`ELASTICSEARCH_NEXT_IMAGE`](ELASTICSEARCH_NEXT_IMAGE), as a forward-compatibility leg.
- Accepts both the SDK's `PlanResourcesResult` and the protobuf `PlanResourcesResponse`.

## Quick start

```java
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.ScalarType;
import dev.cerbos.sdk.CerbosBlockingClient;
import dev.cerbos.sdk.PlanResourcesResult;
import dev.cerbos.sdk.builders.Principal;
import dev.cerbos.sdk.builders.Resource;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

// Plan attribute -> Elasticsearch field, plus the CEL type of every compared field
// (keyed by Elasticsearch field name). Build once and share: Options is immutable.
static final Options OPTIONS = Options.of(Map.of(
        "request.resource.attr.ownerId", "ownerId",
        "request.resource.attr.public", "isPublic"))
    .withScalarTypes(Map.of(
        "ownerId", ScalarType.STRING,
        "isPublic", ScalarType.BOOLEAN));

List<Document> visibleDocuments(CerbosBlockingClient cerbos, ElasticsearchClient es) throws IOException {
    PlanResourcesResult plan = cerbos.plan(
            Principal.newInstance("alice", "user"),
            Resource.newInstance("document"),
            List.of("view"));

    Query authz = switch (ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, OPTIONS)) {
        case Result.AlwaysDenied denied -> null;
        case Result.AlwaysAllowed allowed -> Query.of(q -> q.matchAll(m -> m));
        case Result.Conditional conditional -> {
            String json = new ObjectMapper().writeValueAsString(conditional.query());
            yield Query.of(q -> q.withJson(new StringReader(json)));
        }
    };
    if (authz == null) {
        return List.of(); // nothing is visible; skip the search
    }

    return es.search(s -> s.index("documents").query(q -> q.bool(b -> b.filter(authz))),
                    Document.class)
            .hits().hits().stream().map(h -> h.source()).toList();
}
```

`toElasticsearchQuery` returns a sealed `Result`: `AlwaysAllowed` and `AlwaysDenied` carry no
clause, and `Conditional.query()` is the clause to put in a filter context. Any shape the adapter
cannot translate exactly throws (see [Handling refusals](#handling-refusals)); it never emits a
best-effort filter. The `switch` uses pattern matching, standard from Java 21; on Java 17 use an
`instanceof` chain ending in a `throw`, as [`example/`](example/README.md) does.

### Sending the query to Elasticsearch

The clause is plain JSON-shaped data: serialize it (Jackson, or any JSON library) and hand it to the
[Elasticsearch Java client](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html)
with `withJson()`, as above, or send it as a raw request body.

Always put it in a **filter context** (`bool.filter` or `constant_score`): authorization is not a
relevance signal, and a filter skips scoring and can be cached. To combine it with your own search,
put your query in `bool.must` and the Cerbos clause in `bool.filter`:

```java
Map<String, Object> body = Map.of("query", Map.of("bool", Map.of(
        "must", List.of(Map.of("match", Map.of("title", userSearchTerm))),
        "filter", List.of(conditional.query()))));
```

### Handling different result types

| Result | Meaning | Action |
|---|---|---|
| `Result.AlwaysAllowed` | Unconditional access | Search without an authorization filter (or with `match_all`) |
| `Result.AlwaysDenied` | No access | Return no results and skip the search (or search with `match_none`) |
| `Result.Conditional` | Access depends on document fields | Put `query()` in `bool.filter` |

The three stay distinct because Elasticsearch has no clause that costs nothing
([ADR 0009](../docs/adr/0009-elasticsearch-java-keeps-its-result-tagged-union.md)). For one code
path, collapse them yourself to `match_all` / `match_none`, as
[`example/`](example/README.md#how-each-shape-is-expressed) does, at the cost of a search for a
denial you could have skipped.

## Mapping attributes

Everything the adapter knows about your index is in one immutable `Options` record. Start from
`Options.of(fieldMap)`; each `with…` returns a new instance.

```java
Options options = Options.of(fieldMap)
    .withScalarTypes(scalarTypes)                   // CEL type of every compared field (required)
    .withNestedPaths(Set.of("tagObjects"))          // arrays of objects mapped as `nested`
    .withCollectionFields(Set.of("tags"))           // flat scalar arrays whose size() a policy takes
    .withOperatorOverrides(overrides)
    .withExplicitNullAttributes(Set.of("request.resource.attr.owner"));
```

| Option | Keyed by | Needed when |
|---|---|---|
| `fieldMap` | plan attribute → ES field | Always. An unmapped plan variable throws `UnmappedAttributeException` |
| `scalarTypes` | ES field | A policy compares the field. See [Declaring scalar types](#declaring-scalar-types) |
| `nestedPaths` | ES field | A policy runs `exists` / `all` / `hasIntersection(map(...))` over an array of objects |
| `collectionFields` | ES field | A policy takes `size()` of a flat scalar array. See [Size checks](#size-checks) |
| `operatorOverrides` | plan operator | You need a different lowering. See [Custom operator overrides](#custom-operator-overrides) |
| `explicitNullAttributes` | plan attribute | You send NULL columns to `check()` as explicit `null`. See [Attributes you send as explicit nulls](#attributes-you-send-as-explicit-nulls) |

Positional convenience overloads exist — `toElasticsearchQuery(plan, fieldMap)`, and forms adding
`operatorOverrides`, `nestedPaths` and `explicitNullAttributes` — for both input types. They are the
`Options` form with everything else empty, **including `scalarTypes`**, so they translate a
comparison only where an operator override owns the operator.

### Field map

```java
Map<String, String> fieldMap = Map.of(
    "request.resource.attr.department", "department",      // simple field
    "request.resource.attr.owner.email", "owner.email",    // object sub-field
    "request.principal.attr.role", "role");                // principal attribute
```

Include every attribute path your policies emit, resource (`request.resource.attr.*`) and principal
(`request.principal.attr.*`).

### Declaring scalar types

Declare the CEL type of every compared field with `withScalarTypes(Map<String, ScalarType>)`, keyed
by Elasticsearch field name: `STRING`, `NUMBER`, `BOOLEAN` or `TIMESTAMP`. A flat array
(`collectionFields`) declares its element type; a sub-field of a `nested` path is declared by its
full path (`tags.name`).

**This is required.** Every comparison the adapter emits is a term-level query, and Elasticsearch
coerces the query term onto the field's *mapped* type: `{"term": {"aNumber": {"value": "5"}}}`
matches the number `5`, `"true"` matches the boolean `true`, and a numeric term matches the keyword
`"5"`. CEL's cross-type equality is `false`. The adapter cannot see your mapping, so a comparison
(`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `contains`, `startsWith`, `endsWith`, `matches`,
`field in [...]`, collection membership `x in R.attr.list`, and `hasIntersection`, including over a
`map()` projection) against an undeclared field throws `UnmappedAttributeException`
([#496](https://github.com/cerbos/query-plan-adapters/issues/496)). An overridden operator needs no
declaration.

With a declaration, a literal of the wrong type is answered as CEL answers it: `==` matches nothing,
`!=` holds wherever the field is present, a string operator on a non-string field matches nothing in
either polarity, wrong-typed elements are dropped from `field in [...]` and from a `hasIntersection`
list (a list with none of the right type is false), a value of the wrong type is never an element of
a collection (`"2" in R.attr.aNumberList` is false), and a hierarchy relation over a non-string field matches nothing. A
`TIMESTAMP` field needs an explicit `timestamp()` wrapper in scalar comparisons, because the index no
longer holds the original string.

The declaration states the CEL type, not that the field is compared exactly: `STRING` does not
make an analyzed `text` field safe — see [below](#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject).
A declaration that disagrees with the mapping is a caller bug the adapter cannot detect.

### Nested object collections

Declare every array of objects that a collection macro (`exists`, `all`, `hasIntersection` with
`map`) walks, and map it as `nested`:

```java
Options options = Options.of(Map.of(
        "request.resource.attr.tags", "tags",               // flat keyword array
        "request.resource.attr.tagObjects", "tagObjects"))  // nested object array
    .withNestedPaths(Set.of("tagObjects"))
    .withScalarTypes(Map.of("tags", ScalarType.STRING, "tagObjects.name", ScalarType.STRING));
```

```json
{ "mappings": { "properties": {
    "tagObjects": { "type": "nested",
                    "properties": { "id": { "type": "keyword" }, "name": { "type": "keyword" } } }
} } }
```

| Cerbos expression | Elasticsearch query |
|---|---|
| `tagObjects.exists(t, t.name == "public")` | `nested` + inner condition (lambda variable resolved to the path: `t.name` → `tagObjects.name`) |
| `!tagObjects.all(t, t.name == "public")` | `nested` + a definitely-false inner query |
| `hasIntersection(tagObjects.map(t, t.name), ["a","b"])`, projection on either side | `nested` + `terms` |

A macro over a field not in `nestedPaths` throws `UnmappedAttributeException`. Flat
`hasIntersection` (no `map`) needs no declaration. Positive `exists` over a flat scalar array can
translate a lambda equality between the element and a literal; a negated lambda body still needs a
`nested` mapping, because excluding an equality at document level would also exclude an array
holding both a matching and a non-matching element.

Positive `all` and negated `exists` over a document collection throw: Elasticsearch does not index
empty arrays, so it cannot tell `[]` from a missing field, and CEL treats the first as an empty
collection and the second as an error. Positive `exists`, positive non-empty checks, negated `all`
and negated empty checks are safe and translate.

### Size checks

`size()` lowers to a presence query — `nested` + `match_all` for a `nestedPaths` field, `exists` for
a `collectionFields` field — and only for the emptiness spellings: `> 0`, `>= 1`, `!= 0` (count on
either side), and their negations where the polarity is safe. Any other threshold throws.

A `size()` over a field in neither set throws, before the threshold is checked. The plan looks the
same for `size(tagNames)` (array count), `size(aString)` (string length) and `size(aNumber)` (a CEL
type error), and `exists` would match the indexed empty string and every number — rows the PDP
denies.

One under-grant remains: a flat array whose elements are all `null` (`[null, null]`) is not found by
`exists`, because Elasticsearch does not index `null`. CEL allows that row; the query does not return
it. That is the safe direction.

## Nulls

### Attributes you send as explicit nulls

Elasticsearch does not index a JSON `null`, so an explicitly-null value and a missing field are the
same document to every query the DSL can express. If you send NULL columns to `check()` as explicit
`null` attributes, name them and the adapter **refuses** the comparisons it cannot answer:

```java
Options options = Options.of(fieldMap)
    .withScalarTypes(scalarTypes)
    .withExplicitNullAttributes(Set.of("request.resource.attr.owner"));
```

Under that convention `null != "x"` is true in CEL, and every Elasticsearch spelling of `!= "x"`
either drops that row or over-grants another shape, so `eq`, `ne` and `in` over a declared attribute
throw. Ordering and string operators are unaffected: CEL raises a no-overload error on a null
receiver, which denies exactly as a missing field does.

Leave the set empty (the default) if you omit attributes for NULL columns — that is the convention
Elasticsearch's storage already matches. See [#308](https://github.com/cerbos/query-plan-adapters/issues/308)
and [ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

### NULL attribute representation

Other adapters take a NULL-representation option, because `R.attr.x == null` plans identically
whether a NULL column is sent as explicit `null` (allowed) or omitted (a CEL error, denied). **This
adapter needs none.** Every null-*selecting* direction (`x == null`, `!(x != null)`, positive
membership in a list containing `null`) already throws, and the directions that translate
(`x != null`, `!(x == null)`) lower to `exists`, which denies a document without the field under
either convention. `null/equals/null-literal` and `null/equals/null-literal-on-missing-attribute` are fail-closed for the same reason
([#302](https://github.com/cerbos/query-plan-adapters/issues/302)).

`ne`, negated leaf queries and safe negated membership containing `null` carry an `exists` guard, so
a document missing the field is never authorized by a `must_not` matching it — Cerbos treats a
missing attribute as an evaluation error.

> [!WARNING]
> **Do not guard with `has()`: write `R.attr.x != null`.** The Cerbos planner folds `has(R.attr.x)`
> to true and drops it from the plan: alone it plans as `ALWAYS_ALLOWED`, and
> `has(R.attr.x) && R.attr.y > 0` plans as `R.attr.y > 0`. The filter then returns documents missing `x`
> that `check()` denies, and the adapter, which only sees the plan, cannot restore the guard.
> `R.attr.x != null` stays in the plan, where the adapter translates it or refuses it.

## Custom operator overrides

Replace the default lowering of a plan operator with an `OperatorFunction`, which takes the field
name and the value and returns a Query DSL clause:

```java
Map<String, OperatorFunction> overrides = Map.of(
    "contains", (field, value) -> Map.of("match_phrase", Map.of(field, value)));

Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan,
        Options.of(fieldMap).withScalarTypes(scalarTypes).withOperatorOverrides(overrides));
```

Exactly these operators and polarities reach an override:

| Override key | Reached by |
|---|---|
| `eq` | positive `eq`; negated `eq` (inside the `exists` guard); a bare boolean variable in either polarity; positive `ne` when no `ne` override exists (the `eq` inside `exists AND NOT eq`); negated `ne` |
| `ne` | positive `ne` — replaces the whole `exists AND NOT eq` form, guard included |
| `lt`, `le`, `gt`, `ge` | the positive form; a **negated** ordering operator applies its mirror's override (`!(x < 5)` applies `ge`) |
| `in` | positive membership; negated membership (inside the `exists` guard); the null-aware negated form, with the `null` removed |
| `contains`, `startsWith`, `endsWith`, `matches` | the positive form, and the negated form inside the `exists` guard. A `matches` override receives the raw CEL pattern and replaces both the `^literal` prefix lowering and the regex one |
| `hasIntersection` | the flat positive form only |

Nothing else consults an override: the hierarchy relations, the `^literal` `prefix` form of
`matches` (it ignores a `startsWith` override), the `map()` form of `hasIntersection`, the `size()`
lowerings and every `exists`-shaped null form use the defaults.

Do not use an override to paper over an analyzed `text` field (`term` → `match`): that is a
best-effort match applied to every field, and it turns a mapping mistake into a filter that returns
extra rows. Fix the mapping instead — see
[below](#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject).

## Handling refusals

Every refusal is one of three types, all subclasses of `IllegalArgumentException` (still the
documented base type), so you can route on the type rather than the message:

| Exception | Meaning | What to do |
|---|---|---|
| `UnsupportedPlanShapeException` | Well-formed plan the Query DSL cannot express without scripts | Rewrite the policy, or answer that request another way (per-row `check()`, another store) |
| `UnmappedAttributeException` | A variable missing from `fieldMap`, a collection not in `nestedPaths`, or a compared field missing from `scalarTypes` | Add the declaration |
| `MalformedPlanException` | The plan breaks the planner's wire contract — wrong arity, a lambda without a variable, a literal CEL would reject | A hand-built plan, or an upstream bug to report |

## Index mapping requirements

These are correctness requirements: the adapter never sees your mapping, so a wrong one fails
silently by returning documents the PDP denies.

- **Compare exactly.** Every field in `fieldMap` must be `keyword`, `boolean`, a numeric type or
  `date`. A `text` field is tokenized and lowercased, and the `term` / `terms` / `prefix` /
  `wildcard` / `regexp` queries the adapter emits then match tokens. If a field must be `text` for
  full-text search, add a `keyword` sub-field and map to it — see
  [Why an analyzed mapping is not something the adapter can reject](#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject).
- **Declare scalar types that match the mapping** — see [Declaring scalar types](#declaring-scalar-types).
- **Map collections walked by macros as `nested`**, and declare them in `nestedPaths`.
- **Timestamps.** Map `timestamp()` targets as `date`; ISO strings in a `keyword` field do not order
  as instants. There is no `date_nanos` mode. Timestamp literals must be strict RFC 3339 within the
  CEL range and are refused below millisecond precision, and the attributes you send to Cerbos and
  the indexed values must both be millisecond-exact (`date` stores epoch milliseconds even if its
  format parses more digits). If source strings can be malformed, set `ignore_malformed: true` on the
  field and send the original string to Cerbos: the unindexed value is then excluded by a comparison
  and by its negation, matching CEL's conversion error. Without it Elasticsearch rejects the whole
  document. The corpus proves both polarities with an invalid date string.
- **Hierarchy paths** must be `keyword` (see [Hierarchy relations](#hierarchy-relations)).

## Supported operators

| Cerbos operator | Elasticsearch query |
|---|---|
| `and`, `or`, `not` | `bool.must`, `bool.should` (`minimum_should_match: 1`), `bool.must_not` |
| bare boolean variable (`R.attr.isPublic`) | `term` on `true` (`false` when negated) |
| `eq` | `term`; negated `eq null` maps to `exists` |
| `ne` | `exists` + `bool.must_not` + `term`; positive `ne null` maps to `exists` |
| `lt`, `gt`, `le`, `ge` | `range` |
| `in` | `terms`; a negated list containing `null` adds an `exists` guard |
| `contains` / `startsWith` / `endsWith` | `wildcard` (`*value*`) / `prefix` / `wildcard` (`*value`) |
| `matches` | `prefix` for `^literal`; `regexp` (`flags: NONE`) for fully anchored patterns in the safe subset |
| `timestamp()` in comparisons | `term` / `range` against a `date` field, millisecond-exact values only |
| `hasIntersection` | `terms` (array overlap) |
| `descendentOf` / `ancestorOf` / `overlaps` against a constant path | `prefix` / `terms` (or `match_none`) / `bool.should` of both plus a `term` |
| `size()` emptiness checks | `nested` + `match_all`, or `exists` |
| `exists` / negated `all` over a nested collection | `nested` + inner query / `nested` + definitely-false inner query |
| `hasIntersection` + `map` | `nested` + `terms` |
| `exists` / `all` over a literal value list | `bool.should` / `bool.must` of the substituted body |

Literals: a list or map literal where a scalar is expected (`eq`, `ne`, ordering, string operators,
an `in` / `hasIntersection` element) and a map literal as the collection of `in` throw — a `term` or
`range` compares against one scalar. A non-finite number (`NaN`, `±Inf`, e.g. from `1.0 / 0.0`)
throws, because JSON cannot carry it. An integral double is bound as a `long` only inside
`[-2^63, 2^63)`.

### `matches()`

`^admin` becomes `prefix`; a fully anchored pattern such as `^admin-[0-9]+$` becomes `regexp` with
Lucene's optional operators disabled, so `@` and similar stay literal. Refused: partial non-literal
patterns, inline flags, RE2 shorthand and Unicode classes, POSIX classes, interior anchors,
empty-string-only patterns, unescaped `.` (RE2's dot excludes newline, Lucene's does not), an
unparenthesised top-level `|` (RE2 reads `^a|b$` as two anchored alternatives; write `^(a|b)$`), and
a `{` that does not start a `{n}`, `{n,}` or `{n,m}` repetition.

### Collection macros over known values

A macro over a collection the PDP resolves at plan time — typically a principal attribute, as in
`P.attr.teams.exists(t, R.attr.team == t)` — needs no `nestedPaths`. The planner unrolls it to an
`or`/`and` chain at 10 elements or fewer and sends a literal value list above that
(cerbos/cerbos#2570, cerbos/cerbos#2817); the adapter applies the same fold, uncapped, so both sides
of the threshold give the same query. `t` becomes the element and `t.name` drills into it; results
combine with `bool.should` (`exists`) or `bool.must` (`all`). Because the list is fully known,
positive `all` and negated `exists` translate too. An empty list keeps CEL identity: `exists` is
`match_none`, `all` is `match_all`, each flipping under negation. `exists_one`, `filter`, `map`, and
a `t.path` the element does not carry throw.

`except(list, list)` is a two-list function, not a macro, and has no Query DSL form. It throws by
name wherever it appears, suggesting the macro spelling instead:
`R.attr.tags.exists(t, !(t in ["x"]))`.

### Hierarchy relations

Map the path field as `keyword`. The delimiter comes from the plan (`hierarchy(R.attr.scope, ":")`)
and only the constant side is split; the stored value is compared as a raw string, and `%`, `_`, `[`
match literally.

| Relation | Query |
|---|---|
| field is a strict **descendant** of the constant | `prefix` on `<constant-path><delimiter>` |
| field is a strict **ancestor** of the constant | `terms` over the constant's proper prefixes (`match_none` for a single-segment constant) |
| **overlaps** (inclusive) | `bool.should` of both, plus a `term` on the whole constant |

Which row applies depends on operand order, not the operator name (`descendentOf` is `ancestorOf`
with operands swapped). Two constant operands are decided at translation time: `match_all` if true;
if false the planner should have folded it, so it throws. Negated overlap translates as `exists` +
`bool.must_not`, excluding missing fields and absent to-one parents. Negated `ancestorOf` /
`descendentOf`, an empty delimiter, and a path built by `list()` from a document field throw.

### Unsupported shapes

These throw `UnsupportedPlanShapeException`. The adapter never generates Painless scripts, which
would change the security and performance profile of every filter.

- field-to-field comparisons; a constant string receiver with a field argument
- arithmetic over fields, `int()` / `double()` / `string()` casts, conditional values (CEL ternary, as a
  condition or an operand)
- `except()`; `filter()` / `map()` used as a condition; `exists_one`
- counts other than emptiness; `size()` over an undeclared field; collection-empty checks
- ordered array indexing (`R.attr.tagNames[0] == "public"`) — a `term` matches any position
- positive `all` and negated `exists` over a document collection; negated membership in, or
  negated `hasIntersection` over, a document collection
- negated strict hierarchy relations; paths built from a field or split on an empty delimiter
- list/map literals where a scalar is expected; non-finite numbers; unsupported regex syntax;
  sub-millisecond timestamps
- positive comparisons with `null`, and membership or intersection that would need an explicit null
  value or array element told apart from a missing field

## Conformance contract

The adapter is proved against the shared [conformance corpus](../conformance/README.md): the harness
indexes the 41 seed documents in a real Elasticsearch, translates every plan recorded from the
pinned PDPs, runs the query, and compares the returned ids with the ones `check()` allowed. Against
the current PDP (0.55.0), where the total is every golden case in the tier:

| Tier | Passed / total |
| --- | --- |
| core | 25 / 26 |
| extended | 28 / 80 |
| adversarial | 109 / 289 |

Every case that does not pass is either refused with `UnsupportedPlanShapeException`, never answered
with a wrong filter, or skipped as a planner divergence. The refused shapes are those in
[Unsupported shapes](#unsupported-shapes), and
[`conformance-ledger.json`](conformance-ledger.json) lists each one with the reason. Planner-divergence
cases are skipped, not compared, because the recorded plan and `check()` disagree and no adapter can
pass them. On 0.55.0 that is four extended cases and three adversarial cases. In
`null/has/missing-attribute` and `null/has/composed-with-comparison` the planner drops `has()` from
the plan while `check()` denies documents missing the attribute, so use `R.attr.x != null` for
indexed attributes instead of `has(R.attr.x)`. In `arithmetic/add/int-literal-plus-constant` and `arithmetic/add/int-literal-negated` the planner drops the int type of the literal in `R.attr.x + 1`, so the plan is the double spelling's, while `check()` has no double + int overload and denies every row; write `1.0`. In three `composition/*`
cases a DENY condition over a missing attribute does not fire in `check()`, while the plan negates
it and so excludes the document
([#530](https://github.com/cerbos/query-plan-adapters/issues/530)).

## Mapping hazards

The conformance contract proves the *plan* side. The other half is the *mapping*: **the documents
the query reads must be the documents the application built the resource attributes from.** The
first six rows are the hazards every adapter's mapping faces; the last two are specific to this
adapter, since no other store rewrites a stored value or coerces a query term before comparing.

This adapter **builds no subquery**: a collection is a `nested` field on the same document, and the
emitted DSL has no `has_child`, no `has_parent`, and no terms *lookup* — `terms` always carries an
inline list.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second index is read | — |
| Subtype discrimination | **Caller-owned** | The index or alias you query, and any filtered alias on it. The adapter never sees the index, so it cannot check that the searched documents are the ones the attributes were built from. An alias whose filter differs from your read path, or an index holding several document kinds, needs its discriminator added to your `bool.filter` |
| To-one relation used as a collection | Not applicable — a `nested` field holds exactly the inner objects the application indexed | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced** for the safe polarities, **rejected** for the rest — `relation/exists/to-one-chain`, `relation/size/non-empty-to-one-chain` and `relation/in/to-one-chain` pass; `relation/all/to-one-chain`, `relation/exists/negated-to-one-chain`, `relation/size/zero-to-one-chain`, `relation/size/non-negative-to-one-chain`, `relation/in/negated-to-one-chain`, `relation/has-intersection/negated-to-one-chain` and `relation/size/negated-non-empty-to-one-chain` are `unsupported` in the ledger and throw | None — this is the empty-array limitation, not a mapping choice: Elasticsearch cannot tell a document with no parent from one whose parent has no children, so the polarities that would read that as an allow are refused ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |
| Analyzed (`text`) field mapping | **Caller-owned** | `GET <index>/_mapping`. Every field in `fieldMap` must be `keyword`, `boolean`, numeric or `date`. On a `text` field the emitted `term`, `terms`, `prefix`, `wildcard` and `regexp` queries match tokens, not the stored value. See below |
| Type-blind term coercion | **Rejected** without a declaration, **reproduced** with one | `scalarTypes` must state each field's CEL type and agree with the mapping. Elasticsearch coerces a query term onto the field (`"true"` matches a `boolean`, `"5"` a number), while CEL's cross-type equality is `false`. An undeclared field is refused; a declared one is answered as CEL does ([Declaring scalar types](#declaring-scalar-types)). `ElasticsearchSurfaceTest.aTermQueryCoercesItsValueOntoTheMappedTypeWhichIsWhyScalarTypesAreRequired` measures it against a real server ([#496](https://github.com/cerbos/query-plan-adapters/issues/496)) |

### Why an analyzed mapping is not something the adapter can reject

The plan looks the same whatever the mapping, and the adapter never sees the index, so this
precondition is yours and its failure is silent. `R.attr.aString == "string"` becomes
`{"term": {"aString": {"value": "string"}}}`, which on a `text` field also matches
`"a string of words"` (a token is `string`) and `"STRING"` (the analyzer lowercases) — both
documents `check()` denies. `ElasticsearchSurfaceTest.anAnalyzedMappingWidensEqualityAndTheKeywordSubFieldRestoresIt`
and `…WidensStartsWith` measure the gap against a real server, using the recorded
`string/equals/case-sensitive` / `string/starts-with/case-sensitive` plans so only the mapping differs.

**Fix the mapping, not the operator.** Give the field a `keyword` sub-field and map to it:

```json
{ "aString": { "type": "text", "fields": { "keyword": { "type": "keyword" } } } }
```

```java
Map.entry("request.resource.attr.aString", "aString.keyword")
```

An `operatorOverrides` entry swapping `term` for `match` is not the comparison the policy asked for,
applies to every field, and quietly returns more rows.

## Behaviour changes

- **Breaking.** A comparison against a field with no `scalarTypes` declaration throws
  `UnmappedAttributeException` instead of emitting a query Elasticsearch would coerce
  ([#496](https://github.com/cerbos/query-plan-adapters/issues/496)). The positional overloads
  carry no scalar types, so they translate comparisons only through overrides.
- **Breaking.** Collection membership (`x in R.attr.list`) and `hasIntersection`, flat or over a
  `map()` projection, need the collection's element type in `scalarTypes` too, and drop a literal
  of the wrong type instead of letting Elasticsearch coerce it (`"2"` matched a `double` element 2.0).
- **Breaking.** `size()` over a field declared in neither `nestedPaths` nor `collectionFields`
  throws. `size(aString) > 0` used to emit `exists` (over-granting the empty string) and
  `size(aNumber) > 0` matched rows where CEL errors. A flat `keyword` array now needs
  `withCollectionFields(...)`. The pinned messages for `size/greater-than/string-length`, `size/greater-than/huge-threshold` and
  `size/less-than/huge-threshold` changed. `size(c) != 0` now translates.
- **Breaking.** Four shapes that returned a filter now throw: list/map literals where a scalar is
  expected (and a map literal as `in`'s collection); non-finite numbers; a `null` element in a
  `hasIntersection` literal on the first operand, in a `map()` projection, or inside a nested
  lambda; and a `matches` pattern with a top-level `|` or a non-interval `{`. A hierarchy constant
  with an empty delimiter also throws instead of splitting into characters.
- **Breaking** (for callers with overrides). Overrides reach more of the walk: an `in` override
  applies in every polarity, a negated ordering operator applies its mirror's override, and a
  positive `ne` with no `ne` override applies the `eq` override inside its `exists` guard. The
  [override table](#custom-operator-overrides) is the contract.
- Hierarchy relations `ancestorOf`, `descendentOf`, `overlaps` between a field and a constant path
  translate; 10 corpus shapes that threw `Unexpected hierarchy expression in leaf operand` now
  return a query ([#332](https://github.com/cerbos/query-plan-adapters/issues/332)). A `list()`-built
  path still throws, with its own message.
- `size()` with the count on the right (`0 < size(R.attr.tags)`) translates; it used to be misread as
  `size(c) < 0` and refused ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).
- `hasIntersection` with the `map()` projection on the second operand translates.
- Positive `exists` over a flat scalar collection translates a lambda equality with a literal.
- `except` throws by name in every position, naming the two-list signature and the `exists`
  equivalent. The old lambda-form translation was unreachable from any real plan.
- A bare CEL ternary (`conditional/ternary/boolean-branches`, `relation/ternary/to-one-chain-condition`, and a ternary lambda body) is refused
  with `if (CEL ternary) cannot be expressed…` instead of `if requires exactly 2 operands, got 3`.
- An integral double outside `[-2^63, 2^63)` is bound as a double instead of saturating to
  `Long.MAX_VALUE`.
- Refusals are typed (`UnsupportedPlanShapeException`, `UnmappedAttributeException`,
  `MalformedPlanException`); all extend `IllegalArgumentException`, so existing catches still work.

## Example application

[`example/`](example/README.md) runs the shared [demo domain](../demo/README.md) against a real
Elasticsearch through the official Java client, resolving the adapter as a published Maven
coordinate so its POM scopes are exercised:

```bash
# from the repository root
demo/scripts/run-example.sh elasticsearch-java
```

## Development

### Building

JDK 17+; sources compile with `options.release = 17`. Gradle comes from the committed wrapper. Run
`./gradlew build` from this directory, in a checkout of the **whole repository** (the suites read
`../conformance/`). The container-backed suites need Docker.

### Testing

`./gradlew build` runs all four suites:

| Suite | What it asserts | Needs |
|---|---|---|
| `ElasticsearchAdversarialConformanceTest` | the conformance harness: every recorded golden plan, for both pinned PDPs, returns exactly the documents `check()` allowed, or throws `UnsupportedPlanShapeException` where the ledger says `unsupported` | Elasticsearch (Testcontainers); no PDP |
| `ElasticsearchTranslatorTest` | what can be asked without a store: the null-convention and timestamp-precision refusals, that an unmapped field is not a refusal, and rules over every emitted query (fields mapped, nested scopes, no null literal, escaped wildcards, plain JDK values) | nothing — plans come from `conformance/golden/` |
| `ElasticsearchSurfaceTest` | what a real server does with emitted clauses, and the store facts the corpus reasons cite — unindexed empty arrays (nested and flat), JSON nulls and null elements, an indexed empty string, analyzed fields, Lucene regex including whole-field alternation, wildcard escaping, `date` vs `date_nanos` precision | Elasticsearch (Testcontainers) |
| `ElasticsearchQueryPlanAdapterTest` | shapes no policy can produce — malformed operands, caller-supplied arguments, literal validation — plus a few labelled shapes the corpus does not carry yet | nothing |

The server is pinned in [`ELASTICSEARCH_IMAGE`](ELASTICSEARCH_IMAGE) (`repo:tag@sha256:...`), read
by the suites and by [`example/run.sh`](example/run.sh). To run against the next major, which the
example does not do (its client must match the baseline major):

```bash
ELASTICSEARCH_IMAGE_FILE=ELASTICSEARCH_NEXT_IMAGE ./gradlew test
```
