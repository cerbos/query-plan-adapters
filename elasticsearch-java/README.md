# Cerbos + Elasticsearch Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into an [Elasticsearch](https://www.elastic.co/elasticsearch) Query DSL map. This allows you to enforce Cerbos authorization decisions as native Elasticsearch queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`, and the safe
  `matches` subset described below
- Supports `hasIntersection` for array overlap checks
- Supports field existence checks via `eq`/`ne` against a null value (the planner emits no
  existence operator)
- Supports polarity-safe `size` checks for array emptiness
- Supports polarity-safe collection operators for nested object arrays
- Supports `hasIntersection` + `map` for projecting and matching nested object fields
- Preserves missing-field semantics for safe null comparisons and fails closed when explicit null
  cannot be distinguished from a missing field
- Supports millisecond-exact `timestamp()` comparisons when the mapped Elasticsearch field uses a
  `date` type and indexed values obey the same precision contract
- Handles bare boolean variables (e.g. `request.resource.attr.isPublic`)
- Custom operator overrides for full control over query generation
- Works with both `PlanResourcesResult` (SDK) and `PlanResourcesResponse` (protobuf) inputs

## Requirements

- Java 17+
- [cerbos-sdk-java](https://github.com/cerbos/cerbos-sdk-java) 0.13.0+
- Elasticsearch 8.x

## Installation

This adapter is not published to Maven Central. Copy the source files directly into your project:

1. Copy `ElasticsearchQueryPlanAdapter.java` and `OperatorFunction.java` from [`src/main/java/dev/cerbos/queryplan/elasticsearch/`](src/main/java/dev/cerbos/queryplan/elasticsearch/) into your project.
2. Adjust the `package` declaration to match your project structure.
3. Add the required dependencies:

### Gradle

```kotlin
dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.13.0")
    implementation("com.google.protobuf:protobuf-java:4.27.1")
}
```

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>dev.cerbos</groupId>
        <artifactId>cerbos-sdk-java</artifactId>
        <version>0.13.0</version>
    </dependency>
    <dependency>
        <groupId>com.google.protobuf</groupId>
        <artifactId>protobuf-java</artifactId>
        <version>4.27.1</version>
    </dependency>
</dependencies>
```

> **Note:** The Cerbos SDK declares protobuf as a runtime-only dependency. You must add `protobuf-java` explicitly.

## How it works

Cerbos responds to a `PlanResources` request with one of three filter kinds:

- `KIND_ALWAYS_ALLOWED` &mdash; the principal can access all resources of this type. No query filtering needed.
- `KIND_ALWAYS_DENIED` &mdash; the principal cannot access any resources. Return an empty result.
- `KIND_CONDITIONAL` &mdash; Cerbos returns an expression tree that the adapter converts into an Elasticsearch Query DSL map.

The adapter recursively walks the Cerbos expression tree, resolves attribute references through a field map, and produces a `Map<String, Object>` that can be serialized to JSON and sent to Elasticsearch as a query body.

## Usage

```java
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter;
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Result;

import java.util.Map;

// Define how Cerbos attributes map to Elasticsearch field names
Map<String, String> fieldMap = Map.of(
    "request.resource.attr.department", "department",
    "request.resource.attr.status", "status",
    "request.resource.attr.priority", "priority"
);

// Call PlanResources via the Cerbos SDK
PlanResourcesResult plan = cerbos.plan(
    RequestContext.builder()
        .principal(principal)
        .resource(resource)
        .action("read")
        .build()
);

// Convert the plan to an Elasticsearch query
Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, fieldMap);

switch (result) {
    case Result.AlwaysAllowed allowed -> {
        // Run the search without extra filters
    }
    case Result.AlwaysDenied denied -> {
        // Return empty results
    }
    case Result.Conditional conditional -> {
        // Use conditional.query() as the Elasticsearch query body
        // Serialize to JSON and pass to the ES REST client
        String json = objectMapper.writeValueAsString(
            Map.of("query", conditional.query())
        );
    }
}
```

### Example: Cerbos plan to Elasticsearch DSL

Given a Cerbos policy condition like:

```
(request.resource.attr.aBool == true AND request.resource.attr.aString != "string")
OR request.resource.attr.tags.exists(tag, tag.name == "public")
```

With this field map and nested paths:

```java
Map<String, String> fieldMap = Map.of(
    "request.resource.attr.aBool", "aBool",
    "request.resource.attr.aString", "aString",
    "request.resource.attr.tags", "tags"
);
Set<String> nestedPaths = Set.of("tags");
```

The adapter produces:

```json
{
  "bool": {
    "should": [
      {
        "bool": {
          "must": [
            { "term": { "aBool": { "value": true } } },
            {
              "bool": {
                "must": [
                  { "exists": { "field": "aString" } },
                  { "bool": { "must_not": [{ "term": { "aString": { "value": "string" } } }] } }
                ]
              }
            }
          ]
        }
      },
      {
        "nested": {
          "path": "tags",
          "query": { "term": { "tags.name": { "value": "public" } } }
        }
      }
    ],
    "minimum_should_match": 1
  }
}
```

Flat field conditions (`eq`, `ne`, range, string operators) map directly to their Elasticsearch equivalents. Collection operators on nested objects are wrapped in `nested` queries with lambda variables resolved to the nested path (e.g., `tag.name` becomes `tags.name`).

### Sending the query to Elasticsearch

The adapter produces a `Map<String, Object>` representing an Elasticsearch Query DSL clause. Serialize it to JSON and pass it to the [Elasticsearch Java Client](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html) using `withJson()`.

Authorization conditions are access control filters, not relevance signals. Always place them in a **filter context** (`bool.filter` or `constant_score`) so Elasticsearch skips scoring and can cache the result.

```java
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.StringReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

ObjectMapper objectMapper = new ObjectMapper();
Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, fieldMap);

List<Document> documents = switch (result) {
    case Result.AlwaysAllowed ignored -> {
        SearchResponse<Document> resp = esClient.search(
            s -> s.index("my-index"), Document.class
        );
        yield resp.hits().hits().stream().map(h -> h.source()).toList();
    }
    case Result.AlwaysDenied ignored -> Collections.emptyList();
    case Result.Conditional conditional -> {
        String queryJson = objectMapper.writeValueAsString(
            Map.of("query", Map.of(
                "bool", Map.of("filter", List.of(conditional.query()))
            ))
        );
        SearchResponse<Document> resp = esClient.search(
            s -> s.index("my-index").withJson(new StringReader(queryJson)),
            Document.class
        );
        yield resp.hits().hits().stream().map(h -> h.source()).toList();
    }
};
```

#### Combining with your own queries

Place the Cerbos condition in `bool.filter` alongside your application's relevance query in `bool.must`. This keeps authorization out of scoring while still ranking results by relevance:

```java
case Result.Conditional conditional -> {
    Map<String, Object> combined = Map.of("query", Map.of(
        "bool", Map.of(
            "must", List.of(
                Map.of("match", Map.of("title", userSearchTerm))
            ),
            "filter", List.of(conditional.query())
        )
    ));
    String body = objectMapper.writeValueAsString(combined);
}
```

### Handling different result types

The adapter returns a sealed `Result` type with three variants:

| Result type | Meaning | Action |
|---|---|---|
| `Result.AlwaysAllowed` | Principal has unconditional access | Execute search without authorization filter |
| `Result.AlwaysDenied` | Principal has no access | Return empty results, skip the search |
| `Result.Conditional` | Access depends on resource attributes | Use `query()` in a `bool.filter` clause |

### Field mapping

The field map translates Cerbos attribute paths to Elasticsearch field names:

```java
Map<String, String> fieldMap = Map.of(
    "request.resource.attr.department", "department",      // simple field
    "request.resource.attr.owner.email", "owner.email",    // nested field
    "request.principal.attr.role", "role"                   // principal attribute
);
```

Cerbos plans can reference both resource attributes (`request.resource.attr.*`) and principal attributes (`request.principal.attr.*`). Include all paths your policies emit.

### Custom operator overrides

Override the default query generation for any operator:

```java
Map<String, OperatorFunction> overrides = Map.of(
    "eq", (field, value) -> Map.of("match", Map.of(field, value)),
    "contains", (field, value) -> Map.of("match_phrase", Map.of(field, value))
);

Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, fieldMap, overrides);
```

The `OperatorFunction` interface takes a field name and value, and returns a `Map<String, Object>` representing an Elasticsearch query clause.

### Default operator mappings

| Cerbos operator | Elasticsearch query |
|---|---|
| `eq` | `term`; negated equality with `null` maps to `exists` |
| `ne` | `exists` + `bool.must_not` + `term`; positive `ne null` maps to `exists` |
| `lt`, `gt`, `le`, `ge` | `range` |
| `in` | `terms`; a negated list containing `null` adds an `exists` guard |
| `contains` | `wildcard` (`*value*`) |
| `startsWith` | `prefix` |
| `endsWith` | `wildcard` (`*value`) |
| `matches` | `prefix` for `^literal`; `regexp` with Lucene optional flags disabled for fully anchored patterns in the validated common subset |
| `timestamp` in comparisons | `term` / `range` against a mapped Elasticsearch `date` field, for millisecond-exact values |
| `hasIntersection` | `terms` (array overlap) |
| `ne`/`eq` against `null` | `exists` (ne) / `bool.must_not` + `exists` (eq) |
| Positive non-empty `size` checks; negated empty checks | `exists` or `nested` + `match_all` |
| Positive `exists` (collection) | `nested` + inner query |
| Negated `all` (collection) | `nested` + a definitely-false inner query |
| `except` (collection) | `nested` + `bool.must_not` |
| `hasIntersection` + `map` | `nested` + `terms` |
| `exists`/`all` over a literal value list | `bool.should` / `bool.must` of the substituted lambda body (see below) |

`ne`, negated leaf queries, and safe negated membership containing `null` include an `exists` guard.
Cerbos treats a missing attribute as an evaluation error, so a document with no mapped field must
not become authorized merely because an Elasticsearch `must_not` clause matches it.

Elasticsearch does not index a JSON `null` value unless the mapping defines a `null_value` sentinel,
and this adapter does not accept sentinel configuration. Positive equality with `null`, negated
inequality with `null`, and positive membership lists containing `null` therefore throw. The safe
opposite polarities map to `exists`-guarded queries.

For `matches()`, the adapter translates a simple literal prefix such as `^admin` to `prefix` and
fully anchored patterns such as `^admin-[0-9]+$` through the common RE2/Lucene subset. It rejects
partial non-literal patterns, inline flags, RE2 shorthand and Unicode classes, POSIX classes,
interior anchors, empty-string-only patterns, and unescaped `.`. Lucene optional regex operators are
disabled (`flags: NONE`), so characters such as `@` remain literals. Dot is rejected because RE2
excludes a newline while Lucene's dot includes it.

### Collection macros over known values

A macro whose collection the PDP resolves at plan time — typically a principal attribute, as in
`P.attr.teams.exists(t, R.attr.team == t)` — needs no `nestedPaths` declaration. The Cerbos planner
unrolls it into a plain `or`/`and` chain at 10 elements or fewer and ships the lambda with a literal
value-list collection above that (`maxItems = 10` in the planner's struct matcher;
cerbos/cerbos#2570, cerbos/cerbos#2817). The adapter applies the same fold, uncapped, so the emitted
query is equivalent on both sides of that threshold rather than depending on how many teams a given
principal happens to hold. Elements are substituted into the lambda body — a bare `t` becomes the
element, `t.name` drills into it — and the per-element queries combine into `bool.should`
(`exists`) or `bool.must` (`all`).

The restrictions that apply to macros over a `nested` field do **not** apply here. Positive `all`
and negated `exists` fail closed on a nested collection because Elasticsearch cannot distinguish a
missing collection from an empty one; a literal list is fully known at plan time, so both
translate exactly, including under negation. An empty list keeps CEL identity semantics: `exists`
emits `match_none` and `all` emits `match_all`, each flipping under negation.

`exists_one`, `filter`, `map` and `except` have no flat equivalent and throw over a literal value
list, as does a `t.path` reference that the element does not carry.

### Unsupported query-plan shapes

The adapter fails closed with `IllegalArgumentException` for shapes that Elasticsearch Query DSL
cannot express safely without scripts, including field-to-field comparisons, a constant string
receiver with a document-field argument, arithmetic over document fields, conditional values,
arbitrary collection counts, string lengths, ordered array indexing, positive `all` and negated
`exists` over a document collection, negated membership in a document collection, negated
intersection, collection-empty checks, and membership tests that need to distinguish an explicit
null value or array element from a missing field. Unsupported regex
syntax and sub-millisecond timestamp literals also fail closed. Painless scripts are not generated
by this adapter because they would change the security and performance profile of every
authorization filter.

For `timestamp()` comparisons, map the target field as an Elasticsearch `date`; comparing ISO date
strings stored as `keyword` values does not provide reliable instant ordering. Ordinary `date`
fields store epoch milliseconds even when their input format parses additional fractional digits.
The adapter accepts strict RFC 3339 timestamp literals whose offset-normalized instant is within the
CEL timestamp range, rejects sub-millisecond precision, and callers must also ensure the resource
attributes sent to Cerbos and the corresponding indexed values are millisecond-exact. This adapter
has no `date_nanos` mapping mode.

### NULL attribute representation

Other adapters in this repo take a NULL-representation option, because `R.attr.x == null` compiles
to the same `eq(x, null)` plan node whether the caller sends a NULL column as an explicit `null`
attribute (where `check()` allows the document) or omits the attribute entirely (where CEL raises a
missing-attribute error and `check()` denies it) — and a null-selecting filter over-grants in the
second case.

**This adapter needs no such option.** Elasticsearch cannot index an explicit null distinguishably
from a missing field, so every direction that would *select* null documents already fails closed
(`x == null`, and `!(x != null)`), and the two that translate both lower to `exists`, which denies
a document with no value for the field under either convention. `null-eq` and `null-eq-missing` are
fail-closed for the same underlying reason. See
[#302](https://github.com/cerbos/query-plan-adapters/issues/302).

### Attributes you send as explicit nulls

Elasticsearch does not index a JSON `null`, so an explicitly-null value and a missing field are the
same document to every query the Query DSL can express. If your application sends a NULL column to
`check()` as an explicit `null` attribute, name it — the adapter then **refuses** those comparisons
rather than answering them narrowly:

```java
ElasticsearchQueryPlanAdapter.toElasticsearchQuery(
        planResult, fieldMap, Map.of(), nestedPaths,
        Set.of("request.resource.attr.owner"));
```

Under that convention CEL holds a null *value*, so `null != "x"` is TRUE and the PDP allows the row.
Every Elasticsearch spelling of `!= "x"` either requires the field to exist — dropping exactly that
row — or matches every document missing the field, which over-grants a different shape. Neither is
the decision, so the equality family (`eq`, `ne`, `in`) over a declared attribute throws. Ordering
and string operators are unaffected: a null receiver raises a no-overload error in CEL, which denies
exactly as a document missing the field already does.

Leave the set empty (the default) if your application omits attributes for NULL columns — that
convention is the one Elasticsearch's storage already matches. See
[#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

### Conformance contract

The adapter is differentially tested against Cerbos PDP 0.54.0 `check()` decisions using 21 hostile seed documents and real Elasticsearch queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 74 reference conformance actions plus regex and timestamp probes (76 actions) |
| Fail-closed | 113 reference actions plus ordered list indexing/`get-field`, `int()`/`double()` casts and `filter()`/`map()` used as a condition or a conjunct (121 actions total) |
| Representation-independent | `null-eq-missing` — rejected like every other null-selecting comparison, so no NULL-representation option is required |
| Attribute NULL convention | Declared, in order to REFUSE. Elasticsearch does not index a JSON null, so an explicitly-null value and a missing field are the same document to every query the DSL can express. Pass the attributes you send as explicit nulls in `explicitNullAttributes`, and the equality family over them throws instead of answering narrowly — every spelling of `!= "x"` either requires the field to exist (dropping the row CEL allows) or matches every document missing it (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `check()` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for indexed attributes instead of `has(R.attr.x)` |

The oracle-tested set covers value-first comparisons, literal-safe wildcard matching, safe
null/missing polarities, positive `exists` and non-empty collection checks, negated `all`,
millisecond-exact timestamp ordering, and chained nested paths. The fail-closed set comprises the
unexpressible categories above plus hierarchy expressions, positive explicit-null comparisons,
null-sensitive variable membership, positive `all`, negated `exists`, and collection-emptiness
predicates that require distinguishing an indexed empty array from a missing field.

Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

Elasticsearch does not index empty arrays. An `exists` or nested query therefore cannot distinguish an attribute explicitly set to `[]` from an attribute omitted entirely. CEL treats the former as an empty collection and the latter as an evaluation error, so polarity matters: positive `exists`, positive non-empty checks, negated `all`, and negated empty checks remain safe; the opposite polarities throw rather than authorizing a document with a missing collection.

**Behaviour change.** A `size()` comparison with the count on the **right** — `0 < size(R.attr.tags)`, which the planner preserves from policy source order — now translates. The operand scan found the `size()` wherever it sat but did not mirror the operator, so `0 < size(c)` was read as `size(c) < 0` and refused as an unsupported threshold rather than recognised as the emptiness check it is. A widening: a shape that threw now returns a query ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).

### Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the query select the documents `check()` allows. The other half is the *mapping*: **the documents the query reads must be the documents the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them; those six come first in the table below, in the corpus's order. **A seventh row is appended, specific to this adapter**: an analyzed field mapping. No other store in the repository rewrites a stored value before comparing it, so there is nothing for the other nine adapters to answer — see `conformance/README.md`, "Mapping hazards", on when a hazard is adapter-specific.

This adapter **builds no subquery.** A collection is a `nested` field on the same document (see below), so the `nested` query matches inner objects of the document being scored — it never reaches a second index. The emitted DSL contains no `has_child` and no `has_parent`, and its `terms` queries always carry an inline value list rather than a terms *lookup*.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | Not applicable — no subquery | — |
| Default scope on the target model | Not applicable — no second index is read | — |
| Subtype discrimination | **Caller-owned** | The index or alias you send the query to, and any filtered alias on it. The adapter is handed a plan, never an index, so it cannot check that the documents you search are the ones the resource attributes were built from. An alias whose filter differs from the application's own read path, or an index holding several document kinds, needs its discriminator added to your `bool.filter` yourself |
| To-one relation used as a collection | Not applicable — a `nested` field holds exactly the inner objects the application indexed | — |
| Composite association key | Not applicable — no join, so no key to compose | — |
| Absent to-one parent | **Reproduced** for the safe polarities, **rejected** for the rest — `w1-exists-chain`, `w1-size-chain` and `w1-in-chain` are oracle-tested; `w1-all-chain`, `w1-not-exists-chain`, `w1-size-zero-chain`, `w1-size-nonneg-chain`, `w1-not-in-chain`, `w1-not-hasint-chain` and `w1-not-size-chain` are in `adapterUnsupported` and throw | None — it is the empty-array limitation above, not a mapping choice: Elasticsearch cannot tell a document with no parent from a document whose parent has no children, so the polarities that would read that as an allow are refused ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)) |
| Analyzed (`text`) field mapping | **Caller-owned** | `GET <index>/_mapping`. Every field named in your `fieldMap` must be a type Elasticsearch compares exactly — `keyword`, `boolean`, a numeric type, or `date`. A `text` field is tokenized and lowercased before it is indexed, and the `term`, `terms`, `prefix`, `wildcard` and `regexp` queries this adapter emits then run against those tokens rather than against the stored value. See below |

#### Why an analyzed mapping is not something the adapter can reject

The adapter is handed a plan, never an index. It has no way to read your mapping, and no way to tell an exactly-compared field from an analyzed one — the plan looks identical either way. So this is a precondition you own, and the failure is silent: `R.attr.aString == "string"` becomes `{"term": {"aString": {"value": "string"}}}`, which on a `text` field also selects a document whose `aString` is `"a string of words"` (one of its tokens is `string`) and one whose `aString` is `"STRING"` (the standard analyzer lowercases). Both are documents `check()` denies.

The size of the gap is pinned by `ElasticsearchIntegrationTest.analyzedMappingWidensEqualityAndKeywordSubFieldRestoresIt` and `…WidensStartsWith`, which index the same four documents under an analyzed mapping and an exact one and compare the two row sets against a real Elasticsearch.

**The remedy is the mapping, not an operator override.** If a field has to be `text` for full-text search, give it the standard `keyword` sub-field and point `fieldMap` at the sub-field:

```json
{ "aString": { "type": "text", "fields": { "keyword": { "type": "keyword" } } } }
```

```java
Map.entry("request.resource.attr.aString", "aString.keyword")
```

Do **not** reach for `operatorOverrides` here. An override that swaps `term` for `match` is a best-effort match — it is not the comparison the policy asked for, it applies to every field rather than the analyzed one, and it turns a mapping mistake into an authorization filter that quietly returns more rows. This adapter fails closed everywhere else rather than approximating; an override that approximates gives that up in the one place nothing checks it.

### Nested object queries (collection operators)

When your Cerbos policies use collection operators (`exists`, `all`, `except`) or `hasIntersection` with `map` on arrays of nested objects, pass a `Set<String>` of Elasticsearch field names that use `nested` mappings:

```java
Map<String, String> fieldMap = Map.of(
    "request.resource.attr.tags", "tags",           // flat keyword array
    "request.resource.attr.tagObjects", "tagObjects" // nested object array
);

Set<String> nestedPaths = Set.of("tagObjects");

Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, fieldMap, nestedPaths);
```

The corresponding Elasticsearch mapping must declare these fields as `nested`:

```json
{
  "mappings": {
    "properties": {
      "tagObjects": {
        "type": "nested",
        "properties": {
          "id": { "type": "keyword" },
          "name": { "type": "keyword" }
        }
      }
    }
  }
}
```

Collection operators map to Elasticsearch `nested` queries:

| Cerbos expression | Elasticsearch query |
|---|---|
| `tagObjects.exists(t, t.name == "public")` | `nested` + inner condition |
| `tagObjects.all(t, t.name == "public")` | `must_not` + `nested` + `must_not` (double negation) |
| `tagObjects.except(t, t.name == "public")` | `nested` + `must_not` |
| `hasIntersection(tagObjects.map(t, t.name), ["a","b"])` | `nested` + `terms` |

If a collection operator references a field not declared in `nestedPaths`, the adapter throws `IllegalArgumentException`. Flat `hasIntersection` (without `map`) continues to work without `nestedPaths`.

### Elasticsearch field type considerations

- Every field named in `fieldMap` must be mapped to a type Elasticsearch compares exactly: `keyword` for strings, plus `boolean`, the numeric types and `date`. The `term`, `prefix` and `wildcard` queries this adapter emits are exact and case-sensitive on `keyword`.
- A field that also has to serve full-text search should be `text` **with a `keyword` sub-field**, and `fieldMap` should name the sub-field. Pointing `fieldMap` at the analyzed parent over-grants, and an operator override that swaps `term` for `match` is not a fix — see [Analyzed (`text`) field mapping](#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject) above.

## Building

```bash
gradle build --no-daemon
```

## Testing

Unit tests use JUnit 5 with protobuf builders. Integration tests use [Testcontainers](https://testcontainers.com/) to run Elasticsearch 8.x:

```bash
gradle test --no-daemon
```
