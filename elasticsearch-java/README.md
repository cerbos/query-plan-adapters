# Cerbos + Elasticsearch Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into an [Elasticsearch](https://www.elastic.co/elasticsearch) Query DSL map. This allows you to enforce Cerbos authorization decisions as native Elasticsearch queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`, and the safe
  `matches` subset described below
- Supports `hasIntersection` for array overlap checks
- Supports the hierarchy relations `ancestorOf`, `descendentOf` and `overlaps` between a
  document field and a constant path, lowered to `prefix` / `terms` / `term` queries
- Supports field existence checks via `eq`/`ne` against a null value (the planner emits no
  existence operator)
- Supports polarity-safe `size` checks for array emptiness, over fields the caller declares as
  collections (`nestedPaths` or `collectionFields`)
- Supports polarity-safe `exists` and `all` over nested object arrays
- Supports `hasIntersection` + `map` for projecting and matching nested object fields, with the
  projection on either side
- Preserves missing-field semantics for safe null comparisons and fails closed when explicit null
  cannot be distinguished from a missing field
- Supports millisecond-exact `timestamp()` comparisons when the mapped Elasticsearch field uses a
  `date` type and indexed values obey the same precision contract
- Handles bare boolean variables (e.g. `request.resource.attr.isPublic`)
- Custom operator overrides for full control over query generation
- One immutable `Options` record for every caller declaration, plus positional convenience overloads
- Fails closed with three typed exceptions — `UnsupportedPlanShapeException`,
  `UnmappedAttributeException`, `MalformedPlanException` — all `IllegalArgumentException`s
- Works with both `PlanResourcesResult` (SDK) and `PlanResourcesResponse` (protobuf) inputs

## Requirements

- Java 17+
- [cerbos-sdk-java](https://github.com/cerbos/cerbos-sdk-java) 0.13.0+. The adapter is built and
  tested against 0.20.1, the version [`build.gradle.kts`](build.gradle.kts) pins.
- `protobuf-java`, pinned to the gencode version the cerbos-sdk-java release you chose was built
  with — 4.35.1 for 0.20.1. The SDK declares it at runtime scope only, and an older runtime that
  gRPC drags in transitively throws `RuntimeVersion$ProtobufRuntimeVersionException` at the first
  message decode.
- Elasticsearch 8.x. The baseline the adapter is developed against, and the server
  [`example/`](example/) runs, is pinned in [`ELASTICSEARCH_IMAGE`](ELASTICSEARCH_IMAGE); CI also
  replays the two container-backed suites against the next major, pinned in
  [`ELASTICSEARCH_NEXT_IMAGE`](ELASTICSEARCH_NEXT_IMAGE), as a forward-compatibility leg (see
  [Testing](#testing)).

## Installation

This adapter is not published to Maven Central. Copy the source files directly into your project:

> The build does configure `publishToMavenLocal`, and [`example/`](example/) resolves
> `dev.cerbos:cerbos-elasticsearch` from mavenLocal as a real Maven coordinate. That exists so the
> example executes the POM and Gradle module metadata a consumer would resolve
> ([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)); it is **not** a release, and
> nothing configures the POM metadata or signing Maven Central requires.

1. Copy the whole package directory [`src/main/java/dev/cerbos/queryplan/elasticsearch/`](src/main/java/dev/cerbos/queryplan/elasticsearch/)
   into your project. It is 16 files, and the adapter needs all of them. Five are the public
   surface:
   - `ElasticsearchQueryPlanAdapter.java` — the entry point and its `Options` and `Result` records
   - `OperatorFunction.java` — the type of a caller-supplied operator override
   - `UnsupportedPlanShapeException.java` — a shape the Query DSL cannot express
   - `UnmappedAttributeException.java` — a plan variable the `Options` do not declare
   - `MalformedPlanException.java` — a plan that violates the planner's wire contract

   The rest are package-private collaborators the entry point delegates to, and nothing outside
   the package can name them:
   - `PlanWalker.java` — the one walk over the expression tree, parameterised by `Scope.java`
     (top level or inside a lambda) and `Polarity.java` (which truth value is being proved)
   - `LeafTranslator.java` — one field against one literal: operand resolution, operator
     mirroring, null handling, override routing
   - `CollectionTranslator.java` — `exists` / `all` over nested paths, the value-list fold,
     `hasIntersection` in its flat and `map` forms
   - `HierarchyTranslator.java` — `ancestorOf` / `descendentOf` / `overlaps`
   - `SizeTranslator.java` — `size()` emptiness checks and the collection declaration rule
   - `RegexTranslator.java` — `matches()` and the RE2/Lucene subset it accepts
   - `Queries.java` — the Query DSL builders and each operator's default lowering
   - `PlanValues.java` — plan literals as JDK values, and `timestamp()` validation
   - `Refusals.java` — the exception factories and the refusals raised from more than one place
2. Adjust the `package` declaration in each file to match your project structure.
3. Add the required dependencies. The versions below are the ones [`build.gradle.kts`](build.gradle.kts)
   pins and this adapter is tested against; any cerbos-sdk-java from 0.13.0 works, but the
   `protobuf-java` version must then be the one that SDK release was generated with (its POM
   names it).

### Gradle

```kotlin
dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    implementation("com.google.protobuf:protobuf-java:4.35.1")
}
```

### Maven

```xml
<dependencies>
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
</dependencies>
```

> **Note:** The Cerbos SDK declares protobuf as a runtime-only dependency. You must add `protobuf-java` explicitly, pinned to the gencode version the SDK was built with.

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

### Declaring the index: `Options`

The one-argument call above is the convenience form. Everything the adapter can be told about the
index lives in one immutable record, `ElasticsearchQueryPlanAdapter.Options`, and the positional
overloads (`fieldMap`, `operatorOverrides`, `nestedPaths`, `explicitNullAttributes`) are exactly
that record with the rest left empty:

```java
import dev.cerbos.queryplan.elasticsearch.ElasticsearchQueryPlanAdapter.Options;

Options options = Options.of(fieldMap)
    .withNestedPaths(Set.of("tagObjects"))          // arrays of objects mapped as `nested`
    .withCollectionFields(Set.of("tags"))           // flat arrays of scalars (a `keyword` array)
    .withOperatorOverrides(overrides)
    .withExplicitNullAttributes(Set.of("request.resource.attr.owner"));

Result result = ElasticsearchQueryPlanAdapter.toElasticsearchQuery(plan, options);
```

Every collection is copied on construction and each `with…` returns a new instance, so an
`Options` can be built once and shared. `collectionFields` is the declaration `size()` needs: the
adapter is handed a plan, never a mapping, so it cannot tell `size(tagNames)` from
`size(aString)` — see [Size checks](#size-checks) below.

### Handling refusals

A shape the adapter cannot translate throws rather than emitting a best-effort filter, and the
throw is one of three types so a caller can route on it without matching the message:

| Exception | Meaning | What to do |
|---|---|---|
| `UnsupportedPlanShapeException` | The plan is well-formed but the Query DSL cannot express it without scripts | Rewrite the policy, or answer that request another way (a per-row `check()`, a different store) |
| `UnmappedAttributeException` | The plan names a variable `fieldMap` does not cover, or walks a collection not declared in `nestedPaths` | Add the declaration |
| `MalformedPlanException` | The plan violates the planner's wire contract — wrong arity, a lambda without a variable, a literal CEL itself would reject | A hand-built plan, or an upstream bug to report |

All three extend `IllegalArgumentException`, which remains the documented base type; a caller
catching that keeps working unchanged.

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

An override replaces one named **plan operator**, and exactly these operators and polarities reach it:

| Override key | Reached by |
|---|---|
| `eq` | positive `eq`; negated `eq` (inside the `exists` guard); a bare boolean variable in either polarity; positive `ne` when no `ne` override exists (the `eq` inside `exists AND NOT eq`); negated `ne` |
| `ne` | positive `ne` — the override replaces the whole `exists AND NOT eq` form, guard included |
| `lt`, `le`, `gt`, `ge` | the positive form; a **negated** ordering operator applies the override of its mirror (`!(x < 5)` applies `ge`), because that is the operator it lowers to |
| `in` | positive membership; negated membership (inside the `exists` guard); the null-aware negated form, with the `null` removed |
| `contains`, `startsWith`, `endsWith`, `matches` | the positive form, and the negated form inside the `exists` guard. A `matches` override receives the raw CEL pattern and replaces both the `^literal` prefix lowering and the regex one |
| `hasIntersection` | the flat positive form only |

Nothing else consults an override. The **hierarchy relations** build their `prefix` / `terms` /
`term` clauses from the defaults even though they borrow those shapes — a relation is not
`startsWith` or `in`. The `^literal` form of `matches` emits `prefix` without consulting a
`startsWith` override, for the same reason. The `map()` form of `hasIntersection`, the `size()`
lowerings and every `exists`-shaped null form are built from the defaults too.

### Default operator mappings

| Cerbos operator | Elasticsearch query |
|---|---|
| `eq` | `term`; negated equality with `null` maps to `exists` |
| `ne` | `exists` + `bool.must_not` + `term`; positive `ne null` maps to `exists` |
| `lt`, `gt`, `le`, `ge` | `range` |
| `in` | `terms`; a negated list containing `null` adds an `exists` guard. A map literal (CEL key membership) and a list holding a list or map element throw |
| `contains` | `wildcard` (`*value*`) |
| `startsWith` | `prefix` |
| `endsWith` | `wildcard` (`*value`) |
| `matches` | `prefix` for `^literal`; `regexp` with Lucene optional flags disabled for fully anchored patterns in the validated common subset |
| `timestamp` in comparisons | `term` / `range` against a mapped Elasticsearch `date` field, for millisecond-exact values |
| `hasIntersection` | `terms` (array overlap) |
| `descendentOf` with the field on the descendant side | `prefix` on `<constant-path><delimiter>` |
| `ancestorOf` with the field on the ancestor side | `terms` over the constant path's proper prefixes, or `match_none` when it has none |
| `overlaps` | `bool.should` of both of the above plus a `term` on the whole constant path |
| `ne`/`eq` against `null` | `exists` (ne) / `bool.must_not` + `exists` (eq) |
| Positive non-empty `size` checks (`> 0`, `>= 1`, `!= 0`); negated empty checks | `nested` + `match_all` for a field in `nestedPaths`, `exists` for one in `collectionFields`; any other field throws |
| Positive `exists` (collection) | `nested` + inner query |
| Negated `all` (collection) | `nested` + a definitely-false inner query |
| `hasIntersection` + `map`, with the projection on either side | `nested` + `terms` |
| `exists`/`all` over a literal value list | `bool.should` / `bool.must` of the substituted lambda body (see below) |

A list or map literal on `eq`, `ne`, the ordering operators or the string operators throws: a
`term` or `range` query compares a field against one scalar, and CEL's whole-list equality has no
Query DSL form. A non-finite numeric literal (`NaN`, `±Inf`, which CEL's `1.0 / 0.0` folds to)
throws too, because JSON cannot carry one and a request body would silently quote it as a string.
An integral double is bound as a `long` only inside `[-2^63, 2^63)`; outside that range it stays
a double rather than saturating.

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
excludes a newline while Lucene's dot includes it. Two more constructs are refused because the two
engines read them differently or Lucene rejects them only at query time: an unparenthesised
top-level `|` in an anchored pattern (RE2 parses `^a|b$` as two separately anchored alternatives,
Lucene as the whole field matching `a|b` — write `^(a|b)$`), and a `{` that does not begin a
`{n}`, `{n,}` or `{n,m}` repetition.

### Size checks

`size()` lowers to a presence query — `nested` + `match_all` for a field in `nestedPaths`,
`exists` for one in `collectionFields` — and only for the emptiness spellings (`> 0`, `>= 1`,
`!= 0`, and their negations where the polarity is safe). Any other threshold throws.

The declaration is load-bearing. The adapter is handed a plan, never a mapping, so it cannot tell
`size(R.attr.tagNames)` (an array count) from `size(R.attr.aString)` (a string length) or
`size(R.attr.aNumber)` (a CEL type error): the three plans look identical. `exists` over a string
field matches the indexed empty string, whose CEL size is 0, and over a numeric field matches every
document where CEL raises — both rows the PDP denies. So a `size()` over a field declared in
neither set throws, naming the mechanism, before the threshold is examined.

One under-grant remains once a field *is* declared: Elasticsearch does not index a `null`, so a
flat array whose elements are **all null** (`[null, null]`) is not found by `exists`. CEL counts
those elements and allows the row; the query does not return it. That direction denies a row the
PDP allows, which is the safe one, and it is the mirror of the empty-array limitation below — an
indexed `[]` and a missing field are also the same document to `exists`.

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

`exists_one`, `filter` and `map` have no flat equivalent and throw over a literal value list, as
does a `t.path` reference that the element does not carry.

`except` is not a macro at all. Cerbos `except(list, list)` is a two-list function returning a list
difference — `size(R.attr.tags.except(["x"])) > 0` arrives as `gt(size(except(variable, value-list)), 0)`
— and no lambda form of it exists on the wire. The list difference has no Query DSL translation,
so `except` throws by name wherever it can appear (as a condition, inside `size()`, as a leaf
operand, inside a nested lambda), with the equivalent macro spelling in the message:
`R.attr.tags.exists(t, !(t in ["x"]))`.

### Hierarchy relations

A Cerbos hierarchy is a delimited path, and `ancestorOf` / `descendentOf` / `overlaps` are
statements about **segment** prefixes. Map the field holding the path as `keyword` and the adapter
lowers each relation to term-level queries over the whole stored value:

| Relation | Query |
|---|---|
| the field is a strict **descendant** of the constant | `prefix` on `<constant-path><delimiter>` |
| the field is a strict **ancestor** of the constant | `terms` over the constant path's proper prefixes |
| **overlaps** (inclusive) | `bool.should` of both, plus a `term` on the whole constant path |

`ancestorOf(A, B)` holds when A's segments are a strict prefix of B's, and `descendentOf` is the
same relation with the operands swapped — so which of the two rows above applies depends on the
operand order, not on the operator name. A strict-ancestor test against a single-segment constant
is unsatisfiable (nothing is a proper prefix of one segment), and emits `match_none` rather than an
empty `terms`. Two constant operands are decided at translation time: satisfied means `match_all`,
and unsatisfied is a plan the Cerbos planner should have folded away, so it throws rather than
guessing a filter.

Because `prefix`, `terms` and `term` are term-level queries, path segments containing `%`, `_` or
`[` match literally and need none of the `LIKE ... ESCAPE` machinery the SQL adapters do. The
delimiter is taken from the plan (`hierarchy(R.attr.scope, ":")`), and the constant is the only
side the adapter splits and rejoins — the field's stored value is compared as a raw string. An
empty delimiter throws: splitting on it would produce one segment per character plus a trailing
empty one, and a relation over that segmentation is not the one the policy stated.

**Negated hierarchy relations fail closed.** A SQL adapter gets the exclusion free from
three-valued logic (`NULL LIKE 'x%'` is UNKNOWN, so the row drops), but a `bool.must_not` around a
`prefix`, `terms` or `term` query *matches* a document that has no value for the field — which is
the CEL missing-attribute error the PDP denies on. An `exists`-guarded negation would express it,
exactly as `eq`, `in`, `contains` and `startsWith` already are, and adding one is a change worth
proving against the corpus first.

A hierarchy path **built by `list()` from a document field** also throws: matching it would mean
concatenating a stored value into a path before comparing, and the Query DSL has no computed
operand to do that with — the same missing evaluation step that refuses arithmetic and casts here.

### Unsupported query-plan shapes

The adapter fails closed with `UnsupportedPlanShapeException` (an `IllegalArgumentException`)
for shapes that Elasticsearch Query DSL cannot express safely without scripts, including
field-to-field comparisons, a constant string receiver with a document-field argument, arithmetic
over document fields, conditional values (a CEL ternary, as a condition or as an operand),
`except()`, arbitrary collection counts, `size()` over a field not declared as a collection,
ordered array indexing, positive `all` and negated `exists` over a document collection, negated
membership in a document collection, negated intersection, negated hierarchy relations, hierarchy
paths assembled from a document field or split on an empty delimiter, collection-empty checks,
list and map literals where a scalar is expected, non-finite numeric literals, and membership or
intersection tests that need to distinguish an explicit null value or array element from a
missing field. Unsupported regex syntax and sub-millisecond timestamp literals also fail closed.
Painless scripts are not generated by this adapter because they would change the security and
performance profile of every authorization filter.

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
ElasticsearchQueryPlanAdapter.toElasticsearchQuery(planResult,
        Options.of(fieldMap)
                .withNestedPaths(nestedPaths)
                .withExplicitNullAttributes(Set.of("request.resource.attr.owner")));
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

The adapter is differentially tested against Cerbos PDP 0.54.0 `check()` decisions using 22 hostile seed documents and real Elasticsearch queries. The Spring Data adapter defines the reference semantics for this compatibility snapshot.

| Classification | Coverage |
| --- | --- |
| Oracle-tested | 87 reference conformance actions plus regex and timestamp probes (89 actions) |
| Fail-closed | 105 reference actions plus ordered list indexing/`get-field`, `int()`/`double()` casts and `filter()`/`map()` used as a condition or a conjunct (114 actions total) |
| Representation-independent | `null-eq-missing` — rejected like every other null-selecting comparison, so no NULL-representation option is required |
| Attribute NULL convention | Declared, in order to REFUSE. Elasticsearch does not index a JSON null, so an explicitly-null value and a missing field are the same document to every query the DSL can express. Pass the attributes you send as explicit nulls in `explicitNullAttributes`, and the equality family over them throws instead of answering narrowly — every spelling of `!= "x"` either requires the field to exist (dropping the row CEL allows) or matches every document missing it (cerbos/query-plan-adapters#308) |
| Known planner divergence | `has()` on a missing attribute is folded by the Cerbos planner to `ALWAYS_ALLOWED`, while `check()` denies the missing-attribute documents. Until the planner is fixed, use `R.attr.x != null` for indexed attributes instead of `has(R.attr.x)` |

The oracle-tested set covers value-first comparisons, literal-safe wildcard matching, safe
null/missing polarities, positive `exists` and non-empty collection checks, negated `all`,
millisecond-exact timestamp ordering, chained nested paths, and the hierarchy relations against a
constant path. The fail-closed set comprises the
unexpressible categories above plus positive explicit-null comparisons,
null-sensitive variable membership, positive `all`, negated `exists`, collection-emptiness
predicates that require distinguishing an indexed empty array from a missing field, `size()` over a
field not declared as a collection (a string's length would otherwise read as `exists`, which an
indexed empty string satisfies), an empty hierarchy delimiter, and an anchored `matches()` pattern
whose top-level alternation RE2 and Lucene parse differently.

Every fail-closed shape's error message is pinned in the shared corpus (`conformance/actions.json`) and asserted by this adapter's conformance run, so a classification proves the throw names its declared mechanism rather than merely that something threw.

`ElasticsearchTranslatorTest` asserts the same classification offline, and adds the property the
per-action assertions cannot state: the **distribution of the refusals over the sites in the walk
that raise them**. 115 of the corpus's 205 shapes are refused here — the 114 fail-closed actions
above plus `null-eq-missing` — so it matters whether that happens at one catch-all or at many. It
is twenty sites, and one mechanism — an operand slot holding a computed sub-expression, where
the Query DSL admits only a field and a literal — accounts for 54 of them:

| Rejection site | Actions |
| --- | --- |
| computed leaf operand (arithmetic, casts, a ternary as an operand, `map()`/`filter()`, nested `size()`, a lambda one scope too deep) | 54 |
| field-to-field comparison | 15 |
| explicit null against null, or against a constant | 8 |
| count threshold over a declared collection that is not an emptiness check | 5 |
| string operator whose receiver is the constant | 4 |
| negated `exists` over a collection | 4 |
| positive `all` over a collection | 4 |
| `size()` over a field declared as neither a nested path nor a flat collection | 4 |
| `exists_one`, collection emptiness, negated membership, a ternary as the condition, sub-millisecond timestamp | 2 each |
| `size()` over a computed collection (`filter()`), negated `hasIntersection`, null in a document array, null in an intersection, hierarchy path built from a document field, an empty hierarchy delimiter, a top-level alternation in an anchored `matches()` pattern | 1 each |

The list is asserted **total** — a shape refused by an accident rather than by a declared
limitation matches no site and fails — and the counts are pinned, so a translator change that moves
a shape from one site to another shows up as a diff even though both sites throw and
`conformance/actions.json` is unchanged. Zero refusals are an unmapped field, which is the accident
[#326](https://github.com/cerbos/query-plan-adapters/issues/326) was filed for.

Elasticsearch does not index empty arrays. An `exists` or nested query therefore cannot distinguish an attribute explicitly set to `[]` from an attribute omitted entirely. CEL treats the former as an empty collection and the latter as an evaluation error, so polarity matters: positive `exists`, positive non-empty checks, negated `all`, and negated empty checks remain safe; the opposite polarities throw rather than authorizing a document with a missing collection.

**Behaviour change.** The hierarchy relations `ancestorOf`, `descendentOf` and `overlaps` between a
document field and a constant path now translate. Ten corpus shapes that used to throw
`Unexpected hierarchy expression in leaf operand` return a query, so a caller who was catching that
refusal — or relying on it to route a request elsewhere — now gets a filter instead. A widening
([#332](https://github.com/cerbos/query-plan-adapters/issues/332)). The negated direction and a
path assembled by `list()` from a document field still fail closed; the message for the latter
changed to name its own mechanism rather than the leaf-operand default.

**Behaviour change.** A `size()` comparison with the count on the **right** — `0 < size(R.attr.tags)`, which the planner preserves from policy source order — now translates. The operand scan found the `size()` wherever it sat but did not mirror the operator, so `0 < size(c)` was read as `size(c) < 0` and refused as an unsupported threshold rather than recognised as the emptiness check it is. A widening: a shape that threw now returns a query ([#387](https://github.com/cerbos/query-plan-adapters/issues/387)).

**Behaviour change.** `size()` over a field the caller has declared as neither a nested path nor a
flat collection now **throws**. `size(R.attr.aString) > 0` used to emit `exists aString`, which
matches the indexed empty string CEL says has size 0 — an over-grant — and `size(R.attr.aNumber) > 0`
matched every document where CEL errors. A narrowing: a flat `keyword` array that used to lower to
`exists` with no declaration needs `Options.withCollectionFields(...)` now, and a string or number
that used to return a filter throws. The pinned messages for `string-size`, `size-huge-gt` and
`size-huge-lt` changed accordingly, because the declaration is checked before the threshold. In
the other direction, `size(c) != 0` now translates as the non-emptiness check it is; it used to be
refused as an unsupported threshold.

**Behaviour change.** `except` **throws by name** in every position. The adapter used to translate a
lambda form of `except` to `nested` + `bool.must_not`, but Cerbos `except(list, list)` is a two-list
function and no lambda form exists on the wire, so that translation was unreachable from any real
plan; the real shape (`size(except(...))`, or `except(...)` as a comparison operand) was refused
with an unrelated message. The message now names the function's signature and the equivalent
`exists` macro.

**Behaviour change.** A bare CEL ternary (`ternary-bare`, `w1-ternary-chain-cond`) is refused by a
dedicated message — `if (CEL ternary) cannot be expressed…` — where it used to fall out of the
generic arity check as `if requires exactly 2 operands, got 3`. Same classification, different
pinned string. A ternary that is the body of a nested lambda is refused the same way.

**Behaviour change.** Four shapes that used to **return a filter** now throw: a list or map literal
on `eq`/`ne`/ordering/string operators or as an `in`/`hasIntersection` element, and a map literal as
the collection side of `in`; a non-finite numeric literal; a `null` element in a `hasIntersection`
whose literal sits on the first operand, in a `map()` projection form, or inside a nested lambda
(the second-operand flat form already threw); and a `matches` pattern with a top-level `|` or a
brace that does not begin a `{n}`, `{n,}` or `{n,m}` interval, which Lucene rejected at query time
or read as a different language. A hierarchy constant split on an empty delimiter throws too, where
it used to be split into characters. Each is a narrowing that closes a wrong-filter path.

**Behaviour change.** `hasIntersection` with the `map()` projection on the **second** operand now
translates, mirrored onto the first-operand lowering; it used to be refused as a computed leaf
operand. A widening.

**Behaviour change.** Operator overrides reach more of the walk. An `in` override is now applied to
membership in every polarity (it used to be ignored); a negated ordering operator applies its
mirror's override (`!(x < 5)` applies `ge`) where it used to emit the default `range`; a positive
`ne` with no `ne` override applies the `eq` override inside its `exists` guard where it used to use
the default `term`. A caller who registered one of those overrides sees it applied now. The
[override table](#custom-operator-overrides) is the contract.

**Behaviour change.** An integral double outside `[-2^63, 2^63)` is bound as a double; it used to be
cast to `long`, which saturates to `Long.MAX_VALUE`. Refusals are now typed
(`UnsupportedPlanShapeException`, `UnmappedAttributeException`, `MalformedPlanException`), all
subtypes of the `IllegalArgumentException` that was thrown before, so a caller catching the base
type is unaffected.

### Mapping hazards

The conformance contract above proves the *plan* side — given a policy shape, does the query select the documents `check()` allows. The other half is the *mapping*: **the documents the query reads must be the documents the application put into the resource attributes.** Six ways that can break are catalogued in the shared corpus, and every adapter has to record a position on each of them; those six come first in the table below, in the corpus's order. **Two more rows are appended, specific to this adapter**: an analyzed field mapping, and type-blind term coercion. No other store in the repository rewrites a stored value before comparing it, or coerces a query term onto the field's type, so there is nothing for the other adapters to answer — see `conformance/README.md`, "Mapping hazards", on when a hazard is adapter-specific.

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
| Type-blind term coercion | **Caller-owned** | The attribute's type in the policy must be the field's type in the mapping. Elasticsearch coerces a query term onto the field: `{"term": {"aBool": {"value": "true"}}}` matches a `boolean` field and `{"term": {"aNumber": {"value": "5"}}}` matches a numeric one, while CEL's cross-type equality (`R.attr.aBool == "true"`) is simply `false` and `check()` denies the row. The adapter binds the literal the plan carries and cannot see the mapping, so a policy comparing a string literal against a boolean or numeric attribute — a shape the PDP allows — returns rows here that the PDP denies. Keep the literal's type and the field's type the same, and treat a mapping whose type differs from the attribute's as a bug in the index |

#### Why an analyzed mapping is not something the adapter can reject

The adapter is handed a plan, never an index. It has no way to read your mapping, and no way to tell an exactly-compared field from an analyzed one — the plan looks identical either way. So this is a precondition you own, and the failure is silent: `R.attr.aString == "string"` becomes `{"term": {"aString": {"value": "string"}}}`, which on a `text` field also selects a document whose `aString` is `"a string of words"` (one of its tokens is `string`) and one whose `aString` is `"STRING"` (the standard analyzer lowercases). Both are documents `check()` denies.

The size of the gap is pinned by `ElasticsearchSurfaceTest.anAnalyzedMappingWidensEqualityAndTheKeywordSubFieldRestoresIt` and `…WidensStartsWith`, which index the same four documents under an analyzed mapping and an exact one and compare the two result sets against a real Elasticsearch. Both take their plan from the corpus wire fixture for `cs-eq` / `cs-startswith`, so the only thing that differs between the two runs is the mapping.

**The remedy is the mapping, not an operator override.** If a field has to be `text` for full-text search, give it the standard `keyword` sub-field and point `fieldMap` at the sub-field:

```json
{ "aString": { "type": "text", "fields": { "keyword": { "type": "keyword" } } } }
```

```java
Map.entry("request.resource.attr.aString", "aString.keyword")
```

Do **not** reach for `operatorOverrides` here. An override that swaps `term` for `match` is a best-effort match — it is not the comparison the policy asked for, it applies to every field rather than the analyzed one, and it turns a mapping mistake into an authorization filter that quietly returns more rows. This adapter fails closed everywhere else rather than approximating; an override that approximates gives that up in the one place nothing checks it.

### Nested object queries (collection operators)

When your Cerbos policies use the collection macros `exists` and `all` or `hasIntersection` with `map` on arrays of nested objects, pass a `Set<String>` of Elasticsearch field names that use `nested` mappings:

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
| `!tagObjects.all(t, t.name == "public")` | `nested` + a definitely-false inner query |
| `hasIntersection(tagObjects.map(t, t.name), ["a","b"])` | `nested` + `terms` |

If a collection operator references a field not declared in `nestedPaths`, the adapter throws `UnmappedAttributeException`. Flat `hasIntersection` (without `map`) continues to work without `nestedPaths`; a flat array only needs `collectionFields` when a policy takes its `size()`.

### Elasticsearch field type considerations

- Every field named in `fieldMap` must be mapped to a type Elasticsearch compares exactly: `keyword` for strings, plus `boolean`, the numeric types and `date`. The `term`, `prefix` and `wildcard` queries this adapter emits are exact and case-sensitive on `keyword`.
- A field that also has to serve full-text search should be `text` **with a `keyword` sub-field**, and `fieldMap` should name the sub-field. Pointing `fieldMap` at the analyzed parent over-grants, and an operator override that swaps `term` for `match` is not a fix — see [Analyzed (`text`) field mapping](#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject) above.

## Building

Gradle 8.x (CI pins 8.12) and a JDK 17 or later; the sources are compiled with
`options.release = 17`, so a newer JDK still builds against the Java 17 class library. There is no
Gradle wrapper and no Dockerfile — run Gradle from this directory:

```bash
gradle build --no-daemon
```

## Testing

`gradle build` runs every suite. Run it from a checkout of the **whole repository**: every suite
here reads the shared corpus at `../conformance/`, and the two container-backed suites need Docker
(Testcontainers). Without a local Gradle, the same build runs in the official Gradle image with the
repository root mounted and the Docker socket passed through:

```bash
docker run --rm -v "$(pwd)":/repo -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_RYUK_DISABLED=true --network host \
  -w /repo/elasticsearch-java gradle:8.12-jdk17 gradle build --no-daemon
```

Four suites, and which of them needs a container is the useful distinction:

| Suite | What it asserts | Needs |
|---|---|---|
| `ElasticsearchTranslatorTest` | the Query DSL emitted for every corpus action, against `golden/expectations.json`, and the pinned refusal message for every action `conformance/actions.json` says this adapter must reject | nothing — plans come from `conformance/wire-fixtures/`, so no PDP, no Elasticsearch and no Docker |
| `ElasticsearchAdversarialConformanceTest` | the documents those queries return, against per-row `check()` | a pinned Cerbos PDP and Elasticsearch (Testcontainers) |
| `ElasticsearchSurfaceTest` | what a real server does with an emitted clause, and the store facts the corpus reasons cite — an unindexed empty array (nested and flat), an unindexed JSON null and null array element, an indexed empty string, an analyzed field, Lucene regex including whole-field alternation, wildcard escaping, `date` versus `date_nanos` precision | Elasticsearch (Testcontainers) |
| `ElasticsearchQueryPlanAdapterTest` | the shapes no policy can produce — malformed operands, caller-supplied arguments, literal validation — plus a handful of shapes the corpus does not carry yet, each labelled | nothing |

The Elasticsearch server the two container-backed suites start is pinned in
[`ELASTICSEARCH_IMAGE`](ELASTICSEARCH_IMAGE), as `repo:tag@sha256:...`, and read by
`ElasticsearchTestImage` — and by [`example/run.sh`](example/run.sh), which is why it is a file rather
than a Java constant. `conformance/scripts/validate-corpus.sh` holds one digest per tag repository
wide, so a second spelling of the reference could be left behind on an older server and stay green.

[`ELASTICSEARCH_NEXT_IMAGE`](ELASTICSEARCH_NEXT_IMAGE) pins the next Elasticsearch major, and CI
runs the same build against it as a forward-compatibility leg. It is selected by **filename**, so no
image reference is ever restated outside a `*_IMAGE` file; the example does not run it, because its
client is matched to the baseline's major and `example/run.sh` refuses a mismatch. Locally:

```bash
ELASTICSEARCH_IMAGE_FILE=ELASTICSEARCH_NEXT_IMAGE gradle test --no-daemon
```

## Example application

[`example/`](example/README.md) is a runnable program over the shared
[demo domain](../demo/README.md). It covers the two things none of the suites above can: the
**published** package surface — the example resolves the adapter as a Maven coordinate rather than
compiling its source set, so the POM's dependency scopes are executed — and the **Elasticsearch Java
client**, which nothing above ever hands an emitted clause to.

```bash
# from the repository root
demo/scripts/run-example.sh elasticsearch-java
```

### Regenerating the golden expectations

`golden/expectations.json` holds the Query DSL this adapter is pinned to emit for each corpus
action. It is **data, reviewed as a diff** — never hand-edited, and never regenerated by CI, so a
translator change that moves an emitted query fails the build whatever anyone ran locally:

```bash
docker run --rm -v "$(pwd)":/repo -w /repo/elasticsearch-java \
  gradle:8.12-jdk17 gradle goldenUpdate --no-daemon
```

The entry is the translator's return value **verbatim** — the plan kind, plus the query for a
conditional plan — because the Elasticsearch Query DSL already is JSON and this adapter emits a
`Map<String, Object>` of plain JDK values. There is no rendering step and no client library
involved, so unlike the SQL adapters the file declares no generator version. Object keys are sorted
on the way in, because the adapter builds its queries with `Map.of`, whose iteration order is
randomised per JVM run. Format and rationale: `conformance/README.md`, "Golden expectations".
