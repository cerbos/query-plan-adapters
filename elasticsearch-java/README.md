# Cerbos + Elasticsearch Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into an [Elasticsearch](https://www.elastic.co/elasticsearch) Query DSL map. This allows you to enforce Cerbos authorization decisions as native Elasticsearch queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`
- Supports `hasIntersection` for array overlap checks
- Supports `isSet` for field existence checks
- Supports `size` comparisons for array emptiness checks
- Supports collection operators (`exists`, `all`, `except`) for nested object arrays
- Supports `hasIntersection` + `map` for projecting and matching nested object fields
- Handles null values (`eq`/`ne` with null maps to `exists` queries)
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
            { "bool": { "must_not": [{ "term": { "aString": { "value": "string" } } }] } }
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
| `eq` | `term` (or `bool.must_not` + `exists` when value is `null`) |
| `ne` | `bool.must_not` + `term` (or `exists` when value is `null`) |
| `lt`, `gt`, `le`, `ge` | `range` |
| `in` | `terms` |
| `contains` | `wildcard` (`*value*`) |
| `startsWith` | `prefix` |
| `endsWith` | `wildcard` (`*value`) |
| `hasIntersection` | `terms` (array overlap) |
| `isSet` | `exists` (true) / `bool.must_not` + `exists` (false) |
| `size` (via comparison) | `exists` / `bool.must_not` + `exists` |
| `exists` (collection) | `nested` + inner query |
| `all` (collection) | `bool.must_not` + `nested` + `bool.must_not` |
| `except` (collection) | `nested` + `bool.must_not` |
| `hasIntersection` + `map` | `nested` + `terms` |

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

- Use `keyword` type for string fields that need exact matching (`term`, `prefix`, `wildcard` queries are case-sensitive on `keyword` fields).
- Use `text` type with a custom operator override (`match` instead of `term`) for full-text search fields.

## Building

```bash
gradle build --no-daemon
```

## Testing

Unit tests use JUnit 5 with protobuf builders. Integration tests use [Testcontainers](https://testcontainers.com/) to run Elasticsearch 8.x:

```bash
gradle test --no-daemon
```
