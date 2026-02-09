# Cerbos + Elasticsearch Adapter

An adapter library that takes a [Cerbos](https://cerbos.dev) Query Plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan)) response and converts it into an [Elasticsearch](https://www.elastic.co/elasticsearch) Query DSL map. This allows you to enforce Cerbos authorization decisions as native Elasticsearch queries.

## Features

- Supports logical operators: `and`, `or`, `not`
- Supports comparison operators: `eq`, `ne`, `lt`, `gt`, `le`, `ge`, `in`
- Supports string operators: `contains`, `startsWith`, `endsWith`
- Handles bare boolean variables (e.g. `request.resource.attr.isPublic`)
- Custom operator overrides for full control over query generation
- Works with both `PlanResourcesResult` (SDK) and `PlanResourcesResponse` (protobuf) inputs

## Requirements

- Java 17+
- [cerbos-sdk-java](https://github.com/cerbos/cerbos-sdk-java) 0.13.0+
- Elasticsearch 8.x

## Installation

### Gradle

```kotlin
dependencies {
    implementation("dev.cerbos:cerbos-elasticsearch:0.1.0")
    implementation("dev.cerbos:cerbos-sdk-java:0.13.0")
    implementation("com.google.protobuf:protobuf-java:4.27.1")
}
```

### Maven

```xml
<dependencies>
    <dependency>
        <groupId>dev.cerbos</groupId>
        <artifactId>cerbos-elasticsearch</artifactId>
        <version>0.1.0</version>
    </dependency>
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

### Sending the query to Elasticsearch

The adapter produces a `Map<String, Object>` representing an Elasticsearch Query DSL clause. Serialize it to JSON and pass it to the [Elasticsearch Java Client](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html) using `withJson()`:

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
            Map.of("query", conditional.query())
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

Wrap the Cerbos condition inside a `bool.filter` clause to combine it with your application query without affecting relevance scoring:

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
| `Result.Conditional` | Access depends on resource attributes | Use `query()` as the Elasticsearch query |

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
| `eq` | `term` |
| `ne` | `bool.must_not` + `term` |
| `lt`, `gt`, `le`, `ge` | `range` |
| `in` | `terms` |
| `contains` | `wildcard` (`*value*`) |
| `startsWith` | `prefix` |
| `endsWith` | `wildcard` (`*value`) |

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
