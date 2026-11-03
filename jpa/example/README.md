# jpa adapter example application

A runnable program that uses the adapter as a consumer would, over the shared
[demo domain](../../demo/README.md): plain Hibernate ORM bootstrapped from
[`persistence.xml`](src/main/resources/META-INF/persistence.xml), with no Spring, CDI or
application server, running criteria queries against in-memory H2.

## Run it

```bash
# from the repository root
demo/scripts/run-example.sh jpa
```

Needs `docker` (with compose), `jq` and JDK 17+; Gradle comes from the adapter's wrapper. The
runner starts the pinned Cerbos PDP and diffs the program's JSON output against
`demo/expected.json`. This directory's [`run.sh`](run.sh) does the rest:

1. publishes the adapter to mavenLocal (`../gradlew publishToMavenLocal`);
2. checks the published jar contains the adapter and none of the example's classes;
3. builds this example against the published coordinate, writes out its runtime classpath, and
   checks that classpath holds no Spring;
4. runs the program with `java -cp`, which checks the adapter was loaded from the published jar.

Each PDP call has a 30-second deadline, so a stalled RPC fails instead of hanging.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes), in
[`DemoShapes`](src/main/java/dev/cerbos/example/demo/DemoShapes.java):

| Shape | Expression |
| ----- | ---------- |
| 1 — filtered list | `query.where(filter.toPredicate(root, query, cb))` |
| 2 — `KIND_ALWAYS_ALLOWED` | the same call; the predicate is always true |
| 3 — `KIND_ALWAYS_DENIED` | the same call; the predicate is always false |
| 4 — pagination | a `COUNT` query and `setFirstResult` / `setMaxResults` pages ordered by `id`, each query building its own predicate from the one filter |
| 5 — composition | `cb.and(filter.toPredicate(...), <the application's own predicates>)` |

No shape switches on the plan kind. Shape 3 still runs its query, to show the application's own
predicate cannot bring a denied row back.

`public` maps to `isPublic` to show that the policy's attribute name and the entity's field name
may differ. `region` and `archived` are absent from the mapping on purpose: they are the
application's own fields, used only in shape 5.

## How this example resolves the adapter

`dev.cerbos:cerbos-jpa` resolves from **mavenLocal as a real Maven coordinate**, so its POM is
exercised ([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). It is not a
composite build (`includeBuild("..")`), which would substitute the source tree and skip the
published metadata. Two consequences:

- The example declares `cerbos-sdk-java` itself, because the adapter publishes the SDK at
  **runtime** scope and this program names SDK types.
- It declares **no** `protobuf-java` version: the adapter publishes one at runtime scope matching
  the SDK's gencode, and this example is what proves the POM carries it.

[`spring-data/example/`](../../spring-data/example/) runs the same domain through Spring Data
repositories, over this adapter.
