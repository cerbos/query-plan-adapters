# elasticsearch-java adapter example application

A runnable program that uses the adapter as a consumer would, over the shared
[demo domain](../../demo/README.md): it hands the Query DSL map the adapter returns to the official
Elasticsearch Java client and runs it against a real Elasticsearch server.

## Run it

```bash
# from the repository root
demo/scripts/run-example.sh elasticsearch-java
```

Needs `docker` (with compose), `curl`, `jq` and JDK 17+; Gradle comes from the adapter's wrapper.
The runner starts the pinned Cerbos PDP and diffs the program's JSON output against
`demo/expected.json`. This directory's [`run.sh`](run.sh) does the rest:

1. publishes the adapter to mavenLocal (`../gradlew publishToMavenLocal`);
2. checks the published jar contains the adapter and none of the example's classes;
3. checks the Elasticsearch client's major matches [`../ELASTICSEARCH_IMAGE`](../ELASTICSEARCH_IMAGE)'s;
4. starts Elasticsearch;
5. builds this example against the published coordinate and writes out its runtime classpath;
6. waits for Elasticsearch;
7. runs the program with `java -cp`, which checks the adapter was loaded from the published jar.

Each PDP call has a 30-second deadline, so a stalled RPC fails instead of hanging.

## What it demonstrates

All [five usage shapes](../../demo/README.md#the-five-usage-shapes):

### How each shape is expressed

| Shape | Expression |
| ----- | ---------- |
| 1 — filtered list | the adapter's clause, parsed into a `Query` with `withJson`, as the single entry of `bool.filter` |
| 2 — `KIND_ALWAYS_ALLOWED` | `Result.AlwaysAllowed` carries no clause; the application supplies `match_all` |
| 3 — `KIND_ALWAYS_DENIED` | `Result.AlwaysDenied` carries no clause; the application supplies `match_none` |
| 4 — pagination | the same `bool.filter`, plus `from`/`size` and a `sort` on the `id` keyword field |
| 5 — composition | two more entries in the same `bool.filter`, built with the client's own `TermQuery` builders |

`bool.filter` throughout, not `bool.must`: authorization is a filter, not a relevance signal, so
Elasticsearch skips scoring and can cache it, and composition (shape 5) is a list append.

Shapes 2 and 3 run a real search (`match_all` / `match_none`) rather than skipping the round trip
the [adapter's README](../README.md#handling-different-result-types) allows, because shape 5 has to
show that the application's own predicate cannot resurrect a denied document. An unconditional allow
is its own `Result` variant with no clause, so unlike
[`langchain-chromadb/example/`](../../langchain-chromadb/example/README.md) (where an empty `{}`
filter is rejected by Chroma) there is no empty clause to forward by mistake; an empty
`{"query":{"bool":{"filter":[]}}}` was measured against the pinned server and is accepted anyway.

The adapter emits `bool.minimum_should_match` as the integer `1`, which Elasticsearch accepts but
the client models as a string. The client parses it; this example is the only place that is
checked, and the demo policy (`public || ownerId == principal.id`) exercises it in shape 1.

### The field map and scalar types

```java
private static final Map<String, String> DOCUMENT_FIELDS = Map.of(
        "request.resource.attr.ownerId", "ownerId",
        "request.resource.attr.public", "isPublic");

private static final Options OPTIONS = Options.of(DOCUMENT_FIELDS)
        .withScalarTypes(Map.of(
                "ownerId", ScalarType.STRING,
                "isPublic", ScalarType.BOOLEAN));
```

Without a field map entry the adapter throws. `public` maps to `isPublic` to show that the policy's
attribute name and the index's field name may differ. `region` and `archived` are absent on purpose:
they are the application's own fields, used only in shape 5. The scalar types are required because
Elasticsearch coerces a term onto the field's mapped type (`{"term": {"isPublic": "true"}}` matches
`true`, while CEL's `R.attr.public == "true"` is false); the keys are Elasticsearch field names, and
the types are what `DemoIndex` maps each field as.

### The index mapping

`DemoIndex.recreate` writes an explicit mapping with `dynamic: strict`. Dynamic mapping would make
string fields `text`, and the adapter's `term` query for `ownerId == "alice"` would then match
tokens rather than the stored value — the
[analyzed field hazard](../README.md#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject)
the adapter cannot detect.

### Java 17

The example compiles with `options.release = 17`, the [declared floor](../README.md#requirements),
so [`DemoShapes.authorization`](src/main/java/dev/cerbos/example/demo/DemoShapes.java) matches the
sealed `Result` with an `instanceof` chain rather than a pattern-matching `switch` (standard from
Java 21).

## Layout

| Path                   | What it is                                                                              |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `run.sh`               | publish → check the artifact → start Elasticsearch → build → run. Prints the JSON document on stdout. |
| `build.gradle.kts`     | The coordinate, the exact dependency versions Renovate manages, and `writeRuntimeClasspath`. |
| `settings.gradle.kts`  | Notably has no `includeBuild("..")`; see [Packaging](#packaging).                          |
| `DemoApplication.java` | Entry point: the three inputs it is handed, the provenance check, the emitted document.  |
| `DemoShapes.java`      | One method per usage shape, plus the field map, the scalar types and the plan call.      |
| `DemoIndex.java`       | The store: client wiring, the explicit mapping, and `Map` → `Query`.                     |
| `DemoSeeds.java`       | `demo/seeds.json`, parsed. The principals are looked up here, never written out.         |

The program is launched with `java -cp` from a classpath Gradle writes out, not via `installDist`,
a fat jar or `JavaExec`; `build.gradle.kts` explains what each of those loses. There is no
`gradle.lockfile` (see [below](#why-the-renovate-gate-bites-and-why-there-is-no-lockfile)).

This is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/)
([ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)). It runs one
Elasticsearch major under one JDK.

## Packaging

The four suites under [`../src/test`](../src/test) compile against the adapter's source set, so its
POM and Gradle module metadata, dependency scopes included, are exercised nowhere else. This example
resolves `dev.cerbos:cerbos-elasticsearch` from mavenLocal as a real Maven coordinate
([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). It is also the only place
the Elasticsearch client parses an emitted clause: `ElasticsearchTranslatorTest` compares maps to a
golden asset, and `ElasticsearchAdversarialConformanceTest` posts raw JSON over a bare `HttpClient`.

It is **not a Gradle composite build**. `includeBuild("..")` would substitute the local project for
the coordinate and resolve neither the POM nor the module metadata, so wrong scopes would still
compile and pass. The adapter's POM puts both its dependencies at runtime scope:

```xml
<dependency>
  <groupId>dev.cerbos</groupId><artifactId>cerbos-sdk-java</artifactId>
  <version>0.20.1</version><scope>runtime</scope>
</dependency>
<dependency>
  <groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId>
  <version>4.35.1</version><scope>runtime</scope>
</dependency>
```

So [`build.gradle.kts`](build.gradle.kts) declares `cerbos-sdk-java` itself (the program names
`PlanResourcesResult`, and a runtime-scoped transitive is not on the compile classpath) and does
**not** declare `protobuf-java`, which reaches the runtime classpath through the adapter's metadata.

### The break-tests

Each row is one deliberate edit, reverted afterwards: what `demo/scripts/run-example.sh
elasticsearch-java` did, and what the adapter's own `gradle build` did.

| Break                                                                     | The example                                                                        | `gradle build` |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | -------------- |
| Adapter source made to throw, republished at the **same version** `0.1.0` | fails — `UnsupportedOperationException` from inside the published jar               | fails too (it compiles the same source) |
| Adapter's `protobuf-java` given a `strictly("3.25.5")` downgrade          | fails at resolution, naming `'dev.cerbos:cerbos-elasticsearch:0.1.0' (runtimeElements) --> 'com.google.protobuf:protobuf-java:{strictly 3.25.5}'` | unaffected — the adapter itself resolves fine at 3.25.5 |
| The example's compiled classes added to the adapter's `jar`               | fails — `run.sh` step 2 refuses the artifact before starting anything               | unaffected |
| `includeBuild("..")` added to `settings.gradle.kts`                       | **builds and resolves happily**, then fails at startup: the adapter was loaded from `../build/libs/` | unaffected |
| A dependency version changed to the dynamic `8.15.+`                      | fails at resolution: "Resolution strategy disallows usage of dynamic versions"       | unaffected |
| The Elasticsearch client version changed to `9.0.0`                       | fails at `run.sh` step 3, before the server starts: "the majors must match"           | unaffected |

Row 2 proves the published *metadata* is resolved: the downgrade exists only in the adapter's POM and
module file, and the failure names its `runtimeElements` variant. Row 4 proves the coordinate is not
quietly substituted: a composite build is green at build and resolution time, and only the runtime
check in [`DemoApplication`](src/main/java/dev/cerbos/example/demo/DemoApplication.java) catches it.

**What row 2 also measured.** gRPC's transitive graph does ask for older protobuf (`grpc-protobuf`
3.25.8, `protovalidate` 4.34.1, `dev.cel` 4.33.5), but cerbos-sdk-java 0.20.1 (as 0.19.0 before it)
already requires `protobuf-java:4.35.1` at runtime scope, and that wins. For any consumer declaring
the SDK, the adapter's own protobuf declaration changes nothing today; it is a floor in case the SDK
relaxes its requirement. Do not copy the opposite claim from the other Java adapter's comments.

**The same-version hazard from [`sqlalchemy/example/`](../../sqlalchemy/example/README.md) does not
reproduce here** (there, pip kept a previous same-version install). Measured:
`publishToMavenLocal` overwrites `0.1.0` unconditionally, and Gradle re-reads a `mavenLocal()` module
on every build — with `isChanging = true` and the zero cache TTL both removed, a warmed cache still
picked up a broken republish. Both settings stay in `build.gradle.kts` anyway: the second finding is
Gradle behaviour, not something this build states, and it stops holding if `mavenLocal` is swapped
for another repository.

### The client version is checked against the server pin

`build.gradle.kts` must name the client version as a literal (so a Renovate bump touches this
directory; see below), which makes it a second spelling of [`../ELASTICSEARCH_IMAGE`](../ELASTICSEARCH_IMAGE)'s
tag. `run.sh` step 3 holds them together, as `pgx/example/run.sh`'s DSN-port check does. It compares
**majors**: an 8.x client refuses a 9.x server, and the adapter declares Elasticsearch 8.x, while
within a major Elastic supports a client at or below the server's minor. Exact equality would fail
every client bump until the separate image bump (`renovate.json` manages `ELASTICSEARCH_IMAGE`
through a regex custom manager) landed too.

### The example stays out of the published artifact

Java has no `files` allowlist or nested-module exclusion, so ADR 0002 asks for this to be checked.
`example/` is a **separate Gradle build** with its own [`settings.gradle.kts`](settings.gradle.kts),
not a source set of the adapter, so `../build.gradle.kts`'s `jar` task cannot reach it. `run.sh`
step 2 asserts it both ways on every run: the jar must not contain `dev/cerbos/example/`, and must
contain `ElasticsearchQueryPlanAdapter.class` (otherwise an empty jar would pass).

## Why the Renovate gate bites, and why there is no lockfile

`renovate.json` automerges every non-major bump. The `example` job in
[`.github/workflows/elasticsearch-java.yaml`](../../.github/workflows/elasticsearch-java.yaml) is
what blocks that automerge when an Elasticsearch-client or Cerbos-SDK bump breaks real usage — only
while it stays in that workflow, as the comment on the job says. For the job to run, the bump must
touch `elasticsearch-java/**`, which two things ensure:

1. **Every version is an exact literal**, here or in `../build.gradle.kts`. Renovate only opens a PR
   when a release falls outside the declared constraint, so a dynamic selector (`8.15.+`,
   `latest.release`, `[8.15,8.16)`) would absorb releases silently — the hole
   [#424](https://github.com/cerbos/query-plan-adapters/issues/424) found with `sqlalchemy>=2.0`.
2. **`failOnDynamicVersions()`** makes that a checked property: a dynamic selector fails resolution
   (break-test row 5).

**There is no `gradle.lockfile`**, although the examples on npm, PDM and Go commit theirs.
(`spring-data/example/` has none either, and predates the question.) Renovate can maintain a Gradle
lockfile only by running `./gradlew … --write-locks`, and only where a self-hosted administrator has
enabled `allowedUnsafeExecutions: ["gradleWrapper"]`, and the hosted app does not. A committed lockfile would go stale on the first bump and fail every bump PR
for a reason that is not the bump; a gate that is always red distinguishes nothing.

What stays uncovered: **transitive** versions are not pinned. That gap is smaller in Gradle than in
npm or Python, because a Maven POM declares one version rather than a range, so a new transitive
release does not enter the build on its own. Locking becomes the right answer once Renovate can run
the Gradle wrapper here.
