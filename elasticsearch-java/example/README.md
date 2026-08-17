# elasticsearch-java adapter example application

A runnable program that uses the adapter the way a consumer would — handing the Query DSL map it
returns to the official Elasticsearch Java client — over the shared
[demo domain](../../demo/README.md), against a real Elasticsearch server.

```bash
# from the repository root
demo/scripts/run-example.sh elasticsearch-java
```

Needs `docker` (with compose), `curl`, `jq`, Gradle 8.x and a JDK 17+. The runner starts the pinned
Cerbos PDP; this directory's `run.sh` publishes the adapter to mavenLocal, starts Elasticsearch,
builds this example against the published coordinate and runs the program.

## What this example covers that the adapter's own suites cannot

**Packaging.** All four suites under [`../src/test`](../src/test) compile against the adapter's own
source set, so its POM and Gradle module metadata — dependency scopes included — are executed
nowhere. This example resolves `dev.cerbos:cerbos-elasticsearch` from mavenLocal as a real Maven
coordinate instead. See
[ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

**Not a Gradle composite build**, which is the shortcut this arrangement exists to refuse.
`includeBuild("..")` substitutes the adapter's local project for the declared coordinate and
resolves neither its POM nor its module metadata, so the dependency scopes go unexecuted while
everything still compiles and passes. `cerbos-sdk-java` declaring protobuf at runtime-only scope is
the precedent ADR 0002 names, and this adapter's own POM puts *both* of its dependencies at runtime
scope:

```xml
<dependency>
  <groupId>dev.cerbos</groupId><artifactId>cerbos-sdk-java</artifactId>
  <version>0.19.0</version><scope>runtime</scope>
</dependency>
<dependency>
  <groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId>
  <version>4.35.1</version><scope>runtime</scope>
</dependency>
```

That is why [`build.gradle.kts`](build.gradle.kts) declares `cerbos-sdk-java` for itself — the
program names `PlanResourcesResult`, and a runtime-scoped transitive is not on its compile classpath
— and why it deliberately does **not** declare `protobuf-java`, which reaches the runtime classpath
through the adapter's metadata alone.

**Usage shape.** A harness runs one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), and for this adapter the second half of
that is unusually load-bearing, because the adapter's signature contains no Elasticsearch type at
all: it returns a `Map<String, Object>` of plain JDK values. Nothing in `../src/test` ever asks the
client library to parse one — `ElasticsearchTranslatorTest` compares the map to a golden asset, and
`ElasticsearchAdversarialConformanceTest` posts it as raw JSON over a bare `HttpClient`. This is the
only place a generated `Query` is built from an emitted clause.

## The break-tests

Every claim above was checked by breaking it. Each row is one deliberate edit, reverted afterwards;
the middle column is what `demo/scripts/run-example.sh elasticsearch-java` did, and the right-hand
column is what the adapter's own `gradle build` did with the same edit in place.

| Break                                                                     | The example                                                                        | `gradle build` |
| ------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | -------------- |
| Adapter source made to throw, republished at the **same version** `0.1.0` | fails — `UnsupportedOperationException` from inside the published jar               | fails too (it compiles the same source) |
| Adapter's `protobuf-java` given a `strictly("3.25.5")` downgrade          | fails at resolution, naming `'dev.cerbos:cerbos-elasticsearch:0.1.0' (runtimeElements) --> 'com.google.protobuf:protobuf-java:{strictly 3.25.5}'` | unaffected — the adapter itself resolves fine at 3.25.5 |
| The example's compiled classes added to the adapter's `jar`               | fails — `run.sh` step 2 refuses the artifact before starting anything               | unaffected |
| `includeBuild("..")` added to `settings.gradle.kts`                       | **builds and resolves happily**, then fails at startup: the adapter was loaded from `../build/libs/` | unaffected |
| A dependency version changed to the dynamic `8.15.+`                      | fails at resolution: "Resolution strategy disallows usage of dynamic versions"       | unaffected |
| The Elasticsearch client version changed to `9.0.0`                       | fails at `run.sh` step 3, before the server starts: "the majors must match"           | unaffected |

Row 2 is the one that proves the published *metadata* is resolved rather than merely present: the
downgrade exists only in the adapter's POM and Gradle module file, and the resolution failure names
the adapter's `runtimeElements` variant as the path it came in on. Row 4 is the one that proves the
coordinate is not being quietly substituted, and it is the row worth reading twice — a composite
build produces a green build and a passing resolution, so nothing except the runtime check in
[`DemoApplication`](src/main/java/dev/cerbos/example/demo/DemoApplication.java) notices.

### What row 2 also measured, and it is not what the comments used to say

The protobuf pin's usual justification is that gRPC drags an older `protobuf-java` in transitively
and an older runtime throws `RuntimeVersion$ProtobufRuntimeVersionException` at first message decode.
The transitive graph does contain them — the failure above lists `grpc-protobuf` asking for 3.25.8,
`protovalidate` for 4.34.1, `dev.cel` for 4.33.5 — but at **cerbos-sdk-java 0.19.0 the SDK's own POM
already requires `protobuf-java:4.35.1` at runtime scope**, and that requirement is what wins. So for
a consumer who declares the SDK — as this example does, and as anyone calling `cerbos.plan(...)` must
— the adapter's own declaration changes nothing today. It is a floor against the SDK relaxing that
requirement, not the thing standing between the example and a broken runtime. Stated here because the
opposite claim is easy to copy from the other Java adapter's comments.

### The same-version hazard that bit the Python example does not reproduce here

[`sqlalchemy/example/`](../../sqlalchemy/example/README.md) found that pip treats an installed
distribution of the same version as satisfying a local wheel path, so a rebuilt-but-unbumped artifact
was skipped and the **previous** build stayed installed — turning a hand-checked packaging break into
a green run. Both halves of that were measured here rather than assumed:

- **Publishing.** `publishToMavenLocal` overwrites `0.1.0` unconditionally. There is no
  already-satisfied path to skip.
- **Resolving.** Gradle re-reads a `mavenLocal()` module on every build. With
  `isChanging = true` and the zero cache TTL both removed, warming the cache with a good build and
  then republishing a deliberately broken one *still* failed.

`isChanging` and `cacheChangingModulesFor(0, "seconds")` stay in `build.gradle.kts` anyway: the second
finding is a Gradle behaviour rather than something this build states, and it stops holding the moment
`mavenLocal` is swapped for any other repository. They cost nothing, and the measurement is recorded
so nobody has to re-derive it.

### The client version is a second spelling, and something holds it to the pin

`build.gradle.kts` has to name the Elasticsearch client version as a literal, or the Renovate bump
carrying it never touches this directory (below). That makes it a second spelling of a version this
directory already knows — [`../ELASTICSEARCH_IMAGE`](../ELASTICSEARCH_IMAGE)'s tag — with nothing
holding the two together, which is precisely the hazard that file exists to remove for the *server*.
`run.sh` step 3 closes it, doing the same job `pgx/example/run.sh`'s DSN-port check does.

It compares **majors**, not exact versions. A major mismatch is the real hazard — an 8.x client
refuses a 9.x server outright, and the adapter's README declares Elasticsearch 8.x — while within a
major Elastic supports a client at or below the server's minor. Requiring exact equality would turn
every client bump red until someone bumped the image too, and `renovate.json` extends
`docker:disable`, so image bumps are made by hand: that is a gate failing for a reason that is not the
bump, the same state the lockfile argument below rejects.

## The example stays out of the published artifact

ADR 0002 asks for this to be checked deliberately in Java, which has no equivalent of a `files`
allowlist or Go's nested-module exclusion. What keeps it true is that `example/` is a **separate
Gradle build** with its own [`settings.gradle.kts`](settings.gradle.kts): it is not a source set of
the adapter, so `../build.gradle.kts`'s `jar` task cannot reach it.

That is exactly the kind of fact that stops being true without anyone noticing, so `run.sh` asserts
it on every run — and in **both** directions. The negative half refuses an artifact containing
`dev/cerbos/example/`; the positive half requires the artifact to contain
`ElasticsearchQueryPlanAdapter.class`, because otherwise an empty or wrongly-named jar would satisfy
the negative one and "no example classes in there" would be true of a jar with nothing in it at all.

## How each shape is expressed

| Shape | Expression |
| ----- | ---------- |
| 1 — filtered list | the adapter's clause, parsed into a `Query` with `withJson`, as the single entry of `bool.filter` |
| 2 — `KIND_ALWAYS_ALLOWED` | `Result.AlwaysAllowed` carries no clause; the application supplies `match_all` |
| 3 — `KIND_ALWAYS_DENIED` | `Result.AlwaysDenied` carries no clause; the application supplies `match_none` |
| 4 — pagination | the same `bool.filter`, plus Elasticsearch `from`/`size` and a `sort` on the `id` keyword field |
| 5 — composition | two more entries in the same `bool.filter` array, built with the client's own `TermQuery` builders |

`bool.filter` rather than `bool.must` throughout: an authorization condition is an access control
filter, not a relevance signal, so it belongs in a filter context where Elasticsearch skips scoring
and can cache the result. It is also what makes shape 5 a list append.

### `KIND_ALWAYS_ALLOWED` is accepted, unlike ChromaDB's empty filter

Worth stating because the sibling case is a real bug found the same way: on
[`langchain-chromadb/example/`](../../langchain-chromadb/example/README.md) an unconditional allow
comes back as `filters: {}` — an empty clause, a faithful spelling of "nothing to filter on" — which
Chroma's own validator then **rejects** (`Expected 'where' to have exactly one operator, but got 0`),
and only a program that calls the store discovers it.

This adapter cannot land in that position: an unconditional allow is the distinct sealed variant
`Result.AlwaysAllowed`, which carries no clause at all rather than an empty one, so there is nothing
for the caller to forward by mistake. And the empty spelling would have been fine anyway — measured
against the pinned server rather than assumed: `{"query":{"bool":{"filter":[]}}}` is accepted and
returns every document.

The explicit `{"match_all":{}}` is used instead, for shape 5's sake: the property that shape exists to
check is that the application's own predicate cannot resurrect a denied document, and only a search
that actually runs with both halves in place demonstrates it. An `ALWAYS_DENIED` plan therefore also
runs its search here, with `match_none`, rather than taking the skip-the-round-trip optimisation the
[adapter's README](../README.md#handling-different-result-types) shows.

### One construct the client nearly refuses

The adapter emits `bool.minimum_should_match` as the integer `1`, which is what Elasticsearch's own
JSON accepts. The client models that field as a *string*, and it turns out to parse the integer
anyway — but it is the sort of mismatch that has no other place to surface in this repository, since
the golden asset and the raw-JSON harness both bypass the client's generated types entirely. The
demo domain's policy is `public || ownerId == principal.id`, so shape 1 exercises it on the first
query rather than leaving it to a hostile shape.

## The index mapping is the consumer's half of the contract

`DemoIndex.recreate` writes the mapping out and sets `dynamic: strict`, and both halves are this
adapter's own documented hazard — see
[Analyzed (`text`) field mapping](../README.md#why-an-analyzed-mapping-is-not-something-the-adapter-can-reject).
Dynamic mapping gives a string field the `text` type, which is tokenized and lowercased before it is
indexed, and the `term` query the adapter emits for `ownerId == "alice"` would then run against those
tokens rather than against the stored value. The adapter is handed a plan and never an index, so it
cannot see the difference. An example is the right place to show a consumer writing the mapping that
makes the emitted queries mean what the policy said.

## The field map

Cerbos attribute names are not Elasticsearch field names, so a consumer always writes one of these:

```java
private static final Map<String, String> DOCUMENT_FIELDS = Map.of(
        "request.resource.attr.ownerId", "ownerId",
        "request.resource.attr.public", "isPublic");
```

Without it the adapter has nothing to resolve `request.resource.attr.ownerId` to and throws — which
is itself worth seeing in an example. `public` maps to `isPublic` because the policy's name for the
attribute and the index's name for the field are allowed to differ, which is the point of having a
map at all.

`region` and `archived` are deliberately absent: they are the application's own fields, never
referenced by [`demo/policies/document.yaml`](../../demo/policies/document.yaml), and composing them
with the adapter's clause is shape 5.

## Layout

| Path                   | What it is                                                                              |
| ---------------------- | --------------------------------------------------------------------------------------- |
| `run.sh`               | publish → check the artifact → start Elasticsearch → build → run. Prints the JSON document on stdout. |
| `build.gradle.kts`     | The coordinate, the exact dependency versions Renovate manages, and `writeRuntimeClasspath`. |
| `settings.gradle.kts`  | The file whose *absence* of `includeBuild("..")` is the whole packaging argument.        |
| `DemoApplication.java` | Entry point: the three inputs it is handed, the provenance check, the emitted document.  |
| `DemoShapes.java`      | One method per usage shape, plus the field map and the plan call.                        |
| `DemoIndex.java`       | The store: client wiring, the explicit mapping, and `Map` → `Query`.                     |
| `DemoSeeds.java`       | `demo/seeds.json`, parsed. The principals are looked up here, never written out.         |

There is no `gradle.lockfile`; see below. The program is launched with `java -cp` from a classpath
Gradle writes out, rather than through `installDist`, a fat jar or a `JavaExec` task —
`build.gradle.kts` explains what each of those three loses.

## Why the Renovate gate bites, and why there is no lockfile

`renovate.json` sets `automerge: true` for every non-major bump, so an Elasticsearch-client or
Cerbos-SDK regression's path into `main` is a PR that nobody looks at. The `example` job in
[`.github/workflows/elasticsearch-java.yaml`](../../.github/workflows/elasticsearch-java.yaml) is
what blocks that automerge when the new version breaks real usage — and only while it stays in that
workflow, which is why there is a comment on the job saying so.

For the job to gate anything, the bump has to **touch this adapter's directory**. Two things make it:

1. **Every version is an exact literal**, in `build.gradle.kts` here or in `../build.gradle.kts` for
   the adapter's own dependencies — both under `elasticsearch-java/**`, which is the workflow's path
   filter. Renovate only opens a PR when a release falls outside the declared constraint, so this is
   the half that matters: `sqlalchemy>=2.0` absorbing every future 2.x silently is the hole
   [#424](https://github.com/cerbos/query-plan-adapters/issues/424) found, and Gradle's equivalents
   are the dynamic selectors — `8.15.+`, `latest.release`, `[8.15,8.16)`.
2. **`failOnDynamicVersions()`**, so that stays a checked property rather than a convention. A
   dynamic selector fails resolution instead of quietly reopening the hole; the break-test table
   above has the message it produces.

**There is deliberately no `gradle.lockfile`.** Every example on a toolchain whose lockfile Renovate
maintains commits one — `package-lock.json`, `pdm.lock`, `go.sum` — and Gradle does have dependency
locking, so the omission is a decision rather than an oversight. (`spring-data/example/` has no
lockfile either, and predates the question being asked.)

Renovate can maintain a Gradle lockfile only by executing `./gradlew … --write-locks`, and only when a
self-hosted administrator has enabled `allowedUnsafeExecutions: ["gradleWrapper"]` — which the hosted
app is not, and this repository has no wrapper for in any case. A committed lockfile would therefore
go stale on the very first bump, and this job would fail every bump PR for a reason that is not the
bump: a gate that is always red distinguishes nothing, which is worse than the gap it closes.

What that leaves uncovered is honest to name: **transitive** versions are not pinned. It is a smaller
gap in Gradle than in npm or Python, because a Maven POM declares one soft-required version rather
than a range, so a new transitive release does not enter a build on its own — but it is not zero, and
locking becomes the right answer the day this repository grows a Gradle wrapper.

## Scope

This example is a JSON-printing CLI, not an onboarding artifact — that is
[`spring-data/example/`](../../spring-data/example/), and the floor/ceiling rule in
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) is why both exist.

It runs one Elasticsearch major under one JDK, compiled with `options.release = 17` because 17 is the
floor the [adapter's README](../README.md#requirements) declares. One consequence is visible in
[`DemoShapes.authorization`](src/main/java/dev/cerbos/example/demo/DemoShapes.java): the sealed
`Result` type is matched with an `instanceof` chain rather than the exhaustive `switch` the adapter's
README shows, because pattern matching for `switch` is standard only from Java 21. Both are correct;
a consumer on the declared floor gets the first.
