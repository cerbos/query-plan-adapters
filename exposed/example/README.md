# exposed adapter example application

A runnable program that uses the adapter the way a consumer would — handing the `Op<Boolean>` it
returns to Exposed's own `Query.where { }`, `orderBy`/`limit`/`offset` and DAO `find { }` — over the
shared [demo domain](../../demo/README.md), against H2 in memory.

```bash
# from the repository root
demo/scripts/run-example.sh exposed
```

Needs `docker` (with compose), `jq`, Gradle 8.x and a JDK 17+. The runner starts the pinned Cerbos
PDP; this directory's `run.sh` publishes the adapter to mavenLocal, builds this example against the
published coordinate and runs it. The store runs inside the program's own JVM, so there is nothing
else to start.

Every call this program makes to the PDP carries a **30 second deadline**
(`CerbosClientBuilder.withTimeout`), because a blocking gRPC stub with no deadline waits for ever
and a stalled stream would hold the program — and the CI job running it — until something outside
killed it. A real application wants a deadline on its authorization calls for the same reason.

## What this example covers that the adapter's own suites cannot

**Packaging.** Every suite under [`../src/test`](../src/test) compiles against the adapter's own
source set, so its POM and Gradle module metadata — dependency scopes included — are executed
nowhere. This example resolves `dev.cerbos:cerbos-exposed` from mavenLocal as a real Maven
coordinate instead. See [ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md).

On this adapter that metadata carries an unusual amount of weight, because of what has to be
*absent* from it. Every Exposed module is a `compileOnly` dependency of the adapter, so none of them
appears in the published POM and the consumer's Exposed is the one that runs — the same arrangement
`spring-data` has with Hibernate. A claim of that shape is only checked where a real coordinate is
resolved by a build that brings its own copy, which is here:

```kotlin
implementation("dev.cerbos:cerbos-exposed:0.1.0-alpha.1") { isChanging = true }
implementation("org.jetbrains.exposed:exposed-core:1.5.0")
implementation("org.jetbrains.exposed:exposed-jdbc:1.5.0")
implementation("org.jetbrains.exposed:exposed-dao:1.5.0")
```

**And the two Exposed versions are deliberately different.** The adapter's published jar is compiled
against the 1.0.0 floor ([`../build.gradle.kts`](../build.gradle.kts) explains why that direction and
not the other), while this example pins the latest release. A green run here is therefore the one
place the artifact a consumer actually installs is executed on the newest Exposed, through its real
packaging. `exposed-dao` is not even on the adapter's own compile classpath.

**Usage shape.** A harness runs one flat filtered query. This runs all
[five shapes](../../demo/README.md#the-five-usage-shapes), and shape 5 — the adapter's predicate
ANDed with the application's own — is the one that earns the exercise.

## Not a Gradle composite build

`includeBuild("..")` would be one line shorter and is the shortcut this arrangement exists to refuse:
it substitutes the adapter's local project for the declared coordinate and resolves neither its POM
nor its module metadata, so the dependency scopes go unexecuted while everything still compiles and
passes. `cerbos-sdk-java` declaring protobuf at runtime-only scope is the precedent ADR 0002 names,
and this adapter's published POM is that shape all the way through — everything it declares for
itself is at runtime scope, and the only compile-scope entry is the stdlib the Kotlin plugin adds:

```xml
<dependency>
  <groupId>org.jetbrains.kotlin</groupId><artifactId>kotlin-stdlib</artifactId>
  <version>2.2.0</version><scope>compile</scope>
</dependency>
<dependency>
  <groupId>dev.cerbos</groupId><artifactId>cerbos-sdk-java</artifactId>
  <version>0.20.1</version><scope>runtime</scope>
</dependency>
<dependency>
  <groupId>com.google.protobuf</groupId><artifactId>protobuf-java</artifactId>
  <version>4.35.1</version><scope>runtime</scope>
</dependency>
```

That is the whole file: no Exposed module appears in it, which is the absence the section above is
about.

That is why [`build.gradle.kts`](build.gradle.kts) declares `cerbos-sdk-java` for itself — the
program names `PlanResourcesResult`, and a runtime-scoped transitive is not on its compile classpath
— and why it deliberately does **not** declare `protobuf-java`, which reaches the runtime classpath
through the adapter's metadata alone.

### The provenance check runs at build time, because `installDist` would erase it

The elasticsearch-java example asserts at startup that the adapter class was loaded from a jar
outside the adapter's own build directory, which is what tells a mavenLocal artifact apart from a substituted
composite build. That check cannot survive `installDist`: the `application` plugin **copies** every
dependency into `build/install/demo/lib` before the program runs, so by then every jar on the
classpath sits under this directory whatever it was resolved from.

`installDist` is still what this example uses, because of the output contract — the program is
launched by its generated start script with no Gradle in the process, so stdout carries one JSON
document and not Gradle's lifecycle output. The fact is captured earlier instead:
`writeResolvedClasspath` writes out where each runtime dependency resolved from, and `run.sh` step 4
refuses to launch when the adapter jar turns out to be under `../build/`. Same discriminator, moved
to the last moment it still exists.

## The break-tests

Every claim above was checked by breaking it. Each row is one deliberate edit, reverted afterwards;
the middle column is what `demo/scripts/run-example.sh exposed` did.

| Break | What happened |
| ----- | ------------- |
| `includeBuild("..")` added to `settings.gradle.kts` | **builds and resolves happily** — then `run.sh` step 4 refuses it: "the adapter resolved to `…/exposed/build/libs/cerbos-exposed-0.1.0-alpha.1.jar`, inside its own build directory" |
| A jar containing `dev/cerbos/example/` dropped into `../build/libs` | fails at `run.sh` step 2, before anything is built: "contains example classes — the example must not ship inside the adapter" |
| `CERBOS_HOST` unset | fails at startup, before the store is touched: "CERBOS_HOST is not set — run this program through demo/scripts/run-example.sh exposed" |

Row 1 is the one worth reading twice. A composite build produces a green build, a successful
resolution and five passing shapes; without a check of where the jar came from, nothing notices.

## The example stays out of the published artifact

ADR 0002 asks for this to be checked deliberately on the JVM, which has no equivalent of a `files`
allowlist or Go's nested-module exclusion. What keeps it true is that `example/` is a **separate
Gradle build** with its own [`settings.gradle.kts`](settings.gradle.kts): it is not a source set of
the adapter, so `../build.gradle.kts`'s `jar` task cannot reach it.

That is exactly the kind of fact that stops being true without anyone noticing, so `run.sh` asserts
it on every run, in **both** directions. The negative half refuses an artifact containing
`dev/cerbos/example/`; the positive half requires it to contain
`ExposedQueryPlanAdapter.class`, because otherwise an empty or wrongly-named jar would satisfy the
negative one and "no example classes in there" would be true of a jar with nothing in it at all.

## How each shape is expressed

| Shape | Expression |
| ----- | ---------- |
| 1 — filtered list | `Documents.selectAll().where { filter.toOp() }` |
| 2 — `KIND_ALWAYS_ALLOWED` | the same line: `QueryPlanFilter.AlwaysAllowed.toOp()` is `Op.TRUE` |
| 3 — `KIND_ALWAYS_DENIED` | the same line again: `QueryPlanFilter.AlwaysDenied.toOp()` is `Op.FALSE` |
| 4 — pagination | `.orderBy(Documents.id, SortOrder.ASC).limit(pageSize).offset(n)` composed onto the filtered query |
| 5 — composition | `filter.toOp() and (Documents.archived eq false) and (Documents.region eq "emea")`, with both operands read from `demo/seeds.json` |

Nothing here switches on the plan kind, and that is the adapter's design rather than an omission:
`QueryPlanFilter.toOp()` collapses all three kinds to a single predicate, so composition in shape 5
is one line of code for each of them. The kind is still reported alongside the ids, because
`demo/expected.json` pins it — that is what stops this program returning all 8 rows for `admin-view`
without ever having reached the PDP — and it is read off the adapter's own sealed result rather than
off the SDK's plan predicates, so the adapter cannot classify a plan one way while this program
reports it another.

An `ALWAYS_DENIED` plan still runs its query here. Skipping the store on
`QueryPlanFilter.AlwaysDenied` is a supported optimisation and the reason that variant is distinct
from a bare `Op.FALSE`, but executing `FALSE AND <application predicate>` is what actually
demonstrates the property shape 5 exists to check: that the application's own predicate cannot
resurrect a denied row.

### The DAO cross-check, which is an extra rather than a shape

Shape 1 is run a second time through `DocumentEntity.find { filter.toOp() }` and asserted equal to
the DSL result. It emits no key of its own — the shared runner diffs the document exactly, so a
sixth key would fail there — and the demo domain is a floor rather than a ceiling, which is what
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) says an example may add
to.

What it proves is a claim about the adapter's return **type**: that it hands back an ordinary Exposed
predicate rather than a query it built, so the same value satisfies `Query.where { }` and
`EntityClass.find { }` alike. `ExposedSurfaceTest` asks the same question of an in-process H2, so
what this adds is the *packaged* half: `exposed-dao` is not on the adapter's compile classpath at
all — it is a test-only dependency there, and a dependency this example brings for itself — so this
is where a DAO entity meets the adapter as a resolved coordinate rather than as a source set.

## The mapping

Cerbos attribute names are not column names, so a consumer always writes one of these:

```kotlin
val DOCUMENT_ATTRIBUTES = cerbosMapping {
    "request.resource.attr.ownerId" to Documents.ownerId
    "request.resource.attr.public" to Documents.isPublic
}
```

Without it the adapter has nothing to resolve `request.resource.attr.ownerId` to and raises
`UnmappedAttributeException` rather than guess a column — which is itself worth seeing in an example.
`public` resolves to `Documents.isPublic` (column `is_public`) because the policy's name for the
attribute and the table's name for the column are allowed to differ, which is the point of having a
mapping at all.

`region` and `archived` are deliberately absent: they are the application's own columns, never
referenced by [`demo/policies/document.yaml`](../../demo/policies/document.yaml), and composing them
with the adapter's predicate is shape 5.

### The application's own predicate uses the sugar the adapter never does

Shape 5's second operand is written `Documents.archived eq filter.archived`, and the adapter itself
would never write that for a plan-derived value: `eq` binds a constant through the **column's** type
and rewrites a null operand into `IS NULL`, and a translator can afford neither. An application
comparing its own column to its own constant is exactly the case that sugar is for. The asymmetry is
the shape of the composition a consumer actually writes, which is why it is left visible here rather
than smoothed over.

## Layout

| Path | What it is |
| ---- | ---------- |
| `run.sh` | publish → check the artifact → build → check provenance → run. Prints the JSON document on stdout. |
| `build.gradle.kts` | The coordinate, the exact dependency versions Renovate manages, and `writeResolvedClasspath`. |
| `settings.gradle.kts` | The file whose *absence* of `includeBuild("..")` is the whole packaging argument. |
| `DemoApplication.kt` | Entry point: the two inputs it is handed, the transaction, the emitted document. |
| `DemoShapes.kt` | One method per usage shape, plus the mapping and the plan call. |
| `DemoSchema.kt` | The `documents` table and its DAO entity. |
| `DemoSeeds.kt` | `demo/seeds.json`, parsed. The principals are looked up here, never written out. |

## Why the Renovate gate bites, and why there is no lockfile

`renovate.json` sets `automerge: true` for every non-major bump, so an Exposed or Cerbos-SDK
regression's path into `main` is a PR nobody looks at. The `example` job in
[`.github/workflows/exposed.yaml`](../../.github/workflows/exposed.yaml) is what blocks that
automerge when the new version breaks real usage — and only while it stays in that workflow.

For the job to gate anything the bump has to **touch this adapter's directory**, and two things make
it. Every version is an exact literal, here or in `../build.gradle.kts`, both under `exposed/**`
which is the workflow's path filter: Renovate only opens a PR when a release falls outside the
declared constraint, so a version range would absorb future releases silently and nothing would ever
touch this directory. And `failOnDynamicVersions()` keeps that a checked property rather than a
convention — Gradle's ranges are the dynamic selectors (`1.5.+`, `latest.release`, `[1.5,1.6)`), and
one now fails resolution instead of quietly reopening the hole.

**There is deliberately no `gradle.lockfile`**, for the reason
[`elasticsearch-java/example/`](../../elasticsearch-java/example/README.md#why-the-renovate-gate-bites-and-why-there-is-no-lockfile)
sets out at length: Renovate can only maintain one by executing `./gradlew … --write-locks`, which
the hosted app is not permitted to do and this repository has no wrapper for, so a committed lockfile
would go stale on the first bump and fail every bump PR for a reason that is not the bump.
Transitive versions are therefore unpinned, which is a smaller gap in Gradle than in npm or Python —
a Maven POM declares one soft-required version rather than a range — but it is not zero.

## Scope

This proves plumbing, not semantics. Which rows a filter must return is
[`../src/test`](../src/test)'s question, answered against a hostile corpus with a live PDP as the
oracle on every store that adapter's workflow executes; the policy here is
`public || ownerId == principal.id`, and every shape in the demo domain must be expressible by every
adapter — which is what
[ADR 0001](../../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md) rules out carve-outs for.

Three limits worth naming rather than leaving to be assumed:

- **One store.** H2, in memory. SQLite, PostgreSQL and MySQL are the adapter's own CI matrix, where
  collation, LIKE escaping and cast targets actually differ; nothing about *packaging* does.
- **One Exposed version.** 1.5.0. The 1.0.0 floor is proved by the `floor` leg of the adapter's own
  workflow, which runs the offline suites and the H2 conformance harness against it.
- **The composite-build check is a build-time artifact, not a runtime one.** It reads a file
  `writeResolvedClasspath` produced in the same invocation. A launcher that ran the program without
  that task would skip the check rather than fail it, which is why `run.sh` also refuses an absent or
  empty classpath file.
