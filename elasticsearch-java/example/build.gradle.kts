plugins {
    java
}

group = "dev.cerbos.example"
version = "0.0.1"

// Java 17 is the adapter's floor (README.md "Requirements"), and `options.release` — unlike
// source/targetCompatibility — also constrains the JDK API surface, so this example is compiled
// against exactly the class library a consumer on that floor has. A post-17 API reference fails here
// instead of with NoSuchMethodError on their JVM.
tasks.withType<JavaCompile> {
    options.release = 17
}

repositories {
    // The adapter, and only the adapter. `mavenLocal()` unrestricted would let any stale artifact in
    // ~/.m2 shadow the Central copy of an unrelated dependency, and the resulting build is
    // reproducible on exactly one machine. The filter keeps mavenLocal answering for the one
    // coordinate this example deliberately resolves locally.
    //
    // `dev.cerbos:cerbos-sdk-java` is NOT in that filter on purpose: it is a published artifact and
    // must come from Central like any other third-party dependency.
    mavenLocal {
        content { includeModule("dev.cerbos", "cerbos-elasticsearch") }
    }
    mavenCentral()
}

dependencies {
    // Resolved from mavenLocal as a real Maven coordinate: `gradle -p .. publishToMavenLocal` first,
    // which run.sh does for itself. Why a coordinate and not a composite build is
    // settings.gradle.kts's subject.
    //
    // `isChanging` because the version is fixed while the contents are not: every republish
    // overwrites 0.1.0 in place, and a repository whose modules are cached is entitled to keep
    // serving the copy it resolved first. That hazard is what bit the Python example
    // (cerbos/query-plan-adapters#424, where pip skipped installing a same-version wheel and the
    // PREVIOUS build stayed in place), so it was measured here rather than assumed, in both halves:
    //
    //   - `publishToMavenLocal` overwrites unconditionally. There is no already-satisfied path.
    //   - Gradle re-reads a `mavenLocal()` module on every build. Removing this declaration and the
    //     TTL below, warming the cache with a good build and then republishing a DELIBERATELY broken
    //     one, still failed — so the caching half of the hazard does not reproduce here either.
    //
    // Both lines stay: the second finding is a Gradle behaviour rather than a guarantee this build
    // states, and it stops holding the moment mavenLocal is swapped for any other repository. They
    // cost nothing, and the measurement is recorded so nobody has to re-derive it.
    implementation("dev.cerbos:cerbos-elasticsearch:0.1.0") { isChanging = true }

    // The application calls the SDK itself — `cerbos.plan(...)` returns the `PlanResourcesResult` it
    // hands to the adapter — so it declares the SDK itself. The adapter's own metadata puts
    // cerbos-sdk-java at RUNTIME scope, which is correct (a consumer that never names an SDK type
    // should not compile against one) and is why this line is not redundant. It is also the exact
    // shape of coupling a composite build papers over.
    implementation("dev.cerbos:cerbos-sdk-java:0.19.0")

    // The Elasticsearch Java client — the "ORM" for this adapter, and the thing the adapter's
    // README tells a consumer to hand the emitted map to. Matched to the server major this example
    // runs (../ELASTICSEARCH_IMAGE); the adapter supports Elasticsearch 8.x.
    implementation("co.elastic.clients:elasticsearch-java:8.15.3")

    // Two jobs, both real. `JacksonJsonpMapper` is how the client is given a jakarta.json
    // implementation without pulling in Parsson, and the same ObjectMapper serialises the adapter's
    // `Map<String, Object>` on its way into a typed `Query` and writes the JSON document on stdout.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")

    // No protobuf-java declaration, deliberately: the adapter publishes it at runtime scope, pinned
    // to the gencode cerbos-sdk-java was generated against, and restating the version here would make
    // this example pass whether or not the adapter still declares it — which is the coverage
    // docs/adr/0002-examples-install-the-packed-artifact.md exists to buy. What that pin is worth is
    // measured rather than assumed; see the comment on the `publishing` block in ../build.gradle.kts
    // and the break-test table in README.md.
}

configurations.all {
    resolutionStrategy {
        // The other half of `isChanging` above: without a zero TTL, "changing" still means
        // "re-check once a day".
        cacheChangingModulesFor(0, "seconds")

        // What makes the `example` job in .github/workflows/elasticsearch-java.yaml actually gate a
        // dependency bump.
        //
        // renovate.json automerges every non-major bump, and Renovate only opens a PR when a new
        // release falls OUTSIDE the declared constraint. A version RANGE therefore absorbs future
        // releases silently — the hole cerbos/query-plan-adapters#424 found in a Python `>=` floor —
        // and nothing ever touches this directory, so the job has nothing to gate. Gradle's
        // equivalents are the dynamic selectors: `8.15.+`, `latest.release`, `[8.15,8.16)`. Every
        // version above is therefore an exact literal, and this line is what keeps it that way: a
        // dynamic selector fails resolution rather than quietly reopening the hole.
        //
        // `failOnChangingVersions()` is deliberately NOT enabled — the adapter coordinate above is
        // declared changing on purpose, for the republish-in-place reason given there.
        failOnDynamicVersions()
    }
}

// Writes the RESOLVED runtime classpath, one `java -cp` string, for run.sh to launch with.
//
// Three alternatives were tried and each loses something this needs:
//
//   - The `application` plugin's `installDist` COPIES every dependency into
//     `build/install/<name>/lib`, so by the time the program runs, every jar on its classpath sits
//     under this directory whatever it was resolved from. That defeats
//     DemoApplication.assertAdapterCameFromThePublishedArtifact, whose whole job is to tell a jar
//     resolved from mavenLocal apart from one a Gradle composite build substituted in.
//   - A fat jar has the same problem, plus a ServiceLoader merge this example has no reason to
//     take on: both the Elasticsearch client and gRPC look implementations up that way.
//   - Running the program from a Gradle `JavaExec` task puts Gradle's lifecycle output — and
//     whatever deprecation notice it decides to print that day — on the same stdout that has to
//     carry exactly one JSON document.
//
// So the classpath is written out and `java` is launched directly, with no Gradle in the process and
// every dependency still at the absolute path it was resolved from.
val runtimeClasspathFile = layout.buildDirectory.file("runtime-classpath.txt")

tasks.register("writeRuntimeClasspath") {
    group = "build"
    description = "Write the resolved runtime classpath for demo/scripts/run-example.sh to launch."
    val classpath = sourceSets.main.get().runtimeClasspath
    // Also what wires the compile tasks in: a source set's runtimeClasspath carries the tasks that
    // build it, so declaring it as an input is the task dependency as well as the up-to-date check.
    inputs.files(classpath)
    outputs.file(runtimeClasspathFile)
    doLast {
        runtimeClasspathFile.get().asFile.writeText(classpath.asPath)
    }
}
