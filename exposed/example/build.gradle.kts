import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    // The same compiler the adapter is built with (../build.gradle.kts). Nothing here pins a
    // language or API level: this is the consuming application, so it is free to be written in
    // whatever Kotlin it likes, and the floor the ADAPTER promises is a property of the metadata
    // the adapter emits rather than of this build.
    kotlin("jvm") version "2.3.20"
    // The program is launched by a generated start script, with no Gradle in the process. See the
    // `application` block below for why that matters to the output contract.
    application
}

group = "dev.cerbos.example"
version = "0.0.1"

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
        // Constrains the JDK API surface as well as the bytecode level, exactly as the adapter
        // build does: 17 is the floor the adapter declares, and a stray post-17 reference should
        // fail here rather than with NoSuchMethodError on a consumer's JVM.
        freeCompilerArgs.add("-Xjdk-release=17")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    // The adapter, and only the adapter. An unrestricted `mavenLocal()` would let any stale
    // artifact in ~/.m2 shadow the Central copy of an unrelated dependency, and the resulting
    // build is reproducible on exactly one machine. The filter keeps mavenLocal answering for the
    // one coordinate this example deliberately resolves locally.
    //
    // `dev.cerbos:cerbos-sdk-java` is NOT in that filter on purpose: it is a published artifact
    // and must come from Central like any other third-party dependency.
    mavenLocal {
        content { includeModule("dev.cerbos", "cerbos-exposed") }
    }
    mavenCentral()
}

dependencies {
    // Resolved from mavenLocal as a real Maven coordinate: `gradle -p .. publishToMavenLocal`
    // first, which run.sh does for itself. Why a coordinate and not a composite build is
    // settings.gradle.kts's subject.
    //
    // `isChanging` because the version is fixed while the contents are not: every republish
    // overwrites 0.1.0-alpha.1 in place, and Gradle's default caching for a non-SNAPSHOT version
    // is entitled to keep serving the copy it resolved first. CI is always cold so it would never
    // notice; a developer's tree is warm, and it is the tree where someone checks a packaging
    // break by hand. Paired with the cache TTL below.
    implementation("dev.cerbos:cerbos-exposed:0.1.0-alpha.1") { isChanging = true }

    // The application calls the SDK itself — `cerbos.plan(...)` returns the `PlanResourcesResult`
    // it hands to the adapter — so it declares the SDK itself. The adapter's own metadata puts
    // cerbos-sdk-java at RUNTIME scope, which is correct (a consumer that never names an SDK type
    // should not compile against one) and is why this line is not redundant. It is also the exact
    // shape of coupling a composite build papers over.
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")

    // No protobuf-java declaration, deliberately. The adapter publishes it at runtime scope,
    // pinned to the gencode cerbos-sdk-java was generated against, and that pin is load-bearing:
    // gRPC drags older protobuf-java versions in transitively and an older runtime throws
    // RuntimeVersion$ProtobufRuntimeVersionException at first message decode. Restating the
    // version here would make this example pass whether or not the adapter still declares it,
    // which is the coverage docs/adr/0002-examples-install-the-packed-artifact.md exists to buy.

    // The example brings its OWN Exposed, and that is the point rather than an accident: the
    // adapter publishes every Exposed module at `compileOnly`, so none of them appears in its POM
    // and the consumer's version is the one that runs. Pinned at the LATEST release while the
    // adapter's published jar is compiled against the 1.0.0 floor, so a green run here is the
    // proof that the artifact a consumer installs works on the newest Exposed — through its real
    // packaging, which no suite under ../src/test can ask.
    implementation("org.jetbrains.exposed:exposed-core:1.5.0")
    implementation("org.jetbrains.exposed:exposed-jdbc:1.5.0")
    // For the DAO half of usage shape 1. `EntityClass.find { }` takes the same `Op<Boolean>` the
    // DSL `where { }` does, which is a property of the adapter's return type worth demonstrating
    // rather than asserting in prose.
    implementation("org.jetbrains.exposed:exposed-dao:1.5.0")

    // The store: H2, in memory, inside this program's own JVM, so run.sh has nothing to start.
    runtimeOnly("com.h2database:h2:2.4.240")

    // Reads demo/seeds.json and writes the one JSON document on stdout. The Kotlin module is what
    // lets the seed records be ordinary data classes with no no-arg constructor.
    implementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.1")

    // Exposed logs through SLF4J. Without a provider the facade prints a warning of its own on
    // stderr; with this one its output is on stderr too, which is where everything but the JSON
    // document belongs.
    runtimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

configurations.all {
    resolutionStrategy {
        // The other half of `isChanging` above: without a zero TTL, "changing" still means
        // "re-check once a day".
        cacheChangingModulesFor(0, "seconds")

        // What makes the `example` job in .github/workflows/exposed.yaml actually gate a
        // dependency bump. renovate.json automerges every non-major bump, and Renovate only opens
        // a PR when a new release falls OUTSIDE the declared constraint — so a version RANGE
        // absorbs future releases silently and nothing ever touches this directory, leaving the
        // job with nothing to gate. Gradle's equivalents are the dynamic selectors (`1.5.+`,
        // `latest.release`, `[1.5,1.6)`), so every version above is an exact literal and this line
        // is what keeps it that way.
        //
        // `failOnChangingVersions()` is deliberately NOT enabled: the adapter coordinate above is
        // declared changing on purpose, for the republish-in-place reason given there.
        failOnDynamicVersions()
    }
}

application {
    mainClass = "dev.cerbos.example.demo.DemoApplication"
    // Fixes the paths run.sh launches: `build/install/demo/bin/demo`. Without it the start script
    // is named after the project and run.sh would have to know that name twice over.
    applicationName = "demo"
}

// Where each runtime dependency was RESOLVED from, written before `installDist` copies any of it.
//
// This is the provenance check the other Java examples make at run time, moved to build time
// because it has to survive `installDist`. A Gradle composite build substitutes the adapter's
// local project for the declared coordinate, and the substitution is completely silent: it
// compiles, it resolves, and every shape passes. What gives it away is where the jar came from —
// under the adapter's own `build/` rather than out of the local Maven repository — and
// `installDist` erases exactly that, because it COPIES every dependency into
// `build/install/demo/lib` before the program ever runs.
//
// So the fact is captured while it still exists, and run.sh asserts it. Writing it out here is
// also why `installDist` is affordable at all: without this file the choice would be between a
// clean stdout (no Gradle in the launching process) and a checkable packaging claim.
val resolvedClasspathFile = layout.buildDirectory.file("resolved-runtime-classpath.txt")

tasks.register("writeResolvedClasspath") {
    group = "verification"
    description = "Record where each runtime dependency resolved from, for run.sh to check."
    val classpath = sourceSets.main.get().runtimeClasspath
    // Also what wires the compile tasks in: a source set's runtimeClasspath carries the tasks that
    // build it, so declaring it as an input is the task dependency as well as the up-to-date check.
    inputs.files(classpath)
    outputs.file(resolvedClasspathFile)
    doLast {
        resolvedClasspathFile.get().asFile.writeText(
            classpath.joinToString(separator = "\n") { it.absolutePath } + "\n",
        )
    }
}
