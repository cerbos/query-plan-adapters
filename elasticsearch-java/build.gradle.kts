/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    java
    // For `publishToMavenLocal`. example/ resolves the adapter as a real Maven coordinate rather
    // than through a Gradle composite build, so the example executes the POM and the Gradle module
    // metadata a consumer resolves — dependency scopes included. That is the whole point of
    // docs/adr/0002-examples-install-the-packed-artifact.md, and it is not hypothetical here:
    // cerbos-sdk-java declares protobuf at runtime-only scope in its own module metadata, which a
    // composite build hides and a coordinate exposes.
    `maven-publish`
}

group = "dev.cerbos"
version = "0.1.0"

// `options.release` (unlike source/targetCompatibility) also constrains the JDK API surface:
// compiling on JDK 21 still resolves against the Java 17 class library, so a stray post-17 API
// reference fails at compile time instead of with NoSuchMethodError on a JDK 17 runtime. 17 is the
// floor README.md "Requirements" declares, and the same setting spring-data and example/ use.
tasks.withType<JavaCompile> {
    options.release = 17
    options.compilerArgs.add("-Xlint:deprecation")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    implementation("com.google.protobuf:protobuf-java:4.35.1")

    testImplementation(platform("org.junit:junit-bom:6.1.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    // Testcontainers 2.x throughout. The 2.x modules are published under new artifact ids
    // (`testcontainers-junit-jupiter`, `testcontainers-elasticsearch`), and the 1.x ids stop at
    // 1.21.x — so a 1.x module on this 2.x core is a mixed classpath that happens to link, not a
    // supported combination. All three are held to one version on purpose.
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
    testImplementation("org.testcontainers:testcontainers-elasticsearch:2.0.5")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    // Decodes conformance/wire-fixtures/*.json into the protobuf plan the SDK hands a caller:
    // JsonFormat is protobuf's own canonical JSON mapping, the one the PDP's HTTP API writes
    // (see Corpus.planFromWireFixture).
    testImplementation("com.google.protobuf:protobuf-java-util:4.35.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

tasks.test {
    useJUnitPlatform()
    val strictEvaluation = providers.environmentVariable("ADAPTER_TEST_STRICT_EVALUATION")
        .getOrElse("false")
    require(strictEvaluation in listOf("false", "true")) {
        "ADAPTER_TEST_STRICT_EVALUATION must be false or true"
    }
    inputs.property("strictEvaluation", strictEvaluation)
    systemProperty("adapter.test.strictEvaluation", strictEvaluation)

    // The suites read a shared directory from OUTSIDE this project — the conformance corpus
    // (`ElasticsearchAdversarialConformanceTest`, `ElasticsearchTranslatorTest`,
    // `ElasticsearchSurfaceTest`, `CerbosTestImage`), resolved from `user.dir` at runtime, which
    // Gradle cannot infer. Undeclared, it is not an input: a `gradle build` over a stale `build/`
    // reports BUILD SUCCESSFUL with `:test` UP-TO-DATE and runs nothing after a corpus edit.
    //
    // That is a green local validation of a change it never executed, which is the exact failure
    // the corpus exists to prevent. CI never saw it because it always starts from a clean
    // checkout, so the gap only ever bites the maintainer editing the corpus.
    //
    // The whole directory is declared rather than the specific files. The corpus is read by
    // filename in several places and grows new ones, and a precise list would silently stop
    // covering what it names — the same class of bug one level up.
    inputs.dir(project.file("../conformance"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceCorpus")
    // The golden expectations are read by ElasticsearchTranslatorTest the same way, and they live
    // inside this project rather than outside it — but not under a source set, so Gradle would not
    // see an edit to them either.
    inputs.dir(project.file("golden"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("goldenExpectations")
    // Which pinned Elasticsearch reference the two container-backed suites start: ELASTICSEARCH_IMAGE
    // (the baseline, the server example/run.sh starts too) unless ELASTICSEARCH_IMAGE_FILE names
    // another, which is how the workflow runs the forward-compatibility leg against
    // ELASTICSEARCH_NEXT_IMAGE without restating either reference. A filename rather than an image
    // reference, so every reference stays in a `*_IMAGE` file that
    // conformance/scripts/validate-corpus.sh scans and holds to one digest per tag.
    val elasticsearchImageFile = System.getProperty("elasticsearch.test.image.file")
        ?: System.getenv("ELASTICSEARCH_IMAGE_FILE")
        ?: "ELASTICSEARCH_IMAGE"
    systemProperty("elasticsearch.test.image.file", elasticsearchImageFile)
    // Declared as an input for the same reason as the corpus: ElasticsearchTestImage reads it from
    // `user.dir` at runtime. Undeclared, bumping the server would leave `:test` UP-TO-DATE and the
    // two container-backed suites would report a pass they never ran. The system property above is
    // an input too, so switching files re-runs the task rather than replaying the other leg's pass.
    inputs.file(project.file(elasticsearchImageFile))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("elasticsearchImage")
}

// Rewrites golden/expectations.json from the Query DSL the translator emits today, then asserts
// the rewritten file — so the task fails if regeneration produced something the rules reject.
//
// Regeneration is a DELIBERATE act and the diff is the review, which is why it is a task of its
// own rather than a flag on `test`: CI never runs it, so a translator change that moves the
// emitted query fails there whatever anyone ran locally
// (conformance/README.md, "Golden expectations").
tasks.register<Test>("goldenUpdate") {
    group = "verification"
    description = "Rewrite elasticsearch-java/golden/expectations.json from what the translator emits."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    filter { includeTestsMatching("dev.cerbos.queryplan.elasticsearch.ElasticsearchTranslatorTest") }
    systemProperty("golden.update", "true")
    // The asset this task writes is also an input Gradle tracks for `test`; declaring it as an
    // output here would make the two tasks fight over it, so the task is simply never up to date.
    outputs.upToDateWhen { false }
    inputs.dir(project.file("../conformance"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceCorpus")
    testLogging {
        events("failed")
        showStandardStreams = true
    }
}

// One publication, `dev.cerbos:cerbos-elasticsearch:<version>`, from the `java` component.
//
// What it publishes is the point: the `implementation` dependencies (cerbos-sdk-java,
// protobuf-java) land at RUNTIME scope, which is what example/ resolves and therefore executes. That
// scope is not a formality — it is why example/build.gradle.kts has to declare cerbos-sdk-java for
// itself in order to compile against `PlanResourcesResult`, and it is the half a Gradle composite
// build substitutes away.
//
// The protobuf-java version here is the pin README.md's Install section tells a consumer to add.
// Measured rather than assumed while example/ was being written, and re-checked against the 0.20.1 POM: at cerbos-sdk-java 0.20.1 the SDK's
// own POM already requires protobuf-java 4.35.1 at runtime scope, so for a consumer who declares the
// SDK — as the example does, and as anyone calling `cerbos.plan(...)` must — this line changes
// nothing today. It is a floor against the SDK relaxing that requirement and letting the older
// protobuf-java gRPC drags in transitively win, which throws
// RuntimeVersion$ProtobufRuntimeVersionException at first message decode. Worth knowing before
// reading it as the thing example/ proves: what example/ proves about this metadata is that it is
// resolved at all, and example/README.md records the break-test that establishes it.
//
// The example is never part of the artifact: it is a separate Gradle build under example/ with its
// own settings file, so it is not a source set here and cannot reach the jar. example/run.sh
// asserts that rather than leaving it to inspection — ADR 0002's "examples must stay out of the
// published artifacts they exercise", which for Java is a deliberate check rather than a `files`
// allowlist.
//
// This is `publishToMavenLocal` only. Nothing here configures a Maven Central release, which
// additionally requires POM `name`, `description`, `url`, `licenses`, `developers` and `scm`, plus
// signing — Central rejects a POM without them. README.md's Install section still tells a consumer
// to copy the package's source files, because that is what this adapter ships today; what the example
// resolves is the dependency metadata, which is what it exists to exercise. Wiring up the
// `elasticsearch-java/v*` release is separate work, and it will change this block.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
