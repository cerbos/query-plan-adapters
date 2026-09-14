import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    java
    id("org.springframework.boot") version "3.5.16"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "dev.cerbos.example"
version = "0.0.1"

// Match the adapter build: `options.release` pins the Java 17 API surface as well as the
// language level, so newer compile JDKs can't leak post-17 APIs into the bytecode. Stated once
// because `demoJar` needs it too — the Spring Boot plugin configures its own `bootJar` with a
// target version and cannot infer one for a boot jar the build registers itself.
val javaRelease = 17

tasks.withType<JavaCompile> {
    options.release = javaRelease
}

repositories {
    // The adapter, and only the adapter. `mavenLocal()` unrestricted would let any stale
    // artifact in ~/.m2 shadow the Central copy of an unrelated dependency, and the resulting
    // build is reproducible on exactly one machine. The filter keeps mavenLocal answering for
    // the one coordinate this example deliberately resolves locally.
    //
    // `dev.cerbos:cerbos-sdk-java` is NOT in that filter on purpose: it is a published
    // artifact and must come from Central like any other third-party dependency.
    mavenLocal {
        content { includeModule("dev.cerbos", "cerbos-spring-data") }
    }
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    runtimeOnly("com.h2database:h2")

    // Resolved from mavenLocal as a real Maven coordinate: `gradle -p .. publishToMavenLocal`
    // first, which every entry point here does for itself. What that buys over the composite
    // build this replaced is the adapter's own POM and module metadata — see
    // docs/adr/0002-examples-install-the-packed-artifact.md and settings.gradle.kts.
    //
    // `isChanging` because the version is fixed while the contents are not: every republish
    // overwrites 0.1.0-alpha.1 in place, and Gradle's default caching for a non-SNAPSHOT
    // version is entitled to keep serving the copy it resolved first. CI is always cold so it
    // would never notice; a developer's tree is warm, and it is the tree where someone checks a
    // packaging break by hand. Paired with the cache TTL below.
    implementation("dev.cerbos:cerbos-spring-data:0.1.0-alpha.1") { isChanging = true }
    // The application calls the SDK itself — `cerbos.plan(...)` returns the `PlanResourcesResult`
    // it hands to the adapter — so it declares the SDK itself. The adapter's own metadata puts
    // cerbos-sdk-java at RUNTIME scope, which is correct (a consumer that never names an SDK type
    // should not compile against one) and is why this line is not redundant. It is also the exact
    // shape of coupling a composite build papered over.
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")

    // No protobuf-java declaration. The adapter publishes it at runtime scope, pinned to the
    // gencode cerbos-sdk-java was generated against, and that pin is load-bearing: gRPC drags in
    // older protobuf-java versions transitively and an older runtime throws
    // RuntimeVersion$ProtobufRuntimeVersionException at first message decode. Restating the
    // version here would make this example pass whether or not the adapter still declares it,
    // which is the coverage docs/adr/0002-examples-install-the-packed-artifact.md exists to buy.

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

// The other half of the `isChanging` declaration above: without a zero TTL, "changing" still
// means "re-check once a day".
configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
}

tasks.test { useJUnitPlatform() }

// Two applications share this source tree, so the main class cannot be inferred. `bootRun` and
// `bootJar` stay pointed at the photo-sharing app; the demo-domain program below gets a boot jar
// of its own.
springBoot {
    mainClass = "dev.cerbos.example.photos.PhotosApplication"
}

// The demo-domain program, packaged as its own executable boot jar.
//
// A jar rather than a `JavaExec` task because of the output contract:
// demo/scripts/run-example.sh reads ONE JSON document from run.sh's stdout, and a Gradle task's
// stdout also carries Gradle's own lifecycle output and any deprecation warning it decides to
// print that day. Building the jar (noise to stderr) and then launching `java -jar` (no Gradle in
// the process) keeps stdout the program's alone. dev.cerbos.example.demo.DemoApplication does the
// other half — see its comment on redirecting System.out.
tasks.register<BootJar>("demoJar") {
    group = "build"
    description = "Executable jar for the demo-domain program run by demo/scripts/run-example.sh"
    mainClass = "dev.cerbos.example.demo.DemoApplication"
    classpath = sourceSets.main.get().runtimeClasspath
    targetJavaVersion = JavaVersion.toVersion(javaRelease)
    // A fixed name so run.sh names the artifact without globbing for a version it would then
    // have to keep in step with `version` above.
    archiveFileName = "demo.jar"
}
