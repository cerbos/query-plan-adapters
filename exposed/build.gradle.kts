import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.dsl.KotlinVersion

plugins {
    // The compiler Exposed itself is built with. The LANGUAGE and API levels are pinned lower,
    // below: what a consumer has to be able to read is the metadata this build emits, and that
    // follows `languageVersion`, not the plugin.
    kotlin("jvm") version "2.3.20"
    // For `publishToMavenLocal`. example/ resolves the adapter as a real Maven coordinate rather
    // than through a Gradle composite build, so it executes the POM and the module metadata a
    // consumer resolves (docs/adr/0002-examples-install-the-packed-artifact.md).
    `maven-publish`
}

group = "dev.cerbos"
version = "0.1.0-alpha.1"

repositories {
    mavenCentral()
}

// The Exposed release the TESTS run against: `baseline` unless ADAPTER_TEST_ORM (or
// -Dadapter.test.orm) says otherwise. Both sets are declared here, once, and nothing below
// restates a version.
//
// `baseline` is the release every store leg executes on, and the one example/ pins. `floor` is the
// release the main source set compiles against, and the `floor` leg runs every suite on it again.
//
// An unknown value fails rather than falling back: a typo that quietly ran the baseline would
// report the floor leg green without executing it.
val ormVersionSets = mapOf(
    "baseline" to mapOf("exposed" to "1.5.0"),
    "floor" to mapOf("exposed" to "1.0.0"),
)

// The Exposed release the PUBLISHED JAR is compiled against, and therefore the floor the README
// claims. It is deliberately not the latest release. JetBrains promises that code built against
// an older 1.x keeps working on a newer one and promises nothing in the other direction, so a jar
// compiled against the latest can fail with NoSuchMethodError on the floor even when its source
// would compile there. Compiling against the floor and TESTING against both is the only
// arrangement where a green build proves the claim for the artifact a consumer installs.
//
// It is read back OUT of the map rather than declared as a version variable of its own and
// interpolated into the coordinates below. A top-level `val` feeding a coordinate string is the one
// shape Renovate's Gradle manager resolves, and this repository automerges non-major bumps: a bump
// of the floor would raise what the published jar compiles against, make the README's support claim
// false, and collapse the `floor` leg into a second `baseline` leg, all while staying green.
// Moving the floor is a deliberate, reviewed edit of the map above.
val exposedFloor = ormVersionSets.getValue("floor").getValue("exposed")

// Whether the suites that start containers of their OWN — the two tags `tasks.test` excludes — run
// on this leg. Declared beside the version sets so every value a leg is selected by is in one place.
val CONTAINER_SUITE_MODES = setOf("run", "skip")

val adapterTestOrm = System.getProperty("adapter.test.orm")
    ?: System.getenv("ADAPTER_TEST_ORM")
    ?: "baseline"
val orm = ormVersionSets[adapterTestOrm]
    ?: throw GradleException(
        "ADAPTER_TEST_ORM / -Dadapter.test.orm must be one of ${ormVersionSets.keys}, got '$adapterTestOrm'",
    )

kotlin {
    // Every public declaration states its visibility and its type, so the published surface is a
    // decision rather than an accident of what was left unmarked.
    explicitApi()
    // The stdlib the POM declares. Pinned to the language level rather than following the plugin,
    // so installing the adapter does not raise a consumer's stdlib past what Exposed requires.
    coreLibrariesVersion = "2.2.0"
    compilerOptions {
        jvmTarget = JvmTarget.JVM_17
        // Exposed 1.x requires Kotlin 2.2 or newer. Matching that floor exactly means the adapter
        // never demands a newer compiler than the ORM it sits beside.
        languageVersion = KotlinVersion.KOTLIN_2_2
        apiVersion = KotlinVersion.KOTLIN_2_2
        // Constrains the JDK API surface as well as the bytecode level: compiling on JDK 21 still
        // resolves against the Java 17 class library, so a stray post-17 reference fails here
        // rather than with NoSuchMethodError on a 17 runtime.
        freeCompilerArgs.add("-Xjdk-release=17")
    }
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    // Must match the gencode version cerbos-sdk-java was generated against: an older runtime
    // throws ProtobufRuntimeVersionException.
    implementation("com.google.protobuf:protobuf-java:4.35.1")

    // Exposed is the consuming application's dependency, never this artifact's: `compileOnly`
    // keeps it out of the published POM so installing the adapter cannot move a consumer's ORM
    // version. ALWAYS the floor, whichever set the tests run on (see `exposedFloor` above).
    //
    // exposed-jdbc is required, not incidental: `select` and `selectAll` are defined per transport
    // module, and rendering a table from exposed-core alone needs an @InternalApi accessor. The
    // adapter builds its correlated subqueries with the JDBC `Query`.
    compileOnly("org.jetbrains.exposed:exposed-core:$exposedFloor")
    compileOnly("org.jetbrains.exposed:exposed-jdbc:$exposedFloor")
    // Neither datetime module is declared here, deliberately. A `timestamp()` literal is bound
    // through the mapped column's own type, and the adapter matches the abstract `InstantColumnType`
    // and `OffsetDateTimeColumnType` in exposed-core that both modules' columns extend. So it
    // compiles against neither and loads without either; a consumer has whichever one declares
    // their columns. Both are test dependencies below, where real `timestamp()` columns are built.

    testImplementation("org.jetbrains.exposed:exposed-core:${orm["exposed"]}")
    testImplementation("org.jetbrains.exposed:exposed-jdbc:${orm["exposed"]}")
    testImplementation("org.jetbrains.exposed:exposed-dao:${orm["exposed"]}")
    testImplementation("org.jetbrains.exposed:exposed-java-time:${orm["exposed"]}")
    testImplementation("org.jetbrains.exposed:exposed-kotlin-datetime:${orm["exposed"]}")

    testImplementation(platform("org.junit:junit-bom:6.1.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")

    // The four stores the conformance harness replays the recorded goldens against (selected with
    // ADAPTER_TEST_DB / -Dadapter.test.db). H2 and SQLite run in
    // process; PostgreSQL and MySQL are containers.
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-postgresql:2.0.5")
    testImplementation("org.testcontainers:testcontainers-mysql:2.0.5")
    testImplementation("com.h2database:h2:2.4.240")
    testImplementation("org.xerial:sqlite-jdbc:3.53.4.0")
    testRuntimeOnly("org.postgresql:postgresql:42.7.13")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.7.0")

    // Reads the shared ../conformance/ corpus (golden plans are protobuf JSON).
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.22.1")
    testImplementation("com.google.protobuf:protobuf-java-util:4.35.1")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }

    // Which Exposed the classpath resolved, forwarded so a suite can assert the release it runs IS
    // the one this build selected. An input too, so switching sets re-runs the task rather than
    // replaying the other leg's pass.
    systemProperty("adapter.test.orm", adapterTestOrm)
    systemProperty("adapter.test.exposed.version", orm.getValue("exposed"))
    inputs.property("adapterTestOrm", adapterTestOrm)

    // The suites read these at runtime, so Gradle cannot see them unless they are declared.
    // Without this, `:test` stays UP-TO-DATE after a corpus, ledger or image-pin edit.
    inputs.dir(project.file("../conformance"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceCorpus")
    inputs.file(project.file("conformance-ledger.json"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceLedger")
    inputs.file(project.file("POSTGRES_IMAGE"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("postgresImage")
    inputs.file(project.file("MYSQL_IMAGE"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("mysqlImage")

    // Two tags mark suites that start containers of their OWN to ask a question that does not
    // depend on the store the harness runs on: `server-cross-check` (OfflineRendererTest checking
    // its stub JDBC metadata against real PostgreSQL and MySQL servers) and `docker`
    // (ReviewOperandTypeTest showing a coercion on a real MySQL). Each is a property of the Exposed
    // release or the server image, so it is answered on the default store leg and excluded on the
    // others, and on a leg that sets ADAPTER_TEST_CONTAINER_SUITES=skip. Both skip themselves where
    // there is no Docker at all. The conformance harness carries neither tag.
    val selectedStore = System.getProperty("adapter.test.db") ?: System.getenv("ADAPTER_TEST_DB")
    val containerSuites = System.getProperty("adapter.test.containerSuites")
        ?: System.getenv("ADAPTER_TEST_CONTAINER_SUITES")
        ?: "run"
    if (containerSuites !in CONTAINER_SUITE_MODES) {
        throw GradleException(
            "ADAPTER_TEST_CONTAINER_SUITES / -Dadapter.test.containerSuites must be one of " +
                "$CONTAINER_SUITE_MODES, got '$containerSuites'",
        )
    }
    inputs.property("adapterTestContainerSuites", containerSuites)
    if (containerSuites == "skip" || (selectedStore != null && selectedStore != "h2")) {
        useJUnitPlatform { excludeTags("server-cross-check", "docker") }
    }

    // The store behind the conformance harness: h2 (default), sqlite, postgres or mysql. Forwarded
    // only when set, so the harness's own default and its unknown-value failure stay in one place.
    mapOf(
        "adapter.test.db" to "ADAPTER_TEST_DB",
        "adapter.test.mysql.collation" to "ADAPTER_TEST_MYSQL_COLLATION",
        "adapter.test.mysql.serverPrepStmts" to "ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS",
        "adapter.test.postgres.initdbArgs" to "ADAPTER_TEST_POSTGRES_INITDB_ARGS",
    ).forEach { (property, variable) ->
        val value = System.getProperty(property) ?: System.getenv(variable)
        if (value != null) {
            systemProperty(property, value)
            inputs.property(property, value)
        }
    }
}

// One publication, `dev.cerbos:cerbos-exposed:<version>`, from the `java` component.
//
// What it publishes is the point: `implementation` dependencies (cerbos-sdk-java, protobuf-java)
// land at runtime scope, and the `compileOnly` Exposed modules do not appear at all. example/
// resolves this coordinate out of mavenLocal and therefore proves that resolution.
//
// This is `publishToMavenLocal` only. Nothing here configures a Maven Central release, which also
// needs POM `name`, `description`, `url`, `licenses`, `developers` and `scm`, plus signing.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
