plugins {
    java
    // For `publishToMavenLocal`. example/ resolves the adapter as a real Maven coordinate
    // rather than through a Gradle composite build, so the example executes the POM and the
    // Gradle module metadata a consumer resolves — dependency scopes included. That is the
    // whole point of docs/adr/0002-examples-install-the-packed-artifact.md, and it is not
    // hypothetical here: cerbos-sdk-java declares protobuf at runtime-only scope in its own
    // module metadata, which a composite build hides and a coordinate exposes.
    `maven-publish`
}

group = "dev.cerbos"
version = "0.1.0-alpha.1"

// `options.release` (unlike source/targetCompatibility) also constrains the JDK API
// surface: compiling on JDK 21/25 still resolves against the Java 17 class library, so a
// stray post-17 API reference fails at compile time instead of with NoSuchMethodError on
// a JDK 17 runtime.
tasks.withType<JavaCompile> {
    options.release = 17
    options.compilerArgs.add("-Xlint:deprecation")
}

repositories {
    mavenCentral()
}

// The ORM version set this build compiles and tests against: `baseline` unless ADAPTER_TEST_ORM
// (or -Dadapter.test.orm) says `next`. Both sets are declared here, once, and nothing below
// restates a version.
//
// `baseline` is the Hibernate 6.6 / Spring Data JPA 3.5 line the golden asset was rendered under
// (golden/expectations.json declares `"hibernate": "6.6"`) and the line example/ runs on Spring
// Boot 3.5. `next` is the next major of each — Hibernate 7 and Spring Data JPA 4, the pair Spring
// Boot 4 manages — and is a TEST-ONLY forward-compatibility leg, the spring-data analogue of
// elasticsearch-java/ELASTICSEARCH_NEXT_IMAGE: `hibernate-core` is `compileOnly`, a consumer brings
// their own, and until this leg a consumer on Hibernate 7 was proved nowhere. Hibernate renders
// every statement the golden asset records, so on this leg the translator suite asserts a pinned
// divergence list instead of the asset's bytes (SpringDataTranslatorTest,
// `theAssetDeclaresTheRendererThatWroteIt`), and `goldenUpdate` refuses to run.
//
// An unknown value fails rather than falling back: a typo that quietly ran the baseline would
// report the forward-compatibility leg green without executing it, the same shape of failure
// `ADAPTER_TEST_DB` guards against in the harness.
val ormVersionSets = mapOf(
    "baseline" to mapOf(
        "springDataJpa" to "3.5.13",
        "jakartaPersistence" to "3.2.0",
        "hibernate" to "6.6.54.Final",
    ),
    "next" to mapOf(
        "springDataJpa" to "4.1.1",
        "jakartaPersistence" to "3.2.0",
        "hibernate" to "7.4.8.Final",
    ),
)
val adapterTestOrm = System.getProperty("adapter.test.orm")
    ?: System.getenv("ADAPTER_TEST_ORM")
    ?: "baseline"
val orm = ormVersionSets[adapterTestOrm]
    ?: throw GradleException("ADAPTER_TEST_ORM / -Dadapter.test.orm must be one of "
        + "${ormVersionSets.keys}, got '$adapterTestOrm'")

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    // Must match the gencode version cerbos-sdk-java was generated against (see the README
    // "Pin protobuf-java" gotcha) — older runtimes throw ProtobufRuntimeVersionException.
    implementation("com.google.protobuf:protobuf-java:4.35.1")
    // Spring Data JPA + Jakarta Persistence are provided by the consuming application's
    // Spring Boot BOM (or equivalent). Declaring them as `compileOnly` keeps them out of
    // the published POM as transitive dependencies so they don't pin a specific version on
    // downstream consumers — matching how Spring Data JPA itself marks `hibernate-core`
    // as `<optional>true</optional>`.
    //
    // The consumer's floor is spring-data-jpa 3.5.2, NOT this build-time version:
    // `Specification.unrestricted()` — what an ALWAYS_ALLOWED plan returns — arrived in 3.5.2,
    // so anything older fails with NoSuchMethodError at first translation. Nothing compiles or
    // tests against 3.5.2 itself, so the floor is a documented claim rather than a checked one:
    // if you change what API the adapter uses, re-derive it. The other two statements of it are
    // README.md "Install" and the Javadoc on SpringDataQueryPlanAdapter.alwaysAllowed().
    //
    // The `compileOnly` set follows the selected ORM set rather than staying on the baseline, on
    // purpose: the `next` leg exists to prove that MySqlDoubleCastFunctionContributor and the
    // adapter's classpath-guarded Hibernate probe COMPILE against the next major as well as run
    // on it, and a main source set compiled against 6.6 and merely executed on 7.x would prove
    // half of that. The published POM carries none of these, so the choice never reaches a
    // consumer.
    compileOnly("org.springframework.data:spring-data-jpa:${orm["springDataJpa"]}")
    compileOnly("jakarta.persistence:jakarta.persistence-api:${orm["jakartaPersistence"]}")
    // Hibernate is needed only to compile MySqlDoubleCastFunctionContributor (the MySQL
    // IEEE double-cast registration) and the adapter's classpath-guarded probe for it.
    // `compileOnly` for the same reason as Spring Data JPA above: the consuming
    // application provides its own Hibernate, and the adapter degrades gracefully (plain
    // cb.toDouble casts) when Hibernate is absent at runtime.
    compileOnly("org.hibernate.orm:hibernate-core:${orm["hibernate"]}")

    testImplementation("org.springframework.data:spring-data-jpa:${orm["springDataJpa"]}")
    testImplementation("jakarta.persistence:jakarta.persistence-api:${orm["jakartaPersistence"]}")
    testImplementation(platform("org.junit:junit-bom:6.1.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
    // Real-database legs for AdversarialConformanceTest (selected via ADAPTER_TEST_DB /
    // -Dadapter.test.db): PostgreSQL and MySQL containers + their JDBC drivers.
    testImplementation("org.testcontainers:testcontainers-postgresql:2.0.5")
    testImplementation("org.testcontainers:testcontainers-mysql:2.0.5")
    testRuntimeOnly("org.postgresql:postgresql:42.7.13")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.7.0")
    testImplementation("org.hibernate.orm:hibernate-core:${orm["hibernate"]}")
    testImplementation("com.h2database:h2:2.4.240")
    // Parses seeds.json/actions.json from the shared ../conformance/ corpus (see
    // AdversarialConformanceTest and conformance/README.md).
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
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
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }

    // The suites read a shared directory from OUTSIDE this project — the conformance corpus
    // (`AdversarialConformanceTest`, `SpringDataTranslatorTest`, `CerbosTestImage`), resolved
    // from `user.dir` at runtime, which Gradle cannot infer. Undeclared, it is not an input: a
    // `gradle build` over a stale `build/` reports BUILD SUCCESSFUL with `:test` UP-TO-DATE and
    // runs nothing after a corpus edit.
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
    // The golden expectations are read by SpringDataTranslatorTest the same way, and they live
    // inside this project rather than outside it — but not under a source set, so Gradle would
    // not see an edit to them either.
    inputs.dir(project.file("golden"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("goldenExpectations")
    // The two database pins are read by DatabaseTestImages from `user.dir` at runtime, for the
    // same reason as the corpus. Undeclared, bumping a server would leave `:test` UP-TO-DATE and
    // the real-database leg would report a pass it never ran against the new build. Both are
    // declared whichever leg runs: the harness reads both, and tracking only the selected leg's
    // pin would replay a stale pass after the other one moved.
    inputs.file(project.file("POSTGRES_IMAGE"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("postgresImage")
    inputs.file(project.file("MYSQL_IMAGE"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("mysqlImage")

    // Which ORM set the classpath resolved, forwarded so the translator suite can assert the
    // Hibernate it runs IS the one this build selected rather than infer the leg from the
    // classpath alone — a resolution that drifted to another major would otherwise read as
    // whichever leg it happened to match. An input too, so switching sets re-runs the task
    // rather than replaying the other leg's pass.
    systemProperty("adapter.test.orm", adapterTestOrm)
    inputs.property("adapterTestOrm", adapterTestOrm)

    // Select the database backing AdversarialConformanceTest: h2 (default), postgres, or
    // mysql. The MySQL leg creates its schema with a case-sensitive collation by default
    // (utf8mb4_0900_as_cs); override adapter.test.mysql.collation to reproduce the
    // over-grant on MySQL's default utf8mb4_0900_ai_ci — see the README
    // "Database collation requirements" section.
    val adapterTestDb = System.getProperty("adapter.test.db") ?: System.getenv("ADAPTER_TEST_DB")
    if (adapterTestDb != null) {
        systemProperty("adapter.test.db", adapterTestDb)
    }
    val mysqlCollation = System.getProperty("adapter.test.mysql.collation")
        ?: System.getenv("ADAPTER_TEST_MYSQL_COLLATION")
    if (mysqlCollation != null) {
        systemProperty("adapter.test.mysql.collation", mysqlCollation)
    }
    // The MySQL leg runs with Connector/J's default CLIENT-side prepared statements so the
    // differential oracle pins the adapter's `cast(... as double)` rendering (see the README
    // "MySQL: keeping arithmetic IEEE-faithful" gotcha). Set this to true to run the same
    // leg with server-side prepared statements — both modes must pass.
    val mysqlServerPrep = System.getProperty("adapter.test.mysql.serverPrepStmts")
        ?: System.getenv("ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS")
    if (mysqlServerPrep != null) {
        systemProperty("adapter.test.mysql.serverPrepStmts", mysqlServerPrep)
    }
}

// Rewrites golden/expectations.json from the SQL the translator emits today, then asserts the
// rewritten file — so the task fails if regeneration produced something the rules reject.
//
// Regeneration is a DELIBERATE act and the diff is the review, which is why it is a task of its
// own rather than a flag on `test`: CI never runs it, so a translator change that moves the
// emitted SQL fails there whatever anyone ran locally
// (conformance/README.md, "Golden expectations").
tasks.register<Test>("goldenUpdate") {
    group = "verification"
    description = "Rewrite spring-data/golden/expectations.json from what the translator emits."
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    filter { includeTestsMatching("dev.cerbos.queryplan.springdata.SpringDataTranslatorTest") }
    systemProperty("golden.update", "true")
    // Forwarded for the same reason as on `test`. Under the `next` set Corpus.writeGoldenExpectations
    // refuses before the write (conformance/README.md, "When the generator is an input", rule 2):
    // the asset declares the Hibernate minor that rendered it, and the bytes would be another's.
    systemProperty("adapter.test.orm", adapterTestOrm)
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

// One publication, `dev.cerbos:cerbos-spring-data:<version>`, from the `java` component.
//
// What it publishes is the point: `implementation` dependencies (cerbos-sdk-java,
// protobuf-java) land at runtime scope, and the `compileOnly` ones (spring-data-jpa,
// jakarta.persistence-api, hibernate-core) do not appear at all — a consumer brings its own,
// which is why they are compileOnly in the first place. example/ resolves this coordinate out
// of mavenLocal and therefore proves that resolution, which a composite build substitutes away.
//
// The example is never part of the artifact: it is a separate Gradle build under example/ with
// its own settings file, so it is not a source set here and cannot reach the jar. example/run.sh
// asserts that rather than leaving it to inspection (ADR 0002's "examples must stay out of the
// published artifacts they exercise", which for Java is a deliberate check rather than a
// `files` allowlist).
//
// This is `publishToMavenLocal` only. Nothing here configures a Maven Central release, which
// additionally requires POM `name`, `description`, `url`, `licenses`, `developers` and `scm`,
// plus signing — Central rejects a POM without them. The other Java adapter
// (elasticsearch-java) declares no publishing at all, so there is no convention here to copy
// and inventing one is not this change's job; what the example resolves is the dependency
// metadata, which is what it exists to exercise. Wiring up the `spring-data/v*` release is
// separate work, and it will change this block.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}
