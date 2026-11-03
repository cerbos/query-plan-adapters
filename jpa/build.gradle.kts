/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    java
    // example/ resolves the adapter from mavenLocal so it runs against the published POM.
    // See docs/adr/0002-examples-install-the-packed-artifact.md.
    `maven-publish`
}

group = "dev.cerbos"
version = "0.1.0-alpha.1"

java {
    withJavadocJar()
    withSourcesJar()
}

// `release` also limits the JDK API to Java 17, so a post-17 API call fails at compile time.
tasks.withType<JavaCompile> {
    options.release = 17
    options.compilerArgs.add("-Xlint:deprecation")
}

repositories {
    mavenCentral()
}

// The ORM versions to build and test against, chosen by ADAPTER_TEST_ORM (or -Dadapter.test.orm).
// `baseline` is the Hibernate 6.6 line; `next` is the next major (Hibernate 7), a test-only leg that
// runs every suite again. An unknown value fails. spring-data/build.gradle.kts pairs each set with a
// Spring Data JPA version. `baseline` compiles against Jakarta Persistence 3.1, the version Hibernate
// 6.6 implements, so the adapter cannot reach for 3.2-only API (README.md "Install").
val ormVersionSets = mapOf(
    "baseline" to mapOf(
        "jakartaPersistence" to "3.1.0",
        "hibernate" to "6.6.54.Final",
    ),
    "next" to mapOf(
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
    // "Pin protobuf-java" gotcha); older runtimes throw ProtobufRuntimeVersionException.
    implementation("com.google.protobuf:protobuf-java:4.35.1")
    // The consumer brings Jakarta Persistence and its provider, so they stay out of the published
    // POM. These follow the selected ORM set so the `next` leg also compiles against Hibernate 7.
    compileOnly("jakarta.persistence:jakarta.persistence-api:${orm["jakartaPersistence"]}")
    // Only for MySqlDoubleCastFunctionContributor and its classpath probe; without Hibernate at
    // runtime the adapter falls back to plain cb.toDouble casts.
    compileOnly("org.hibernate.orm:hibernate-core:${orm["hibernate"]}")

    testImplementation("jakarta.persistence:jakarta.persistence-api:${orm["jakartaPersistence"]}")
    testImplementation(platform("org.junit:junit-bom:6.1.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
    // PostgreSQL and MySQL legs of AdversarialConformanceTest (ADAPTER_TEST_DB).
    testImplementation("org.testcontainers:testcontainers-postgresql:2.0.5")
    testImplementation("org.testcontainers:testcontainers-mysql:2.0.5")
    testRuntimeOnly("org.postgresql:postgresql:42.7.13")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.7.0")
    testImplementation("org.hibernate.orm:hibernate-core:${orm["hibernate"]}")
    testImplementation("com.h2database:h2:2.4.240")
    // Reads the shared ../conformance/ corpus (golden plans are protobuf JSON).
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    testImplementation("com.google.protobuf:protobuf-java-util:4.35.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }

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

    inputs.property("adapterTestOrm", adapterTestOrm)

    // AdversarialConformanceTest's database: h2 (default), postgres or mysql. The PostgreSQL
    // initdb arguments and the MySQL collation can be overridden to reproduce the over-grant; see
    // README.md "Database collation requirements".
    val adapterTestDb = System.getProperty("adapter.test.db") ?: System.getenv("ADAPTER_TEST_DB")
    if (adapterTestDb != null) {
        systemProperty("adapter.test.db", adapterTestDb)
    }
    val postgresInitdbArgs = System.getProperty("adapter.test.postgres.initdbArgs")
        ?: System.getenv("ADAPTER_TEST_POSTGRES_INITDB_ARGS")
    if (postgresInitdbArgs != null) {
        systemProperty("adapter.test.postgres.initdbArgs", postgresInitdbArgs)
    }
    val mysqlCollation = System.getProperty("adapter.test.mysql.collation")
        ?: System.getenv("ADAPTER_TEST_MYSQL_COLLATION")
    if (mysqlCollation != null) {
        systemProperty("adapter.test.mysql.collation", mysqlCollation)
    }
    // true runs the MySQL leg with server-side prepared statements; both modes must pass. See
    // README.md "MySQL: keeping arithmetic IEEE-faithful".
    val mysqlServerPrep = System.getProperty("adapter.test.mysql.serverPrepStmts")
        ?: System.getenv("ADAPTER_TEST_MYSQL_SERVER_PREP_STMTS")
    if (mysqlServerPrep != null) {
        systemProperty("adapter.test.mysql.serverPrepStmts", mysqlServerPrep)
    }
}

// publishToMavenLocal only: no Maven Central release is configured yet, so there is no signing.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Cerbos Jakarta Persistence adapter")
                description.set("Translates Cerbos query plans into Jakarta Persistence criteria predicates")
                url.set("https://github.com/cerbos/query-plan-adapters/tree/main/jpa")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("cerbosdev")
                        name.set("Cerbos Developers")
                        email.set("sdk@cerbos.dev")
                    }
                }
                scm {
                    url.set("https://github.com/cerbos/query-plan-adapters")
                }
            }
        }
    }
}
