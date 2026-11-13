/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    `java-library`
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
// `baseline` is the Hibernate 6.6 line; `next` is the next major (Hibernate 7 / Spring Data JPA 4),
// a test-only leg that runs every suite again. An unknown value fails. Keep the Hibernate and
// Jakarta Persistence versions equal to jpa/build.gradle.kts's.
val ormVersionSets = mapOf(
    "baseline" to mapOf(
        "springDataJpa" to "3.5.13",
        "jakartaPersistence" to "3.1.0",
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
    // The translator. Its types (AttributeMapping, Options, the refusals) are this adapter's API,
    // so it is an `api` dependency.
    api("dev.cerbos:cerbos-jpa:0.1.0-alpha.1")
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    // Must match the gencode version cerbos-sdk-java was generated against (see jpa/README.md,
    // "Pin protobuf-java"); older runtimes throw ProtobufRuntimeVersionException.
    implementation("com.google.protobuf:protobuf-java:4.36.2")
    // The consumer brings Spring Data JPA, Jakarta Persistence and Hibernate, so they stay out of
    // the published POM. The consumer floor is spring-data-jpa 3.5.2, for
    // `Specification.unrestricted()`; nothing tests that version, so re-check it when the adapter
    // starts using new API (it is also stated in README.md "Install" and on of()).
    // These follow the selected ORM set so the `next` leg also compiles against Spring Data JPA 4.
    compileOnly("org.springframework.data:spring-data-jpa:${orm["springDataJpa"]}")
    compileOnly("jakarta.persistence:jakarta.persistence-api:${orm["jakartaPersistence"]}")

    testImplementation("org.springframework.data:spring-data-jpa:${orm["springDataJpa"]}")
    testImplementation("jakarta.persistence:jakarta.persistence-api:${orm["jakartaPersistence"]}")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.hibernate.orm:hibernate-core:${orm["hibernate"]}")
    testImplementation("com.h2database:h2:2.5.252")
    // Reads the shared ../conformance/ corpus (golden plans are protobuf JSON).
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.23.0")
    testImplementation("com.google.protobuf:protobuf-java-util:4.36.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }

    // The suites read these at runtime, so Gradle cannot see them unless they are declared.
    // Without this, `:test` stays UP-TO-DATE after a corpus or ledger edit.
    inputs.dir(project.file("../conformance"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceCorpus")
    inputs.file(project.file("conformance-ledger.json"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceLedger")
    inputs.property("adapterTestOrm", adapterTestOrm)
}

// publishToMavenLocal only: no Maven Central release is configured yet, so there is no signing.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Cerbos Spring Data adapter")
                description.set("Translates Cerbos query plans into Spring Data JPA Specifications")
                url.set("https://github.com/cerbos/query-plan-adapters/tree/main/spring-data")
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
