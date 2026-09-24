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
version = "0.1.0"

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

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    // At least the protobuf version cerbos-sdk-java was generated with; an older runtime throws
    // ProtobufRuntimeVersionException on first decode.
    implementation("com.google.protobuf:protobuf-java:4.35.1")

    testImplementation(platform("org.junit:junit-bom:6.1.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    // Testcontainers 2.x renamed its modules; keep all three on the same version.
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter:2.0.5")
    testImplementation("org.testcontainers:testcontainers-elasticsearch:2.0.5")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    // JsonFormat decodes the recorded plans in conformance/golden/ (see Corpus.goldens).
    testImplementation("com.google.protobuf:protobuf-java-util:4.35.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

tasks.test {
    useJUnitPlatform()
    // The suites read these at runtime, so Gradle cannot see them unless they are declared.
    // Without this, `:test` stays UP-TO-DATE after a corpus or ledger edit.
    inputs.dir(project.file("../conformance"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceCorpus")
    inputs.file(project.file("conformance-ledger.json"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceLedger")
    // The file naming the Elasticsearch image the container-backed suites start. CI sets
    // ELASTICSEARCH_IMAGE_FILE=ELASTICSEARCH_NEXT_IMAGE for the next-major leg.
    val elasticsearchImageFile = System.getProperty("elasticsearch.test.image.file")
        ?: System.getenv("ELASTICSEARCH_IMAGE_FILE")
        ?: "ELASTICSEARCH_IMAGE"
    systemProperty("elasticsearch.test.image.file", elasticsearchImageFile)
    // An input, so bumping the image or switching files re-runs the tests.
    inputs.file(project.file(elasticsearchImageFile))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("elasticsearchImage")
}

// publishToMavenLocal only: no Maven Central release is configured yet, so there is no signing.
publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Cerbos Elasticsearch adapter")
                description.set("Translates Cerbos query plans into Elasticsearch Query DSL filters")
                url.set("https://github.com/cerbos/query-plan-adapters/tree/main/elasticsearch-java")
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
