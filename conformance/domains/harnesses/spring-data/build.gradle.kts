/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

// Not part of any CI suite: ../../../../spring-data/gradlew test (H2). It reuses the jpa domain
// mappings, since this adapter takes jpa's AttributeMapping as is.
plugins {
    java
}

repositories {
    mavenCentral()
}

sourceSets.test {
    java.srcDir("../jpa/src/test/java")
    java.exclude("**/DomainHarnessTest.java")
}

dependencies {
    testImplementation("dev.cerbos:cerbos-spring-data:0.1.0-alpha.1")
    testImplementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    testImplementation("com.google.protobuf:protobuf-java:4.36.2")
    testImplementation("com.google.protobuf:protobuf-java-util:4.36.2")
    testImplementation("org.springframework.data:spring-data-jpa:3.5.13")
    testImplementation("jakarta.persistence:jakarta.persistence-api:3.2.0")
    testImplementation("org.hibernate.orm:hibernate-core:6.6.59.Final")
    testImplementation("com.h2database:h2:2.5.252")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.23.0")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1g"
    systemProperty("user.timezone", "UTC")
    outputs.upToDateWhen { false }
}
