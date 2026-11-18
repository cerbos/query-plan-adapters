/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

// Not part of any CI suite: ADAPTER_DOMAIN_STORE=h2 ../../../../jpa/gradlew test
plugins {
    java
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("dev.cerbos:cerbos-jpa:0.1.0-alpha.1")
    testImplementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    testImplementation("com.google.protobuf:protobuf-java:4.36.2")
    testImplementation("com.google.protobuf:protobuf-java-util:4.36.2")
    testImplementation("jakarta.persistence:jakarta.persistence-api:3.1.0")
    testImplementation("org.hibernate.orm:hibernate-core:6.6.54.Final")
    testImplementation("com.h2database:h2:2.5.252")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.23.0")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.postgresql:postgresql:42.7.14")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.7.0")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1g"
    systemProperty("user.timezone", "UTC")
    outputs.upToDateWhen { false }
    testLogging { showStandardStreams = true }
}
