/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

// Not part of any CI suite: ADAPTER_DOMAIN_STORE=h2 ../../../../exposed/gradlew test
plugins {
    kotlin("jvm") version "2.4.21"
}

repositories {
    mavenCentral()
}

val exposed = "1.6.1"

dependencies {
    testImplementation("dev.cerbos:cerbos-exposed:0.1.0-alpha.1")
    testImplementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    testImplementation("com.google.protobuf:protobuf-java:4.36.2")
    testImplementation("com.google.protobuf:protobuf-java-util:4.36.2")
    testImplementation("org.jetbrains.exposed:exposed-core:$exposed")
    testImplementation("org.jetbrains.exposed:exposed-jdbc:$exposed")
    testImplementation("org.jetbrains.exposed:exposed-dao:$exposed")
    testImplementation("org.jetbrains.exposed:exposed-java-time:$exposed")
    testImplementation("com.h2database:h2:2.5.252")
    testImplementation("org.xerial:sqlite-jdbc:3.54.1.0")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.23.0")
    testImplementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.23.0")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")
    testRuntimeOnly("org.postgresql:postgresql:42.7.14")
    testRuntimeOnly("com.mysql:mysql-connector-j:9.7.0")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1g"
    systemProperty("user.timezone", "UTC")
    outputs.upToDateWhen { false }
}
