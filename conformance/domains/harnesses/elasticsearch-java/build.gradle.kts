/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

// Not part of any CI suite: ADAPTER_DOMAIN_ES_URL=http://localhost:49200 ../../../../elasticsearch-java/gradlew test
plugins {
    java
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation("dev.cerbos:cerbos-elasticsearch:0.1.0")
    testImplementation("dev.cerbos:cerbos-sdk-java:0.20.1")
    testImplementation("com.google.protobuf:protobuf-java:4.36.2")
    testImplementation("com.google.protobuf:protobuf-java-util:4.36.2")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.23.0")
    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.20")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1g"
    outputs.upToDateWhen { false }
}
