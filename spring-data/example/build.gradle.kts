/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

import org.springframework.boot.gradle.tasks.bundling.BootJar

plugins {
    java
    id("org.springframework.boot") version "3.5.16"
    id("io.spring.dependency-management") version "1.1.7"
}

group = "dev.cerbos.example"
version = "0.0.1"

// Compile against the Java 17 API, as the adapter does. `demoJar` needs the value too.
val javaRelease = 17

tasks.withType<JavaCompile> {
    options.release = javaRelease
}

repositories {
    // mavenLocal serves only the adapter, so a stale ~/.m2 cannot shadow any other dependency.
    mavenLocal {
        content { includeModule("dev.cerbos", "cerbos-spring-data") }
    }
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    runtimeOnly("com.h2database:h2")

    // Run `../gradlew -p .. publishToMavenLocal` first (the scripts do). `isChanging` because every
    // publish overwrites the same version; see cacheChangingModulesFor below.
    implementation("dev.cerbos:cerbos-spring-data:0.1.0-alpha.1") { isChanging = true }
    // The adapter puts the SDK at runtime scope, and this app calls `cerbos.plan(...)` directly.
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")

    // No protobuf-java here: the adapter's POM must supply it (see ADR 0002).

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

// Otherwise a changing module is re-checked only once a day.
configurations.all {
    resolutionStrategy.cacheChangingModulesFor(0, "seconds")
}

tasks.test { useJUnitPlatform() }

// Two programs share this source tree; `bootRun` and `bootJar` run the photo-sharing app.
springBoot {
    mainClass = "dev.cerbos.example.photos.PhotosApplication"
}

// The demo-domain program as its own boot jar. run.sh launches it with `java -jar` so stdout
// carries only its JSON, with no Gradle output.
tasks.register<BootJar>("demoJar") {
    group = "build"
    description = "Executable jar for the demo-domain program run by demo/scripts/run-example.sh"
    mainClass = "dev.cerbos.example.demo.DemoApplication"
    classpath = sourceSets.main.get().runtimeClasspath
    targetJavaVersion = JavaVersion.toVersion(javaRelease)
    // Fixed so run.sh does not depend on `version`.
    archiveFileName = "demo.jar"
}
