/*
 * Copyright 2021-2026 Zenauth Ltd.
 * SPDX-License-Identifier: Apache-2.0
 */

plugins {
    java
}

group = "dev.cerbos.example"
version = "0.0.1"

// Compile against the Java 17 API, the adapter's floor.
tasks.withType<JavaCompile> {
    options.release = 17
}

repositories {
    // mavenLocal serves only the adapter, so a stale ~/.m2 cannot shadow any other dependency.
    mavenLocal {
        content { includeModule("dev.cerbos", "cerbos-jpa") }
    }
    mavenCentral()
}

dependencies {
    // Run `../gradlew -p .. publishToMavenLocal` first (run.sh does). `isChanging` because every
    // publish overwrites the same version; see cacheChangingModulesFor below.
    implementation("dev.cerbos:cerbos-jpa:0.1.0-alpha.1") { isChanging = true }

    // The adapter puts the SDK at runtime scope, and this app calls `cerbos.plan(...)` directly.
    implementation("dev.cerbos:cerbos-sdk-java:0.20.1")

    // Plain Hibernate ORM, bootstrapped from META-INF/persistence.xml: no Spring, no container.
    implementation("org.hibernate.orm:hibernate-core:6.6.59.Final")
    runtimeOnly("com.h2database:h2:2.5.252")

    implementation("com.fasterxml.jackson.core:jackson-databind:2.23.0")

    // No protobuf-java here: the adapter's POM must supply it (see ADR 0002).
}

configurations.all {
    resolutionStrategy {
        // Otherwise a changing module is re-checked only once a day.
        cacheChangingModulesFor(0, "seconds")

        // Exact versions only, so every dependency bump is a PR that runs the example job.
        // failOnChangingVersions() stays off because the adapter is declared changing.
        failOnDynamicVersions()
    }
}

// Writes the resolved runtime classpath for run.sh to pass to `java -cp`. installDist and a fat jar
// would copy the jars, which hides whether the adapter came from mavenLocal (DemoApplication checks
// this), and a JavaExec task would put Gradle output on stdout.
val runtimeClasspathFile = layout.buildDirectory.file("runtime-classpath.txt")

tasks.register("writeRuntimeClasspath") {
    group = "build"
    description = "Write the resolved runtime classpath for demo/scripts/run-example.sh to launch."
    val classpath = sourceSets.main.get().runtimeClasspath
    // Also makes this task depend on compilation.
    inputs.files(classpath)
    outputs.file(runtimeClasspathFile)
    doLast {
        runtimeClasspathFile.get().asFile.writeText(classpath.asPath)
    }
}
