plugins {
    java
    id("org.springframework.boot") version "3.5.1"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "dev.cerbos.example"
version = "0.0.1"

// Match the adapter build: `options.release` pins the Java 17 API surface as well as the
// language level, so newer compile JDKs can't leak post-17 APIs into the bytecode.
tasks.withType<JavaCompile> {
    options.release = 17
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    runtimeOnly("com.h2database:h2")

    // Pulled in via the composite-build include in settings.gradle.kts — points at ../
    implementation("dev.cerbos:cerbos-spring-data:0.1.0-alpha.1")
    implementation("dev.cerbos:cerbos-sdk-java:0.18.0")
    // Match the protobuf-java gencode the SDK was generated against; older versions throw
    // RuntimeVersion$ProtobufRuntimeVersionException at first message decode.
    implementation("com.google.protobuf:protobuf-java:4.33.5")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
}

tasks.test { useJUnitPlatform() }
