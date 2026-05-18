plugins {
    java
}

group = "dev.cerbos"
version = "0.1.0-alpha.1"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.18.0")
    implementation("com.google.protobuf:protobuf-java:4.31.1")
    implementation("org.springframework.data:spring-data-jpa:3.5.1")
    implementation("jakarta.persistence:jakarta.persistence-api:3.2.0")

    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:1.21.3")
    testImplementation("org.testcontainers:junit-jupiter:1.21.3")
    testImplementation("org.hibernate.orm:hibernate-core:6.6.18.Final")
    testImplementation("com.h2database:h2:2.3.232")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
    // Propagate Cerbos PDP connection details to the test JVM so SpringDataIntegrationTest can
    // choose between the Testcontainers-managed PDP (default) and an externally-managed one
    // (e.g. spawned by docker-compose for CI / `scripts/run-e2e.sh`).
    val cerbosHost = System.getenv("CERBOS_HOST")
    val cerbosPort = System.getenv("CERBOS_PORT")
    if (cerbosHost != null) {
        environment("CERBOS_HOST", cerbosHost)
    }
    if (cerbosPort != null) {
        environment("CERBOS_PORT", cerbosPort)
    }
}
