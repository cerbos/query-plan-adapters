plugins {
    java
}

group = "dev.cerbos"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("dev.cerbos:cerbos-sdk-java:0.19.0")
    implementation("com.google.protobuf:protobuf-java:4.35.1")

    testImplementation(platform("org.junit:junit-bom:6.1.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.testcontainers:testcontainers:2.0.5")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.testcontainers:elasticsearch:1.21.4")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.22.1")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.18")
}

tasks.test {
    useJUnitPlatform()

    // The suites read two shared directories from OUTSIDE this project — the conformance corpus
    // (`ElasticsearchAdversarialConformanceTest`, `CerbosTestImage`) and the shared policy suite
    // (`ElasticsearchIntegrationTest`) — both resolved from `user.dir` at runtime, which Gradle
    // cannot infer. Undeclared, they are not inputs: a `gradle build` over a stale `build/`
    // reports BUILD SUCCESSFUL with `:test` UP-TO-DATE and runs nothing after a corpus edit.
    //
    // That is a green local validation of a change it never executed, which is the exact failure
    // the corpus exists to prevent. CI never saw it because it always starts from a clean
    // checkout, so the gap only ever bites the maintainer editing the corpus.
    //
    // The whole directory is declared rather than the specific files. The corpus is read by
    // filename in several places and grows new ones (the wire fixtures are next), and a precise
    // list would silently stop covering what it names — the same class of bug one level up.
    inputs.dir(project.file("../conformance"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("conformanceCorpus")
    // Goes away with the shared policy suite itself (cerbos/query-plan-adapters#385).
    inputs.dir(project.file("../policies"))
        .withPathSensitivity(PathSensitivity.RELATIVE)
        .withPropertyName("sharedPolicies")
}
