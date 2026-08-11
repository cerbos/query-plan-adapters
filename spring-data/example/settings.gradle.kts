rootProject.name = "cerbos-spring-data-photos-example"

// No `includeBuild("..")`. This example used to consume the adapter through a Gradle composite
// build, which substitutes the local source tree for the declared coordinate — and therefore
// never resolves the POM or the Gradle module metadata the adapter actually publishes. That is
// half of what an example exists to prove
// (docs/adr/0002-examples-install-the-packed-artifact.md), and the class of bug it hides is not
// hypothetical: cerbos-sdk-java declares protobuf at runtime-only scope in its own module
// metadata.
//
// The adapter is resolved from mavenLocal instead — see the repository filter in
// build.gradle.kts — so `gradle -p .. publishToMavenLocal` is a prerequisite of every entry
// point here. run.sh, scripts/smoke.sh and scripts/smoke-edge-cases.sh each do it themselves.
