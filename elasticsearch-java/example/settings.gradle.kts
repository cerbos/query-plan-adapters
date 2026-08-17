rootProject.name = "cerbos-elasticsearch-example"

// No `includeBuild("..")`, deliberately.
//
// A Gradle composite build substitutes the adapter's local source tree for the declared coordinate,
// and therefore resolves neither the POM nor the Gradle module metadata the adapter publishes. That
// metadata is half of what an example exists to prove
// (docs/adr/0002-examples-install-the-packed-artifact.md), and the class of bug it hides is not
// hypothetical: cerbos-sdk-java declares protobuf at runtime-only scope in its own module metadata,
// and this adapter's own POM does the same for both of its dependencies.
//
// The adapter is resolved from mavenLocal instead — see the filtered repository in
// build.gradle.kts — so `gradle -p .. publishToMavenLocal` is a prerequisite of every entry point
// here. run.sh does it itself, and then asserts that what got loaded was the published jar rather
// than a substituted source tree, because a composite build would put this file back in a way
// nothing else would notice.
