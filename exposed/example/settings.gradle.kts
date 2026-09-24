rootProject.name = "cerbos-exposed-example"

// No `includeBuild("..")`, and its absence is the whole packaging argument.
//
// One line here would turn the declared coordinate into a Gradle composite build, which
// substitutes the adapter's local source tree for it and therefore resolves neither its POM nor
// its Gradle module metadata. Everything would still compile and every shape would still pass,
// while the half of the published surface an example exists to execute — dependency scopes above
// all — went untouched
// (docs/adr/0002-examples-install-the-packed-artifact.md). `cerbos-sdk-java` declaring
// protobuf-java at runtime-only scope is the precedent that ADR names, and this adapter's own POM
// puts BOTH of its dependencies there.
//
// The adapter is resolved from mavenLocal instead — see the repository filter in build.gradle.kts
// — so `gradle -p .. publishToMavenLocal` is a prerequisite of every entry point here, which
// run.sh does for itself. run.sh also refuses to launch when the resolved adapter jar turns out to
// sit inside the adapter's own build directory, because a composite build is otherwise entirely
// silent.
