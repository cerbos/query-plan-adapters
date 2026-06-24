rootProject.name = "cerbos-spring-data-photos-example"

// Consume the local cerbos-spring-data adapter source tree directly via a Gradle composite
// build — no need to publish the adapter to mavenLocal first. The included build's
// group/name/version (dev.cerbos:cerbos-spring-data:0.1.0-alpha.1) auto-substitutes for the
// declared dependency below.
includeBuild("..")
