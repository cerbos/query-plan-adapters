# Example applications install the packed artifact, not adapter source

Each example builds its adapter into a real distributable — `npm pack`, `pdm build`,
`publishToMavenLocal` — and installs that, rather than linking the adapter's source directory.
Go is the one exception and uses a `replace` directive.

## Why

Every conformance harness imports its adapter from source (`from "."`, the module under test).
That leaves the published surface entirely unexercised: a broken `exports` map, a missing type
declaration, a file omitted from the `files` allowlist, or an unsatisfiable peer range would ship
without failing anything. Installing the packed artifact is the only way the example covers
something the harness structurally cannot.

This is not hypothetical. `cerbos-sdk-java` declares protobuf at runtime-only scope in its Gradle
metadata, which a composite build hides and a real Maven coordinate exposes.
`spring-data/example/` is a composite build today and therefore misses exactly this class of bug.

## Consequences

Go cannot participate. There is no packaging step, and a Go example either uses `replace` — which
is not what a consumer does — or resolves a published tag, which would never test the change under
review. The Go examples use `replace` and prove usage shapes only. Their READMEs should say so
rather than implying packaging coverage they do not have.

Examples must stay out of the published artifacts they exercise. The TypeScript adapters already
get this for free from their `files` allowlists, which name `lib/` and `src/` explicitly; Go gets
it from nested-module exclusion. Python and Java need it checked deliberately.
