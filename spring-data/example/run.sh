#!/usr/bin/env bash
#
# The spring-data half of `demo/scripts/run-example.sh spring-data`: publish the adapter, build
# the demo-domain program against that published coordinate, run it. The PDP is already up and
# reachable at $CERBOS_HOST — the shared runner owns it.
#
# The store is H2, in memory, inside the program's own JVM, so there is nothing to start here.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/cases.json.
#
# Pre-reqs: gradle (8.x), JDK 17+.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"

# 1. Build the adapter and install it into mavenLocal.
#
# This is the Java form of "pack the adapter into a real distributable and install THAT"
# (docs/adr/0002-examples-install-the-packed-artifact.md). It replaces a Gradle composite build,
# which substituted the adapter's source tree for the declared coordinate and therefore resolved
# neither its POM nor its Gradle module metadata — so dependency scopes, the exact thing that
# bites a Java consumer, went unexecuted. cerbos-sdk-java declaring protobuf at runtime-only
# scope is the shape of bug that hides behind a composite build.
echo "==> gradle publishToMavenLocal (dev.cerbos:cerbos-spring-data)" >&2
gradle -p "${ADAPTER_DIR}" publishToMavenLocal --no-daemon >&2

# 2. The example must stay OUT of the artifact it exercises. TypeScript adapters get this from
#    their `files` allowlist and Go from nested-module exclusion; Java has neither, so ADR 0002
#    asks for it to be checked deliberately. It holds today because example/ is a separate Gradle
#    build rather than a source set of the adapter — which is exactly the kind of fact that stops
#    being true without anyone noticing.
found_jar=0
for jar in "${ADAPTER_DIR}"/build/libs/cerbos-spring-data-*.jar; do
  [[ -f "${jar}" ]] || continue
  found_jar=1
  # Listed into a variable rather than piped into grep: under `set -o pipefail`, a `grep -q` that
  # matches early closes the pipe, `jar` dies of SIGPIPE, and the pipeline reports failure — so
  # the `if` would read as "no example classes" in exactly the case this exists to catch.
  entries="$(jar tf "${jar}")"
  if grep -q '^dev/cerbos/example/' <<<"${entries}"; then
    echo "$(basename "${jar}") contains example classes — the example must not ship inside" \
      "the adapter (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
    exit 1
  fi
done
if (( found_jar == 0 )); then
  echo "no adapter jar under ${ADAPTER_DIR}/build/libs" >&2
  exit 1
fi

# 3. Build the demo-domain program's own executable jar.
#
#    A jar, and then a plain `java -jar`, because of the output contract: stdout must carry one
#    JSON document and nothing else, and a Gradle invocation's stdout also carries Gradle's
#    lifecycle output and whatever deprecation notice it decides to print that day. Building here
#    (with Gradle's own output on stderr) and launching without Gradle in the process keeps
#    stdout the program's alone. DemoApplication does the other half by redirecting System.out.
echo "==> gradle demoJar" >&2
gradle demoJar --no-daemon >&2

# 4. Run. stdout is the JSON document and nothing else.
#
#    demo/ is passed in rather than derived inside the program: how many directories up the
#    repository root sits is this script's business, not the application's. CERBOS_HOST comes
#    from the runner and is inherited; the program refuses to start without it.
echo "==> java -jar build/libs/demo.jar" >&2
java "-Ddemo.dir=${REPO_ROOT}/demo" -jar build/libs/demo.jar
