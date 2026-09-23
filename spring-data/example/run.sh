#!/usr/bin/env bash
#
# Called by `demo/scripts/run-example.sh spring-data`, which starts the PDP and sets CERBOS_HOST.
# Publishes the adapter, builds the demo-domain program against it and runs it on in-memory H2.
#
# stdout carries exactly one JSON document, diffed against demo/expected.json. Everything else
# goes to stderr.
#
# Pre-reqs: JDK 17+. Gradle comes from the adapter's wrapper.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"

# 1. Install the adapter into mavenLocal. The example resolves it as a Maven coordinate
#    (docs/adr/0002-examples-install-the-packed-artifact.md).
echo "==> gradlew publishToMavenLocal (dev.cerbos:cerbos-spring-data)" >&2
"${ADAPTER_DIR}/gradlew" -p "${ADAPTER_DIR}" publishToMavenLocal --no-daemon >&2

# 2. The example must not ship inside any adapter jar (ADR 0002).
found_jar=0
for jar in "${ADAPTER_DIR}"/build/libs/cerbos-spring-data-*.jar; do
  [[ -f "${jar}" ]] || continue
  found_jar=1
  # Not piped: under pipefail, an early `grep -q` match would kill `jar` with SIGPIPE.
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

# 3. Build the demo-domain jar. Running it with plain `java -jar` keeps Gradle output off stdout.
echo "==> gradlew demoJar" >&2
"${ADAPTER_DIR}/gradlew" -p "${EXAMPLE_DIR}" demoJar --no-daemon >&2

# 4. Run. CERBOS_HOST is inherited from the shared runner.
echo "==> java -jar build/libs/demo.jar" >&2
java "-Ddemo.dir=${REPO_ROOT}/demo" -jar build/libs/demo.jar
