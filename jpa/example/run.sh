#!/usr/bin/env bash
#
# Called by `demo/scripts/run-example.sh jpa`, which starts the PDP and sets CERBOS_HOST.
# Publishes the adapter, builds this example against the published artifact and runs it with
# plain Hibernate ORM on in-memory H2.
#
# stdout carries exactly one JSON document, diffed against demo/expected.json. Everything else
# goes to stderr.
#
# Pre-reqs: JDK 17+. Gradle comes from the adapter's wrapper.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

ARTIFACT_ID="cerbos-jpa"
ADAPTER_PACKAGE_PATH="dev/cerbos/queryplan/jpa"
EXAMPLE_PACKAGE_PATH="dev/cerbos/example"

cd "${EXAMPLE_DIR}"

# 1. Install the adapter into mavenLocal. The example resolves it as a Maven coordinate
#    (docs/adr/0002-examples-install-the-packed-artifact.md); step 4 checks that.
echo "==> gradlew publishToMavenLocal (dev.cerbos:${ARTIFACT_ID})" >&2
"${ADAPTER_DIR}/gradlew" -p "${ADAPTER_DIR}" publishToMavenLocal --no-daemon >&2

# 2. The example must not ship inside any adapter jar (ADR 0002). Also check the main jar contains
#    the adapter, so an empty jar cannot pass.
found_jar=0
for jar in "${ADAPTER_DIR}"/build/libs/"${ARTIFACT_ID}"-*.jar; do
  [[ -f "${jar}" ]] || continue
  found_jar=1
  # Not piped: under pipefail, an early `grep -q` match would kill `jar` with SIGPIPE.
  entries="$(jar tf "${jar}")"
  if grep -q "^${EXAMPLE_PACKAGE_PATH}/" <<<"${entries}"; then
    echo "$(basename "${jar}") contains example classes — the example must not ship inside the" \
      "adapter (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
    exit 1
  fi
  # The javadoc and sources jars carry no classes.
  [[ "${jar}" == *-javadoc.jar || "${jar}" == *-sources.jar ]] && continue
  if ! grep -q "^${ADAPTER_PACKAGE_PATH}/JpaQueryPlanAdapter.class$" <<<"${entries}"; then
    echo "$(basename "${jar}") does not contain the adapter itself, so the check above proved" \
      "nothing about what it excludes" >&2
    exit 1
  fi
done
if (( found_jar == 0 )); then
  echo "no adapter jar under ${ADAPTER_DIR}/build/libs" >&2
  exit 1
fi

# 3. Build the example and write its resolved runtime classpath. Step 4 launches `java` directly,
#    so stdout carries no Gradle output; see build.gradle.kts for why not installDist or a fat jar.
echo "==> gradlew writeRuntimeClasspath" >&2
"${ADAPTER_DIR}/gradlew" -p "${EXAMPLE_DIR}" writeRuntimeClasspath --no-daemon >&2

CLASSPATH_FILE="${EXAMPLE_DIR}/build/runtime-classpath.txt"
[[ -s "${CLASSPATH_FILE}" ]] || { echo "no resolved classpath at ${CLASSPATH_FILE}" >&2; exit 1; }

# The classpath must hold no Spring: this example proves the adapter needs none.
if tr ':' '\n' <"${CLASSPATH_FILE}" | grep -q '/spring-'; then
  echo "the example's runtime classpath contains Spring; the jpa adapter must not need it" >&2
  exit 1
fi

# Print where the key artifacts were resolved from: ~/.m2 for the adapter means the published jar.
# `|| true` because grep exits non-zero on no match; that case is reported below.
resolved="$(tr ':' '\n' <"${CLASSPATH_FILE}" \
  | grep -E "/(${ARTIFACT_ID}|cerbos-sdk-java|hibernate-core|protobuf-java)-[0-9]" || true)"
if [[ -z "${resolved}" ]]; then
  echo "the resolved classpath in ${CLASSPATH_FILE} names none of the expected artifacts" >&2
  exit 1
fi
printf '%s\n' "${resolved}" >&2

# 4. Run. CERBOS_HOST is inherited from the shared runner. The program checks the adapter was
#    loaded from the published jar.
echo "==> java DemoApplication" >&2
java \
  "-Ddemo.dir=${REPO_ROOT}/demo" \
  "-Dadapter.dir=${ADAPTER_DIR}" \
  -cp "$(cat "${CLASSPATH_FILE}")" \
  dev.cerbos.example.demo.DemoApplication
