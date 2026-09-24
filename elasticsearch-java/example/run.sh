#!/usr/bin/env bash
#
# Called by `demo/scripts/run-example.sh elasticsearch-java`, which starts the PDP and sets
# CERBOS_HOST. Publishes the adapter, starts Elasticsearch, builds this example against the
# published artifact and runs it.
#
# stdout carries exactly one JSON document, diffed against demo/expected.json. Everything else
# goes to stderr.
#
# Pre-reqs: docker, curl, JDK 17+. Gradle comes from the adapter's wrapper.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

ARTIFACT_ID="cerbos-elasticsearch"
ADAPTER_PACKAGE_PATH="dev/cerbos/queryplan/elasticsearch"
EXAMPLE_PACKAGE_PATH="dev/cerbos/example"

# The same pin the adapter's container-backed suites use.
ES_IMAGE="$(cat "${ADAPTER_DIR}/ELASTICSEARCH_IMAGE")"
ES_CONTAINER="cerbos-demo-elasticsearch-java"
# Not the default 9200, which the adapter's own suites or another local server may be using.
ES_PORT=19200
ES_URL="http://127.0.0.1:${ES_PORT}"

cleanup() {
  local status=$?
  if (( status != 0 )); then
    # The checks below can fail before the container exists.
    if docker container inspect "${ES_CONTAINER}" >/dev/null 2>&1; then
      echo "==> elasticsearch-java example failed (exit ${status}): Elasticsearch container logs" >&2
      # The image logs to stdout; send it to stderr.
      docker logs --tail 40 "${ES_CONTAINER}" >&2 || true
    else
      echo "==> elasticsearch-java example failed (exit ${status}) before the store was started" >&2
    fi
  fi
  docker rm -f "${ES_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cd "${EXAMPLE_DIR}"

# 1. Install the adapter into mavenLocal. The example resolves it as a Maven coordinate
#    (docs/adr/0002-examples-install-the-packed-artifact.md); step 7 checks that.
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
  if ! grep -q "^${ADAPTER_PACKAGE_PATH}/ElasticsearchQueryPlanAdapter.class$" <<<"${entries}"; then
    echo "$(basename "${jar}") does not contain the adapter itself, so the check above proved" \
      "nothing about what it excludes" >&2
    exit 1
  fi
done
if (( found_jar == 0 )); then
  echo "no adapter jar under ${ADAPTER_DIR}/build/libs" >&2
  exit 1
fi

# 3. The client version in build.gradle.kts and the server image must share a major: the 8.x
#    client refuses a 9.x server. Strip the digest first, since it contains a colon.
es_image_tag="${ES_IMAGE%%@*}"
es_image_tag="${es_image_tag##*:}"
es_client_version="$(sed -n 's/.*co\.elastic\.clients:elasticsearch-java:\([0-9][^"]*\)".*/\1/p' \
  "${EXAMPLE_DIR}/build.gradle.kts")"
if [[ -z "${es_client_version}" ]]; then
  echo "no co.elastic.clients:elasticsearch-java version found in build.gradle.kts — the check" \
    "below has stopped describing anything" >&2
  exit 1
fi
if [[ "${es_client_version%%.*}" != "${es_image_tag%%.*}" ]]; then
  echo "the Elasticsearch client is ${es_client_version} and ${ADAPTER_DIR}/ELASTICSEARCH_IMAGE" \
    "pins server ${es_image_tag} — the majors must match" >&2
  exit 1
fi
echo "==> client ${es_client_version} against server ${es_image_tag}" >&2

# 4. Start Elasticsearch now so it warms up during the build. Security is off, as in the adapter's
#    suites.
echo "==> starting ${ES_IMAGE} on ${ES_PORT}" >&2
docker rm -f "${ES_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${ES_CONTAINER}" -p "${ES_PORT}:9200" \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  "${ES_IMAGE}" >/dev/null

# 5. Build the example and write its resolved runtime classpath. Step 7 launches `java` directly,
#    so stdout carries no Gradle output; see build.gradle.kts for why not installDist or a fat jar.
echo "==> gradlew writeRuntimeClasspath" >&2
"${ADAPTER_DIR}/gradlew" -p "${EXAMPLE_DIR}" writeRuntimeClasspath --no-daemon >&2

CLASSPATH_FILE="${EXAMPLE_DIR}/build/runtime-classpath.txt"
[[ -s "${CLASSPATH_FILE}" ]] || { echo "no resolved classpath at ${CLASSPATH_FILE}" >&2; exit 1; }

# Print where the key artifacts were resolved from: ~/.m2 for the adapter means the published jar.
# `|| true` because grep exits non-zero on no match; that case is reported below.
resolved="$(tr ':' '\n' <"${CLASSPATH_FILE}" \
  | grep -E "/(${ARTIFACT_ID}|cerbos-sdk-java|elasticsearch-java|protobuf-java)-[0-9]" || true)"
if [[ -z "${resolved}" ]]; then
  echo "the resolved classpath in ${CLASSPATH_FILE} names none of the expected artifacts" >&2
  exit 1
fi
printf '%s\n' "${resolved}" >&2

# 6. Wait for Elasticsearch.
echo "==> waiting for Elasticsearch" >&2
ready=0
for _ in {1..90}; do
  if curl -fsS "${ES_URL}/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
(( ready == 1 )) || { echo "Elasticsearch failed to start" >&2; exit 1; }

# 7. Run. CERBOS_HOST is inherited from the shared runner.
echo "==> java DemoApplication" >&2
java \
  "-Ddemo.dir=${REPO_ROOT}/demo" \
  "-Dadapter.dir=${ADAPTER_DIR}" \
  "-Delasticsearch.url=${ES_URL}" \
  -cp "$(cat "${CLASSPATH_FILE}")" \
  dev.cerbos.example.demo.DemoApplication
