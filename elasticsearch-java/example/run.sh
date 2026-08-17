#!/usr/bin/env bash
#
# The elasticsearch-java half of `demo/scripts/run-example.sh elasticsearch-java`: publish the
# adapter, start the store, build this example against the published coordinate, run it. The PDP is
# already up and reachable at $CERBOS_HOST — the shared runner owns it.
#
# The Elasticsearch server is this script's job rather than the runner's. demo/docker-compose.yml
# holds the one thing every example genuinely shares, and every store is somebody else's; putting
# them there would grow it into the language switch the split exists to avoid. Starting it here is
# also what keeps `demo/scripts/run-example.sh elasticsearch-java` a single command on a laptop.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.
#
# Pre-reqs: docker, curl, gradle (8.x), JDK 17+.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

# The coordinate this example resolves. Spelled once, and used for both the publish check below and
# the artifact the run is asserted to have loaded.
ARTIFACT_ID="cerbos-elasticsearch"
ADAPTER_PACKAGE_PATH="dev/cerbos/queryplan/elasticsearch"
EXAMPLE_PACKAGE_PATH="dev/cerbos/example"

# The server the adapter is proved against, read rather than restated: ../ELASTICSEARCH_IMAGE is the
# same file ../src/test/java/.../ElasticsearchTestImage.java reads, so the Elasticsearch this example
# sends queries to is the one the conformance corpus is replayed against, and bumping it is one edit.
ES_IMAGE="$(cat "${ADAPTER_DIR}/ELASTICSEARCH_IMAGE")"
ES_CONTAINER="cerbos-demo-elasticsearch-java"
# 19200, not Elasticsearch's default 9200: the adapter's two container-backed suites start
# Elasticsearch servers of their own, and a machine running one on the default port would otherwise
# have this example seeding and searching somebody else's index. Nothing else names this number —
# it is passed to the program as -Delasticsearch.url.
ES_PORT=19200
ES_URL="http://127.0.0.1:${ES_PORT}"

cleanup() {
  local status=$?
  if (( status != 0 )); then
    # Only if there is a container to ask, because the checks below run before it is started and
    # `docker logs` on a missing container prints an error that reads as the cause.
    if docker container inspect "${ES_CONTAINER}" >/dev/null 2>&1; then
      echo "==> elasticsearch-java example failed (exit ${status}): Elasticsearch container logs" >&2
      # Tailed rather than dumped whole: Elasticsearch logs a few hundred lines of structured JSON
      # on a healthy start, and the reason a run failed is at the end of them or not in them at all.
      # `>&2` alone carries both streams: it points this command's stdout — which is where the image
      # logs — at stderr, and stderr is already there.
      docker logs --tail 40 "${ES_CONTAINER}" >&2 || true
    else
      echo "==> elasticsearch-java example failed (exit ${status}) before the store was started" >&2
    fi
  fi
  docker rm -f "${ES_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

cd "${EXAMPLE_DIR}"

# 1. Build the adapter and install it into mavenLocal.
#
# The Java form of "pack the adapter into a real distributable and install THAT"
# (docs/adr/0002-examples-install-the-packed-artifact.md). Why a coordinate rather than the Gradle
# composite build that would be one line shorter is settings.gradle.kts's subject, and README.md has
# the break-tests that establish it; step 7's provenance check is what enforces it on every run.
#
# `publishToMavenLocal` overwrites the version in place, so unlike pip there is no already-satisfied
# path that could leave the PREVIOUS build installed. See build.gradle.kts on `isChanging` for the
# consuming side of that, which was measured too.
echo "==> gradle publishToMavenLocal (dev.cerbos:${ARTIFACT_ID})" >&2
gradle -p "${ADAPTER_DIR}" publishToMavenLocal --no-daemon >&2

# 2. The example must stay OUT of the artifact it exercises. TypeScript adapters get this from their
#    `files` allowlist and Go from nested-module exclusion; Java has neither, so ADR 0002 asks for it
#    to be checked deliberately. It holds today because example/ is a separate Gradle build rather
#    than a source set of the adapter — which is exactly the kind of fact that stops being true
#    without anyone noticing.
#
#    Both directions are asserted. Without the positive half an empty or wrongly-named jar would sail
#    through the negative one, and "no example classes in there" would be true of a jar with nothing
#    in it at all.
found_jar=0
for jar in "${ADAPTER_DIR}"/build/libs/"${ARTIFACT_ID}"-*.jar; do
  [[ -f "${jar}" ]] || continue
  found_jar=1
  # Listed into a variable rather than piped into grep: under `set -o pipefail`, a `grep -q` that
  # matches early closes the pipe, `jar` dies of SIGPIPE, and the pipeline reports failure — so the
  # `if` would read as "no example classes" in exactly the case this exists to catch.
  entries="$(jar tf "${jar}")"
  if grep -q "^${EXAMPLE_PACKAGE_PATH}/" <<<"${entries}"; then
    echo "$(basename "${jar}") contains example classes — the example must not ship inside the" \
      "adapter (docs/adr/0002-examples-install-the-packed-artifact.md)" >&2
    exit 1
  fi
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

# 3. The client and the server agree on a major.
#
#    The Elasticsearch Java client version is a literal in build.gradle.kts — it has to be, or the
#    Renovate bump that carries it never touches this directory and the `example` job gates nothing
#    (README.md, "Why the Renovate gate bites") — so it is a second spelling of a version this
#    directory already knows, with nothing holding the two together. This is that something, and it
#    is the same job pgx/example/run.sh's DSN-port check does.
#
#    MAJOR, not the exact version. A major mismatch is the real hazard: the 8.x client refuses a 9.x
#    server outright, and the adapter's README declares Elasticsearch 8.x. Within a major Elastic
#    supports a client at or below the server's minor, and requiring exact equality would turn every
#    client bump red until someone bumped the image too — a gate failing for a reason that is not the
#    bump, which is the state README.md argues against for the lockfile.
# The digest comes off FIRST. It is introduced by `@` but contains a colon of its own
# (`@sha256:<hex>`), so taking the last colon-delimited field of the whole reference yields the hex.
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

# 4. Start the store. First, so it warms up while this example is resolved and built — the readiness
#    wait below is what actually gates the run, not this line.
#
#    Security off and plain HTTP, the same configuration the adapter's own container-backed suites
#    use: what this example proves is packaging and usage shapes, and TLS would only add a
#    certificate to trust.
echo "==> starting ${ES_IMAGE} on ${ES_PORT}" >&2
docker rm -f "${ES_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${ES_CONTAINER}" -p "${ES_PORT}:9200" \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" \
  "${ES_IMAGE}" >/dev/null

# 5. Build this example against the published coordinate, and write out the resolved runtime
#    classpath for step 6 to launch with.
#
#    The classpath is written rather than laid out as a directory of jars (`installDist`) or packed
#    into a fat jar, because both of those COPY every dependency — and then the program cannot tell a
#    jar resolved from mavenLocal apart from one a composite build substituted in, which is the check
#    step 7 rests on. build.gradle.kts has the longer version.
#
#    Gradle's own output goes to stderr, and step 7 launches without Gradle in the process, which is
#    what keeps stdout the program's alone. DemoApplication does the other half by redirecting
#    System.out.
echo "==> gradle writeRuntimeClasspath" >&2
gradle writeRuntimeClasspath --no-daemon >&2

CLASSPATH_FILE="${EXAMPLE_DIR}/build/runtime-classpath.txt"
[[ -s "${CLASSPATH_FILE}" ]] || { echo "no resolved classpath at ${CLASSPATH_FILE}" >&2; exit 1; }

# What actually got resolved, on stderr where a human reading a CI log will see it — full paths,
# because for the adapter the path IS the finding: `.../.m2/repository/...` is the published artifact,
# anything under ${ADAPTER_DIR} is a substituted local build. DemoApplication refuses to run on the
# second, and this is the same fact in a form a human can read on a green run.
#
# Taken into a variable and checked, rather than piped straight to stderr. `grep` that matches nothing
# exits non-zero, and under `set -o pipefail` that would end the run here with no message at all —
# which is also the case worth reporting, since a classpath naming none of these artifacts means this
# diagnostic has silently stopped describing anything.
resolved="$(tr ':' '\n' <"${CLASSPATH_FILE}" \
  | grep -E "/(${ARTIFACT_ID}|cerbos-sdk-java|elasticsearch-java|protobuf-java)-[0-9]" || true)"
if [[ -z "${resolved}" ]]; then
  echo "the resolved classpath in ${CLASSPATH_FILE} names none of the expected artifacts" >&2
  exit 1
fi
printf '%s\n' "${resolved}" >&2

# 6. Wait for the store. Deliberately after the build: the container has been starting underneath
#    step 5, so by here there is usually nothing left to wait for, and a build error reports itself
#    without first blocking on a server it will never use.
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

# 7. Run. stdout is the JSON document and nothing else.
#
#    demo/ and the adapter directory are passed in rather than derived inside the program: how many
#    directories up the repository root sits is this script's business, not the application's.
#    CERBOS_HOST comes from the runner and is inherited; the program refuses to start without it.
echo "==> java DemoApplication" >&2
java \
  "-Ddemo.dir=${REPO_ROOT}/demo" \
  "-Dadapter.dir=${ADAPTER_DIR}" \
  "-Delasticsearch.url=${ES_URL}" \
  -cp "$(cat "${CLASSPATH_FILE}")" \
  dev.cerbos.example.demo.DemoApplication
