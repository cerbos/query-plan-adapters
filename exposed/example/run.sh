#!/usr/bin/env bash
#
# The exposed half of `demo/scripts/run-example.sh exposed`: publish the adapter, build the
# demo-domain program against that published coordinate, run it. The PDP is already up and
# reachable at $CERBOS_HOST — the shared runner owns it.
#
# The store is H2, in memory, inside the program's own JVM, so there is nothing to start here.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.
#
# Pre-reqs: gradle (8.x), JDK 17+.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

# The coordinate this example resolves, spelled once and used by every check below.
ARTIFACT_ID="cerbos-exposed"
ADAPTER_CLASS_PATH="dev/cerbos/queryplan/exposed/ExposedQueryPlanAdapter.class"
EXAMPLE_PACKAGE_PATH="dev/cerbos/example"

# `application { applicationName = "demo" }` in build.gradle.kts fixes both of these.
LAUNCHER="${EXAMPLE_DIR}/build/install/demo/bin/demo"
CLASSPATH_FILE="${EXAMPLE_DIR}/build/resolved-runtime-classpath.txt"

# Arguments are joined with a space, so a long message can be wrapped across continuation lines.
fail() { echo "$*" >&2; exit 1; }

cd "${EXAMPLE_DIR}"

# 0. The shared corpus this program reads. Checked here rather than left to the JVM, so a run
#    against a half-checked-out tree fails before Gradle resolves anything.
[[ -f "${REPO_ROOT}/demo/seeds.json" ]] || \
  fail "no demo/seeds.json under ${REPO_ROOT} — run this through demo/scripts/run-example.sh exposed"

# 0b. The example runs the Exposed release the adapter calls its `baseline`.
#
#     The adapter's README and workflow both say so, and nothing else holds it true: the adapter's
#     version sets live in a map Renovate cannot resolve (deliberately, so the support floor cannot be
#     automerged away), while the versions below are plain literals it bumps and automerges. So a bump
#     arrives HERE first, and this check is what turns it into a red example job on that pull request
#     rather than a silent drift: moving the baseline is a reviewed edit of ../build.gradle.kts, with
#     the golden asset regenerated under the new minor.
baseline="$(sed -n 's/^ *"baseline" to mapOf("exposed" to "\([^"]*\)").*/\1/p' "${ADAPTER_DIR}/build.gradle.kts")"
[[ -n "${baseline}" ]] || { echo "could not read the baseline Exposed version from ${ADAPTER_DIR}/build.gradle.kts" >&2; exit 1; }
pinned="$(sed -n 's/.*"org\.jetbrains\.exposed:exposed-[a-z-]*:\([^"]*\)".*/\1/p' "${EXAMPLE_DIR}/build.gradle.kts" | sort -u)"
if [[ "${pinned}" != "${baseline}" ]]; then
  echo "the example pins Exposed '$(tr '\n' ' ' <<<"${pinned}")' but the adapter's baseline is '${baseline}':" \
    "every exposed-* module here must be the baseline (../build.gradle.kts, ormVersionSets)" >&2
  exit 1
fi

# 1. Build the adapter and install it into mavenLocal.
#
# This is the Java form of "pack the adapter into a real distributable and install THAT"
# (docs/adr/0002-examples-install-the-packed-artifact.md). What it buys over the Gradle composite
# build that would be one line shorter is the adapter's own POM and module metadata — and on this
# adapter that metadata is unusually load-bearing, because every Exposed module is `compileOnly`
# there and must therefore appear in NEITHER. The example brings its own Exposed; if the adapter
# ever started publishing one, this is where the two would collide.
echo "==> gradle publishToMavenLocal (dev.cerbos:${ARTIFACT_ID})" >&2
gradle -p "${ADAPTER_DIR}" publishToMavenLocal --no-daemon --console=plain >&2

# 2. The example must stay OUT of the artifact it exercises. TypeScript adapters get this from
#    their `files` allowlist and Go from nested-module exclusion; Java and Kotlin have neither, so
#    ADR 0002 asks for it to be checked deliberately. It holds today because example/ is a separate
#    Gradle build rather than a source set of the adapter — which is exactly the kind of fact that
#    stops being true without anyone noticing.
#
#    Both directions are asserted. Without the positive half an empty or wrongly-named jar would
#    sail through the negative one, and "no example classes in there" would be true of a jar with
#    nothing in it at all.
found_jar=0
for jar in "${ADAPTER_DIR}"/build/libs/"${ARTIFACT_ID}"-*.jar; do
  [[ -f "${jar}" ]] || continue
  found_jar=1
  # Listed into a variable rather than piped into grep: under `set -o pipefail`, a `grep -q` that
  # matches early closes the pipe, `jar` dies of SIGPIPE, and the pipeline reports failure — so the
  # `if` would read as "no example classes" in exactly the case this exists to catch.
  entries="$(jar tf "${jar}")"
  if grep -q "^${EXAMPLE_PACKAGE_PATH}/" <<<"${entries}"; then
    fail "$(basename "${jar}") contains example classes — the example must not ship inside the" \
      "adapter (docs/adr/0002-examples-install-the-packed-artifact.md)"
  fi
  if ! grep -q "^${ADAPTER_CLASS_PATH}\$" <<<"${entries}"; then
    fail "$(basename "${jar}") does not contain the adapter itself, so the check above proved" \
      "nothing about what it excludes"
  fi
done
(( found_jar == 1 )) || fail "no adapter jar under ${ADAPTER_DIR}/build/libs"

# 3. Build the demo-domain program, and record where each of its dependencies resolved from.
#
#    `installDist` rather than a Gradle `JavaExec` task, because of the output contract: stdout
#    must carry one JSON document and nothing else, and a Gradle invocation's stdout also carries
#    Gradle's lifecycle output and whatever deprecation notice it decides to print that day.
#    Building here (with Gradle's own output on stderr) and launching the generated start script
#    with no Gradle in the process keeps stdout the program's alone. DemoApplication does the other
#    half by redirecting System.out.
#
#    `writeResolvedClasspath` is what makes that affordable: `installDist` COPIES every dependency
#    into build/install/demo/lib, so by the time the program runs there is nothing left to tell a
#    jar resolved from mavenLocal apart from one a composite build substituted in. Step 4 asserts
#    that fact while it still exists.
echo "==> gradle installDist writeResolvedClasspath" >&2
gradle installDist writeResolvedClasspath --no-daemon --console=plain >&2

[[ -s "${CLASSPATH_FILE}" ]] || fail "no resolved classpath at ${CLASSPATH_FILE}"
[[ -x "${LAUNCHER}" ]] || fail "no start script at ${LAUNCHER}"

# 4. The adapter came from the published artifact, not from its own build directory.
#
#    A single line in settings.gradle.kts — `includeBuild("..")` — turns the declared coordinate
#    into a Gradle composite build, which substitutes the adapter's local project for it.
#    Everything still compiles, everything still resolves, and every shape still passes, while the
#    POM and module metadata this example exists to execute are never read at all. The one thing
#    that gives it away is the path the jar was resolved from.
adapter_jar="$(grep -E "/${ARTIFACT_ID}-[0-9][^/]*\.jar\$" "${CLASSPATH_FILE}" || true)"
[[ -n "${adapter_jar}" ]] || \
  fail "the resolved classpath in ${CLASSPATH_FILE} names no ${ARTIFACT_ID} jar, so the check" \
    "here has stopped describing anything"
case "${adapter_jar}" in
  "${ADAPTER_DIR}"/*)
    fail "the adapter resolved to ${adapter_jar}, inside its own build directory — the declared" \
      "coordinate has been substituted with the local project, so neither its POM nor its Gradle" \
      "module metadata was read (docs/adr/0002-examples-install-the-packed-artifact.md)" ;;
esac
echo "==> dev.cerbos:${ARTIFACT_ID} resolved from ${adapter_jar}" >&2

# 5. Run. stdout is the JSON document and nothing else.
#
#    demo/ is passed in rather than derived inside the program: how many directories up the
#    repository root sits is this script's business, not the application's. It travels through
#    JAVA_OPTS because that is the hook a generated start script forwards to the JVM; the program
#    refuses to start without it, so a start script that stopped forwarding would fail loudly
#    rather than pick a directory of its own. CERBOS_HOST comes from the runner and is inherited;
#    the program refuses to start without that too.
#
#    The value is wrapped in double quotes INSIDE the variable. The generated start script splits
#    JAVA_OPTS into words itself (through `xargs`, which honours quotes), so an unquoted path
#    holding a space would arrive as two arguments and the program would be handed half a path.
echo "==> ${LAUNCHER}" >&2
JAVA_OPTS="\"-Ddemo.dir=${REPO_ROOT}/demo\"" exec "${LAUNCHER}"
