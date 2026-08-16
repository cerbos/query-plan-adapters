#!/usr/bin/env bash
#
# The convex half of `demo/scripts/run-example.sh convex`: start the backend, pack the adapter,
# install THAT, deploy the functions against it, build the client, run it. The PDP is already up
# and reachable at $CERBOS_HOST — the shared runner owns it.
#
# The Convex backend is this script's job rather than the runner's. demo/docker-compose.yml holds
# the one thing every example genuinely shares, and nearly every store is somebody else's; putting
# them there would grow it into the language switch the split exists to avoid. Starting it here is
# also what keeps `demo/scripts/run-example.sh convex` a single command on a laptop.
#
# It is started from the ADAPTER's compose file rather than a copy, so the backend this example
# proves the packaging against is the one the adapter is developed and tested against, and the
# image pin — tag AND digest, which conformance/scripts/validate-corpus.sh asserts on every service
# image in the repository — stays in one place. The ports are the one thing overridden.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

# 13210/13211, not Convex's default 3210/3211: those are the ports `npm run convex:up` and the
# adapter's own CI bind, and a demo backend holding them would let this example deploy over the
# functions a conformance run is using — and read its data. demo/docker-compose.yml publishes the
# PDP on 13592/13593 for exactly the same reason. src/main.ts reaches this through $CONVEX_URL, so
# neither number is written down twice.
CONVEX_PORT=13210
CONVEX_SITE_PROXY_PORT=13211
CONVEX_URL="http://127.0.0.1:${CONVEX_PORT}"

# `PORT` and `SITE_PROXY_PORT` are the names ../docker-compose.yml interpolates, and they are set
# for compose alone rather than exported: `PORT` is a variable half the Node ecosystem reads, and
# an exported one would reach `npm ci`, the Convex CLI and the example itself.
#
# The project name must NOT be `cerbos-demo-convex`: that is the one demo/scripts/run-example.sh
# gives the PDP it started, and a `compose down` here would be scoped to the same project and take
# that PDP with it — the example would then fail with ECONNREFUSED against $CERBOS_HOST and read
# as a broken adapter rather than as two compose files sharing a namespace.
COMPOSE=(
  env "PORT=${CONVEX_PORT}" "SITE_PROXY_PORT=${CONVEX_SITE_PROXY_PORT}"
  docker compose -f "${ADAPTER_DIR}/docker-compose.yml" -p cerbos-demo-convex-backend
)

cd "${EXAMPLE_DIR}"
rm -f cerbos-orm-convex-*.tgz

cleanup() {
  local status=$?
  if (( status != 0 )); then
    echo "==> convex example failed (exit ${status}): Convex backend logs" >&2
    "${COMPOSE[@]}" logs --no-color backend >&2 2>/dev/null || true
  fi
  # `-v` because the backend's state is a named volume: a run that left the demo documents behind
  # would seed on top of them, and the next run's counts would depend on the previous one.
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# 1. Start the backend. First, so it warms up while the adapter is packed and installed — the
#    readiness wait below is what actually gates the deploy, not this line.
echo "==> starting the Convex backend on ${CONVEX_PORT}" >&2
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d >&2

# 2. Build and pack the adapter into the artifact npm would publish.
echo "==> packing @cerbos/orm-convex" >&2
(cd "${ADAPTER_DIR}" && npm run build) >&2
TARBALL="$(cd "${ADAPTER_DIR}" && npm pack --silent --pack-destination "${EXAMPLE_DIR}")"
echo "==> packed ${TARBALL}" >&2

# 3. Install the example's own pinned tree from the committed lockfile, then the tarball on top.
#
# The tarball is NOT a package.json dependency and NOT in the lockfile: its integrity hash changes
# on every build, which would make `npm ci` fail on a lockfile that was correct when committed.
# `--no-save --no-package-lock` keeps both files exactly as committed while still resolving the
# adapter — and its `@cerbos/core` peer range against the copy this example declares — the way a
# consumer's install would.
echo "==> npm ci" >&2
npm ci >&2
echo "==> installing the packed adapter" >&2
npm install --no-save --no-package-lock "./${TARBALL}" >&2

# 4. Wait for the backend. Deliberately after the install: the container has been starting
#    underneath steps 2-3, so by here there is usually nothing left to wait for.
echo "==> waiting for the Convex backend" >&2
for _ in $(seq 1 60); do
  if curl -sf "${CONVEX_URL}/version" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -sf "${CONVEX_URL}/version" >/dev/null || {
  echo "the Convex backend did not become ready" >&2
  # The trap dumps its logs, which are empty when the container never got as far as running. The
  # container's own state is the other half of that answer.
  "${COMPOSE[@]}" ps >&2 || true
  exit 1
}

# 5. Deploy the functions in convex/. This is the packaging proof: Convex's bundler resolves
#    `@cerbos/orm-convex` out of node_modules through its published `exports` map, and type-checks
#    the push against the `types` the tarball shipped. A `files` allowlist that omitted lib/ would
#    fail here, before a single plan is made.
CONVEX_SELF_HOSTED_URL="${CONVEX_URL}"
CONVEX_SELF_HOSTED_ADMIN_KEY="$("${COMPOSE[@]}" exec -T backend ./generate_admin_key.sh 2>/dev/null | tail -1)"
export CONVEX_SELF_HOSTED_URL CONVEX_SELF_HOSTED_ADMIN_KEY
[[ -n "${CONVEX_SELF_HOSTED_ADMIN_KEY}" ]] || {
  echo "could not generate a Convex admin key" >&2
  exit 1
}
echo "==> npx convex deploy" >&2
npx convex deploy -y >&2

# 6. Compile the client, from cold. This is the other half of the packaging proof:
#    `moduleResolution: nodenext` reads the adapter's `exports` map, so a broken one fails HERE
#    rather than shipping to a consumer.
#
#    `tsc --build` is incremental, and the state it reuses is keyed on this example's own sources
#    — which do not change when the adapter's packaging does. A warm lib/ and tsconfig.tsbuildinfo
#    therefore let a broken `exports` map compile clean, which is the exact break this step
#    exists to catch. CI is always cold; a developer's tree is not, and it is the tree where
#    someone checks the break by hand.
echo "==> tsc" >&2
rm -rf lib tsconfig.tsbuildinfo
npm run build >&2

# 7. Run. stdout is the JSON document and nothing else.
echo "==> node lib/main.js" >&2
CONVEX_URL="${CONVEX_URL}" node lib/main.js
