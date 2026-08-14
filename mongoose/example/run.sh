#!/usr/bin/env bash
#
# The mongoose half of `demo/scripts/run-example.sh mongoose`: start the store, pack the adapter,
# install THAT, build the example against it, run it. The PDP is already up and reachable at
# $CERBOS_HOST — the shared runner owns it.
#
# The MongoDB server is this script's job rather than the runner's. demo/docker-compose.yml holds
# the one thing every example genuinely shares, and nearly every store is somebody else's;
# putting them there would grow it into the language switch the split exists to avoid. Starting it
# here is also what keeps `demo/scripts/run-example.sh mongoose` a single command on a laptop.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document, which the shared runner diffs against demo/expected.json.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

# The baseline server, read rather than restated. `npm run mongo` and the baseline leg of
# .github/workflows/mongoose.yaml read the same file, so the server this example proves the
# packaging against is the one the adapter is developed and tested against, and bumping it is one
# edit. A literal here would be a third copy that conformance/scripts/validate-corpus.sh could
# only hold to one digest per TAG — nothing holds two tags equal, so moving the adapter off 7.0
# would leave the example behind, still pinned and still green.
MONGO_IMAGE="$(cat "${ADAPTER_DIR}/MONGO_IMAGE")"
MONGO_CONTAINER="cerbos-demo-mongoose"
# 27117, not MongoDB's default 27017: that is the port `npm run mongo` and the adapter's own CI
# bind, and a demo server holding it would leave one of the two silently reading the other's data.
# src/main.ts's connection string is the other half of this number.
MONGO_PORT=27117

cd "${EXAMPLE_DIR}"
rm -f cerbos-orm-mongoose-*.tgz

cleanup() {
  local status=$?
  if (( status != 0 )); then
    echo "==> mongoose example failed (exit ${status}): MongoDB container logs" >&2
    docker logs "${MONGO_CONTAINER}" >&2 2>&1 || true
  fi
  docker rm -f "${MONGO_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# 1. Start the store. First, so it warms up while the adapter is packed and the example compiled —
#    the readiness wait below is what actually gates the run, not this line.
echo "==> starting ${MONGO_IMAGE} on ${MONGO_PORT}" >&2
docker rm -f "${MONGO_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${MONGO_CONTAINER}" -p "${MONGO_PORT}:27017" "${MONGO_IMAGE}" >/dev/null

# 2. Build and pack the adapter into the artifact npm would publish.
echo "==> packing @cerbos/orm-mongoose" >&2
(cd "${ADAPTER_DIR}" && npm run build) >&2
TARBALL="$(cd "${ADAPTER_DIR}" && npm pack --silent --pack-destination "${EXAMPLE_DIR}")"
echo "==> packed ${TARBALL}" >&2

# 3. Install the example's own pinned tree from the committed lockfile, then the tarball on top.
#
# The tarball is NOT a package.json dependency and NOT in the lockfile: its integrity hash changes
# on every build, which would make `npm ci` fail on a lockfile that was correct when committed.
# `--no-save --no-package-lock` keeps both files exactly as committed while still resolving the
# adapter the way a consumer's install would, against a `mongoose` this example owns and the
# adapter never declares.
echo "==> npm ci" >&2
npm ci >&2
echo "==> installing the packed adapter" >&2
npm install --no-save --no-package-lock "./${TARBALL}" >&2

# 4. Compile, from cold. This is half of the packaging proof: `moduleResolution: nodenext` reads
#    the adapter's `exports` map, so a broken one fails HERE rather than shipping to a consumer.
#
#    `tsc --build` is incremental, and the state it reuses is keyed on this example's own sources
#    — which do not change when the adapter's packaging does. A warm lib/ and tsconfig.tsbuildinfo
#    therefore let a broken `exports` map compile clean, which is the exact break this step
#    exists to catch. CI is always cold; a developer's tree is not, and it is the tree where
#    someone checks the break by hand.
echo "==> tsc" >&2
rm -rf lib tsconfig.tsbuildinfo
npm run build >&2

# 5. Wait for the store. Deliberately after the build: the container has been starting underneath
#    steps 2-4, so by here there is usually nothing left to wait for, and a compile error reports
#    itself without first blocking on a server it will never use.
echo "==> waiting for MongoDB" >&2
for _ in {1..30}; do
  if docker exec "${MONGO_CONTAINER}" mongosh --quiet \
    --eval 'db.runCommand({ ping: 1 }).ok' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "${MONGO_CONTAINER}" mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok' \
  >/dev/null 2>&1 || { echo "MongoDB failed to start" >&2; exit 1; }

# 6. Run. stdout is the JSON document and nothing else.
echo "==> node lib/main.js" >&2
node lib/main.js
