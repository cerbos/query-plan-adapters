#!/usr/bin/env bash
#
# The langchain-chromadb half of `demo/scripts/run-example.sh langchain-chromadb`: start ChromaDB,
# pack the adapter, install THAT, build the example against it, run it. The PDP is already up and
# reachable at $CERBOS_HOST — the shared runner owns it.
#
# The ChromaDB container is this script's job rather than the runner's. demo/docker-compose.yml
# holds the one thing every example genuinely shares, and every store past that is somebody else's;
# putting them there would grow it into the language switch the split exists to avoid. Starting it
# here is also what keeps `demo/scripts/run-example.sh langchain-chromadb` a single command on a
# laptop.
#
# The image is READ from ../CHROMA_IMAGE rather than restated here — the same constant this
# adapter's suites, its `npm run chroma` helper and its workflow already share. A second copy is a
# second thing to bump, and conformance/scripts/validate-corpus.sh requires every reference to a
# service image to carry the same tag AND digest; a reference that reads the file cannot drift from
# it at all.
#
# Everything this script prints for a human goes to stderr. stdout carries exactly one JSON
# document whose cases and expected results come from demo/cases.json.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

# 18234, not the 8234 `npm run chroma` and this adapter's CI bind: a demo ChromaDB holding that
# port would let this example create and delete collections inside the server a conformance run is
# using. demo/docker-compose.yml publishes the PDP on 13592/13593 for exactly the same reason.
# src/main.ts reaches this through $CHROMA_URL, so the number is not written down twice.
CHROMA_PORT=18234
CHROMA_URL="http://127.0.0.1:${CHROMA_PORT}"
# Named rather than anonymous so the trap below can dump its logs and remove it. The name is
# demo-specific for the same reason the port is: `docker rm -f` on a shared name would take out a
# container a conformance run had started.
CHROMA_CONTAINER="cerbos-demo-chromadb"

cd "${EXAMPLE_DIR}"
rm -f cerbos-langchain-chromadb-*.tgz

cleanup() {
  local status=$?
  if (( status != 0 )); then
    echo "==> langchain-chromadb example failed (exit ${status}): ChromaDB logs" >&2
    docker logs "${CHROMA_CONTAINER}" >&2 2>&1 || true
  fi
  docker rm -f "${CHROMA_CONTAINER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

# 1. Start ChromaDB. First, so it warms up while the adapter is packed and installed — the
#    readiness wait below is what actually gates the run, not this line.
echo "==> starting ChromaDB on ${CHROMA_PORT}" >&2
docker rm -f "${CHROMA_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CHROMA_CONTAINER}" -p "${CHROMA_PORT}:8000" \
  "$(cat "${ADAPTER_DIR}/CHROMA_IMAGE")" >/dev/null

# 2. Build and pack the adapter into the artifact npm would publish.
echo "==> packing @cerbos/langchain-chromadb" >&2
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

# 4. Compile, from cold. This is half of the packaging proof: `moduleResolution: nodenext` reads
#    the adapter's `exports` map, so a broken one fails HERE rather than shipping to a consumer.
#
#    `tsc --build` is incremental, and the state it reuses is keyed on this example's own sources
#    — which do not change when the adapter's packaging does. A warm lib/ and tsconfig.tsbuildinfo
#    therefore let a broken `exports` map compile clean, which is the exact break this step exists
#    to catch. CI is always cold; a developer's tree is not, and it is the tree where someone
#    checks the break by hand.
echo "==> tsc" >&2
rm -rf lib tsconfig.tsbuildinfo
npm run build >&2

# 5. Wait for ChromaDB. Deliberately after the install: the container has been starting underneath
#    steps 2-4, so by here there is usually nothing left to wait for.
echo "==> waiting for ChromaDB" >&2
for _ in $(seq 1 60); do
  if curl -sf "${CHROMA_URL}/api/v2/heartbeat" >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -sf "${CHROMA_URL}/api/v2/heartbeat" >/dev/null || {
  echo "ChromaDB did not become ready at ${CHROMA_URL}" >&2
  # The trap dumps its logs, which are empty when the container never got as far as running. The
  # container's own state is the other half of that answer.
  docker ps -a --filter "name=${CHROMA_CONTAINER}" >&2 || true
  exit 1
}

# 6. Run. stdout is the JSON document and nothing else.
echo "==> node lib/main.js" >&2
CHROMA_URL="${CHROMA_URL}" node lib/main.js
