#!/usr/bin/env bash
# The demo-domain example for the MongoDB Ruby adapter.
#
# Invoked by demo/scripts/run-example.sh, which starts the pinned PDP over demo/policies/ and
# sets CERBOS_HOST. The MongoDB server is this script's job rather than the runner's:
# demo/docker-compose.yml holds the one thing every example shares, and the store is not it.
#
# stdout carries exactly one JSON document and nothing else. Everything a human might read goes
# to stderr.
set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "$0")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${ADAPTER_DIR}/.." && pwd)"

cd "${EXAMPLE_DIR}"

# The baseline server, read rather than restated: the adversarial harness and the baseline CI
# leg read the same file, so the server this example proves the packaging against is the one the
# adapter is tested against, and bumping it is one edit.
MONGO_IMAGE="$(tr -d '[:space:]' < "${ADAPTER_DIR}/MONGO_IMAGE")"
MONGO_CONTAINER=""

cleanup() {
  local status=$?
  if [[ -n "${MONGO_CONTAINER}" ]]; then
    if (( status != 0 )); then
      echo "==> example failed (exit ${status}): MongoDB container logs" >&2
      docker logs "${MONGO_CONTAINER}" >&2 2>&1 || true
    fi
    docker rm -f "${MONGO_CONTAINER}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

# 1. Start the store first, so it warms up while the gem is built. On a port Docker picks for
#    this run: a fixed one could be held by another run's server, and the example would then
#    read somebody else's documents.
echo "==> starting ${MONGO_IMAGE}" >&2
MONGO_CONTAINER="$(docker run -d --rm -p 127.0.0.1::27017 "${MONGO_IMAGE}")"
MONGO_PORT="$(docker port "${MONGO_CONTAINER}" 27017/tcp | head -n1 | sed 's/.*://')"

# 2. Build the adapter into a real distributable and install THAT, not the source directory.
#    Installing the gem is what executes the gemspec's `files` allowlist and its
#    required_ruby_version — none of which any harness touches, because every harness loads lib/
#    directly.
echo "==> gem build cerbos-mongodb.gemspec" >&2
GEM_FILE="${EXAMPLE_DIR}/cerbos-mongodb.gem"
(cd "${ADAPTER_DIR}" && gem build cerbos-mongodb.gemspec --output "${GEM_FILE}" >&2)

# 3. The example must not ship inside the artifact it exercises. RubyGems needs this checked
#    deliberately: a careless `spec.files = Dir["**/*"]` would sweep example/ into the gem.
if tar -xOf "${GEM_FILE}" data.tar.gz | tar -tzf - | grep -q -E '^(example|spec|scripts)/|^conformance-ledger\.json$'; then
  echo "cerbos-mongodb.gem carries example/, spec/, scripts/ or the ledger — the gem must ship lib/ only" >&2
  exit 1
fi

# 4. Unpack the artifact's CONTENTS and resolve the adapter from there, so a lib/ file missing
#    from the allowlist fails at `require` here rather than for the first consumer.
echo "==> gem unpack into vendor/" >&2
rm -rf "${EXAMPLE_DIR}/vendor"
mkdir -p "${EXAMPLE_DIR}/vendor"
gem unpack "${GEM_FILE}" --target="${EXAMPLE_DIR}/vendor" >&2
gem unpack --spec "${GEM_FILE}" --target="${EXAMPLE_DIR}/vendor" >&2
mv "${EXAMPLE_DIR}"/vendor/cerbos-mongodb-*.gemspec "${EXAMPLE_DIR}/vendor/cerbos-mongodb/cerbos-mongodb.gemspec"

echo "==> bundle install" >&2
export BUNDLE_PATH="${EXAMPLE_DIR}/.bundle-path"
export BUNDLE_GEMFILE="${EXAMPLE_DIR}/Gemfile"
bundle install >&2

# 5. Wait for the store. After the build, so there is usually nothing left to wait for.
echo "==> waiting for MongoDB" >&2
for _ in $(seq 1 30); do
  docker exec "${MONGO_CONTAINER}" mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' >/dev/null 2>&1 && break
  sleep 1
done
docker exec "${MONGO_CONTAINER}" mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' >/dev/null 2>&1 ||
  { echo "MongoDB failed to start" >&2; exit 1; }

# 6. Run. stdout is the JSON document; app.rb moves its own chatter to stderr. Not `exec`: the
#    trap above has to remove the container when the program ends.
echo "==> ruby app.rb" >&2
DEMO_DIR="${REPO_ROOT}/demo" \
  MONGODB_URI="mongodb://127.0.0.1:${MONGO_PORT}/cerbos_demo?directConnection=true" \
  bundle exec ruby app.rb
