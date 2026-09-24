#!/usr/bin/env bash
# Runs the suites of the MongoDB Ruby adapter.
#
#   ./scripts/test.sh                                                  # every suite
#   ./scripts/test.sh spec/adapter_contract_spec.rb spec/mongoid_spec.rb   # offline: no MongoDB
#   ADAPTER_TEST_MONGO_IMAGE_FILE=MONGO_NEXT_IMAGE ./scripts/test.sh spec/conformance_spec.rb
#
# Ruby and Bundler come from the host. No suite starts a PDP: the conformance harness replays the
# plans and decisions recorded under conformance/golden/. Docker starts the one service it
# needs, MongoDB, pinned in MONGO_IMAGE (or MONGO_NEXT_IMAGE, the forward-compatibility leg), and
# only when that suite is selected.
#
# The server publishes on a port Docker picks for this run and nothing else can hold, never a
# fixed one, so a run cannot read another run's documents.
set -euo pipefail

cd "$(dirname "$0")/.."

mongo_image_file="${ADAPTER_TEST_MONGO_IMAGE_FILE:-MONGO_IMAGE}"
case "${mongo_image_file}" in
  MONGO_IMAGE|MONGO_NEXT_IMAGE) ;;
  *) echo "ADAPTER_TEST_MONGO_IMAGE_FILE must be MONGO_IMAGE or MONGO_NEXT_IMAGE" >&2; exit 2 ;;
esac

# Only the conformance harness needs MongoDB. For the unit suites nothing is started at all —
# which is the assertion, not a saving: a unit suite that quietly grew a query fails here instead
# of passing beside a running server.
needs_mongo=1
if [[ $# -gt 0 ]]; then
  needs_mongo=0
  for spec in "$@"; do
    case "${spec}" in
      *conformance*) needs_mongo=1 ;;
    esac
  done
fi

bundle check >/dev/null 2>&1 || bundle install --quiet

if [[ "${needs_mongo}" -eq 0 ]]; then
  echo "==> no MongoDB: these suites are offline" >&2
  exec env -u MONGODB_URI bundle exec rspec "$@"
fi

mongo_image="$(tr -d '[:space:]' < "${mongo_image_file}")"

container=""
cleanup() { [[ -z "${container}" ]] || docker rm -f "${container}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "==> ${mongo_image}" >&2
container="$(docker run -d --rm -p 127.0.0.1::27017 "${mongo_image}")"
port="$(docker port "${container}" 27017/tcp | head -n1 | sed 's/.*://')"
export MONGODB_URI="mongodb://127.0.0.1:${port}/cerbos_mongodb_ruby_conformance?directConnection=true"

healthy=0
for _ in $(seq 1 60); do
  if docker exec "${container}" mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done
if [[ "${healthy}" -ne 1 ]]; then
  echo "MongoDB did not become healthy within 60 seconds" >&2
  docker logs "${container}" >&2 || true
  exit 1
fi

bundle exec rspec "$@"
