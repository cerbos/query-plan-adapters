#!/usr/bin/env bash
# Runs the suites of the MongoDB Ruby adapter.
#
#   ./scripts/test.sh                                      # every suite
#   ./scripts/test.sh spec/translator_spec.rb              # offline: no PDP, no MongoDB
#   ADAPTER_TEST_MONGO_IMAGE_FILE=MONGO_NEXT_IMAGE ./scripts/test.sh spec/adversarial_conformance_spec.rb
#
# Ruby and Bundler come from the host. Docker starts the two services the adversarial harness
# needs, and only when a suite that needs them is selected:
#
# * the PDP, pinned by tag AND digest from conformance/CERBOS_VERSION and
#   conformance/CERBOS_IMAGE_DIGEST, loading conformance/policies/ — the repository's only
#   policy suite for semantics (ADR 0008);
# * MongoDB, pinned in MONGO_IMAGE (or MONGO_NEXT_IMAGE, the forward-compatibility leg).
#
# Both publish on a port Docker picks for this run and nothing else can hold, never a fixed
# one: a fixed port is how a suite ends up planning against another run's PDP, possibly on
# another corpus revision or evaluation mode (cerbos/query-plan-adapters#476).
set -euo pipefail

cd "$(dirname "$0")/.."

strict_evaluation="${ADAPTER_TEST_STRICT_EVALUATION-false}"
case "${strict_evaluation}" in
  false|true) ;;
  *) echo "ADAPTER_TEST_STRICT_EVALUATION must be false or true" >&2; exit 2 ;;
esac

mongo_image_file="${ADAPTER_TEST_MONGO_IMAGE_FILE:-MONGO_IMAGE}"
case "${mongo_image_file}" in
  MONGO_IMAGE|MONGO_NEXT_IMAGE) ;;
  *) echo "ADAPTER_TEST_MONGO_IMAGE_FILE must be MONGO_IMAGE or MONGO_NEXT_IMAGE" >&2; exit 2 ;;
esac

# Only the adversarial harness needs the services. The translator unit test replays
# conformance/wire-fixtures/ and the contract suite builds its own plans, so for them nothing is
# started at all — which is the assertion, not a saving: a suite that quietly grew a
# plan_resources call or a query fails here instead of passing beside a running PDP.
needs_services=1
if [[ $# -gt 0 ]]; then
  needs_services=0
  for spec in "$@"; do
    case "${spec}" in
      *adversarial*) needs_services=1 ;;
    esac
  done
fi

bundle check >/dev/null 2>&1 || bundle install --quiet

if [[ "${needs_services}" -eq 0 ]]; then
  echo "==> no PDP, no MongoDB: these suites are offline" >&2
  exec env -u CERBOS_HOST -u MONGODB_URI bundle exec rspec "$@"
fi

cerbos_image="ghcr.io/cerbos/cerbos:$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)@$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
mongo_image="$(tr -d '[:space:]' < "${mongo_image_file}")"

containers=()
cleanup() { [[ ${#containers[@]} -eq 0 ]] || docker rm -f "${containers[@]}" >/dev/null 2>&1 || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "==> Cerbos ${cerbos_image} (strictEvaluation=${strict_evaluation}), ${mongo_image}" >&2
policies="$(cd ../conformance/policies && pwd)"
containers+=("$(docker run -d --rm -p 127.0.0.1::3593 -v "${policies}:/policies:ro" \
  -e CERBOS_NO_TELEMETRY=1 "${cerbos_image}" server \
  --set=storage.driver=disk --set=storage.disk.directory=/policies \
  --set=telemetry.disabled=true "--set=engine.strictEvaluation=${strict_evaluation}" \
  --log-level=error)")
cerbos_container="${containers[-1]}"
containers+=("$(docker run -d --rm -p 127.0.0.1::27017 "${mongo_image}")")
mongo_container="${containers[-1]}"

host_port() { docker port "$1" "$2" | head -n1 | sed 's/.*://'; }
CERBOS_HOST="127.0.0.1:$(host_port "${cerbos_container}" 3593/tcp)"
MONGODB_URI="mongodb://127.0.0.1:$(host_port "${mongo_container}" 27017/tcp)/cerbos_mongodb_ruby_adversarial?directConnection=true"
export CERBOS_HOST MONGODB_URI

healthy=0
for _ in $(seq 1 60); do
  if docker exec "${cerbos_container}" /cerbos healthcheck --insecure >/dev/null 2>&1 &&
    docker exec "${mongo_container}" mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done
if [[ "${healthy}" -ne 1 ]]; then
  echo "The PDP or MongoDB did not become healthy within 60 seconds" >&2
  docker logs "${cerbos_container}" >&2 || true
  exit 1
fi

bundle exec rspec "$@"
