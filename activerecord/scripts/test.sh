#!/usr/bin/env bash
# Runs the ActiveRecord adapter's suites in Docker. The PDP tag and digest come from
# conformance/CERBOS_VERSION and conformance/CERBOS_IMAGE_DIGEST.
#
#   ./scripts/test.sh                                    # all the specs
#   ./scripts/test.sh spec/adversarial_conformance_spec.rb
#   RUBY_VERSION=3.3 ./scripts/test.sh                   # a different version of Ruby
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export ADAPTER_TEST_STRICT_EVALUATION="${ADAPTER_TEST_STRICT_EVALUATION-false}"
case "${ADAPTER_TEST_STRICT_EVALUATION}" in
  false|true) ;;
  *) echo "ADAPTER_TEST_STRICT_EVALUATION must be false or true" >&2; exit 1 ;;
esac
export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

compose() { docker compose "$@"; }

cleanup() { compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Only the adversarial suite needs a PDP. The others run with `--no-deps`, so no PDP starts.
# This also proves they stay offline: one that starts calling the PDP will fail.
needs_pdp=1
if [[ $# -gt 0 ]]; then
  needs_pdp=0
  for spec in "$@"; do
    case "${spec}" in
      *adversarial*) needs_pdp=1 ;;
    esac
  done
fi

echo "==> Cerbos ${CERBOS_VERSION}, Ruby ${RUBY_VERSION}, ActiveRecord ${ACTIVERECORD_VERSION}"
compose build tests
if [[ "${needs_pdp}" -eq 1 ]]; then
  compose run --rm tests bundle exec rspec "$@"
else
  echo "==> no PDP: these suites are offline" >&2
  compose run --rm --no-deps tests bundle exec rspec "$@"
fi
