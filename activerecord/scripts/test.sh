#!/usr/bin/env bash
# Runs the suites of the ActiveRecord adapter in Docker. The PDP is pinned by tag and digest,
# from conformance/CERBOS_VERSION and conformance/CERBOS_IMAGE_DIGEST.
#
#   ./scripts/test.sh                                    # all the specs
#   ./scripts/test.sh spec/adversarial_conformance_spec.rb
#   RUBY_VERSION=3.2 ./scripts/test.sh                   # a different version of Ruby
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

compose() { docker compose "$@"; }

cleanup() { compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Only the adversarial harness needs a PDP. The other two suites are offline — the translator
# unit test replays conformance/wire-fixtures/ and the contract suite builds its own plans — so
# they run with `--no-deps` and no PDP is started at all.
#
# That is not a saving, it is the assertion. compose declares the dependency, so a suite that
# quietly grew a `plan_resources` call would still pass with the PDP running beside it; started
# without one, it fails.
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
