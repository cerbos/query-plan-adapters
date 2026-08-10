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

echo "==> Cerbos ${CERBOS_VERSION}, Ruby ${RUBY_VERSION}, ActiveRecord ${ACTIVERECORD_VERSION}"
compose build tests
compose run --rm tests bundle exec rspec "$@"
