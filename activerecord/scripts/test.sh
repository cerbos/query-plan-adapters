#!/usr/bin/env bash
# Runs the ActiveRecord adapter's suites in Docker. None needs a PDP: the conformance suite
# replays the plans and decisions recorded in ../conformance/golden/.
#
#   ./scripts/test.sh                                    # all the specs
#   ./scripts/test.sh spec/conformance_spec.rb
#   RUBY_VERSION=3.3 ACTIVERECORD_VERSION=7.1 ./scripts/test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

cleanup() { docker compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> Ruby ${RUBY_VERSION}, ActiveRecord ${ACTIVERECORD_VERSION}"
docker compose build tests
docker compose run --rm tests bundle exec rspec "$@"
