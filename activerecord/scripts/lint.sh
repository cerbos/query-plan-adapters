#!/usr/bin/env bash
# Run the linter in the same container the suites use.
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
export CERBOS_VERSION
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

docker compose build tests
# No PDP needed: standardrb only reads source.
docker compose run --rm --no-deps tests bundle exec standardrb "$@"
