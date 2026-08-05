#!/usr/bin/env bash
# Runs the linter in the same container as the suites.
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
export CERBOS_VERSION
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

docker compose build tests
# No PDP is necessary, because standardrb reads only the source.
docker compose run --rm --no-deps tests bundle exec standardrb "$@"
