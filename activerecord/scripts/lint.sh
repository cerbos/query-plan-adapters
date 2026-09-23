#!/usr/bin/env bash
# Runs the linter in the test container.
#
#   ./scripts/lint.sh                     # bundle exec rake lint
#   ./scripts/lint.sh --autocorrect lib/  # arguments go to rubocop directly
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

docker compose build tests
# No PDP needed. `CI` is passed through so the rake task uses the GitHub formatter in CI.
if [[ $# -gt 0 ]]; then
  docker compose run --rm --no-deps -e CI tests bundle exec rubocop "$@"
else
  docker compose run --rm --no-deps -e CI tests bundle exec rake lint
fi
