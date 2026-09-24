#!/usr/bin/env bash
# Runs the linter in the test container.
#
#   ./scripts/lint.sh                     # bundle exec rake lint
#   ./scripts/lint.sh --autocorrect lib/  # arguments go to rubocop directly
set -euo pipefail

cd "$(dirname "$0")/.."

export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

docker compose build tests
# `CI` is passed through so the rake task uses the GitHub formatter in CI.
if [[ $# -gt 0 ]]; then
  docker compose run --rm -e CI tests bundle exec rubocop "$@"
else
  docker compose run --rm -e CI tests bundle exec rake lint
fi
