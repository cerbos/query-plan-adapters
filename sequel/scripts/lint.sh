#!/usr/bin/env bash
# Runs the linter in the test container.
#
#   ./scripts/lint.sh                     # standardrb over the whole gem
#   ./scripts/lint.sh --fix lib/          # arguments go to standardrb directly
set -euo pipefail

cd "$(dirname "$0")/.."

export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export SEQUEL_VERSION="${SEQUEL_VERSION:-}"

docker compose build tests
# Standard reads only the source: no store, no PDP.
docker compose run --rm tests bundle exec standardrb "$@"
