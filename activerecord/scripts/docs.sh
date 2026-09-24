#!/usr/bin/env bash
# Builds the YARD docs in the test container. Fails on warnings or undocumented objects.
#
#   ./scripts/docs.sh                     # bundle exec rake docs
set -euo pipefail

cd "$(dirname "$0")/.."

export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

docker compose build tests
docker compose run --rm tests bundle exec rake docs
