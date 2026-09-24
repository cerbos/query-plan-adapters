#!/usr/bin/env bash
# Runs the linter in the same container as the suites.
set -euo pipefail

cd "$(dirname "$0")/.."

# The store images, pinned beside this adapter by tag and digest. Compose interpolates every
# service, so they are exported even for a run that starts neither.
POSTGRES_IMAGE="$(tr -d '[:space:]' < POSTGRES_IMAGE)"
MYSQL_IMAGE="$(tr -d '[:space:]' < MYSQL_IMAGE)"
export POSTGRES_IMAGE MYSQL_IMAGE
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export SEQUEL_VERSION="${SEQUEL_VERSION:-}"

docker compose build tests
docker compose run --rm --no-deps tests bundle exec standardrb "$@"
