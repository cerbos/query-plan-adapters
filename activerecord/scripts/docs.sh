#!/usr/bin/env bash
# Builds the YARD docs in the test container. Fails on warnings or undocumented objects.
#
#   ./scripts/docs.sh                     # bundle exec rake docs
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export ACTIVERECORD_VERSION="${ACTIVERECORD_VERSION:-8.0}"

docker compose build tests
# No PDP needed: YARD reads only the source.
docker compose run --rm --no-deps tests bundle exec rake docs
