#!/usr/bin/env bash
# Starts the example application and its PDP, and keeps them in operation.
#
#   ./scripts/run.sh
#   curl 'http://localhost:4567/photos?action=view&user=ben&tenant=acme&tags=public'
#
# Use CTRL-C to stop the components.
set -euo pipefail

cd "$(dirname "$0")/.."

CERBOS_VERSION="$(tr -d '[:space:]' < ../../conformance/CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' < ../../conformance/CERBOS_IMAGE_DIGEST)"
export CERBOS_VERSION CERBOS_IMAGE_DIGEST
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export EXAMPLE_PORT="${EXAMPLE_PORT:-4567}"

echo "==> Cerbos ${CERBOS_VERSION}, Ruby ${RUBY_VERSION}, port ${EXAMPLE_PORT}"
docker compose up --build
