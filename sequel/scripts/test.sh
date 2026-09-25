#!/usr/bin/env bash
# Runs the Sequel adapter's suites in Docker. None needs a PDP: the conformance suite replays
# the plans and decisions recorded in ../conformance/golden/.
#
#   ./scripts/test.sh                                    # all the specs
#   ./scripts/test.sh spec/conformance_spec.rb
#   ADAPTER_TEST_DB=postgres ./scripts/test.sh spec/conformance_spec.rb   # or mysql
#   RUBY_VERSION=3.3 SEQUEL_VERSION="= 5.69.0" ./scripts/test.sh
set -euo pipefail

cd "$(dirname "$0")/.."

export RUBY_VERSION="${RUBY_VERSION:-4.0}"
export SEQUEL_VERSION="${SEQUEL_VERSION:-}"
export ADAPTER_TEST_DB="${ADAPTER_TEST_DB:-sqlite}"
POSTGRES_IMAGE="$(<POSTGRES_IMAGE)"
MYSQL_IMAGE="$(<MYSQL_IMAGE)"
export POSTGRES_IMAGE MYSQL_IMAGE

case "${ADAPTER_TEST_DB}" in
  sqlite) export DATABASE_URL="" ;;
  postgres)
    export COMPOSE_PROFILES="${ADAPTER_TEST_DB}"
    export DATABASE_URL="postgres://postgres:conformance@postgres/conformance"
    ;;
  mysql)
    export COMPOSE_PROFILES="${ADAPTER_TEST_DB}"
    export DATABASE_URL="trilogy://root:conformance@mysql/conformance"
    ;;
  *)
    echo "ADAPTER_TEST_DB must be sqlite, postgres or mysql, not '${ADAPTER_TEST_DB}'" >&2
    exit 1
    ;;
esac

cleanup() { docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> Ruby ${RUBY_VERSION}, Sequel ${SEQUEL_VERSION:-newest 5.x}, ${ADAPTER_TEST_DB}"
docker compose build tests
if [[ "${ADAPTER_TEST_DB}" != sqlite ]]; then
  docker compose up --detach --wait "${ADAPTER_TEST_DB}"
fi
docker compose run --rm tests bundle exec rspec "$@"
