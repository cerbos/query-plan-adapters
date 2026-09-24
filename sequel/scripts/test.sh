#!/usr/bin/env bash
# Runs the Sequel adapter's suites in Docker. None needs a PDP: the conformance suite replays
# the plans and decisions recorded in ../conformance/golden/.
#
#   ./scripts/test.sh                                    # all the specs, on SQLite
#   ./scripts/test.sh spec/conformance_spec.rb
#   RUBY_VERSION=3.2 SEQUEL_VERSION="= 5.60.0" ./scripts/test.sh
#   ADAPTER_TEST_DB=postgres ./scripts/test.sh spec/conformance_spec.rb
#   ADAPTER_TEST_DB=mysql ./scripts/test.sh spec/conformance_spec.rb
set -euo pipefail

cd "$(dirname "$0")/.."

# The store images, pinned beside this adapter by tag and digest. Compose interpolates every
# service, so they are exported even for a run that starts neither.
POSTGRES_IMAGE="$(tr -d '[:space:]' < POSTGRES_IMAGE)"
MYSQL_IMAGE="$(tr -d '[:space:]' < MYSQL_IMAGE)"
export POSTGRES_IMAGE MYSQL_IMAGE
export RUBY_VERSION="${RUBY_VERSION:-3.4}"
export SEQUEL_VERSION="${SEQUEL_VERSION:-}"

# The store the conformance harness replays the corpus on. An unknown value fails here rather
# than falling back, because a typo that quietly ran SQLite would report a store as covered that
# nothing executed. The contract suite refuses any store but SQLite itself.
export ADAPTER_TEST_DB="${ADAPTER_TEST_DB:-sqlite}"
case "${ADAPTER_TEST_DB}" in
  sqlite) export DATABASE_URL="" ;;
  postgres) export DATABASE_URL="postgres://cerbos:cerbos@postgres-store:5432/cerbos" ;;
  mysql) export DATABASE_URL="trilogy://root:cerbos@mysql-store:3306/cerbos" ;;
  *) echo "ADAPTER_TEST_DB must be sqlite, postgres or mysql" >&2; exit 1 ;;
esac

cleanup() { docker compose down --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "==> Ruby ${RUBY_VERSION}, Sequel ${SEQUEL_VERSION:-newest 5.x}, store ${ADAPTER_TEST_DB}"
docker compose build tests
if [[ "${ADAPTER_TEST_DB}" != "sqlite" ]]; then
  # Named explicitly, which starts a service behind a profile; `--wait` holds until its
  # healthcheck passes, so the harness never races the server's first boot.
  docker compose up -d --wait "${ADAPTER_TEST_DB}-store"
fi
docker compose run --rm tests bundle exec rspec "$@"
