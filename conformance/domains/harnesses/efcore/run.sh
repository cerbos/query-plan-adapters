#!/usr/bin/env bash
# Domain stress harness: ./run.sh [sqlite|postgres|sqlserver] [domain...]
# Writes conformance/domains/results/efcore/<store>.json. Not part of the adapter's CI. Builds the
# adapter from ../../../../efcore.
set -euo pipefail
cd "$(dirname "$0")"
store="${1:-sqlite}"; shift || true
case "$store" in
  postgres)
    docker run -d --name dom-net-postgres -p 55432:5432 -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_INITDB_ARGS="--locale-provider=icu --icu-locale=en-US" "$(cat ../../../../efcore/POSTGRES_IMAGE)" >/dev/null
    trap 'docker rm -f dom-net-postgres >/dev/null' EXIT
    until docker exec dom-net-postgres pg_isready -U postgres -h localhost >/dev/null 2>&1; do sleep 1; done; sleep 2 ;;
  sqlserver)
    docker run -d --name dom-net-sqlserver -p 51433:1433 -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=Domains_Passw0rd \
      "$(cat ../../../../efcore/SQLSERVER_IMAGE)" >/dev/null
    trap 'docker rm -f dom-net-sqlserver >/dev/null' EXIT
    until docker logs dom-net-sqlserver 2>&1 | grep -q 'Recovery is complete'; do sleep 2; done; sleep 5 ;;
esac
dotnet run -c Release -- "$store" "$@"
