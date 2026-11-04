#!/usr/bin/env bash
# Runs the sqlrewrite example against the demo domain: starts PostgreSQL, builds and starts the
# service with config.yaml, then runs main.go, which prints the shapes document on stdout.
# demo/scripts/run-example.sh sets CERBOS_HOST and diffs the output.
#
# Go has no packaging step, so the example resolves the service's module through a `replace`
# directive (docs/adr/0002-examples-install-the-packed-artifact.md); the service itself is built
# from cmd/sqlrewrite and reached over HTTP, the way a deployment reaches it.

set -euo pipefail

EXAMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ADAPTER_DIR="$(cd "${EXAMPLE_DIR}/.." && pwd)"

POSTGRES_IMAGE="postgres:17-alpine@sha256:266d3c257594e195b309ae9d1805e54c788f1d7b375f56bf6c367aac6c917e7b"
PG_CONTAINER="cerbos-demo-sqlrewrite"
PG_PORT=15433
PG_USER=cerbos_demo
PG_PASSWORD=cerbos_demo
PG_DB=cerbos_demo
SERVICE_PORT=13594

BUILD_DIR="$(mktemp -d)"
SERVICE_PID=""

cleanup() {
  local status=$?
  if (( status != 0 )); then
    echo "==> sqlrewrite example failed (exit ${status})" >&2
    [[ -f "${BUILD_DIR}/service.log" ]] && cat "${BUILD_DIR}/service.log" >&2
    if docker container inspect "${PG_CONTAINER}" >/dev/null 2>&1; then
      docker logs "${PG_CONTAINER}" >&2 || true
    fi
  fi
  [[ -n "${SERVICE_PID}" ]] && kill "${SERVICE_PID}" 2>/dev/null || true
  docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
  rm -rf "${BUILD_DIR}"
}
trap cleanup EXIT INT TERM

cd "${EXAMPLE_DIR}"

for check in "dsn = \"postgres://${PG_USER}:${PG_PASSWORD}@127.0.0.1:${PG_PORT}/${PG_DB}?" \
  "serviceURL = \"http://127.0.0.1:${SERVICE_PORT}\""; do
  grep -qF "${check}" main.go || { echo "main.go does not contain ${check}: the two halves of that address must agree" >&2; exit 1; }
done
grep -qF "listenAddr: \"127.0.0.1:${SERVICE_PORT}\"" config.yaml \
  || { echo "config.yaml does not listen on ${SERVICE_PORT}" >&2; exit 1; }

echo "==> starting ${POSTGRES_IMAGE} on ${PG_PORT}" >&2
docker rm -f "${PG_CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${PG_CONTAINER}" -p "${PG_PORT}:5432" \
  -e POSTGRES_USER="${PG_USER}" \
  -e POSTGRES_PASSWORD="${PG_PASSWORD}" \
  -e POSTGRES_DB="${PG_DB}" \
  "${POSTGRES_IMAGE}" >/dev/null

export GOFLAGS="-mod=readonly"
echo "==> go build" >&2
(cd "${ADAPTER_DIR}" && CGO_ENABLED=0 go build -o "${BUILD_DIR}/sqlrewrite" ./cmd/sqlrewrite) >&2
go mod download >&2
go mod verify >&2
go build -o "${BUILD_DIR}/example" . >&2

echo "==> starting the service on ${SERVICE_PORT}" >&2
"${BUILD_DIR}/sqlrewrite" server --conf.path=config.yaml >"${BUILD_DIR}/service.log" 2>&1 &
SERVICE_PID=$!

echo "==> waiting for PostgreSQL and the service" >&2
ready=0
for _ in {1..60}; do
  if (( $(docker logs "${PG_CONTAINER}" 2>&1 \
    | grep -c 'database system is ready to accept connections' || true) >= 2 )) \
    && docker exec "${PG_CONTAINER}" pg_isready -q -U "${PG_USER}" -d "${PG_DB}" \
    && curl -fsS -o /dev/null -X POST -H 'Content-Type: application/json' -d '{}' \
      "http://127.0.0.1:${SERVICE_PORT}/grpc.health.v1.Health/Check"; then
    ready=1
    break
  fi
  sleep 1
done
(( ready == 1 )) || { echo "PostgreSQL or the service failed to start" >&2; exit 1; }

echo "==> ${BUILD_DIR}/example" >&2
"${BUILD_DIR}/example"
