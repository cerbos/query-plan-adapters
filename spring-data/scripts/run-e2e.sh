#!/usr/bin/env bash
# End-to-end test runner: starts a real Cerbos PDP container via docker compose, waits for it
# to be healthy, then runs the JUnit suite against it. Mirrors what the Prisma adapter does
# with `cerbos run -- jest`.
#
# Exit status mirrors gradle's. The container is torn down on success, failure, or interrupt.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}/.."

cleanup() {
  echo "==> Tearing down Cerbos PDP container"
  docker compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "==> Starting Cerbos PDP container (ghcr.io/cerbos/cerbos:latest)"
docker compose up -d --wait cerbos

CERBOS_HOST="${CERBOS_HOST:-localhost}"
CERBOS_PORT="${CERBOS_PORT:-3593}"
export CERBOS_HOST CERBOS_PORT

echo "==> Cerbos PDP is healthy at ${CERBOS_HOST}:${CERBOS_PORT}"

# Stream the PDP's audit/decision logs to a file. Decision-log JSON lines have callId/method
# entries that prove each test really called PlanResources against the live PDP.
AUDIT_LOG="$(mktemp -t cerbos-audit-XXXXXX.log)"
docker compose logs -f cerbos --no-color >"${AUDIT_LOG}" 2>&1 &
AUDIT_PID=$!
trap 'cleanup; kill "${AUDIT_PID}" 2>/dev/null || true; rm -f "${AUDIT_LOG}"' EXIT INT TERM

echo "==> Running tests against external PDP (audit log → ${AUDIT_LOG})"

GRADLE_ARGS=(test --rerun-tasks --no-daemon)
if [ "$#" -gt 0 ]; then
  GRADLE_ARGS+=("$@")
fi

if command -v gradle >/dev/null 2>&1; then
  gradle "${GRADLE_ARGS[@]}"
  TEST_EXIT=$?
else
  echo "==> No local gradle found; falling back to gradle:8.12-jdk17 Docker image"
  # The docker socket mount is required by AdversarialConformanceTest, which always spawns its
  # own PDP (with its own hostile policy set) via Testcontainers even in external-PDP mode.
  docker run --rm \
    -v "$(pwd)/..":/app \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -e TESTCONTAINERS_RYUK_DISABLED=true \
    --network host \
    -e CERBOS_HOST="${CERBOS_HOST}" \
    -e CERBOS_PORT="${CERBOS_PORT}" \
    -w /app/spring-data \
    gradle:8.12-jdk17 \
    gradle "${GRADLE_ARGS[@]}"
  TEST_EXIT=$?
fi

# Stop following the audit log and summarise what the PDP actually served.
kill "${AUDIT_PID}" 2>/dev/null || true
wait "${AUDIT_PID}" 2>/dev/null || true

PLAN_COUNT=$(grep -c '"PlanResources"' "${AUDIT_LOG}" 2>/dev/null || true)
CHECK_COUNT=$(grep -c '"CheckResources"' "${AUDIT_LOG}" 2>/dev/null || true)

echo
echo "==> Cerbos PDP audit summary"
echo "    PlanResources calls served: ${PLAN_COUNT:-0}"
echo "    CheckResources calls served: ${CHECK_COUNT:-0}"
echo "    Audit log archived at: ${AUDIT_LOG}"
echo
echo "==> Sample decision log entries:"
grep -E '"kind"|"action"|"method"|PlanResources' "${AUDIT_LOG}" 2>/dev/null | head -5 || true

# Don't auto-rm the audit log on success — leave it for inspection.
trap 'cleanup; kill "${AUDIT_PID}" 2>/dev/null || true' EXIT INT TERM

exit "${TEST_EXIT}"
