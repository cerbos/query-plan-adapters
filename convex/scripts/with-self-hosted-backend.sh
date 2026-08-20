#!/usr/bin/env bash

# Run a command after starting the pinned self-hosted Convex backend, deploying the harness
# functions, and generating the gitignored client API. This is the native equivalent of the
# integration-test workflow's setup, with cleanup scoped to this invocation's Compose project.

set -euo pipefail

cd "$(dirname "$0")/.."

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <command> [argument ...]" >&2
  exit 2
fi

backend_url="http://127.0.0.1:3210"
compose_project="convex-adapterctl-$$"
backend_started=0

compose() {
  PORT=3210 \
    SITE_PROXY_PORT=3211 \
    CONVEX_CLOUD_ORIGIN="${backend_url}" \
    CONVEX_SITE_ORIGIN="http://127.0.0.1:3211" \
    docker compose -p "${compose_project}" "$@"
}

cleanup() {
  if [[ "${backend_started}" -eq 1 ]]; then
    compose down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

compose up -d
backend_started=1

ready=0
for _ in {1..30}; do
  if curl -sf "${backend_url}/version" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 2
done

if [[ "${ready}" -ne 1 ]]; then
  echo "Convex backend failed to start" >&2
  compose logs backend >&2 || true
  exit 1
fi

admin_key="$(compose exec -T backend ./generate_admin_key.sh 2>/dev/null | tail -1)"
if [[ -z "${admin_key}" ]]; then
  echo "Convex backend did not generate an admin key" >&2
  exit 1
fi

CONVEX_SELF_HOSTED_URL="${backend_url}" \
  CONVEX_SELF_HOSTED_ADMIN_KEY="${admin_key}" \
  npx convex deploy -y
CONVEX_SELF_HOSTED_URL="${backend_url}" \
  CONVEX_SELF_HOSTED_ADMIN_KEY="${admin_key}" \
  npx convex codegen

CONVEX_URL="${backend_url}" "$@"
