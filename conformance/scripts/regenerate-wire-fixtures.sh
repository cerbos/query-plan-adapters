#!/usr/bin/env bash
# Regenerates conformance/wire-fixtures/*.json: for every action in actions.json, captures the
# exact PlanResources response the pinned Cerbos PDP (see CERBOS_VERSION) returns for
# policies/adversarial.yaml. These are golden wire fixtures -- they pin planner *shape*
# (operand order, operator choice, filter kind) independent of any adapter, so a PDP upgrade
# that silently changes wire output for a hostile shape is caught by diffing this directory
# instead of by an adapter test failing for the wrong reason.
#
# Requires: docker, curl, jq. Run deliberately (not in CI) after confirming a PDP version bump
# is intentional -- commit the resulting diff in its own commit so reviewers can see exactly
# what the planner's wire contract changed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
FIXTURE_TMP="$(mktemp -d)"
cd "${CONFORMANCE_DIR}"

CERBOS_VERSION="$(tr -d '[:space:]' <CERBOS_VERSION)"
CONTAINER_NAME="cerbos-conformance-fixtures"
HTTP_PORT=3592

cleanup() {
  echo "==> Tearing down Cerbos PDP container"
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  rm -rf "${FIXTURE_TMP}"
}
trap cleanup EXIT INT TERM

echo "==> Starting Cerbos PDP ${CERBOS_VERSION} with policies/adversarial.yaml"
docker run -d --rm \
  --name "${CONTAINER_NAME}" \
  -p "${HTTP_PORT}:3592" \
  -v "${CONFORMANCE_DIR}/policies:/policies:ro" \
  -e CERBOS_NO_TELEMETRY=1 \
  "ghcr.io/cerbos/cerbos:${CERBOS_VERSION}" \
  server --set=storage.disk.directory=/policies >/dev/null

echo "==> Waiting for the PDP to become healthy"
PDP_HEALTHY=false
for _ in $(seq 1 30); do
  if docker exec "${CONTAINER_NAME}" /cerbos healthcheck >/dev/null 2>&1; then
    PDP_HEALTHY=true
    break
  fi
  sleep 1
done
if [[ "${PDP_HEALTHY}" != true ]]; then
  echo "Cerbos PDP did not become healthy within 30 seconds" >&2
  docker logs "${CONTAINER_NAME}" >&2 || true
  exit 1
fi

PRINCIPAL="$(jq -c '.principal' seeds.json)"
RESOURCE_KIND="$(jq -r '.resourceKind' seeds.json)"

mkdir -p wire-fixtures

ACTIONS="$(jq -r '
  .conformance[],
  .expectedUnsupported[].action,
  .knownDivergences[].action
' actions.json | sort -u)"
COUNT=0
while IFS= read -r action; do
  BODY="$(jq -nc \
    --arg action "${action}" \
    --argjson principal "${PRINCIPAL}" \
    --arg resourceKind "${RESOURCE_KIND}" \
    '{requestId: ("conformance-" + $action), action: $action, principal: $principal, resource: {kind: $resourceKind, attr: {}}}')"

  RESPONSE="$(curl --fail-with-body --silent --show-error \
    -X POST "http://localhost:${HTTP_PORT}/api/plan/resources" \
    -H 'Content-Type: application/json' \
    -d "${BODY}")"

  # Strip fields that vary per-invocation (call id, request id echo) so the fixture only pins
  # planner-meaningful content: filter shape, resource kind, policy version, validation errors.
  echo "${RESPONSE}" | jq -e \
    --arg action "${action}" \
    --arg resourceKind "${RESOURCE_KIND}" '
      if $action == "ts-window" or $action == "ts-vf" then
        walk(
          if (
            type == "object"
            and .expression?.operator == "timestamp"
            and (.expression.operands[0].value? | type) == "string"
          ) then
            .expression.operands[0].value = "__NOW_MINUS_24H__"
          else
            .
          end
        )
      else
        .
      end
      | if .action != $action then
        error("response action does not match request")
      elif .resourceKind != $resourceKind then
        error("response resourceKind does not match request")
      elif (
        .filter.kind != "KIND_ALWAYS_ALLOWED"
        and .filter.kind != "KIND_ALWAYS_DENIED"
        and .filter.kind != "KIND_CONDITIONAL"
      ) then
        error("response has no recognized filter kind")
      else
        del(.requestId, .cerbosCallId)
      end
    ' >"${FIXTURE_TMP}/${action}.json"
  COUNT=$((COUNT + 1))
done <<<"${ACTIONS}"

rm -f wire-fixtures/*.json
mv "${FIXTURE_TMP}"/*.json wire-fixtures/
echo "==> Captured ${COUNT} wire fixtures into wire-fixtures/ (Cerbos ${CERBOS_VERSION})"
