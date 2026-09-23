#!/usr/bin/env bash
# Regenerates conformance/wire-fixtures{,-strict}/: the PlanResources response the pinned PDP
# returns for every action in actions.json, in both evaluation modes. The fixtures pin planner
# shape independent of any adapter, so a PDP bump that changes wire output shows up as a diff.
#
# Requires: docker, curl, jq.
#
# Run it locally after a deliberate PDP bump and commit the diff on its own. conformance.yaml runs
# it too and fails on any `git diff`, so output must be deterministic: no timestamps or ids.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
FIXTURE_TMP="$(mktemp -d)"
cd "${CONFORMANCE_DIR}"

CERBOS_VERSION="$(tr -d '[:space:]' <CERBOS_VERSION)"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' <CERBOS_IMAGE_DIGEST)"
CERBOS_IMAGE="ghcr.io/cerbos/cerbos:${CERBOS_VERSION}@${CERBOS_IMAGE_DIGEST}"
CONTAINER_NAME="cerbos-conformance-fixtures-$$"

cleanup() {
  echo "==> Tearing down Cerbos PDP container"
  docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
  rm -rf "${FIXTURE_TMP}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for strict in false true; do
  destination=wire-fixtures
  if [[ "${strict}" == true ]]; then
    destination=wire-fixtures-strict
  fi
  mkdir -p "${FIXTURE_TMP}/${destination}"

  echo "==> Starting Cerbos PDP ${CERBOS_VERSION}, strictEvaluation=${strict}"
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -p "127.0.0.1::3592" \
    -v "${CONFORMANCE_DIR}/policies:/policies:ro" \
    -e CERBOS_NO_TELEMETRY=1 \
    "${CERBOS_IMAGE}" \
    server --set=storage.disk.directory=/policies \
    --set="engine.strictEvaluation=${strict}" >/dev/null
  HTTP_PORT="$(docker port "${CONTAINER_NAME}" 3592/tcp | sed 's/.*://')"

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

  ACTIONS="$(jq -r '
    .conformance[],
    .expectedUnsupported[].action,
    .nullRepresentationOmitted[].action,
    .knownDivergences[].action
  ' actions.json | sort -u)"
  COUNT=0
  while IFS= read -r action; do
    BODY="$(jq -nc \
      --arg action "${action}" \
      --argjson principal "${PRINCIPAL}" \
      --arg resourceKind "${RESOURCE_KIND}" \
      '{requestId: ("conformance-" + $action), action: $action, principal: $principal, resource: {kind: $resourceKind, attr: {}}}')"

    if ! RESPONSE="$(curl --fail-with-body --silent --show-error \
      -X POST "http://localhost:${HTTP_PORT}/api/plan/resources" \
      -H 'Content-Type: application/json' \
      -d "${BODY}")"; then
      echo "Plan failed: action=${action}, strictEvaluation=${strict}: ${RESPONSE}" >&2
      exit 1
    fi

    # Drop per-call fields (call id, request id) so the fixture pins only planner output.
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
      ' >"${FIXTURE_TMP}/${destination}/${action}.json"
    COUNT=$((COUNT + 1))
  done <<<"${ACTIONS}"

  echo "==> Captured ${COUNT} wire fixtures (strictEvaluation=${strict})"
  docker rm -f "${CONTAINER_NAME}" >/dev/null
done

# Publish only after BOTH captures succeed; a failure leaves the old pair intact.
for destination in wire-fixtures wire-fixtures-strict; do
  mkdir -p "${destination}"
  rm -f "${destination}"/*.json
  mv "${FIXTURE_TMP}/${destination}"/*.json "${destination}/"
done
