#!/usr/bin/env bash
# Engine error contracts, separate from the adapter corpus: principal-dependent errors must
# agree between Check and Plan; nonfinite resource expressions pin a Plan serialization bug.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROBE_DIR="${CONFORMANCE_DIR}/evaluation-modes"
CERBOS_VERSION="$(tr -d '[:space:]' <"${CONFORMANCE_DIR}/CERBOS_VERSION")"
CERBOS_IMAGE_DIGEST="$(tr -d '[:space:]' <"${CONFORMANCE_DIR}/CERBOS_IMAGE_DIGEST")"
CERBOS_IMAGE="ghcr.io/cerbos/cerbos:${CERBOS_VERSION}@${CERBOS_IMAGE_DIGEST}"
PROBE_TMP="$(mktemp -d)"
CONTAINER_ID=""
cleanup() {
  if [[ -n "${CONTAINER_ID}" ]]; then
    docker rm -f "${CONTAINER_ID}" >/dev/null 2>&1 || true
  fi
  rm -rf "${PROBE_TMP}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

NONFINITE_ACTIONS='["nan-ord-ternary","nan-ord-ternary-vf","nan-ord-le","nan-ord-inf","not-nan-ord-le"]'
ACTIONS='["missing-deny","type-deny","variable-deny","derived-deny","unrelated","missing-allow"]'
for strict in false true; do
  compile_args=()
  if [[ "${strict}" == true ]]; then
    compile_args+=(--strict-evaluation)
  fi
  if ! docker run --rm -v "${CONFORMANCE_DIR}/policies:/policies:ro" "${CERBOS_IMAGE}" \
    compile "${compile_args[@]}" /policies >"${PROBE_TMP}/compile.log" 2>&1; then
    cat "${PROBE_TMP}/compile.log" >&2
    exit 1
  fi
  if docker run --rm -v "${PROBE_DIR}/invalid-regex:/policies:ro" "${CERBOS_IMAGE}" \
    compile "${compile_args[@]}" /policies >"${PROBE_TMP}/invalid-regex.log" 2>&1; then
    echo "Invalid literal regex compiled with strictEvaluation=${strict}" >&2
    exit 1
  fi
  if ! grep -q 'invalid matches argument' "${PROBE_TMP}/invalid-regex.log"; then
    cat "${PROBE_TMP}/invalid-regex.log" >&2
    echo 'Expected a compile-time invalid matches argument diagnostic' >&2
    exit 1
  fi

  CONTAINER_ID="$(docker run -d --rm -p 127.0.0.1::3592 \
    -v "${CONFORMANCE_DIR}/policies:/policies:ro" -e CERBOS_NO_TELEMETRY=1 \
    "${CERBOS_IMAGE}" server --set=storage.disk.directory=/policies \
    "--set=engine.strictEvaluation=${strict}")"
  healthy=false
  for _ in $(seq 1 30); do
    if docker exec "${CONTAINER_ID}" /cerbos healthcheck >/dev/null 2>&1; then
      healthy=true
      break
    fi
    sleep 1
  done
  if [[ "${healthy}" != true ]]; then
    docker logs "${CONTAINER_ID}" >&2
    exit 1
  fi
  port="$(docker port "${CONTAINER_ID}" 3592/tcp | sed 's/.*://')"
  for principal_name in hostile valid; do
    principal="$(jq -c --arg name "${principal_name}" '.[$name]' "${PROBE_DIR}/principals.json")"
    expected="$(jq -nc --argjson actions "${ACTIONS}" --arg strict "${strict}" --arg principal "${principal_name}" '
      reduce $actions[] as $action ({};
        .[$action] = (if $action == "missing-allow" or
          ($principal == "hostile" and $strict == "true" and $action != "unrelated")
          then "EFFECT_DENY" else "EFFECT_ALLOW" end))')"
    body="$(jq -nc --argjson principal "${principal}" --argjson actions "${ACTIONS}" '
      {requestId:"evaluation-probe",principal:$principal,resources:[{resource:{kind:"evaluation_probe",id:"probe",attr:{}},actions:$actions}]}')"
    response="$(curl --fail-with-body --silent --show-error -H 'Content-Type: application/json' \
      -d "${body}" "http://127.0.0.1:${port}/api/check/resources")"
    if ! jq -e --argjson expected "${expected}" \
      '(.results | length) == 1 and .results[0].actions == $expected' <<<"${response}" >/dev/null; then
      echo "Check mismatch: strict=${strict}, principal=${principal_name}; expected=${expected}; response=${response}" >&2
      exit 1
    fi
    while IFS= read -r action; do
      kind="$(jq -r --arg action "${action}" 'if .[$action] == "EFFECT_ALLOW" then "KIND_ALWAYS_ALLOWED" else "KIND_ALWAYS_DENIED" end' <<<"${expected}")"
      body="$(jq -nc --argjson principal "${principal}" --arg action "${action}" '
        {requestId:"evaluation-probe",principal:$principal,action:$action,resource:{kind:"evaluation_probe",attr:{}}}')"
      response="$(curl --fail-with-body --silent --show-error -H 'Content-Type: application/json' \
        -d "${body}" "http://127.0.0.1:${port}/api/plan/resources")"
      if ! jq -e --arg kind "${kind}" '.filter.kind == $kind' <<<"${response}" >/dev/null; then
        echo "Plan mismatch: strict=${strict}, principal=${principal_name}, action=${action}; expected=${kind}; response=${response}" >&2
        exit 1
      fi
    done < <(jq -r '.[]' <<<"${ACTIONS}")
  done
  # These unguarded resource expressions compile but cannot serialize their 0.55 plans.
  # Keep the observed upstream error explicit; adapter suites exercise equivalent unfurled plans.
  for value in true false; do
    body="$(jq -nc --argjson value "${value}" --argjson actions "${NONFINITE_ACTIONS}" '{
      requestId:"nonfinite-check",principal:{id:"probe",roles:["USER"]},
      resources:[{resource:{kind:"nonfinite_plan_probe",id:"probe",attr:{aBool:$value}},actions:$actions}]}')"
    response="$(curl --fail-with-body --silent --show-error -H 'Content-Type: application/json' \
      -d "${body}" "http://127.0.0.1:${port}/api/check/resources")"
    effect=EFFECT_DENY
    if [[ "${value}" == true ]]; then effect=EFFECT_ALLOW; fi
    expected="$(jq -nc --arg effect "${effect}" '{
      "nan-ord-ternary":$effect,"nan-ord-ternary-vf":$effect,"nan-ord-inf":$effect,
      "nan-ord-le":"EFFECT_DENY","not-nan-ord-le":"EFFECT_ALLOW"}')"
    if ! jq -e --argjson expected "${expected}" \
      '(.results | length) == 1 and .results[0].actions == $expected' <<<"${response}" >/dev/null; then
      echo "Nonfinite Check mismatch: strict=${strict}, aBool=${value}; response=${response}" >&2
      exit 1
    fi
  done
  while IFS= read -r action; do
    body="$(jq -nc --arg action "${action}" '{requestId:"nonfinite-plan",
      principal:{id:"probe",roles:["USER"]},action:$action,resource:{kind:"nonfinite_plan_probe",attr:{}}}')"
    status="$(curl --silent --show-error -H 'Content-Type: application/json' \
      -d "${body}" -o "${PROBE_TMP}/nonfinite.json" -w '%{http_code}' \
      "http://127.0.0.1:${port}/api/plan/resources")"
    diagnostic="invalid NaN value"
    if [[ "${action}" == nan-ord-inf ]]; then diagnostic="invalid +Inf value"; fi
    if [[ "${status}" != 500 ]] || ! jq -e --arg diagnostic "${diagnostic}" \
      '.code == 2 and (.message | contains($diagnostic))' "${PROBE_TMP}/nonfinite.json" >/dev/null; then
      echo "Expected upstream nonfinite Plan serialization failure: strict=${strict}, action=${action}, status=${status}" >&2
      cat "${PROBE_TMP}/nonfinite.json" >&2
      exit 1
    fi
  done < <(jq -r '.[]' <<<"${NONFINITE_ACTIONS}")
  docker rm -f "${CONTAINER_ID}" >/dev/null
  CONTAINER_ID=""
  echo "Evaluation contract passed: strictEvaluation=${strict} (Check, Plan, invalid regex)"
done
