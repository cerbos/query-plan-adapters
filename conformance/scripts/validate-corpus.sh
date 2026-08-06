#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VALIDATION_TMP="$(mktemp -d)"

cleanup() {
  rm -rf "${VALIDATION_TMP}"
}
trap cleanup EXIT INT TERM

cd "${CONFORMANCE_DIR}"

sed -n 's/^[[:space:]]*- actions: \["\([^"]*\)"\].*/\1/p' \
  policies/adversarial.yaml | sort >"${VALIDATION_TMP}/policy-actions"

jq -r '
  .conformance[],
  .expectedUnsupported[].action,
  .nullRepresentationOmitted[].action,
  .knownDivergences[].action
' actions.json | sort >"${VALIDATION_TMP}/classified-actions"

if duplicates="$(uniq -d "${VALIDATION_TMP}/policy-actions")" && [[ -n "${duplicates}" ]]; then
  echo "Duplicate policy actions:"
  echo "${duplicates}"
  exit 1
fi

if duplicates="$(uniq -d "${VALIDATION_TMP}/classified-actions")" && [[ -n "${duplicates}" ]]; then
  echo "Actions classified more than once:"
  echo "${duplicates}"
  exit 1
fi

if ! diff -u "${VALIDATION_TMP}/policy-actions" "${VALIDATION_TMP}/classified-actions"; then
  echo "Every policy action must be classified exactly once in actions.json"
  exit 1
fi

if ! jq -e '
  all(
    .knownDivergences[];
    (.adapters | type == "array" and length > 0 and length == (unique | length))
    and all(.adapters[]; type == "string" and length > 0)
  )
' actions.json >/dev/null; then
  echo "Each known divergence must name a non-empty, duplicate-free adapters list"
  exit 1
fi

jq -r '.conformance[]' actions.json | sort -u >"${VALIDATION_TMP}/conformance-actions"
jq -r '.expectedUnsupported[].action' actions.json | sort -u >"${VALIDATION_TMP}/expected-unsupported-actions"
jq -r '.adapterUnsupported | to_entries[] | .key as $adapter | .value[] | [$adapter, .action] | @tsv' \
  actions.json | sort >"${VALIDATION_TMP}/adapter-unsupported"

if duplicates="$(uniq -d "${VALIDATION_TMP}/adapter-unsupported")" && [[ -n "${duplicates}" ]]; then
  echo "Duplicate adapterUnsupported entries:"
  echo "${duplicates}"
  exit 1
fi

while IFS=$'\t' read -r adapter action; do
  if ! grep -Fqx "${action}" "${VALIDATION_TMP}/conformance-actions"; then
    echo "adapterUnsupported.${adapter} references non-conformance action: ${action}"
    exit 1
  fi
done <"${VALIDATION_TMP}/adapter-unsupported"

jq -r '
  (.adapterSupportedExpected // {})
  | to_entries[]
  | .key as $adapter
  | .value[]
  | [$adapter, .action]
  | @tsv
' actions.json | sort >"${VALIDATION_TMP}/adapter-supported-expected"

if duplicates="$(uniq -d "${VALIDATION_TMP}/adapter-supported-expected")" && [[ -n "${duplicates}" ]]; then
  echo "Duplicate adapterSupportedExpected entries:"
  echo "${duplicates}"
  exit 1
fi

while IFS=$'\t' read -r adapter action; do
  if ! grep -Fqx "${action}" "${VALIDATION_TMP}/expected-unsupported-actions"; then
    echo "adapterSupportedExpected.${adapter} references non-expectedUnsupported action: ${action}"
    exit 1
  fi
done <"${VALIDATION_TMP}/adapter-supported-expected"

find wire-fixtures -type f -name '*.json' -exec basename {} .json \; |
  sort >"${VALIDATION_TMP}/fixture-actions"

if ! diff -u "${VALIDATION_TMP}/policy-actions" "${VALIDATION_TMP}/fixture-actions"; then
  echo "Every policy action must have exactly one golden wire fixture"
  exit 1
fi

resource_kind="$(jq -r '.resourceKind' seeds.json)"
while IFS= read -r action; do
  fixture="wire-fixtures/${action}.json"
  if ! jq -e \
    --arg action "${action}" \
    --arg resourceKind "${resource_kind}" '
      .action == $action
      and .resourceKind == $resourceKind
      and (
        .filter.kind == "KIND_ALWAYS_ALLOWED"
        or .filter.kind == "KIND_ALWAYS_DENIED"
        or .filter.kind == "KIND_CONDITIONAL"
      )
    ' "${fixture}" >/dev/null; then
    echo "Invalid golden wire fixture content: ${fixture}"
    exit 1
  fi
done <"${VALIDATION_TMP}/policy-actions"

for action in ts-window ts-vf; do
  fixture="wire-fixtures/${action}.json"
  if ! jq -e '
    [
      ..
      | objects
      | select(.expression?.operator == "timestamp")
      | .expression.operands[0].value?
      | select(. != null)
    ] == ["__NOW_MINUS_24H__"]
  ' "${fixture}" >/dev/null; then
    echo "Dynamic now()-24h timestamp is not normalized in ${fixture}"
    exit 1
  fi
done

# CERBOS_VERSION is the single source of truth for the pinned PDP: every workflow and test
# harness reads it. Some files cannot read another file (Compose files, echo strings, go.mod
# requirements), so every hardcoded restatement anywhere in the repository is asserted to
# agree instead of de-duplicated. A repo-wide scan rather than a fixed path list: the fixed
# list once missed spring-data/example/docker-compose.yml running `latest` in CI.
pinned_version="$(tr -d '[:space:]' <CERBOS_VERSION)"
REPO_ROOT="$(cd "${CONFORMANCE_DIR}/.." && pwd)"

version_drift=0
while IFS=: read -r file _ match; do
  # Extract the tag: everything after the last `cerbos:` up to a digest/quote/space/paren.
  tag="$(printf '%s' "${match}" | sed -n 's|.*ghcr\.io/cerbos/cerbos:\([^@)"'\''[:space:]]*\).*|\1|p')"
  [[ -z "${tag}" ]] && continue
  # Workflow/script interpolations (`cerbos:${CERBOS_VERSION}` and language equivalents)
  # read the pinned file at runtime and cannot drift.
  case "${tag}" in
    '$'*|'%'*|'{'*) continue ;;
  esac
  if [[ "${tag}" != "${pinned_version}" ]]; then
    echo "${file#"${REPO_ROOT}"/} pins Cerbos '${tag}', expected ${pinned_version} (conformance/CERBOS_VERSION)"
    version_drift=1
  fi
done < <(grep -rn 'ghcr\.io/cerbos/cerbos:' "${REPO_ROOT}" \
  --include='*.yml' --include='*.yaml' --include='*.sh' \
  --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=.claude --exclude-dir=lib \
  --exclude-dir=build --exclude-dir=.venv --exclude-dir=.gradle --exclude-dir=bin || true)
if [[ "${version_drift}" -ne 0 ]]; then
  exit 1
fi

# The Go modules pin the Cerbos wire gencode (cerbos/api/genpb) separately from the PDP
# image; a CERBOS_VERSION bump that leaves a stale genpb would silently test new planner
# output against old generated types.
for gomod in "${REPO_ROOT}"/ent/go.mod "${REPO_ROOT}"/pgx/go.mod; do
  genpb_version="$(sed -n 's|.*github\.com/cerbos/cerbos/api/genpb v\([^[:space:]]*\).*|\1|p' "${gomod}")"
  if [[ -n "${genpb_version}" && "${genpb_version}" != "${pinned_version}" ]]; then
    echo "${gomod#"${REPO_ROOT}"/} pins cerbos/api/genpb v${genpb_version}, expected ${pinned_version} (conformance/CERBOS_VERSION)"
    exit 1
  fi
done

seed_count="$(jq '.seeds | length' seeds.json)"
unique_seed_count="$(jq -r '.seeds[].id' seeds.json | sort -u | wc -l | tr -d '[:space:]')"
if [[ "${seed_count}" != "${unique_seed_count}" ]]; then
  echo "Seed ids must be unique"
  exit 1
fi

# derived-fields.json materialises README.md's "Deterministic derived fields" rules once for every
# harness. Nothing downstream can catch a wrong value there: each harness feeds the same entry to
# both the stored row and the check() oracle, so a bad value makes both sides agree for the wrong
# reason. These assertions are the only independent restatement of the rules.
if ! jq -e '
  (.fields | type) == "array"
  and (.fields | length) > 0
  and (.fields | length) == (.fields | unique | length)
  and all(.fields[]; type == "string" and length > 0)
' derived-fields.json >/dev/null; then
  echo "derived-fields.json must declare a non-empty, duplicate-free fields list"
  exit 1
fi

jq -r '.seeds[].id' seeds.json | sort >"${VALIDATION_TMP}/seed-ids"
jq -r '.derived | keys[]' derived-fields.json | sort >"${VALIDATION_TMP}/derived-ids"
if ! diff -u "${VALIDATION_TMP}/seed-ids" "${VALIDATION_TMP}/derived-ids"; then
  echo "derived-fields.json must carry exactly one entry per seed id"
  exit 1
fi

if ! jq -e '
  (.fields | sort) as $fields
  | all(.derived[]; keys == $fields)
' derived-fields.json >/dev/null; then
  echo "Every derived-fields.json entry must carry exactly the fields it declares"
  exit 1
fi

if ! jq -e '
  all(.derived[];
    ((.createdBy | type) == "string")
    and ((.aDouble | type) == "number" or .aDouble == null)
    and ((.createdAt | type) == "string" or .createdAt == null)
    and ((.scope | type) == "string" or .scope == null)
    and ((.labels | type) == "array")
    and all(.labels[]; type == "string" or . == null))
' derived-fields.json >/dev/null; then
  echo "derived-fields.json entries have the wrong value types"
  exit 1
fi

derived_drift="$(jq -r -s '
  .[1].derived as $derived
  | .[0].seeds[]
  | . as $seed
  | $derived[$seed.id] as $entry
  | [
      (if $entry.createdBy != (
         if $seed.aNumber >= 2 then "2024-06-01T00:00:00Z" else "2026-06-01T00:00:00Z" end
       ) then "createdBy" else empty end),
      (if $entry.aDouble != (
         {"a1": -0.6, "a2": 0.25, "a3": null} as $fixed
         | if ($fixed | has($seed.id)) then $fixed[$seed.id] else $seed.aNumber + 0.3 end
       ) then "aDouble" else empty end),
      (if $entry.createdAt != (
         {
           "a1": "2020-03-15T10:30:00Z",
           "a2": "2037-01-01T00:00:00Z",
           "a3": null,
           "a4": "2024-06-01T00:00:00Z",
           "a5": "2020-03-15T10:30:00.123456Z"
         } as $fixed
         | if ($fixed | has($seed.id)) then $fixed[$seed.id]
           elif $seed.aNumber >= 2 then "2036-06-06T06:06:06Z"
           else "2021-05-05T05:05:05Z" end
       ) then "createdAt" else empty end)
    ]
  | select(length > 0)
  | "  \($seed.id): \(join(", "))"
' seeds.json derived-fields.json)"
if [[ -n "${derived_drift}" ]]; then
  echo "derived-fields.json disagrees with the derived-field rules in README.md:"
  echo "${derived_drift}"
  exit 1
fi

echo "Corpus valid: $(wc -l <"${VALIDATION_TMP}/policy-actions" | tr -d '[:space:]') actions, ${seed_count} seeds"
