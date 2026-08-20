#!/usr/bin/env bash
#
# Integrity checks for the demo domain. Runs in every adapter's example job, needs no PDP, no
# database and no network.
#
# The demo domain's expectations are hardcoded id lists, unlike conformance/'s, which are computed
# from a live oracle. That is the right trade for proving plumbing — a frozen list reads as
# documentation and is the better tripwire — but it means the lists can rot to something that
# passes vacuously. These checks are what stops that.
#
#   1. Structural: every catalog case is well-formed.
#      run-example.sh diffs the complete projected result, so an example that skips a case fails.
#   2. Non-degeneracy: the expectations still discriminate — a proper subset somewhere, and
#      shape 5 differing from both of the two filters it composes.
#   3. Pin reuse: the demo domain has no PDP version of its own. run-example.sh injects a
#      non-default $CERBOS_HOST, so live example execution proves the runtime endpoint contract.
#   4. Every discovered adapter manifest has a runnable example/run.sh.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"

SEEDS="${DEMO_DIR}/seeds.json"
CASES="${DEMO_DIR}/cases.json"

failures=0
fail() { echo "  ✗ $*" >&2; failures=$((failures + 1)); }

for f in "${SEEDS}" "${CASES}"; do
  [[ -f "${f}" ]] || { echo "missing ${f}" >&2; exit 1; }
  jq -e . "${f}" >/dev/null || { echo "${f} is not valid JSON" >&2; exit 1; }
done

ADAPTERS="$(for manifest in "${REPO_ROOT}"/*/adapterctl.json; do
  basename "$(dirname "${manifest}")"
done | sort)"
[[ -n "${ADAPTERS}" ]] || { echo "no adapterctl.json manifests discovered" >&2; exit 1; }

# Shared jq preamble: the seed id list, and the id set the APPLICATION's own predicate selects on
# its own. Both are derived from seeds.json so neither can drift from the rows the examples load.
read -r -d '' JQ_LIB <<'JQ' || true
def seed_ids: [.documents[].id];
def app_ids:
  (.applicationFilter | to_entries | map(select(.key != "description"))) as $pred
  | [ .documents[] | select(. as $d | all($pred[]; $d[.key] == .value)) | .id ];
JQ

SEED_IDS="$(jq -c "${JQ_LIB} seed_ids | sort" "${SEEDS}")"
APP_IDS="$(jq -c "${JQ_LIB} app_ids | sort" "${SEEDS}")"
SEED_COUNT="$(jq -r 'length' <<<"${SEED_IDS}")"

echo "==> demo domain: ${SEED_COUNT} seed rows, application filter selects $(jq -r 'length' <<<"${APP_IDS}")"

# ---------------------------------------------------------------------------------------------
# 1. Structural
# ---------------------------------------------------------------------------------------------
echo "==> [1/4] structural: well-formed catalog cases"

if ! jq -e --argjson seedIds "${SEED_IDS}" '
  .schemaVersion == 1
  and (.cases | type == "array" and length > 0)
  and ([.cases[].id] | length == (unique | length))
  and all(.cases[];
    .id == (.operation + "/" + .principal + "/" + .action)
    and (.principal | type == "string" and test("^[a-z0-9-]+$"))
    and (.action | type == "string" and test("^[a-z0-9-]+$"))
    and (.expected.kind == "KIND_CONDITIONAL"
      or .expected.kind == "KIND_ALWAYS_ALLOWED"
      or .expected.kind == "KIND_ALWAYS_DENIED")
    and (.expected.ids | type == "array")
    and .expected.ids == (.expected.ids | sort)
    and (.expected.ids | length) == (.expected.ids | unique | length)
    and ((.expected.ids - $seedIds) | length) == 0
    and (if .operation == "filtered" then
      .expected.kind == "KIND_CONDITIONAL" and .pagination == null
    elif .operation == "alwaysAllowed" then
      .expected.kind == "KIND_ALWAYS_ALLOWED"
      and .expected.ids == ($seedIds | sort)
      and .pagination == null
    elif .operation == "alwaysDenied" then
      .expected.kind == "KIND_ALWAYS_DENIED"
      and .expected.ids == []
      and .pagination == null
    elif .operation == "paginated" then
      (.pagination.pageSize | type == "number" and . >= 1)
      and (.pagination.pageSizes | type == "array" and length >= 2)
      and all(.pagination.pageSizes[]; type == "number" and . >= 1)
      and (.pagination.pageSizes | add) == (.expected.ids | length)
      and (.pagination as $pagination
        | all($pagination.pageSizes[:-1][]; . == $pagination.pageSize)
        and $pagination.pageSizes[-1] <= $pagination.pageSize)
    elif .operation == "composed" then .pagination == null
    else false end)
  )
' "${CASES}" >/dev/null; then
  fail "cases.json must contain unique, well-formed catalog cases"
fi

# ---------------------------------------------------------------------------------------------
# 2. Non-degeneracy
# ---------------------------------------------------------------------------------------------
echo "==> [2/4] non-degeneracy: the expectations still discriminate"

# The analogue of the conformance degeneracy guard. Without it these lists can rot to all-empty,
# or to every-seed-row, and still pass every diff.
proper_subset_operations="$(jq -r --argjson seedIds "${SEED_IDS}" '
  [ .cases[]
    | select(.operation == "filtered" or .operation == "composed")
    | select((.expected.ids | length) > 0 and (.expected.ids | length) < ($seedIds | length))
    | .operation
  ] | unique | length
' "${CASES}")"
if (( proper_subset_operations < 2 )); then
  fail "filtered and composed must each contain at least one proper, non-empty subset of the seed rows"
fi

# Shape 5 is only worth running if it differs from BOTH filters it composes. If it equals the
# adapter's filter, the example could drop the application predicate and still pass; if it equals
# the application predicate's own result, the example could drop the ADAPTER and still pass — and
# that second one is an authorization hole that reads as a green build.
while IFS= read -r problem; do
  fail "${problem}"
done < <(jq -r --argjson appIds "${APP_IDS}" --argjson seedIds "${SEED_IDS}" '
  .cases as $cases
  | .cases[]
  | select(.operation == "composed")
  | . as $case
  | ($cases | map(select(
      .operation != "composed" and .operation != "paginated"
      and .principal == $case.principal and .action == $case.action
    )) | first) as $base
  | (if $case.expected.kind == "KIND_ALWAYS_ALLOWED" then ($seedIds | sort)
     elif $case.expected.kind == "KIND_ALWAYS_DENIED" then []
     else ($base.expected.ids // null) end) as $unfiltered
  | ($case.principal + "/" + $case.action) as $key
  | [
      (select($unfiltered == null)
        | "composed/\($key): KIND_CONDITIONAL needs a matching filtered case"),

      # The composed answer must BE the intersection. This recomputes shape 5 from shape 1 and
      # seeds.json, so the two hardcoded lists cannot rot independently of each other.
      (select($unfiltered != null and ($case.expected.ids != ($unfiltered - ($unfiltered - $appIds) | sort)))
        | "composed/\($key): expected \(($unfiltered - ($unfiltered - $appIds) | sort) | join(",")), got \($case.expected.ids | join(","))"),

      (select($case.expected.kind == "KIND_CONDITIONAL" and $case.expected.ids == $unfiltered)
        | "composed/\($key): equals the adapter filter alone — the application predicate is a no-op here"),
      (select($case.expected.kind == "KIND_CONDITIONAL" and $case.expected.ids == ($appIds | sort))
        | "composed/\($key): equals the application predicate alone — an example that never called the adapter would pass")
    ][]
' "${CASES}")

# A paginated entry pages over an answer another shape already pins, so the two must agree.
while IFS= read -r problem; do
  fail "${problem}"
done < <(jq -r '
  .cases as $cases
  | .cases[]
  | select(.operation == "paginated")
  | . as $case
  | ($cases | map(select(
      .operation != "paginated" and .operation != "composed"
      and .principal == $case.principal and .action == $case.action
    )) | first) as $base
  | ($case.principal + "/" + $case.action) as $key
  | [
      (select($base == null)
        | "paginated/\($key): no unpaginated case pins this principal/action"),
      (select($base != null and $base.expected.ids != $case.expected.ids)
        | "paginated/\($key): paged ids disagree with unpaginated ids"),
      (select($base != null and $base.expected.kind != $case.expected.kind)
        | "paginated/\($key): plan kind disagrees with the unpaginated case")
    ][]
' "${CASES}")

# Every principal and action named in cases.json has to exist. A typo in an action name makes
# the PDP return ALWAYS_DENIED, and the example would then assert an empty list forever without
# anything looking wrong.
#
# The action names come out of the `actions:` blocks specifically, not out of every quoted list
# item in the file — `roles:` is also a list of bare lowercase names, and a scan loose enough to
# pick those up would accept `admin` as a valid action.
policy_actions="$(awk '
  /^[[:space:]]*-?[[:space:]]*actions:[[:space:]]*$/ { inactions = 1; next }
  inactions && /^[[:space:]]*-[[:space:]]*"?[a-z0-9-]+"?[[:space:]]*$/ {
    gsub(/^[[:space:]]*-[[:space:]]*"?|"?[[:space:]]*$/, ""); print; next
  }
  { inactions = 0 }
' "${DEMO_DIR}/policies/document.yaml" | sort -u)"
if [[ -z "${policy_actions}" ]]; then
  fail "no actions parsed from demo/policies/document.yaml — the extraction above is broken"
fi

# An action with NO rule is how KIND_ALWAYS_DENIED is produced, so `alwaysDenied` must name one
# that the policy does not grant, and every other shape must name one that it does. Deriving both
# from the policy keeps the action name out of this script: renaming `publish` in the policy
# cannot leave a stale literal here.
while IFS= read -r shape_ref; do
  shape="${shape_ref%% *}"
  ref="${shape_ref#* }"
  principal="${ref%%/*}"
  action="${ref##*/}"

  if ! jq -e --arg p "${principal}" 'any(.principals[]; .id == $p)' "${SEEDS}" >/dev/null; then
    fail "cases.json refers to principal '${principal}', which is not in seeds.json"
  fi

  granted=0
  grep -qx "${action}" <<<"${policy_actions}" && granted=1

  if [[ "${shape}" == "alwaysDenied" ]]; then
    (( granted == 0 )) || \
      fail "alwaysDenied/${ref}: '${action}' IS granted by demo/policies/document.yaml, so the planner will not return KIND_ALWAYS_DENIED"
  elif jq -e --arg id "${shape}/${ref}" \
    'any(.cases[]; .id == $id and .expected.kind != "KIND_ALWAYS_DENIED")' "${CASES}" >/dev/null; then
    (( granted == 1 )) || \
      fail "${shape}/${ref}: no rule in demo/policies/document.yaml grants '${action}'"
  fi
done < <(jq -r '.cases[] | "\(.operation) \(.principal)/\(.action)"' "${CASES}")

# ---------------------------------------------------------------------------------------------
# 3. PDP pin reuse
# ---------------------------------------------------------------------------------------------
echo "==> [3/4] PDP pin: one pin in the repository, reused by the demo domain"

pinned_version="$(tr -d '[:space:]' <"${REPO_ROOT}/conformance/CERBOS_VERSION")"
pinned_digest="$(tr -d '[:space:]' <"${REPO_ROOT}/conformance/CERBOS_IMAGE_DIGEST")"
pinned_image="ghcr.io/cerbos/cerbos:${pinned_version}@${pinned_digest}"

# The demo domain gets no version file of its own. Two pins would eventually name two builds, and
# the one an example actually ran against would be whichever it happened to read.
for stray in CERBOS_VERSION CERBOS_IMAGE_DIGEST; do
  [[ -e "${DEMO_DIR}/${stray}" ]] && \
    fail "demo/${stray} must not exist — the pin lives in conformance/ and is reused"
done

if ! grep -qF "${pinned_image}" "${DEMO_DIR}/docker-compose.yml"; then
  fail "demo/docker-compose.yml must pin ${pinned_image}"
fi

# ---------------------------------------------------------------------------------------------
# 4. Every adapter has a runnable example/run.sh
# ---------------------------------------------------------------------------------------------
echo "==> [4/4] example coverage: every adapter has a runnable example/run.sh"

# Adapter manifests are the roster. Discovery means registering an adapter immediately demands an
# example without adding it to a second central list.
#
# There is no per-adapter opt-out and no environment variable to switch it off: a gate with an
# escape hatch is not a gate, and the reason there is nothing to opt out WITH is ADR 0001, the
# same argument demo/README.md's check 4 states in full.
#
# Existence and the mode bit are separate failures because they need different fixes, and because
# `-e` alone is not enough: run-example.sh executes the script directly, so a run.sh committed
# without its mode bit is not a runnable example either. Testing existence alone would pass it
# here and fail later in the example job, with a message about the runner rather than the mode bit.
for adapter in ${ADAPTERS}; do
  runner="${REPO_ROOT}/${adapter}/example/run.sh"
  if [[ ! -e "${runner}" ]]; then
    fail "${adapter} has no example/run.sh — every discovered adapter needs an" \
      "example application (demo/README.md)"
  elif [[ ! -x "${runner}" ]]; then
    fail "${adapter}/example/run.sh is not executable — demo/scripts/run-example.sh invokes it" \
      "directly, so chmod +x it"
  fi
done

if (( failures > 0 )); then
  echo >&2
  echo "demo domain validation failed with ${failures} problem(s)" >&2
  exit 1
fi

echo "==> demo domain OK"
