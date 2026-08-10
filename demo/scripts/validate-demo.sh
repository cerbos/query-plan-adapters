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
#   1. Structural: expected.json declares exactly the five usage shapes, and every entry in each
#      is well-formed for that shape. run-example.sh diffs the WHOLE shapes object exactly, so an
#      example that skips a shape or an entry fails there; this check is what makes that diff
#      mean "all five shapes" rather than "whatever expected.json happened to contain".
#   2. Non-degeneracy: the expectations still discriminate — a proper subset somewhere, and
#      shape 5 differing from both of the two filters it composes.
#   3. Pin reuse: the demo domain has no PDP version of its own and every example reaches the one
#      in conformance/ — at $CERBOS_HOST, never at an address of its own. That second half is
#      not fussiness: 3592/3593 are the ports every adapter's `cerbos run` test sidecar binds,
#      so a hardcoded default does not fail, it silently plans against the wrong policy suite.
#   4. Every adapter has an example/ directory. Landed DISABLED — it cannot pass until the last
#      child of cerbos/query-plan-adapters#349 merges, and turning it on is #360's job.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${DEMO_DIR}/.." && pwd)"

SEEDS="${DEMO_DIR}/seeds.json"
EXPECTED="${DEMO_DIR}/expected.json"
ACTIONS="${REPO_ROOT}/conformance/actions.json"

# Turning this on is cerbos/query-plan-adapters#360, once every adapter has an example.
REQUIRE_ALL_EXAMPLES="${DEMO_REQUIRE_ALL_EXAMPLES:-0}"

# The five usage shapes from cerbos/query-plan-adapters#349, in emission order.
SHAPES=(filtered alwaysAllowed alwaysDenied paginated composed)

failures=0
fail() { echo "  ✗ $*" >&2; failures=$((failures + 1)); }

for f in "${SEEDS}" "${EXPECTED}" "${ACTIONS}"; do
  [[ -f "${f}" ]] || { echo "missing ${f}" >&2; exit 1; }
  jq -e . "${f}" >/dev/null || { echo "${f} is not valid JSON" >&2; exit 1; }
done

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
echo "==> [1/4] structural: five usage shapes, well-formed entries"

declared="$(jq -r '.shapes | keys_unsorted | join(",")' "${EXPECTED}")"
expected_shapes="$(IFS=,; echo "${SHAPES[*]}")"
if [[ "${declared}" != "${expected_shapes}" ]]; then
  fail "expected.json declares shapes [${declared}], expected [${expected_shapes}]"
fi

for shape in "${SHAPES[@]}"; do
  if ! jq -e --arg s "${shape}" '.shapes[$s].description | type == "string" and length > 0' \
    "${EXPECTED}" >/dev/null; then
    fail "shape '${shape}' has no description"
  fi
  if ! jq -e --arg s "${shape}" '.shapes[$s].results | type == "object" and length > 0' \
    "${EXPECTED}" >/dev/null; then
    fail "shape '${shape}' has no results"
  fi
done

# Every entry, in every shape: a "<principal>/<action>" key, a real plan kind, and an id list that
# is sorted, duplicate-free and drawn from the seed rows. Sorted ids are what makes run-example.sh
# able to diff textually without a per-store ordering carve-out.
while IFS= read -r problem; do
  fail "${problem}"
done < <(jq -r --argjson seedIds "${SEED_IDS}" '
  ["KIND_CONDITIONAL", "KIND_ALWAYS_ALLOWED", "KIND_ALWAYS_DENIED"] as $kinds
  | .shapes
  | to_entries[]
  | .key as $shape
  | .value.results
  | to_entries[]
  | .key as $k | .value as $v
  | [
      (select($k | test("^[a-z0-9-]+/[a-z0-9-]+$") | not)
        | "\($shape)/\($k): key must be \"<principal>/<action>\""),
      (select($kinds | index($v.kind) | not)
        | "\($shape)/\($k): unknown plan kind \"\($v.kind // "")\""),
      (select($v.ids | type != "array")
        | "\($shape)/\($k): ids must be an array"),
      (select(($v.ids | type == "array") and ($v.ids != ($v.ids | sort)))
        | "\($shape)/\($k): ids must be sorted"),
      (select(($v.ids | type == "array") and (($v.ids | length) != ($v.ids | unique | length)))
        | "\($shape)/\($k): ids must be duplicate-free"),
      (select(($v.ids | type == "array") and (($v.ids - $seedIds) | length > 0))
        | "\($shape)/\($k): ids not in seeds.json: \(($v.ids - $seedIds) | join(","))")
    ][]
' "${EXPECTED}")

# Per-shape structure. alwaysAllowed/alwaysDenied exist to pin a plan KIND, so an entry carrying
# the wrong one would leave that kind untested while still looking covered.
while IFS= read -r problem; do
  fail "${problem}"
done < <(jq -r --argjson seedIds "${SEED_IDS}" '
  [
    (.shapes.filtered.results | to_entries[] | select(.value.kind != "KIND_CONDITIONAL")
      | "filtered/\(.key): must be KIND_CONDITIONAL, got \(.value.kind)"),

    (.shapes.alwaysAllowed.results | to_entries[] | select(.value.kind != "KIND_ALWAYS_ALLOWED")
      | "alwaysAllowed/\(.key): must be KIND_ALWAYS_ALLOWED, got \(.value.kind)"),
    (.shapes.alwaysAllowed.results | to_entries[] | select(.value.ids != ($seedIds | sort))
      | "alwaysAllowed/\(.key): an unconditional plan must return every seed row"),

    (.shapes.alwaysDenied.results | to_entries[] | select(.value.kind != "KIND_ALWAYS_DENIED")
      | "alwaysDenied/\(.key): must be KIND_ALWAYS_DENIED, got \(.value.kind)"),
    (.shapes.alwaysDenied.results | to_entries[] | select(.value.ids != [])
      | "alwaysDenied/\(.key): an unconditional denial must return no rows"),

    (.shapes.paginated.results | to_entries[]
      | .key as $k | .value as $v
      | [
          (select((($v.pageSize | type) != "number") or $v.pageSize < 1)
            | "paginated/\($k): pageSize must be a positive number"),
          (select(($v.pageSizes | type) != "array" or ($v.pageSizes | length) < 2)
            | "paginated/\($k): pageSizes must span more than one page"),
          (select(($v.pageSizes | type) == "array" and ($v.pageSizes | any(. < 1)))
            | "paginated/\($k): an empty page is not a page"),
          (select(($v.pageSizes | type) == "array"
                  and ($v.pageSizes | add // 0) != ($v.ids | length))
            | "paginated/\($k): page sizes sum to \($v.pageSizes | add // 0), ids has \($v.ids | length)"),
          (select(($v.pageSizes | type) == "array"
                  and ($v.pageSizes[:-1] | any(. != $v.pageSize)))
            | "paginated/\($k): every page but the last must be full (\($v.pageSize))"),
          (select(($v.pageSizes | type) == "array" and ($v.pageSizes[-1:] | any(. > $v.pageSize)))
            | "paginated/\($k): the last page cannot exceed pageSize")
        ][])
  ][]
' "${EXPECTED}")

# ---------------------------------------------------------------------------------------------
# 2. Non-degeneracy
# ---------------------------------------------------------------------------------------------
echo "==> [2/4] non-degeneracy: the expectations still discriminate"

# The analogue of the conformance degeneracy guard. Without it these lists can rot to all-empty,
# or to every-seed-row, and still pass every diff.
proper_subsets="$(jq -r --argjson seedIds "${SEED_IDS}" '
  [ .shapes.filtered.results, .shapes.composed.results
    | to_entries[]
    | select((.value.ids | length) > 0 and (.value.ids | length) < ($seedIds | length))
  ] | length
' "${EXPECTED}")"
if (( proper_subsets < 2 )); then
  fail "filtered and composed must each contain at least one proper, non-empty subset of the seed rows"
fi

# Shape 5 is only worth running if it differs from BOTH filters it composes. If it equals the
# adapter's filter, the example could drop the application predicate and still pass; if it equals
# the application predicate's own result, the example could drop the ADAPTER and still pass — and
# that second one is an authorization hole that reads as a green build.
while IFS= read -r problem; do
  fail "${problem}"
done < <(jq -r --argjson appIds "${APP_IDS}" --argjson seedIds "${SEED_IDS}" '
  .shapes.filtered.results as $filtered
  | .shapes.alwaysAllowed.results as $allowed
  | .shapes.composed.results
  | to_entries[]
  | .key as $k | .value as $v
  | (if $v.kind == "KIND_ALWAYS_ALLOWED" then ($seedIds | sort)
     elif $v.kind == "KIND_ALWAYS_DENIED" then []
     else ($filtered[$k].ids // null) end) as $unfiltered
  | [
      (select($unfiltered == null)
        | "composed/\($k): KIND_CONDITIONAL needs the same key under `filtered` to compose from"),

      # The composed answer must BE the intersection. This recomputes shape 5 from shape 1 and
      # seeds.json, so the two hardcoded lists cannot rot independently of each other.
      (select($unfiltered != null and ($v.ids != ($unfiltered - ($unfiltered - $appIds) | sort)))
        | "composed/\($k): expected \(($unfiltered - ($unfiltered - $appIds) | sort) | join(",")), got \($v.ids | join(","))"),

      (select($v.kind == "KIND_CONDITIONAL" and $v.ids == $unfiltered)
        | "composed/\($k): equals the adapter filter alone — the application predicate is a no-op here"),
      (select($v.kind == "KIND_CONDITIONAL" and $v.ids == ($appIds | sort))
        | "composed/\($k): equals the application predicate alone — an example that never called the adapter would pass")
    ][]
' "${EXPECTED}")

# A paginated entry pages over an answer another shape already pins, so the two must agree.
while IFS= read -r problem; do
  fail "${problem}"
done < <(jq -r '
  (.shapes.filtered.results + .shapes.alwaysAllowed.results + .shapes.alwaysDenied.results) as $unpaged
  | .shapes.paginated.results
  | to_entries[]
  | .key as $k | .value as $v
  | [
      (select($unpaged[$k] == null)
        | "paginated/\($k): no unpaginated shape pins this principal/action"),
      (select($unpaged[$k] != null and $unpaged[$k].ids != $v.ids)
        | "paginated/\($k): pages union to \($v.ids | join(",")), unpaginated is \($unpaged[$k].ids | join(","))"),
      (select($unpaged[$k] != null and $unpaged[$k].kind != $v.kind)
        | "paginated/\($k): plan kind \($v.kind) disagrees with \($unpaged[$k].kind)")
    ][]
' "${EXPECTED}")

# Every principal and action named in expected.json has to exist. A typo in an action name makes
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
    fail "expected.json refers to principal '${principal}', which is not in seeds.json"
  fi

  granted=0
  grep -qx "${action}" <<<"${policy_actions}" && granted=1

  if [[ "${shape}" == "alwaysDenied" ]]; then
    (( granted == 0 )) || \
      fail "alwaysDenied/${ref}: '${action}' IS granted by demo/policies/document.yaml, so the planner will not return KIND_ALWAYS_DENIED"
  elif jq -e --arg s "${shape}" --arg k "${ref}" \
    '.shapes[$s].results[$k].kind != "KIND_ALWAYS_DENIED"' "${EXPECTED}" >/dev/null; then
    (( granted == 1 )) || \
      fail "${shape}/${ref}: no rule in demo/policies/document.yaml grants '${action}'"
  fi
done < <(jq -r '.shapes | to_entries[] | .key as $s | .value.results | keys[] | "\($s) \(.)"' \
  "${EXPECTED}")

# ---------------------------------------------------------------------------------------------
# 3. PDP pin reuse
# ---------------------------------------------------------------------------------------------
echo "==> [3/4] PDP pin: one pin in the repository, reused, and reached at \$CERBOS_HOST"

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

# Every adapter that HAS an example must reach that PDP: either through the shared compose file
# above, or through a compose file of its own that names the same tag AND digest.
# (conformance/scripts/validate-corpus.sh scans the whole repository for the same agreement; this
# is restated here because the example job runs validate-demo.sh on its own.)
#
# Same file set as validate-corpus.sh, deliberately: things that RUN, never prose, and never
# installed or built output. An example's node_modules holds a copy of the adapter's own README,
# and two scans disagreeing about whether that counts would make one of them wrong.
SOURCE_INCLUDES=(
  --include='*.yml' --include='*.yaml' --include='*.sh' --include='*.py' --include='*.go'
  --include='*.java' --include='*.kts' --include='*.ts' --include='*.js' --include='*.json'
  --include='Dockerfile' --include='*_IMAGE'
)
SOURCE_EXCLUDES=(
  --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=lib --exclude-dir=build
  --exclude-dir=.venv --exclude-dir=.gradle --exclude-dir=bin --exclude-dir=dist
  --exclude-dir=generated --exclude-dir=__pypackages__
)
for adapter in $(jq -r '.adapters[]' "${ACTIONS}"); do
  example_dir="${REPO_ROOT}/${adapter}/example"
  [[ -d "${example_dir}" ]] || continue
  while IFS= read -r ref; do
    # An interpolation (`cerbos:${CERBOS_VERSION}`) reads the pinned files at runtime and cannot
    # drift, so it is already correct by construction.
    case "${ref}" in *'ghcr.io/cerbos/cerbos:$'*|*'ghcr.io/cerbos/cerbos:{'*) continue ;; esac
    [[ "${ref}" == "${pinned_image}" ]] || \
      fail "${adapter}/example pins Cerbos as '${ref}', expected '${pinned_image}'"
  done < <(grep -rhoE 'ghcr\.io/cerbos/cerbos:[^@"'"'"' )]*(@sha256:[0-9a-f]{64})?' \
    "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" "${example_dir}" 2>/dev/null || true)

  # ...and must reach it at the address the runner sets, never one of its own. Both examples that
  # existed when this check was written had shipped `?? "localhost:3593"`, which is not a harmless
  # default: 3592/3593 are the ports every adapter's `cerbos run` test sidecar binds, and it is
  # why demo/docker-compose.yml publishes the demo PDP on 13592/13593 instead. An unset
  # CERBOS_HOST therefore did not fail — the example planned against a sidecar loaded with
  # `policies/` rather than `demo/policies/`, and the mismatch against expected.json read as an
  # adapter bug. The rule was already in demo/README.md's "What an example must do" and both
  # examples broke it anyway, which is what makes it a check rather than prose.
  #
  # A CLIENT address specifically. `demo/docker-compose.yml` maps "13592:3592", and the
  # container-side half of a port mapping is the PDP's own listen port, which is correct.
  while IFS= read -r addr; do
    fail "${adapter}/example hardcodes the PDP address '${addr}' — read \$CERBOS_HOST instead (demo/README.md)"
  done < <(grep -rhoE '(localhost|127\.0\.0\.1):359[23]' \
    "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" "${example_dir}" 2>/dev/null || true)
done

# ---------------------------------------------------------------------------------------------
# 4. Every adapter has an example (DISABLED — see the header)
# ---------------------------------------------------------------------------------------------
missing=()
have=()
for adapter in $(jq -r '.adapters[]' "${ACTIONS}"); do
  if [[ -x "${REPO_ROOT}/${adapter}/example/run.sh" ]]; then
    have+=("${adapter}")
  else
    missing+=("${adapter}")
  fi
done

if [[ "${REQUIRE_ALL_EXAMPLES}" == "1" ]]; then
  echo "==> [4/4] example coverage: enforced"
  if (( ${#missing[@]} > 0 )); then
    fail "no runnable example/run.sh for: ${missing[*]}"
  fi
else
  echo "==> [4/4] example coverage: ${#have[@]}/$(( ${#have[@]} + ${#missing[@]} )) adapters" \
    "(check disabled until cerbos/query-plan-adapters#360; still missing: ${missing[*]:-none})"
fi

if (( failures > 0 )); then
  echo >&2
  echo "demo domain validation failed with ${failures} problem(s)" >&2
  exit 1
fi

echo "==> demo domain OK"
