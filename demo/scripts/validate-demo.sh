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
#   5. Principal provenance: an example reads its principal out of seeds.json rather than
#      restating one inline.

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
echo "==> [1/5] structural: five usage shapes, well-formed entries"

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
echo "==> [2/5] non-degeneracy: the expectations still discriminate"

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
echo "==> [3/5] PDP pin: one pin in the repository, reused, and reached at \$CERBOS_HOST"

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
  # CERBOS_HOST therefore did not fail — the example planned against whichever sidecar held those
  # ports rather than the one loaded with `demo/policies/`, and the mismatch against expected.json
  # read as an adapter bug. The rule was already in demo/README.md's "What an example must do" and both
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
  echo "==> [4/5] example coverage: enforced"
  if (( ${#missing[@]} > 0 )); then
    fail "no runnable example/run.sh for: ${missing[*]}"
  fi
else
  echo "==> [4/5] example coverage: ${#have[@]}/$(( ${#have[@]} + ${#missing[@]} )) adapters" \
    "(check disabled until cerbos/query-plan-adapters#360; still missing: ${missing[*]:-none})"
fi

# ---------------------------------------------------------------------------------------------
# 5. Principal provenance
# ---------------------------------------------------------------------------------------------
echo "==> [5/5] principal provenance: examples read their principal from seeds.json"

# Every example plans for the principals seeds.json declares, and is supposed to look them up
# there rather than write one out. Nothing said so.
#
# Unlike the hardcoded PDP address check 3 catches, a restated principal does NOT fail quietly: it
# matches what the corpus carries until someone edits seeds.json, and then that example's frozen
# id lists stop matching and it fails loudly — as an adapter bug rather than as the misinvocation
# it is. So this is a latency problem, not a correctness hole, and it earns a check only because
# six more examples are queued (cerbos/query-plan-adapters#349): a rule constraining examples
# being written is worth more than one added after they exist.
#
# What makes it checkable is that a principal is an id AND its roles, and only the roles are
# unobtainable any other way. An example naming `alice` is fine and unavoidable — it is the
# lookup key, the expected.json entry key, and the word a printed line uses. An example naming
# `alice` NEXT TO `user` has restated the corpus record, because roles are what the policy's
# rules are keyed on and the corpus is the only place they come from.
#
# A role that is ALSO a principal id cannot carry that signal, and `admin` is both here. Every
# example emits `"admin/admin-view": filtered("admin", "admin-view")` two lines from an `alice`,
# so pairing on it would fail all four; suppressing that pair is not tuning around a false
# positive, it is that the two readings are genuinely indistinguishable from the literals alone.
# Note what is and is not dropped: `admin` leaves the ROLE set only. It is still a principal id,
# so `{ id: "admin", roles: ["user"] }` pairs like any other. The consequence is stated rather
# than hidden: a restatement of the `admin` principal ALONE, with only its `admin` role, is not
# caught. Restating the other two is, and an example that looks two principals up in the corpus
# and writes the third out by hand is not the mistake this exists to stop.
PRINCIPAL_IDS="$(jq -r '[.principals[].id] | unique | join(",")' "${SEEDS}")"
PRINCIPAL_ROLES="$(jq -r '([.principals[].roles[]] - [.principals[].id]) | unique | join(",")' \
  "${SEEDS}")"

# Both derived from seeds.json rather than spelled here, for the same reason check 2 derives the
# action names from the policy: renaming a principal or a role in the corpus must not leave a
# stale literal in this script silently guarding nothing.
#
# And the corpus can empty that second list — give every principal a role named after a principal
# and there is nothing left to pair, so the scan below would pass every example vacuously. That is
# a corpus problem with a corpus fix (one principal holding one role no principal is named after),
# which is why it fails here rather than being worked around in the scan.
if [[ -z "${PRINCIPAL_IDS}" || -z "${PRINCIPAL_ROLES}" ]]; then
  fail "seeds.json must declare a principal, and a role no principal is NAMED after —" \
    "otherwise every role reads as an id and the scan below guards nothing"
fi

# Extracts every string literal outside comments, and reports two things per example:
#
#   INLINE  a window of consecutive lines carrying a principal id and a role as two distinct
#           literals — the signature of `{ id: "alice", roles: ["user"] }` in any of the five
#           languages, however it is wrapped.
#   CORPUS  whether the example names seeds.json and its principals array in code at all.
#
# Comments are skipped rather than scanned, because an id in a comment or a printed line of
# output is a legitimate mention: spring-data's photo domain documents `?user=alice&role=user` in
# a Javadoc block and must keep passing unmodified. Only string LITERALS count, and they are
# matched WHOLE, so that same domain's `new Album("a1", "acme", "alice", …)` rows and
# `@RequestParam(defaultValue = "user")` defaults stay clear of each other, and a printed
# `"planning for alice as user"` is one literal matching neither. It is the pairing that means
# something, not either half.
#
# The one shape that reads as a restatement without being one is a diagnostic passing the id and
# the role as SEPARATE literals — `console.error("principal", "alice", "role", "user")`. Nothing
# in the literals distinguishes that from building a principal out of them; put both words in the
# message, or interpolate the id, and it is a whole literal matching neither.
#
# And the window cuts the other way too: an id and a role bound to separate variables far enough
# apart — `const id = "bob"` with `const roles = ["user"]` six lines later — is a restatement this
# does not see. Widening the window is not the fix, because the shapes block every example emits
# puts `alice` and `admin` within a few lines of each other and a wide enough window starts
# reporting those. This catches a principal WRITTEN OUT, which is the mistake a new example makes
# by copying a literal; it is not a proof that one was not assembled piecewise.
#
# One lexer nuance in the same spirit: `#` ends a line, which is right for shell, YAML and Python
# and wrong for a shell parameter expansion like `${ref##*/}`, where it drops the rest of that
# line. It costs a finding rather than inventing one, so it stays the simple rule.
read -r -d '' AWK_PRINCIPALS <<'AWK' || true
BEGIN {
  n = split(IDS, a, ",");   for (i = 1; i <= n; i++) isId[a[i]] = 1
  n = split(ROLES, a, ","); for (i = 1; i <= n; i++) isRole[a[i]] = 1
}

# Comment state is per file; so is the window, which must not straddle two files.
FNR == 1 { inBlock = 0; inDoc = 0; head = 1; tail = 0; split("", lit); split("", ln) }

{
  code = ""
  i = 1
  len = length($0)
  while (i <= len) {
    if (inBlock) {                                  # inside /* ... */
      p = index(substr($0, i), "*/")
      if (p == 0) break
      i += p + 1; inBlock = 0; continue
    }
    if (inDoc) {                                    # inside a Python docstring
      p = index(substr($0, i), docDelim)
      if (p == 0) break
      i += p + 2; inDoc = 0; continue
    }
    c = substr($0, i, 1)
    if (substr($0, i, 3) == "\"\"\"" || substr($0, i, 3) == "'''") {
      inDoc = 1; docDelim = substr($0, i, 3); i += 3; continue
    }
    if (substr($0, i, 2) == "/*") { inBlock = 1; i += 2; continue }
    if (substr($0, i, 2) == "//" || c == "#") break  # to end of line
    # A template literal is one literal, not a window onto the quotes inside it: `${a} 'b'` must
    # not yield `b`, and an apostrophe in interpolated prose must not swallow the rest of the line.
    if (c == "\"" || c == "'" || c == "`") {
      q = c; i++; buf = ""
      while (i <= len) {
        ch = substr($0, i, 1)
        if (ch == "\\") { i += 2; continue }
        if (ch == q) { i++; break }
        buf = buf ch; i++
      }
      tail++; lit[tail] = buf; ln[tail] = FNR
      code = code " " buf " "                       # a path or key may sit inside the quotes
      continue
    }
    code = code c
    i++
  }

  if (code ~ /seeds\.json/) sawSeeds = 1
  if (code ~ /principals/)  sawPrincipals = 1

  while (head <= tail && ln[head] < FNR - WINDOW + 1) { delete lit[head]; delete ln[head]; head++ }

  # Deduplicated on the MESSAGE, not on the pair of literals that produced it. Every window
  # holding both halves re-examines the same pair, and one line can hold a literal several times
  # over — `{ id: "admin", roles: ["admin"], fallback: "admin" }` is three pairs saying one thing.
  # Keying on the indices deduplicates the wrong one and reports the same sentence three times,
  # which inflates the failure count the summary line prints.
  for (x = head; x <= tail; x++) {
    if (!isId[lit[x]]) continue
    for (y = head; y <= tail; y++) {
      if (x == y || !isRole[lit[y]]) continue
      message = sprintf("INLINE %s:%d: id \"%s\" alongside role \"%s\" (line %d)",
        FILENAME, ln[x], lit[x], lit[y], ln[y])
      if (message in seen) continue
      seen[message] = 1
      print message
    }
  }
}

END {
  if (!sawSeeds)      print "CORPUS demo/seeds.json"
  if (!sawPrincipals) print "CORPUS its principals array"
}
AWK

for adapter in $(jq -r '.adapters[]' "${ACTIONS}"); do
  example_dir="${REPO_ROOT}/${adapter}/example"
  [[ -d "${example_dir}" ]] || continue

  # Same file set as check 3 and as validate-corpus.sh: things that RUN, never prose, and never
  # installed or built output. Minus the Cerbos policies, which that set includes as *.yaml.
  #
  # A policy is the one file where writing a role out is the POINT — it is what the PDP keys its
  # rules on, and an example is free to carry policies of its own (spring-data's photo domain
  # does). Two rules four lines apart, one granting `user` and one granting `admin`, pair exactly
  # like a restated principal and would be reported as "look it up in demo/seeds.json", which is
  # meaningless advice for a policy. spring-data's happen to sit ten lines apart, so this is a
  # trap the next example walks into rather than a failure today.
  #
  # Identified by the apiVersion every Cerbos policy declares, not by living under policies/: a
  # path rule is a carve-out the next adapter has to know about, and ADR 0001 is about not having
  # those. A file that says what it is gets taken at its word.
  sources=()
  while IFS= read -r file; do
    grep -qE '^[[:space:]]*apiVersion:[[:space:]]*"?api\.cerbos\.dev/' "${file}" && continue
    sources+=("${file}")
  done < <(grep -rlE '.' "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" \
    "${example_dir}" 2>/dev/null || true)
  if (( ${#sources[@]} == 0 )); then
    fail "${adapter}/example has no scannable source files — there is nothing here to scan"
    continue
  fi

  # A four-line window rather than a single line: prettier and google-java-format both break a
  # principal literal across its `id:` and `roles:` lines, and a per-line scan would see one half
  # of it at a time and report neither.
  while IFS= read -r finding; do
    case "${finding}" in
      INLINE\ *)
        location="${finding#INLINE }"
        fail "${adapter}/example restates a principal at ${location#"${REPO_ROOT}/"} — look it" \
          "up in demo/seeds.json instead (demo/README.md)"
        ;;
      CORPUS\ *)
        fail "${adapter}/example never reads ${finding#CORPUS } — its principal must come from" \
          "demo/seeds.json, not be restated (demo/README.md)"
        ;;
    esac
  done < <(awk -v IDS="${PRINCIPAL_IDS}" -v ROLES="${PRINCIPAL_ROLES}" -v WINDOW=4 \
    "${AWK_PRINCIPALS}" "${sources[@]}")
done

if (( failures > 0 )); then
  echo >&2
  echo "demo domain validation failed with ${failures} problem(s)" >&2
  exit 1
fi

echo "==> demo domain OK"
