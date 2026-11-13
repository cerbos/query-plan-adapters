#!/usr/bin/env bash
# Offline integrity checks for the conformance corpus: no Docker, no PDP, no toolchain beyond jq.
# Runs in every adapter's CI. The generator (`go -C conformance/generator run . -check`) owns
# everything it writes — the policy, resources.json, the goldens and their degeneracy rules — so
# this checks only what sits outside it:
#
#   1. each <adapter>/conformance-ledger.json against conformance/README.md, "The ledger", its lacks
#      mapping only store capabilities some seed in seeds.json requires to a reason and its stores;
#   2. the PDP pin: pdp-versions.json is well-formed, and every restatement of it agrees;
#   3. the ent and pgx vendored translator trees are byte-identical;
#   4. the dataset's to-one relation resolves;
#   5. each adapter README's "Conformance contract" table matches its ledger on the current PDP.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${CONFORMANCE_DIR}/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT INT TERM
cd "${CONFORMANCE_DIR}"

failures=0
fail() { echo "  ✗ $*" >&2; failures=$((failures + 1)); }

# ---- 2 (first, because 1 needs the tags). The PDP pin. -----------------------------------------
if ! jq -e '
  def pin: type == "object" and (keys == ["digest", "tag"])
    and (.tag | test("^[0-9]+\\.[0-9]+\\.[0-9]+$")) and (.digest | test("^sha256:[0-9a-f]{64}$"));
  keys == ["current", "previous"] and (.current | pin) and (.previous | pin)
    and .current.tag != .previous.tag
' pdp-versions.json >/dev/null; then
  echo "pdp-versions.json must hold exactly current and previous, each {tag: X.Y.Z, digest: sha256:<64 hex>}, with two different tags" >&2
  exit 1
fi
current_tag="$(jq -r .current.tag pdp-versions.json)"
current_digest="$(jq -r .current.digest pdp-versions.json)"
pdp_tags="$(jq -c '[.current.tag, .previous.tag]' pdp-versions.json)"
# The store capabilities some seed requires: the only names a ledger's lacks may give.
capabilities="$(jq -c '[.seeds[].requires // empty | .[]] | unique' seeds.json)"
# The stores a lacks entry may scope itself to: conformance/README.md, "The ledger".
stores='["chroma","convex","elasticsearch","h2","mongo","mysql","postgres","sqlite","sqlserver"]'
[[ -d "golden/${current_tag}" ]] || { echo "golden/${current_tag}/ is missing: run the generator" >&2; exit 1; }
find "golden/${current_tag}" -name '*.json' | sed "s|^golden/${current_tag}/||; s|\.json$||" \
  | sort >"${TMP}/case-ids"

# ---- 1. Ledgers. The roster is every directory holding one. ----------------------------------
roster=()
while IFS= read -r ledger; do
  adapter="$(basename "$(dirname "${ledger}")")"
  roster+=("${adapter}")
  if ! jq -e . "${ledger}" >/dev/null 2>&1; then
    fail "${adapter}/conformance-ledger.json is not valid JSON"
    continue
  fi
  while IFS= read -r problem; do
    fail "${adapter}/conformance-ledger.json: ${problem}"
  done < <(jq -r --arg adapter "${adapter}" --argjson tags "${pdp_tags}" --argjson capabilities "${capabilities}" \
    --argjson stores "${stores}" '
    def text: type == "string" and length > 0;
    if (keys - ["lacks"]) != ["adapter", "cases"] then "top-level keys must be adapter, cases and an optional lacks"
    elif .adapter != $adapter then "adapter is \(.adapter | tojson), expected \($adapter | tojson)"
    elif (.cases | type) != "object" then "cases must be an object"
    elif has("lacks") and ((.lacks | type) != "object" or (.lacks | length) == 0
        or ((.lacks | keys) - $capabilities) != [])
      then "lacks must map capabilities some seed requires (\($capabilities)) to an entry"
    else
      (.lacks // {} | to_entries[] | .key as $cap | .value
        | if type != "object" or (keys - ["reason", "stores"]) != [] then "lacks.\($cap): entry must be an object of reason and an optional stores"
          elif (.reason | text | not) then "lacks.\($cap): reason must be a non-empty string"
          elif has("stores") and ((.stores | type) != "array" or (.stores | length) == 0
              or ((.stores - $stores) != []) or (.stores | length) != (.stores | unique | length))
            then "lacks.\($cap): stores must be a non-empty list of distinct stores from \($stores)"
          else empty end),
      (if ([.cases | keys_unsorted[]] != [.cases | keys[]]) then "case ids must be sorted" else empty end),
      (.cases | to_entries[] | .key as $id | .value
        | if type != "object" then "\($id): entry must be an object"
          elif (keys - ["status", "reason", "issue", "pdp"]) != [] then "\($id): unknown keys \(keys - ["status", "reason", "issue", "pdp"])"
          elif (.status | IN("unsupported", "divergent") | not) then "\($id): status must be unsupported or divergent"
          elif (.reason | text | not) then "\($id): reason must be a non-empty string"
          elif .status == "divergent" and (.issue | text | not) then "\($id): a divergent entry needs an issue"
          elif has("pdp") and ((.pdp | type) != "array" or (.pdp | length) == 0 or ((.pdp - $tags) != []))
            then "\($id): pdp must be a non-empty subset of \($tags)"
          else empty end)
    end
  ' "${ledger}")
  jq -r '.cases | keys[]' "${ledger}" 2>/dev/null | sort >"${TMP}/ledger-ids" || true
  while IFS= read -r stale; do
    fail "${adapter}/conformance-ledger.json names ${stale}, which has no golden under golden/${current_tag}/"
  done < <(comm -23 "${TMP}/ledger-ids" "${TMP}/case-ids")
done < <(find "${REPO_ROOT}" -mindepth 2 -maxdepth 2 -name conformance-ledger.json | sort)
[[ "${#roster[@]}" -gt 0 ]] || fail "no <adapter>/conformance-ledger.json found: the roster is empty"

# ---- 2 (cont.). Restatements of the current pin. ---------------------------------------------
# Files that cannot read pdp-versions.json (Compose files, go.mod) restate it. Tag and digest are
# checked together: a right tag with another build's digest reads as pinned and is not (#322).
# Markdown is excluded: a README telling consumers how to run their own PDP is not a test input.
SOURCE_INCLUDES=(
  --include='*.yml' --include='*.yaml' --include='*.sh' --include='*.py' --include='*.go'
  --include='*.java' --include='*.kt' --include='*.kts' --include='*.ts' --include='*.js'
  --include='*.json' --include='*.cs' --include='*.csproj' --include='Dockerfile'
)
SOURCE_EXCLUDES=(
  --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=.claude --exclude-dir=lib
  --exclude-dir=build --exclude-dir=.venv --exclude-dir=.gradle --exclude-dir=bin
  --exclude-dir=__pypackages__ --exclude-dir=.agents --exclude-dir=.out-of-scope
  --exclude-dir=dist --exclude-dir=.gems --exclude-dir=.bundle-path --exclude-dir=golden
  --exclude-dir=obj --exclude-dir=packages
)
# `lib/` is build output on the TypeScript adapters but source on the Ruby gem, so Ruby gets its
# own pass without that exclusion.
RUBY_INCLUDES=(--include='*.rb' --include='*.gemspec' --include='Gemfile' --include='Rakefile')
RUBY_EXCLUDES=()
for exclusion in "${SOURCE_EXCLUDES[@]}"; do
  [[ "${exclusion}" == "--exclude-dir=lib" ]] || RUBY_EXCLUDES+=("${exclusion}")
done
source_grep() {
  grep "$@" "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" || true
  grep "$@" "${RUBY_INCLUDES[@]}" "${RUBY_EXCLUDES[@]}" || true
}
source_grep -rl '' "${REPO_ROOT}" 2>/dev/null | grep -q '/lib/.*\.rb$' \
  || fail "the source scan reaches no .rb file under lib/: restore the Ruby scan pass"
# `*.kts` matches build.gradle.kts and nothing under src/, so without `*.kt` a Kotlin adapter's
# source is invisible to every scan below.
source_grep -rl '' "${REPO_ROOT}" 2>/dev/null | grep -q '\.kt$' \
  || fail "the source scan reaches no .kt file: restore --include='*.kt'"

expected_image="ghcr.io/cerbos/cerbos:${current_tag}@${current_digest}"
while IFS=: read -r file _ match; do
  reference="$(printf '%s' "${match}" | grep -oE 'ghcr\.io/cerbos/cerbos:[^")'\''[:space:]]*' | head -1)"
  case "${reference}" in *':$'*|*':%'*|*':{'*) continue ;; esac  # read at runtime
  [[ "${reference}" == "${expected_image}" ]] \
    || fail "${file#"${REPO_ROOT}"/} pins '${reference}', expected ${expected_image} (pdp-versions.json .current)"
done < <(source_grep -rn 'ghcr\.io/cerbos/cerbos:' "${REPO_ROOT}")

# The Go modules decode plans with cerbos/api/genpb, which must match the current PDP.
for gomod in "${REPO_ROOT}"/ent/go.mod "${REPO_ROOT}"/pgx/go.mod; do
  genpb="$(sed -n 's|.*github\.com/cerbos/cerbos/api/genpb v\([^[:space:]]*\).*|\1|p' "${gomod}")"
  [[ -z "${genpb}" || "${genpb}" == "${current_tag}" ]] \
    || fail "${gomod#"${REPO_ROOT}"/} pins cerbos/api/genpb v${genpb}, expected v${current_tag}"
done

# ---- 3. The vendored Go translator. ----------------------------------------------------------
# ent and pgx each vendor it so a consumer pulls in one module. The copies must stay identical,
# or a fix can land in one alone (#319). Per-module code belongs in that module's render.go.
VENDORED="internal/queryplan"
if [[ ! -d "${REPO_ROOT}/ent/${VENDORED}" || ! -d "${REPO_ROOT}/pgx/${VENDORED}" ]]; then
  fail "ent/${VENDORED} or pgx/${VENDORED} is missing: the sync check would guard nothing"
elif ! diff -ru --label "ent/${VENDORED}" --label "pgx/${VENDORED}" \
  "${REPO_ROOT}/ent/${VENDORED}" "${REPO_ROOT}/pgx/${VENDORED}"; then
  fail "the vendored translator trees have drifted: apply the change to both copies"
fi

# ---- 4. The dataset's to-one relation. -------------------------------------------------------
# A dangling parentSeedId reads as "no parent" on both sides, so it would pass for the wrong reason.
while IFS= read -r problem; do
  fail "seeds.json: ${problem}"
done < <(jq -r '
  (.seeds | map(.id)) as $ids
  | .seeds[] | .parentSeedId as $parent
  | if $parent == .id then "\(.id): parentSeedId names its own row"
    elif $parent != null and ($ids | index($parent)) == null then "\(.id): parentSeedId \($parent | tojson) is not a seed id"
    else empty end
' seeds.json)

# ---- 5. README contract tables. ---------------------------------------------------------------
# Each adapter's README carries `| <tier> | <passed> / <total> |` for the current PDP. The numbers
# follow from the goldens and the ledger: total is every case in the tier, passed drops the cases
# the ledger names for this tag and the planner divergences every harness skips. A table left
# behind by a corpus or ledger change is caught here instead of by a reviewer's arithmetic.
find "golden/${current_tag}" -name '*.json' -exec cat {} + \
  | jq -s --arg tag "${current_tag}" \
    'map({id, tier, skipped: ((.plannerDivergence.pdp // []) | index($tag) != null)})' \
    >"${TMP}/tiers.json"
for adapter in "${roster[@]}"; do
  readme="${REPO_ROOT}/${adapter}/README.md"
  rows="$(grep -E '^\| (core|extended|adversarial) \| [0-9]+ / [0-9]+ \|' "${readme}" 2>/dev/null || true)"
  if [[ "$(printf '%s' "${rows}" | grep -c '^|')" -ne 3 ]]; then
    fail "${adapter}/README.md needs exactly one '| <tier> | <passed> / <total> |' row per tier (core, extended, adversarial)"
    continue
  fi
  while IFS=$'\t' read -r tier want; do
    have="$(printf '%s\n' "${rows}" | sed -n "s/^| ${tier} | \([0-9]* \/ [0-9]*\) |.*/\1/p")"
    [[ "${have}" == "${want}" ]] \
      || fail "${adapter}/README.md: ${tier} reads '${have}', the goldens and ledger give '${want}' on ${current_tag}"
  done < <(jq -r --arg tag "${current_tag}" --slurpfile tiers "${TMP}/tiers.json" '
    (.cases | with_entries(select(.value.pdp == null or (.value.pdp | index($tag) != null))) | keys) as $ledgered
    | $tiers[0] | group_by(.tier)[]
    | "\(.[0].tier)\t\(map(select((.skipped | not) and (.id as $id | $ledgered | index($id) == null))) | length) / \(length)"
  ' "${REPO_ROOT}/${adapter}/conformance-ledger.json")
done

if [[ "${failures}" -gt 0 ]]; then
  echo "Corpus invalid: ${failures} problem(s)" >&2
  exit 1
fi
echo "Corpus valid: $(wc -l <"${TMP}/case-ids" | tr -d '[:space:]') cases on PDP ${current_tag}; ledgers: ${roster[*]}"
