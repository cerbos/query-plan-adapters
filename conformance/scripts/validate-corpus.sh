#!/usr/bin/env bash
# Offline integrity checks for the conformance corpus: no Docker, no PDP, no toolchain beyond jq.
# Runs in every adapter's CI. The generator (`go -C conformance/generator run . -check`) owns
# everything it writes — the policy, resources.json, the goldens and their degeneracy rules — so
# this checks only what sits outside it:
#
#   1. each <adapter>/conformance-ledger.json against conformance/README.md, "The ledger";
#   2. the PDP pin: pdp-versions.json is well-formed, and every restatement of it agrees;
#   3. service image pinning (repo:tag@sha256, one digest per tag);
#   4. the ent and pgx vendored translator trees are byte-identical;
#   5. the dataset's to-one relation resolves.
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
  done < <(jq -r --arg adapter "${adapter}" --argjson tags "${pdp_tags}" '
    def text: type == "string" and length > 0;
    if keys != ["adapter", "cases"] then "top-level keys must be exactly adapter and cases"
    elif .adapter != $adapter then "adapter is \(.adapter | tojson), expected \($adapter | tojson)"
    elif (.cases | type) != "object" then "cases must be an object"
    else
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
  --include='*.json' --include='Dockerfile' --include='*_IMAGE'
)
SOURCE_EXCLUDES=(
  --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=.claude --exclude-dir=lib
  --exclude-dir=build --exclude-dir=.venv --exclude-dir=.gradle --exclude-dir=bin
  --exclude-dir=__pypackages__ --exclude-dir=.agents --exclude-dir=.out-of-scope
  --exclude-dir=dist --exclude-dir=.gems --exclude-dir=.bundle-path --exclude-dir=golden
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

# ---- 3. Service images. ----------------------------------------------------------------------
# Pinned per harness, not centrally: a shared file under conformance/ would re-run every adapter's
# CI on each bump. The rule is shared instead. Add a repository here when you add a service.
IMAGE_REPOSITORIES=(
  "postgres" "mysql" "mongo" "chromadb/chroma"
  "docker.elastic.co/elasticsearch/elasticsearch" "ghcr.io/get-convex/convex-backend"
)
: >"${TMP}/image-refs"
for repository in "${IMAGE_REPOSITORIES[@]}"; do
  escaped="${repository//./\\.}"
  matched=0
  # A leading character class rather than \b keeps URLs such as `jdbc:mysql://…` out.
  while IFS=: read -r file _ match; do
    matched=$((matched + 1))
    reference="${match}"
    [[ "${reference}" == "${repository}"* ]] || reference="${reference:1}"
    relative="${file#"${REPO_ROOT}"/}"
    if [[ ! "${reference}" =~ ^${escaped}:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}$ ]]; then
      fail "${relative} references '${reference}': pin service images as repo:tag@sha256:<64 hex>"
      continue
    fi
    printf '%s\t%s\t%s\n' "${reference%@*}" "${reference#*@}" "${relative}" >>"${TMP}/image-refs"
  done < <(source_grep -rnoIE "(^|[^A-Za-z0-9._/:-])${escaped}:[A-Za-z0-9._-]+(@sha256:[0-9a-fA-F]*)?" \
    "${REPO_ROOT}")
  # A repository nothing references is a guard watching nothing.
  [[ "${matched}" -gt 0 ]] || fail "no reference to image repository '${repository}': remove the entry or fix the scan"
done
while IFS= read -r conflict; do
  fail "one image tag, two digests: ${conflict}"
done < <(sort -u "${TMP}/image-refs" | awk -F'\t' '
  { if (!($1 in seen)) { seen[$1] = $2; where[$1] = $3 }
    else if (seen[$1] != $2) { print $1 ": " seen[$1] " (" where[$1] ") vs " $2 " (" $3 ")" } }')

# ---- 4. The vendored Go translator. ----------------------------------------------------------
# ent and pgx each vendor it so a consumer pulls in one module. The copies must stay identical,
# or a fix can land in one alone (#319). Per-module code belongs in that module's render.go.
VENDORED="internal/queryplan"
if [[ ! -d "${REPO_ROOT}/ent/${VENDORED}" || ! -d "${REPO_ROOT}/pgx/${VENDORED}" ]]; then
  fail "ent/${VENDORED} or pgx/${VENDORED} is missing: the sync check would guard nothing"
elif ! diff -ru --label "ent/${VENDORED}" --label "pgx/${VENDORED}" \
  "${REPO_ROOT}/ent/${VENDORED}" "${REPO_ROOT}/pgx/${VENDORED}"; then
  fail "the vendored translator trees have drifted: apply the change to both copies"
fi

# ---- 5. The dataset's to-one relation. -------------------------------------------------------
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

if [[ "${failures}" -gt 0 ]]; then
  echo "Corpus invalid: ${failures} problem(s)" >&2
  exit 1
fi
echo "Corpus valid: $(wc -l <"${TMP}/case-ids" | tr -d '[:space:]') cases on PDP ${current_tag}; ledgers: ${roster[*]}"
