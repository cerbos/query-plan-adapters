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

# Closed entry schemas, so a misspelled key cannot be silently ignored.
if ! jq -e '
  def text: type == "string" and length > 0;
  def entry($required; $optional):
    type == "object"
    and (($required - keys) | length == 0)
    and ((keys - ($required + $optional)) | length == 0)
    and all(to_entries[] | select(.key != "messages" and .key != "adapters"); .value | text);
  def entries($required; $optional):
    type == "array" and all(.[]; entry($required; $optional));
  def per_adapter($required):
    type == "object" and all(.[]; entries($required; []));
  def check($bucket; $valid):
    if $valid then true else error("invalid " + $bucket + " entry schema") end;
  check("conformance"; .conformance | type == "array" and all(.[]; text))
  and check("adapterUnsupported"; .adapterUnsupported | per_adapter(["action", "reason", "message"]))
  and check("adapterSupportedExpected"; .adapterSupportedExpected | per_adapter(["action", "reason"]))
  and check("expectedUnsupported";
    (.expectedUnsupported | entries(["action", "shape", "messages"]; ["reason"]))
    and all(.expectedUnsupported[]; .messages | type == "object" and all(.[]; text)))
  and check("nullRepresentationOmitted";
    (.nullRepresentationOmitted | entries(["action", "reason", "messages"]; ["relatedIssue"]))
    and all(.nullRepresentationOmitted[]; .messages | type == "object" and all(.[]; text)))
  and check("knownDivergences";
    (.knownDivergences | entries(["action", "reason", "adapters"]; ["relatedIssue"]))
    and all(.knownDivergences[]; .adapters | type == "array" and all(.[]; text)))
  and check("degenerateOracles";
    (.degenerateOracles | entries(["action", "oracle", "reason"]; []))
    and all(.degenerateOracles[]; .oracle == "empty" or .oracle == "total"))
' actions.json >/dev/null; then
  echo "actions.json entries must use their declared keys and non-empty metadata types" >&2
  exit 1
fi

sed -n 's/^[[:space:]]*- actions: \["\([^"]*\)"\].*/\1/p' \
  policies/adversarial.yaml | sort >"${VALIDATION_TMP}/policy-rule-actions"
uniq "${VALIDATION_TMP}/policy-rule-actions" >"${VALIDATION_TMP}/policy-actions"

jq -r '
  .conformance[],
  .expectedUnsupported[].action,
  .nullRepresentationOmitted[].action,
  .knownDivergences[].action
' actions.json | sort >"${VALIDATION_TMP}/classified-actions"

# One rule per action. Only `compose-*` actions (#487) repeat, since they exist to combine rules;
# elsewhere a repeat silently ORs a second condition into a shape.
if duplicates="$(uniq -d "${VALIDATION_TMP}/policy-rule-actions" | grep -v '^compose-' || true)" \
  && [[ -n "${duplicates}" ]]; then
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

# `degenerateOracles` exempts an action from the non-degeneracy sweep, so each entry must name a
# real, oracle-compared action exactly once (never a knownDivergences one).
jq -r '.degenerateOracles[].action' actions.json | sort >"${VALIDATION_TMP}/degenerate-actions"
if duplicates="$(uniq -d "${VALIDATION_TMP}/degenerate-actions")" && [[ -n "${duplicates}" ]]; then
  echo "Actions listed more than once in degenerateOracles:"
  echo "${duplicates}"
  exit 1
fi
if unknown="$(comm -23 "${VALIDATION_TMP}/degenerate-actions" "${VALIDATION_TMP}/classified-actions")" \
  && [[ -n "${unknown}" ]]; then
  echo "degenerateOracles names actions the corpus does not classify:"
  echo "${unknown}"
  exit 1
fi
if ! jq -e '
  ([.knownDivergences[].action] - ([.knownDivergences[].action] - [.degenerateOracles[].action]))
  | length == 0
' actions.json >/dev/null; then
  echo "A knownDivergences action is never oracle-compared and cannot be listed in degenerateOracles"
  exit 1
fi

if ! jq -e '
  .adapters as $roster
  | all(
    .knownDivergences[];
    (.adapters | type == "array" and length > 0 and length == (unique | length))
    and all(.adapters[]; type == "string" and length > 0)
    and ((.adapters - $roster) | length == 0)
  )
' actions.json >/dev/null; then
  echo "Each known divergence must name a non-empty, duplicate-free adapters list drawn from the roster"
  exit 1
fi

# `adapters` is the roster every per-adapter key is checked against.
if ! jq -e '
  (.adapters | type) == "array"
  and (.adapters | length) > 0
  and (.adapters | length) == (.adapters | unique | length)
  and all(.adapters[]; type == "string" and length > 0)
' actions.json >/dev/null; then
  echo "actions.json must declare a non-empty, duplicate-free adapters roster"
  exit 1
fi

if ! jq -e '
  .adapters as $adapters
  | ((.adapterUnsupported // {}) | keys) + ((.adapterSupportedExpected // {}) | keys)
  | all(. as $adapter | $adapters | index($adapter) != null)
' actions.json >/dev/null; then
  echo "adapterUnsupported / adapterSupportedExpected name an adapter missing from the roster"
  exit 1
fi

# Each refusal pins the message its adapter must raise, so a harness proves the declared mechanism
# threw rather than an unrelated error (cerbos/query-plan-adapters#326).
if ! jq -e '
  all((.adapterUnsupported // {})[][];
    (.message | type) == "string" and (.message | length) > 0
    and (.reason | type) == "string" and (.reason | length) > 0)
' actions.json >/dev/null; then
  echo "Every adapterUnsupported entry must carry a non-empty reason and message"
  exit 1
fi

# expectedUnsupported `messages` must name exactly the adapters that reject the shape: every
# adapter that has not promoted it.
messages_drift="$(jq -r '
  .adapters as $adapters
  | (.adapterSupportedExpected // {}) as $promoted
  | .expectedUnsupported[]
  | . as $entry
  | ($adapters | map(select(. as $a | ($promoted[$a] // []) | any(.action == $entry.action) | not))) as $expected
  | (($entry.messages // {}) | keys) as $got
  | (($expected - $got) | map("missing " + .)) + (($got - $expected) | map("unexpected " + .)) as $drift
  | select(($drift | length) > 0)
  | "  \($entry.action): \($drift | join(", "))"
' actions.json)"
if [[ -n "${messages_drift}" ]]; then
  echo "expectedUnsupported messages must name exactly the adapters that reject the shape:"
  echo "${messages_drift}"
  exit 1
fi

if ! jq -e '
  all(.expectedUnsupported[]; all(.messages[]; type == "string" and length > 0))
' actions.json >/dev/null; then
  echo "Every expectedUnsupported message must be a non-empty string"
  exit 1
fi

# A nullRepresentationOmitted action is rejected by every adapter, so it pins a message for each.
null_messages_drift="$(jq -r '
  .adapters as $adapters
  | .nullRepresentationOmitted[]
  | . as $entry
  | (($entry.messages // {}) | keys) as $got
  | ((($adapters - $got) | map("missing " + .)) + (($got - $adapters) | map("unexpected " + .))) as $drift
  | select(($drift | length) > 0)
  | "  \($entry.action): \($drift | join(", "))"
' actions.json)"
if [[ -n "${null_messages_drift}" ]]; then
  echo "nullRepresentationOmitted messages must name every adapter in the roster:"
  echo "${null_messages_drift}"
  exit 1
fi

if ! jq -e '
  all(.nullRepresentationOmitted[]; all(.messages[]; type == "string" and length > 0))
' actions.json >/dev/null; then
  echo "Every nullRepresentationOmitted message must be a non-empty string"
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

for fixture_dir in wire-fixtures wire-fixtures-strict; do
  find "${fixture_dir}" -type f -name '*.json' -exec basename {} .json \; |
    sort >"${VALIDATION_TMP}/fixture-actions"

  if ! diff -u "${VALIDATION_TMP}/policy-actions" "${VALIDATION_TMP}/fixture-actions"; then
    echo "Every policy action must have exactly one golden wire fixture"
    exit 1
  fi

  resource_kind="$(jq -r '.resourceKind' seeds.json)"
  while IFS= read -r action; do
    fixture="${fixture_dir}/${action}.json"
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
    fixture="${fixture_dir}/${action}.json"
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
done

# CERBOS_VERSION and CERBOS_IMAGE_DIGEST are the single source of truth for the PDP pin. Files that
# cannot read them (Compose files, go.mod, echo strings) restate them, so the whole repository is
# scanned for restatements. Tag and digest are checked together: a right tag with another build's
# digest reads as pinned and is not (cerbos/query-plan-adapters#322).
pinned_version="$(tr -d '[:space:]' <CERBOS_VERSION)"
pinned_digest="$(tr -d '[:space:]' <CERBOS_IMAGE_DIGEST)"
REPO_ROOT="$(cd "${CONFORMANCE_DIR}/.." && pwd)"

if [[ ! "${pinned_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "conformance/CERBOS_IMAGE_DIGEST must hold a sha256:<64 hex> digest, got '${pinned_digest}'"
  exit 1
fi

# Markdown is excluded: a README telling consumers how to run their own PDP is not a test input.
# `*_IMAGE` files hold image references shared by an npm script and a workflow; matching the
# pattern means new ones are scanned automatically.
SOURCE_INCLUDES=(
  --include='*.yml' --include='*.yaml' --include='*.sh' --include='*.py' --include='*.go'
  --include='*.java' --include='*.kts' --include='*.ts' --include='*.js' --include='*.json'
  --include='Dockerfile' --include='*_IMAGE'
)
SOURCE_EXCLUDES=(
  --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=.claude --exclude-dir=lib
  --exclude-dir=build --exclude-dir=.venv --exclude-dir=.gradle --exclude-dir=bin
  --exclude-dir=__pypackages__ --exclude-dir=.agents --exclude-dir=.out-of-scope
  --exclude-dir=dist --exclude-dir=.gems --exclude-dir=.bundle-path
)

# `lib/` is committed build output on the TypeScript adapters but source on the Ruby gem, so Ruby
# gets its own pass without the `lib` exclusion.
RUBY_INCLUDES=(
  --include='*.rb' --include='*.gemspec' --include='Gemfile' --include='Rakefile'
)
RUBY_EXCLUDES=()
for exclusion in "${SOURCE_EXCLUDES[@]}"; do
  [[ "${exclusion}" == "--exclude-dir=lib" ]] && continue
  RUBY_EXCLUDES+=("${exclusion}")
done

# Every source scan goes through here, so all checks see the same file types.
source_grep() {
  grep "$@" "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" || true
  grep "$@" "${RUBY_INCLUDES[@]}" "${RUBY_EXCLUDES[@]}" || true
}

# Prove the scan reaches Ruby source under `lib/`; otherwise re-excluding it would pass silently.
if ! source_grep -rl '' "${REPO_ROOT}" 2>/dev/null | grep -q '/lib/.*\.rb$'; then
  echo "The source scan reaches no .rb file under a lib/ directory, so a Ruby adapter's"
  echo "implementation is invisible to every check below. Restore the Ruby scan pass."
  exit 1
fi

version_drift=0
while IFS=: read -r file _ match; do
  # Extract the tag: everything after the last `cerbos:` up to a digest/quote/space/paren.
  tag="$(printf '%s' "${match}" | sed -n 's|.*ghcr\.io/cerbos/cerbos:\([^@)"'\''[:space:]]*\).*|\1|p')"
  [[ -z "${tag}" ]] && continue
  # Interpolations such as `cerbos:${CERBOS_VERSION}` read the pin at runtime and cannot drift.
  case "${tag}" in
    '$'*|'%'*|'{'*) continue ;;
  esac
  relative="${file#"${REPO_ROOT}"/}"
  if [[ "${tag}" != "${pinned_version}" ]]; then
    echo "${relative} pins Cerbos '${tag}', expected ${pinned_version} (conformance/CERBOS_VERSION)"
    version_drift=1
  fi
  digest="$(printf '%s' "${match}" \
    | sed -n 's|.*ghcr\.io/cerbos/cerbos:[^@)"'\''[:space:]]*@\(sha256:[0-9a-f]*\).*|\1|p')"
  if [[ -z "${digest}" ]]; then
    echo "${relative} pins Cerbos by tag only; append @${pinned_digest} (conformance/CERBOS_IMAGE_DIGEST)"
    version_drift=1
  elif [[ "${digest}" != "${pinned_digest}" ]]; then
    echo "${relative} pins Cerbos digest '${digest}', expected ${pinned_digest} (conformance/CERBOS_IMAGE_DIGEST)"
    version_drift=1
  fi
done < <(source_grep -rn 'ghcr\.io/cerbos/cerbos:' "${REPO_ROOT}")
if [[ "${version_drift}" -ne 0 ]]; then
  exit 1
fi

# Service images are pinned per harness, not centrally: a shared file under conformance/ would
# re-run every adapter's CI on each bump. The rule is shared instead: every repository listed here
# must appear as `repo:tag@sha256:<digest>`, with one digest per tag across the repository.
#
# Add a repository here when you add a service. The Cerbos image is absent because the scan above
# checks it more strictly. `gradle` is absent because nothing references a Gradle image; both
# Java adapters pin Gradle through their wrapper.
IMAGE_REPOSITORIES=(
  "postgres"
  "mysql"
  "mongo"
  "chromadb/chroma"
  "docker.elastic.co/elasticsearch/elasticsearch"
  "ghcr.io/get-convex/convex-backend"
)

image_drift=0
: >"${VALIDATION_TMP}/image-refs"
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
      echo "${relative} references '${reference}': service images must be pinned as repo:tag@sha256:<64 hex>"
      image_drift=1
      continue
    fi
    printf '%s\t%s\t%s\n' "${reference%@*}" "${reference#*@}" "${relative}" \
      >>"${VALIDATION_TMP}/image-refs"
  done < <(source_grep -rnoIE "(^|[^A-Za-z0-9._/:-])${escaped}:[A-Za-z0-9._-]+(@sha256:[0-9a-fA-F]*)?" \
    "${REPO_ROOT}")
  # A repository nothing references is a guard watching nothing: a moved constant or a stale
  # entry would otherwise read as green.
  if [[ "${matched}" -eq 0 ]]; then
    echo "No reference to image repository '${repository}' was found: either the scan no longer"
    echo "reaches the file that pins it, or the service is gone and the entry should be removed."
    image_drift=1
  fi
done

# One `repo:tag`, one digest: harnesses sharing a tag must test the same build.
tag_conflicts="$(sort -u "${VALIDATION_TMP}/image-refs" | awk -F'\t' '
  { if (!($1 in seen)) { seen[$1] = $2; where[$1] = $3 }
    else if (seen[$1] != $2) { print "  " $1 ": " seen[$1] " (" where[$1] ") vs " $2 " (" $3 ")" } }
')"
if [[ -n "${tag_conflicts}" ]]; then
  echo "The same image tag is pinned to more than one digest:"
  echo "${tag_conflicts}"
  image_drift=1
fi

if [[ "${image_drift}" -ne 0 ]]; then
  exit 1
fi

# ent and pgx each vendor the translator so a consumer pulls in one module. The two copies must stay
# byte-identical, or a fix can land in one alone (cerbos/query-plan-adapters#319). Anything
# per-module belongs in that module's render.go, outside this tree.
VENDORED_TRANSLATOR="internal/queryplan"

# A missing tree would make the diff below pass vacuously.
for module in ent pgx; do
  if [[ ! -d "${REPO_ROOT}/${module}/${VENDORED_TRANSLATOR}" ]]; then
    echo "${module}/${VENDORED_TRANSLATOR} is missing: the sync check below would guard nothing."
    exit 1
  fi
done

if ! diff -ru \
  --label "ent/${VENDORED_TRANSLATOR}" --label "pgx/${VENDORED_TRANSLATOR}" \
  "${REPO_ROOT}/ent/${VENDORED_TRANSLATOR}" "${REPO_ROOT}/pgx/${VENDORED_TRANSLATOR}"; then
  echo "The vendored translator trees have drifted. Both modules must carry the identical"
  echo "${VENDORED_TRANSLATOR}: apply the change to both copies, or move whatever is genuinely"
  echo "per-module into that module's render.go."
  exit 1
fi

# The Go modules pin cerbos/api/genpb separately; it must match CERBOS_VERSION.
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

# `parentSeedId` must name a real seed. A dangling id reads as "no parent" on both sides of every
# differential, so harnesses would agree for the wrong reason.
relation_drift="$(jq -r '
  (.seeds | map(.id)) as $ids
  | .seeds[]
  | .parentSeedId as $parent
  | if (has("parentSeedId") | not) then "  \(.id): carries no parentSeedId key"
    elif $parent == .id then "  \(.id): parentSeedId names its own row"
    elif ($parent != null) and (($ids | index($parent)) == null)
      then "  \(.id): parentSeedId \"\($parent)\" is not a seed id"
    else empty end
' seeds.json)"
if [[ -n "${relation_drift}" ]]; then
  echo "seeds.json parentSeedId references are broken:"
  echo "${relation_drift}"
  exit 1
fi

# The relation must reach all three depths: no parent, a parent, and a grandparent
# (`parent.inner`).
relation_depths="$(jq -r '
  (.seeds | map({(.id): .parentSeedId}) | add) as $parent
  | (.seeds | map(select(.parentSeedId == null)) | length) as $none
  | (.seeds | map(select(.parentSeedId != null and $parent[.parentSeedId] == null)) | length) as $one
  | (.seeds | map(select(.parentSeedId != null and $parent[.parentSeedId] != null)) | length) as $two
  | [ (if $none == 0 then "no seed is parentless" else empty end),
      (if $one  == 0 then "no seed has a parent without an inner" else empty end),
      (if $two  == 0 then "no seed reaches parent.inner" else empty end) ]
  | select(length > 0)
  | join(", ")
' seeds.json)"
if [[ -n "${relation_depths}" ]]; then
  echo "seeds.json parentSeedId chains are degenerate: ${relation_depths}"
  exit 1
fi

# derived-fields.json encodes README.md's "Deterministic derived fields" rules. Harnesses feed the
# same value to the stored row and the oracle, so only this check can catch a wrong one.
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
    and ((.updatedAt | type) == "string" or .updatedAt == null)
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
         {"h5": "not-a-timestamp"} as $fixed
         | if ($fixed | has($seed.id)) then $fixed[$seed.id]
           elif $seed.aNumber >= 2 then "2024-06-01T00:00:00Z" else "2026-06-01T00:00:00Z" end
       ) then "createdBy" else empty end),
      (if $entry.aDouble != (
         {"a1": -0.6, "a2": 0.25, "a3": null, "g1": -9.5e18} as $fixed
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
       ) then "createdAt" else empty end),
      (if $entry.updatedAt != (
         {"a1": "2020-03-15T10:30:00.000Z", "a4": "2024-06-01T00:00:00Z"}[$seed.id]
       ) then "updatedAt" else empty end)
    ]
  | select(length > 0)
  | "  \($seed.id): \(join(", "))"
' seeds.json derived-fields.json)"
if [[ -n "${derived_drift}" ]]; then
  echo "derived-fields.json disagrees with the derived-field rules in README.md:"
  echo "${derived_drift}"
  exit 1
fi

# `scope` and `labels` have no rule to re-derive, so their per-seed tables are restated here. A
# checker's copy can only fail loudly; it never feeds a row or an oracle.
cat >"${VALIDATION_TMP}/expected-tables" <<'JSON'
{
  "a1": { "scope": "dept",                  "labels": ["gold", "silver"] },
  "a2": { "scope": "dept.eng",              "labels": [] },
  "a3": { "scope": "dept.eng.platform",     "labels": [] },
  "a4": { "scope": "dept.eng.platform.obs", "labels": [] },
  "a5": { "scope": "dept.engineering",      "labels": [] },
  "a6": { "scope": "dept.sales",            "labels": [null, "silver"] },
  "a7": { "scope": null,                    "labels": [] },
  "a8": { "scope": "",                      "labels": ["silver"] },
  "a9": { "scope": "50%",                   "labels": [] },
  "b1": { "scope": "50%:a_b:x",             "labels": [] },
  "b2": { "scope": "50x:a_b:y",             "labels": [] },
  "b3": { "scope": "50%:aXb:y",             "labels": [] },
  "b4": { "scope": "50%:a_b",               "labels": [] },
  "b5": { "scope": "dept.eng.platform2",    "labels": [] },
  "b6": { "scope": "50%.a_b",               "labels": [] },
  "c1": { "scope": "Dept.Eng",              "labels": ["Gold"] },
  "c2": { "scope": "dept.eng.",             "labels": [] },
  "d1": { "scope": "[env]:prod:eu",         "labels": [] },
  "d2": { "scope": "e:prod:eu",             "labels": [] },
  "e1": { "scope": null,                    "labels": [] },
  "f1": { "scope": null,                    "labels": [] },
  "g1": { "scope": null,                    "labels": [] },
  "h1": { "scope": null,                    "labels": [] },
  "h2": { "scope": null,                    "labels": [] },
  "h3": { "scope": null,                    "labels": [] },
  "h4": { "scope": null,                    "labels": [] },
  "h5": { "scope": null,                    "labels": [] },
  "h6": { "scope": null,                    "labels": [] },
  "h7": { "scope": null,                    "labels": [] }
}
JSON
jq -S '.derived | map_values({scope, labels})' derived-fields.json \
  >"${VALIDATION_TMP}/actual-tables"
if ! diff -u <(jq -S . "${VALIDATION_TMP}/expected-tables") "${VALIDATION_TMP}/actual-tables"; then
  echo "derived-fields.json scope/labels disagree with the tables in README.md"
  exit 1
fi

echo "Corpus valid: $(wc -l <"${VALIDATION_TMP}/policy-actions" | tr -d '[:space:]') actions, ${seed_count} seeds"
