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

# `adapters` is the canonical roster every other per-adapter key is checked against. Without it
# each check would have to restate "the eleven adapters", and an adapter added to one list but not
# another would look consistent.
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

# Every throwing classification pins the substring that adapter's error must contain, so a
# harness proves the throw is the DECLARED mechanism rather than a mapper typo or an unrelated
# validation (cerbos/query-plan-adapters#326). A classification whose message is missing or empty
# would degrade the harness assertion back to a bare "it threw".
if ! jq -e '
  all((.adapterUnsupported // {})[][];
    (.message | type) == "string" and (.message | length) > 0
    and (.reason | type) == "string" and (.reason | length) > 0)
' actions.json >/dev/null; then
  echo "Every adapterUnsupported entry must carry a non-empty reason and message"
  exit 1
fi

# An expectedUnsupported shape is rejected by every adapter that has not promoted it, so its
# `messages` key set is exactly that complement — not a subset. A missing key is an adapter whose
# harness would have nothing to assert; a stray one is a message no harness reads.
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

# A `nullRepresentationOmitted` action is rejected by EVERY adapter — the two conventions are
# indistinguishable on the wire, so no adapter can translate it — hence the full roster with no
# promotions to subtract. It is as fail-closed as anything in the two groups above, so it pins its
# message the same way rather than leaving each harness with a hardcoded literal.
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

# CERBOS_VERSION and CERBOS_IMAGE_DIGEST are the single source of truth for the pinned PDP: every
# workflow and test harness reads them. Some files cannot read another file (Compose files, echo
# strings, go.mod requirements), so every hardcoded restatement anywhere in the repository is
# asserted to agree instead of de-duplicated. A repo-wide scan rather than a fixed path list: the
# fixed list once missed spring-data/example/docker-compose.yml running `latest` in CI.
#
# The tag and the digest are checked TOGETHER. Checking the tag alone accepts a reference whose
# digest belongs to some other build entirely — which is what a digest is for, so a half-validated
# reference is worse than none: it reads as pinned and is not (cerbos/query-plan-adapters#322).
pinned_version="$(tr -d '[:space:]' <CERBOS_VERSION)"
pinned_digest="$(tr -d '[:space:]' <CERBOS_IMAGE_DIGEST)"
REPO_ROOT="$(cd "${CONFORMANCE_DIR}/.." && pwd)"

if [[ ! "${pinned_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "conformance/CERBOS_IMAGE_DIGEST must hold a sha256:<64 hex> digest, got '${pinned_digest}'"
  exit 1
fi

# Markdown is excluded on purpose: a README telling a *consumer* how to start a PDP of their own
# is prose, not something a harness runs, and holding it to the corpus pin would be a claim about
# the reader's environment rather than about this repository's tests.
#
# `*_IMAGE` is a convention, not one file's name: a harness whose image cannot live in source
# (an npm script and a workflow both need it) puts the reference in a `<SERVICE>_IMAGE` file and
# both read it. Matching the pattern rather than the filename means the next one is scanned
# without editing this list — a bespoke `--include` for each is how the second such file ends up
# silently unchecked.
SOURCE_INCLUDES=(
  --include='*.yml' --include='*.yaml' --include='*.sh' --include='*.py' --include='*.go'
  --include='*.java' --include='*.kts' --include='*.ts' --include='*.js' --include='*.json'
  --include='Dockerfile' --include='*_IMAGE'
)
SOURCE_EXCLUDES=(
  --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=.claude --exclude-dir=lib
  --exclude-dir=build --exclude-dir=.venv --exclude-dir=.gradle --exclude-dir=bin
  --exclude-dir=__pypackages__ --exclude-dir=.agents --exclude-dir=.out-of-scope
  --exclude-dir=dist
)

version_drift=0
while IFS=: read -r file _ match; do
  # Extract the tag: everything after the last `cerbos:` up to a digest/quote/space/paren.
  tag="$(printf '%s' "${match}" | sed -n 's|.*ghcr\.io/cerbos/cerbos:\([^@)"'\''[:space:]]*\).*|\1|p')"
  [[ -z "${tag}" ]] && continue
  # Workflow/script interpolations (`cerbos:${CERBOS_VERSION}` and language equivalents)
  # read the pinned files at runtime and cannot drift.
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
done < <(grep -rn 'ghcr\.io/cerbos/cerbos:' "${REPO_ROOT}" \
  "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" || true)
if [[ "${version_drift}" -ne 0 ]]; then
  exit 1
fi

# Every other service image a test or workflow starts is pinned per harness rather than centrally:
# a shared file would live under conformance/, and conformance/** re-runs all ten adapter
# workflows, so bumping mongoose's server would cost nine irrelevant CI runs. What is shared is the
# RULE, enforced here: a repository named below must appear everywhere as `repo:tag@sha256:<64 hex>`
# — the tag says which release a reader is looking at, the digest says which build a green run
# actually proved, and a `repo:tag` may resolve to only one digest across the whole repository, so
# two harnesses cannot silently test different builds of the same nominal version.
#
# Adding a service means adding its repository here. That is the point of the list, not an obstacle
# to route around: a repository nothing scans is a repository nothing keeps pinned. ghcr.io/cerbos/
# cerbos is deliberately absent — the scan above already holds it to a stricter rule (the exact
# version and digest the corpus declares), and listing it twice would report each drift twice.
IMAGE_REPOSITORIES=(
  "postgres"
  "mysql"
  "mongo"
  "chromadb/chroma"
  "docker.elastic.co/elasticsearch/elasticsearch"
  "ghcr.io/get-convex/convex-backend"
  "gradle"
)

image_drift=0
: >"${VALIDATION_TMP}/image-refs"
for repository in "${IMAGE_REPOSITORIES[@]}"; do
  escaped="${repository//./\\.}"
  matched=0
  # A leading character class rather than \b: it keeps `jdbc:mysql://…` and `postgres://…` out
  # (both are URLs, not image references) while still matching a reference opened by a quote,
  # a space or the start of a line.
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
  done < <(grep -rnoIE "(^|[^A-Za-z0-9._/:-])${escaped}:[A-Za-z0-9._-]+(@sha256:[0-9a-fA-F]*)?" \
    "${REPO_ROOT}" "${SOURCE_INCLUDES[@]}" "${SOURCE_EXCLUDES[@]}" || true)
  # A repository nobody references is a guard watching nothing — the same vacuity the corpus's
  # degeneracy guard exists to catch, applied here. It fires when a harness moves its constant
  # into a file type SOURCE_INCLUDES does not reach, or when a service is dropped and its entry
  # left behind, both of which otherwise read as green.
  if [[ "${matched}" -eq 0 ]]; then
    echo "No reference to image repository '${repository}' was found: either the scan no longer"
    echo "reaches the file that pins it, or the service is gone and the entry should be removed."
    image_drift=1
  fi
done

# One `repo:tag`, one digest. Without this, two harnesses sharing a nominal version — the Postgres
# leg of drizzle, prisma, ent and pgx all name the same tag — could pin it to different builds and
# still report themselves as testing the same server.
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

# The two Go modules are standalone — each vendors the translator under its own
# internal/queryplan so a consumer pulls in only the one — which means the same source exists
# twice and a semantic fix can land in one copy alone. Nothing else notices: the corpus catches it
# only if some action happens to exercise the fixed shape, and the hostile-plan invariants pinned
# by the unit suites never come off a real planner wire at all
# (cerbos/query-plan-adapters#319). So the trees are held byte-identical and diffed here, in the
# script both adapter workflows already run — including on a change under the other adapter's
# directory, since each workflow triggers on `conformance/**` as well as its own.
#
# Byte-identical rather than identical-modulo-an-allowlist on purpose: an allowlist is a place for
# a real divergence to hide as a comment tweak. Anything genuinely per-module belongs in that
# module's render.go, which is outside this tree.
VENDORED_TRANSLATOR="internal/queryplan"

# A tree that is not there would make the diff below pass by comparing nothing, so assert both
# exist first — the same vacuity guard the image scan applies to a repository nothing references.
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

# `scope` and `labels` are per-seed tables with no rule to re-derive from, so they are restated
# here instead. A restatement inside a checker is not a second source of truth: unlike the ten
# harness copies it replaced, it never feeds a stored row or an oracle, so it cannot make both
# sides of a differential agree for the wrong reason — it can only fail loudly. Without it these
# forty values, which drive the hier-* and label oracles on every adapter at once, are checked by
# nothing.
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
  "e1": { "scope": null,                    "labels": [] }
}
JSON
jq -S '.derived | map_values({scope, labels})' derived-fields.json \
  >"${VALIDATION_TMP}/actual-tables"
if ! diff -u <(jq -S . "${VALIDATION_TMP}/expected-tables") "${VALIDATION_TMP}/actual-tables"; then
  echo "derived-fields.json scope/labels disagree with the tables in README.md"
  exit 1
fi

echo "Corpus valid: $(wc -l <"${VALIDATION_TMP}/policy-actions" | tr -d '[:space:]') actions, ${seed_count} seeds"
