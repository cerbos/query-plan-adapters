#!/usr/bin/env bash
# Bumps the pinned PDPs: `current` becomes `previous`, the new tag becomes `current`, and the
# generator re-records both golden directories and golden/CHANGES.md. Run it locally, on a branch:
#
#   conformance/scripts/bump-pdp.sh           # the latest Cerbos release
#   conformance/scripts/bump-pdp.sh 0.56.0    # a specific release
#
# The digest is resolved from the registry, never typed: a right tag carrying another build's
# digest reads as pinned and is not (cerbos/query-plan-adapters#322). Review golden/CHANGES.md,
# then run every adapter's conformance suite against both versions (conformance/README.md,
# "Bumping the PDP").
#
# Also rewrites every restatement of the pin (the Compose files) and moves the Go modules'
# cerbos/api/genpb requirement to the new tag.
#
# Requires: docker (with buildx), jq, go, curl.
set -euo pipefail

if [[ $# -gt 1 ]]; then
  echo "Usage: $0 [new-tag]   (for example 0.56.0; defaults to the latest Cerbos release)" >&2
  exit 2
fi
if [[ $# -eq 1 && -n "$1" ]]; then
  new_tag="${1#v}"
else
  echo "==> Looking up the latest Cerbos release" >&2
  # gh is authenticated when installed; plain curl is anonymous (60 requests an hour) unless
  # GITHUB_TOKEN is set.
  if command -v gh >/dev/null 2>&1 && new_tag="$(gh api repos/cerbos/cerbos/releases/latest --jq .tag_name 2>/dev/null)"; then
    :
  elif new_tag="$(curl -fsSL ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} \
      https://api.github.com/repos/cerbos/cerbos/releases/latest | jq -er .tag_name)"; then
    :
  else
    echo "Could not look up the latest Cerbos release; pass the tag instead: $0 0.56.0" >&2
    exit 1
  fi
  new_tag="${new_tag#v}"
fi
if [[ ! "${new_tag}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "'${new_tag}' is not a release tag such as 0.56.0" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PDP_VERSIONS="${CONFORMANCE_DIR}/pdp-versions.json"

current_tag="$(jq -er .current.tag "${PDP_VERSIONS}")"
current_digest="$(jq -er .current.digest "${PDP_VERSIONS}")"
dropped_tag="$(jq -er .previous.tag "${PDP_VERSIONS}")"
if [[ "${new_tag}" == "${current_tag}" ]]; then
  echo "pdp-versions.json already pins ${new_tag} as current; nothing to do."
  echo "(If an earlier bump stopped partway, finish it with: go -C conformance/generator run . && conformance/scripts/validate-corpus.sh)"
  exit 0
fi
if [[ "$(printf '%s\n%s\n' "${current_tag}" "${new_tag}" | sort -V | tail -n1)" != "${new_tag}" ]]; then
  echo "${new_tag} is older than the current pin ${current_tag}; refusing to bump backwards." >&2
  exit 1
fi

reference="ghcr.io/cerbos/cerbos:${new_tag}"
echo "==> Resolving ${reference}" >&2
new_digest="$(docker buildx imagetools inspect "${reference}" --format '{{json .Manifest}}' \
  | jq -er .digest)"
if [[ ! "${new_digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
  echo "${reference} resolved to '${new_digest}', which is not a sha256 digest" >&2
  exit 1
fi

# The committed layout exactly: one line per PDP, so a bump is a two-line diff.
entry() { jq -cn --arg tag "$1" --arg digest "$2" '{tag: $tag, digest: $digest}' | sed 's/,/, /; s/":/": /g'; }
{
  printf '{\n'
  printf '  "current": %s,\n' "$(entry "${new_tag}" "${new_digest}")"
  printf '  "previous": %s\n' "$(entry "${current_tag}" "${current_digest}")"
  printf '}\n'
} >"${PDP_VERSIONS}.tmp"
jq -e . "${PDP_VERSIONS}.tmp" >/dev/null
mv "${PDP_VERSIONS}.tmp" "${PDP_VERSIONS}"
echo "==> pdp-versions.json: current ${new_tag} (${new_digest}), previous ${current_tag}" >&2

REPO_ROOT="$(cd "${CONFORMANCE_DIR}/.." && pwd)"

# Files that cannot read pdp-versions.json (Compose files) restate the current pin;
# validate-corpus.sh fails until each one names the new tag and digest.
old_image="ghcr.io/cerbos/cerbos:${current_tag}@${current_digest}"
new_image="ghcr.io/cerbos/cerbos:${new_tag}@${new_digest}"
while IFS= read -r file; do
  sed -i.bak "s|${old_image}|${new_image}|g" "${file}" && rm -f "${file}.bak"
  echo "==> restated pin updated in ${file#"${REPO_ROOT}"/}" >&2
done < <(grep -rlF "${old_image}" "${REPO_ROOT}" \
  --exclude-dir=.git --exclude-dir=node_modules --exclude-dir=golden || true)

# The Go modules decode plans with the PDP's own API types, pinned to the current tag
# (validate-corpus.sh checks ent/go.mod and pgx/go.mod). Each example requires the adapter through
# a replace directive, so it moves in the same step or its go.mod goes stale.
for module in ent pgx ent/example pgx/example; do
  gomod="${REPO_ROOT}/${module}/go.mod"
  grep -q 'github.com/cerbos/cerbos/api/genpb ' "${gomod}" || continue
  echo "==> ${module}: github.com/cerbos/cerbos/api/genpb@v${new_tag}" >&2
  go -C "${REPO_ROOT}/${module}" get "github.com/cerbos/cerbos/api/genpb@v${new_tag}"
  go -C "${REPO_ROOT}/${module}" mod tidy
done

# The old `previous` stops being tested, so ledger entries scoped to it go: an entry scoped to it
# alone is deleted, and the tag is dropped from a list that names others (conformance/README.md,
# "Bumping the PDP"). validate-corpus.sh rejects a `pdp` tag pdp-versions.json no longer records.
for ledger in "${REPO_ROOT}"/*/conformance-ledger.json; do
  jq -e --arg tag "${dropped_tag}" '[.cases[] | .pdp? // [] | index($tag)] | any' "${ledger}" \
    >/dev/null || continue
  jq --arg tag "${dropped_tag}" '
    .cases |= with_entries(
      if (.value | has("pdp")) then .value.pdp -= [$tag] else . end
      | select((.value | has("pdp") | not) or (.value.pdp | length) > 0))
  ' "${ledger}" >"${ledger}.tmp"
  mv "${ledger}.tmp" "${ledger}"
  echo "==> ${ledger#"${REPO_ROOT}"/}: dropped entries scoped to ${dropped_tag}" >&2
done

go -C "${CONFORMANCE_DIR}/generator" run .
"${SCRIPT_DIR}/validate-corpus.sh"

cat >&2 <<NEXT

==> Bumped the PDP: current ${new_tag}, previous ${current_tag} (${dropped_tag} dropped).
Next:
  1. Review conformance/golden/CHANGES.md: every plan, allowed set and plan error that moved.
  2. Run every adapter's conformance harness (CLAUDE.md, "Commands"); fix, or add a ledger entry
     (with "pdp" when the change is specific to one version), for anything that breaks.
  3. Commit it all as one PR, with golden/CHANGES.md as the description.
NEXT
