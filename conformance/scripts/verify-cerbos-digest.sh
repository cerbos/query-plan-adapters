#!/usr/bin/env bash
# Asserts each PDP pinned in conformance/pdp-versions.json — `current` and `previous` — carries the
# digest its tag actually resolves to.
#
# validate-corpus.sh proves every file agrees with the pin, but not that the tag/digest pair is
# right. Docker pulls `repo:tag@digest` by digest, so a version bump that keeps the old digest
# would keep testing the old build (cerbos/query-plan-adapters#322). This needs a registry, so it
# runs once, in conformance.yaml. Only the PDP is checked: Cerbos tags are immutable, service
# image tags are not.
#
# Requires: docker (with buildx), jq.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PDP_VERSIONS="$(cd "${SCRIPT_DIR}/.." && pwd)/pdp-versions.json"

status=0
for slot in current previous; do
  tag="$(jq -er ".${slot}.tag" "${PDP_VERSIONS}")"
  pinned="$(jq -er ".${slot}.digest" "${PDP_VERSIONS}")"
  reference="ghcr.io/cerbos/cerbos:${tag}"

  echo "==> Resolving ${reference} (${slot})"
  resolved="$(docker buildx imagetools inspect "${reference}" --format '{{json .Manifest}}' \
    | jq -r .digest)"

  if [[ "${resolved}" != "${pinned}" ]]; then
    cat >&2 <<EOF
conformance/pdp-versions.json .${slot} does not carry the digest of the tag it claims to pin.

  ${reference}
    resolves to ${resolved}
    pdp-versions.json says ${pinned}

The generator pulls \`${reference}@${pinned}\`, which Docker resolves by digest and not by tag, so
the goldens record whatever build that digest names, not ${tag}. Bump with
conformance/scripts/bump-pdp.sh, which resolves the digest for you, rather than by hand.
EOF
    status=1
    continue
  fi
  echo "Cerbos ${tag} resolves to ${pinned}"
done
exit "${status}"
