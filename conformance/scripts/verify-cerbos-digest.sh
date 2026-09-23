#!/usr/bin/env bash
# Asserts CERBOS_IMAGE_DIGEST is what ghcr.io/cerbos/cerbos:$CERBOS_VERSION actually resolves to.
#
# validate-corpus.sh proves every file agrees with the pin, but not that the tag/digest pair is
# right. Docker pulls `repo:tag@digest` by digest, so a version bump that keeps the old digest
# would keep testing the old build (cerbos/query-plan-adapters#322). This needs a registry, so it
# runs once, in conformance.yaml. Only the PDP is checked: Cerbos tags are immutable, service
# image tags are not.
#
# Requires: docker (with buildx).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFORMANCE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${CONFORMANCE_DIR}"

pinned_version="$(tr -d '[:space:]' <CERBOS_VERSION)"
pinned_digest="$(tr -d '[:space:]' <CERBOS_IMAGE_DIGEST)"
reference="ghcr.io/cerbos/cerbos:${pinned_version}"

echo "==> Resolving ${reference}"
resolved="$(docker buildx imagetools inspect "${reference}" --format '{{.Manifest.Digest}}' | tail -1)"

if [[ "${resolved}" != "${pinned_digest}" ]]; then
  cat >&2 <<EOF
conformance/CERBOS_IMAGE_DIGEST does not match the tag it claims to pin.

  ${reference}
    resolves to ${resolved}
    CERBOS_IMAGE_DIGEST says ${pinned_digest}

Every harness pulls \`${reference}@${pinned_digest}\`, which Docker resolves by digest and not by
tag — so the suites are testing whatever build that digest names, not ${pinned_version}. If the
version bump is intentional, update the digest together with the tag:

  docker buildx imagetools inspect ${reference} --format '{{.Manifest.Digest}}' \\
    > conformance/CERBOS_IMAGE_DIGEST

then re-run conformance/scripts/regenerate-wire-fixtures.sh and review the fixture diff.
EOF
  exit 1
fi

echo "Cerbos ${pinned_version} resolves to ${pinned_digest}"
