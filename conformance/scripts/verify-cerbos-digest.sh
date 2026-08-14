#!/usr/bin/env bash
# Asserts that CERBOS_IMAGE_DIGEST is the digest ghcr.io/cerbos/cerbos:$CERBOS_VERSION actually
# resolves to.
#
# Why this is separate from validate-corpus.sh: that script is offline and runs in every adapter
# workflow and on developer machines with no Docker. This one talks to a registry, so it runs once,
# from .github/workflows/conformance.yaml — the workflow that already owns the pinned-PDP contract.
#
# Why it exists at all: validate-corpus.sh proves every restatement in the repository agrees with
# the two corpus files. It cannot prove the PAIR is right. A CERBOS_VERSION bump that leaves the old
# digest behind is internally consistent and repo-wide green, and because Docker resolves
# `repo:tag@digest` BY DIGEST and ignores the tag, every harness would go on testing the old build
# while every file claims the new version (cerbos/query-plan-adapters#322).
#
# Only the PDP is checked. The service images are pinned to the build a suite was proved against on
# purpose, and their tags — the Postgres, MySQL and MongoDB majors — are expected to move underneath
# that pin, so asserting those still resolve to our digest would fail the corpus on somebody else's
# release. A Cerbos release tag is immutable, so for the PDP the two must agree.
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
