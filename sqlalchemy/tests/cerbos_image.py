"""The pinned Cerbos PDP image, defined once for every suite in this harness.

Both the unit suite (``conftest.py``) and the adversarial conformance suite start their own PDP
container. Restating the reference in each of them is how one suite ends up proving the adapter
against a different planner than the other.

The reference carries a tag *and* a digest: the tag records which release this is, the digest makes
the pin immune to the tag being re-pointed. ``conformance/scripts/validate-corpus.sh`` asserts that
every restatement of the reference anywhere in the repository agrees with these two corpus files.
"""

import os

CONFORMANCE_DIR = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "conformance")
)

with open(os.path.join(CONFORMANCE_DIR, "CERBOS_VERSION"), encoding="utf-8") as _f:
    CERBOS_VERSION = _f.read().strip()

with open(os.path.join(CONFORMANCE_DIR, "CERBOS_IMAGE_DIGEST"), encoding="utf-8") as _f:
    CERBOS_IMAGE_DIGEST = _f.read().strip()

CERBOS_IMAGE = f"ghcr.io/cerbos/cerbos:{CERBOS_VERSION}@{CERBOS_IMAGE_DIGEST}"
