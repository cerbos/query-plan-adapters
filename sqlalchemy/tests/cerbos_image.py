# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The pinned Cerbos PDP image, read from the corpus pin files.

The digest guards against the tag being re-pointed.
``conformance/scripts/validate-corpus.sh`` checks every copy of the pin agrees.
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
