# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0


class UnsupportedPlanError(ValueError):
    """A plan shape this adapter cannot express as a DuckDB filter that returns exactly the rows
    the PDP allows. The adapter refuses rather than emit a broader filter."""
