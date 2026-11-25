# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The one error ``get_filter`` raises for a plan it cannot translate."""


class UnsupportedPlanError(ValueError):
    """Raised when a plan holds a shape this adapter cannot express as a Polars expression.

    The adapter refuses rather than emit a best-effort filter, since a wrong filter returns
    rows the PDP denies. Invalid caller configuration, such as an attribute that resolves to
    no column, raises ``KeyError`` or ``TypeError`` instead.
    """
