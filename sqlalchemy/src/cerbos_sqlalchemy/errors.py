# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The one error ``get_query`` raises for a plan it cannot translate."""


class UnsupportedPlanError(ValueError):
    """Raised when a plan holds a shape this adapter cannot express as a SQL filter.

    The adapter refuses rather than emit a best-effort filter, since a wrong filter
    returns rows the PDP denies. It subclasses ``ValueError`` so existing handlers
    still catch it. Operator overrides that cannot translate a shape should raise it too.

    Invalid caller configuration, such as an unmapped attribute or a missing
    ``table_mapping``, still raises ``ValueError``, ``KeyError`` or ``TypeError``.
    """
