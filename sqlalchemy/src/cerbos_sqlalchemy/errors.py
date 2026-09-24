# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The one error ``get_query`` raises for a plan it cannot translate."""


class UnsupportedPlanError(ValueError):
    """The plan holds a shape this adapter cannot express as a SQL filter.

    Raised instead of emitting a best-effort filter: a wrong filter returns rows the PDP
    denies, while this error is a bug report. It subclasses ``ValueError``, which is what
    these refusals raised before the type existed, so existing ``except ValueError``
    handlers keep catching it. An operator override that cannot translate the shape it
    was handed should raise it too.

    Invalid caller configuration -- an unknown null representation, an unmapped attribute,
    a missing ``table_mapping`` -- is not a plan refusal and keeps raising ``ValueError``,
    ``KeyError`` or ``TypeError``.
    """
