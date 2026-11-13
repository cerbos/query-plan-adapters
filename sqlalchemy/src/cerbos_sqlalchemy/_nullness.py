# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""``IS NULL`` and ``IS NOT NULL`` over an expression of any shape.

SQLAlchemy gives ``NOT`` and ``IS`` the same precedence, so ``not_(x).is_(None)`` renders
``NOT x IS NULL``, which PostgreSQL reads as ``NOT (x IS NULL)``.
"""

from typing import Any

from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.elements import BindParameter, ColumnClause, Grouping, Null


def _grouped(expression: Any) -> Any:
    if isinstance(
        expression, (ColumnClause, InstrumentedAttribute, Grouping, BindParameter, Null)
    ):
        return expression
    if hasattr(expression, "self_group"):
        return Grouping(expression)
    return expression


def is_null(expression: Any) -> Any:
    return _grouped(expression).is_(None)


def is_not_null(expression: Any) -> Any:
    return _grouped(expression).isnot(None)
