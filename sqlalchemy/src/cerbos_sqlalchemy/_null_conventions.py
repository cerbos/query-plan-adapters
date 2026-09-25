# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""How a caller represents a NULL column, and what that does to a comparison.

The plan is the same under either convention, so the caller declares it. See #302 and #308.
"""

from typing import Any, Literal

from sqlalchemy import and_, case, literal, not_, or_

from cerbos_sqlalchemy._operators import scalar_kind
from cerbos_sqlalchemy._plan import Expr, Operand, Value, Variable
from cerbos_sqlalchemy.errors import UnsupportedPlanError

# How the caller sends a NULL column to check(). See get_query().
NullAttributeRepresentation = Literal["explicit", "omitted"]

_REPRESENTATIONS = ("explicit", "omitted")

# The only operators CEL answers definitely over a null, so the only ones a
# per-attribute declaration settles. Everything else uses the call-level fallback.
EQUALITY_FAMILY = frozenset({"eq", "ne", "in"})


def validate_representations(
    null_attribute_representation: Any,
    attribute_null_representation: dict[str, Any] | None,
    attr_map: dict[str, Any],
) -> dict[str, NullAttributeRepresentation]:
    """Validate both options and return the per-attribute declarations."""
    if null_attribute_representation not in _REPRESENTATIONS:
        raise ValueError(
            "null_attribute_representation must be 'explicit' or 'omitted', got "
            f"{null_attribute_representation!r}"
        )
    declarations = attribute_null_representation or {}
    for attribute, convention in declarations.items():
        if convention not in _REPRESENTATIONS:
            raise ValueError(
                "attribute_null_representation values must be 'explicit' or "
                f"'omitted', got {convention!r} for {attribute!r}"
            )
        if attribute not in attr_map:
            raise ValueError(
                f"attribute_null_representation names {attribute!r}, which is not "
                "in the attribute column map"
            )
    return declarations


def _carries_null_operand(operand: Operand) -> bool:
    if not isinstance(operand, Value):
        return False
    value = operand.value
    if value is None:
        return True
    return isinstance(value, list) and any(member is None for member in value)


def _compared_attribute_and_literal(node: Operand) -> tuple[str, Value] | None:
    """Return ``(variable_name, literal)`` for a variable-vs-literal comparison, else None."""
    if not isinstance(node, Expr) or node.operator not in EQUALITY_FAMILY:
        return None
    operands = node.operands
    if len(operands) != 2:
        return None
    variable, value = operands
    if isinstance(value, Variable):
        variable, value = value, variable
    if not isinstance(variable, Variable) or not isinstance(value, Value):
        return None
    return variable.name, value


def _null_operand_error(operator: str) -> UnsupportedPlanError:
    return UnsupportedPlanError(
        f"Cannot translate `{operator}` against a null operand under "
        'null_attribute_representation="omitted": a NULL column sends no '
        "attribute, so Cerbos evaluates the comparison as a missing-attribute "
        "error (deny) while a NULL-selecting filter would return those rows. "
        'Send NULL columns as explicit nulls and use "explicit", or keep this '
        "shape out of the policy."
    )


def assert_no_null_comparison_operands(
    node: Operand,
    declarations: dict[str, NullAttributeRepresentation],
    fallback: str,
) -> None:
    """Reject null literal operands under the ``omitted`` representation.

    An omitted NULL makes CEL error and deny, while ``IS NULL`` would return the row.
    The scan matches any operand, not a list of operators, so shapes like
    ``hasIntersection`` and future operators are covered. The one exception is
    ``eq``/``ne`` between an attribute and a bare null, which the translator renders
    as UNKNOWN for a NULL column (``omitted_null_comparison``). See #551.
    """
    if not isinstance(node, Expr):
        return
    operator = node.operator
    operands = node.operands

    # An attribute-vs-literal comparison uses that attribute's declaration.
    # Other shapes cannot be tied to a column here, so they use the fallback.
    compared = _compared_attribute_and_literal(node)
    if compared is not None:
        variable, value = compared
        convention = declarations.get(variable, fallback)
        if (
            convention == "omitted"
            and _carries_null_operand(value)
            and not (operator in ("eq", "ne") and value.value is None)
        ):
            raise _null_operand_error(operator)
        return

    if fallback == "omitted" and any(_carries_null_operand(o) for o in operands):
        raise _null_operand_error(operator)
    for operand in operands:
        assert_no_null_comparison_operands(operand, declarations, fallback)


def omitted_null_comparison(operator: str, column: Any, overridden: bool) -> Any:
    """Render ``eq``/``ne`` against null for an attribute on the omitted convention.

    A NULL column sends no attribute, so CEL answers with a missing-attribute error,
    and a present column is never equal to null. The ``CASE`` has no ``ELSE``, so a
    NULL column is UNKNOWN, which stays UNKNOWN under any enclosing ``NOT``; a
    present one is FALSE for ``eq`` and TRUE for ``ne``. See #551.

    An overridden operator, or an attribute mapped to something that is not a SQL
    expression (e.g. a relation marker), cannot be rendered this way, so it is refused.
    """
    if overridden or not hasattr(column, "isnot"):
        raise _null_operand_error(operator)
    return case((column.isnot(None), literal(operator == "ne")))


def definite_equality(
    operator: str,
    left_column: Any,
    right: Any,
    left_explicit: bool,
    right_explicit: bool,
) -> Any:
    """Render an equality that is never SQL UNKNOWN for an explicit-null side.

    CEL compares an explicit null as a value, so SQL's UNKNOWN would leave an
    enclosing NOT nothing to flip. Not ``is_distinct_from``: that is symmetric,
    but an undeclared side's NULL is a missing attribute that CEL denies, so it
    must stay UNKNOWN. A null-safe operator would match the two NULLs and over-grant.
    """
    left_kind, right_kind = scalar_kind(left_column), scalar_kind(right)
    if left_kind and right_kind and left_kind != right_kind:
        equality = (
            and_(left_column.is_(None), right.is_(None))
            if left_explicit and right_explicit
            else literal(False)
        )
        result = not_(equality) if operator == "ne" else equality
        for operand, explicit in (
            (left_column, left_explicit),
            (right, right_explicit),
        ):
            if not explicit and hasattr(operand, "is_"):
                result = case((operand.isnot(None), result))
        return result

    present = []
    if left_explicit:
        present.append(left_column.isnot(None))
    if right_explicit:
        present.append(right.isnot(None))
    equality = and_(*present, left_column == right)
    if left_explicit and right_explicit:
        equality = or_(and_(left_column.is_(None), right.is_(None)), equality)
    return not_(equality) if operator == "ne" else equality


def with_null_conventions(
    operator: str,
    left: Any,
    right: Any,
    left_explicit: bool,
    right_explicit: bool,
    plain: Any,
    overridden: bool,
) -> Any:
    """Apply the declared NULL conventions to a comparison, else return ``plain``.

    ``plain`` is passed in so an operator override is honoured. An overridden
    ``eq``/``ne`` is left alone, since rewriting it would discard the override.
    ``in`` only gains an ANDed presence guard, which composes with an override.
    """
    if operator == "in" and not left_explicit and hasattr(left, "is_"):
        return case((left.isnot(None), plain))
    if (left_explicit or right_explicit) and right is not None:
        if operator in ("eq", "ne"):
            if not overridden:
                return definite_equality(
                    operator, left, right, left_explicit, right_explicit
                )
        elif (
            operator == "in"
            and left_explicit
            and hasattr(left, "isnot")
            # Literal lists only. A stored collection may hold a null, and
            # `null in coll` is then TRUE, so a presence guard would under-grant.
            and isinstance(right, list)
            # A null member already adds a definite `IS NULL` disjunct.
            and not any(member is None for member in right)
        ):
            return and_(left.isnot(None), plain)
    return plain
