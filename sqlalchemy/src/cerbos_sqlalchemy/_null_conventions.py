"""How a caller represents a NULL column, and what that does to a comparison.

The planner emits the same ``eq(attr, null)`` node whichever convention the caller uses, so
``get_query`` is told: once per call (``null_attribute_representation``) and optionally per
attribute (``attribute_null_representation``). See cerbos/query-plan-adapters#302 and #308.
"""

from __future__ import annotations

from typing import Any, Dict, Literal, Tuple, Union

from cerbos_sqlalchemy._operators import scalar_kind
from cerbos_sqlalchemy._plan import Expr, Operand, Value, Variable
from sqlalchemy import and_, case, literal, not_, or_

# How the caller represents a NULL column when building the attributes it sends
# to check(). See get_query() and
# https://github.com/cerbos/query-plan-adapters/issues/302.
NullAttributeRepresentation = Literal["explicit", "omitted"]

_REPRESENTATIONS = ("explicit", "omitted")

# The operators CEL evaluates to a definite boolean over a null value, and so the
# only ones an attribute's declared convention can settle. Anything else -- a
# collection macro, hasIntersection, a string match -- keeps using the call-level
# fallback, because the declaration says nothing about what its null means there.
EQUALITY_FAMILY = frozenset({"eq", "ne", "in"})


def validate_representations(
    null_attribute_representation: Any,
    attribute_null_representation: Union[Dict[str, Any], None],
    attr_map: Dict[str, Any],
) -> Dict[str, NullAttributeRepresentation]:
    """Check both options and return the per-attribute declarations (empty when absent)."""
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


def _compared_attribute_and_literal(node: Operand) -> Union[Tuple[str, Value], None]:
    """Destructure a binary comparison between a plan variable and a literal.

    Returns ``(variable_name, literal_operand)`` in either operand order, or
    ``None`` when the node is not that shape.
    """
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


def _null_operand_error(operator: str) -> ValueError:
    return ValueError(
        f"Cannot translate `{operator}` against a null operand under "
        'null_attribute_representation="omitted": a NULL column sends no '
        "attribute, so Cerbos evaluates the comparison as a missing-attribute "
        "error (deny) while a NULL-selecting filter would return those rows. "
        'Send NULL columns as explicit nulls and use "explicit", or keep this '
        "shape out of the policy."
    )


def assert_no_null_comparison_operands(
    node: Operand,
    declarations: Dict[str, NullAttributeRepresentation],
    fallback: str,
) -> None:
    """Reject every null literal operand under the ``omitted`` representation.

    A NULL column then carries no attribute at all, so CEL raises a
    missing-attribute error and ``check()`` denies the row -- ``IS NULL`` would
    return exactly the rows the PDP refuses (cerbos/query-plan-adapters#302).

    The scan matches on the OPERAND, never on an allowlist of operators. A null
    constant reaches a NULL-selecting predicate through more shapes than the
    obvious ``eq``/``ne``/``in`` -- ``hasIntersection`` carries one in its value
    list too -- and any operator added later would silently escape a list that
    has to be maintained by hand.

    The rejection is also deliberately wider than the over-granting shapes:
    ``ne(x, null)`` on its own is aligned, but negation is applied around the
    built predicate rather than pushed into the leaf, so a leaf cannot tell
    whether an enclosing ``not`` will flip ``IS NOT NULL`` back into a
    NULL-selecting predicate. Rejecting every null operand is correct under any
    nesting; narrowing it requires negation-parity tracking.
    """
    if not isinstance(node, Expr):
        return
    operator = node.operator
    operands = node.operands

    # A comparison between a mapped attribute and a literal is decided by that
    # attribute's own declaration, which is what lets one call carry both
    # conventions (cerbos/query-plan-adapters#308). Confined to that shape: a
    # null buried in a macro over a literal list reaches a comparison long
    # after this scan, and nothing here can say which column it will land
    # against, so those keep using the call-level fallback.
    compared = _compared_attribute_and_literal(node)
    if compared is not None:
        variable, value = compared
        declared = declarations.get(variable)
        if declared is not None:
            if declared == "omitted" and _carries_null_operand(value):
                raise _null_operand_error(operator)
            return

    if fallback == "omitted" and any(_carries_null_operand(o) for o in operands):
        raise _null_operand_error(operator)
    for operand in operands:
        assert_no_null_comparison_operands(operand, declarations, fallback)


def definite_equality(
    operator: str,
    left_column: Any,
    right: Any,
    left_explicit: bool,
    right_explicit: bool,
) -> Any:
    """Render an equality that can never be SQL UNKNOWN.

    An attribute the caller sends as an explicit null holds a null VALUE in
    CEL, so equality against a non-null operand is a definite FALSE,
    inequality a definite TRUE, and two nulls are EQUAL. SQL answers
    UNKNOWN to all three, which excludes the row under BOTH polarities --
    so the NOT an enclosing negation applies has nothing definite to flip.

    Deliberately not ``is_distinct_from``. Two reasons, and the second is
    the load-bearing one: the same expression has to render on SQLite,
    PostgreSQL and MySQL -- and a null-safe equality is SYMMETRIC while this
    rewrite must not be. When only ONE side declares the convention, the
    other side's NULL is a MISSING attribute on the check side, so CEL raises
    an error and denies; only the asymmetric expansion below keeps
    propagating UNKNOWN for it. A null-safe operator would match the two
    NULLs and over-grant.
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
    """The comparison with the declared NULL conventions applied, else ``plain``.

    ``plain`` is the ordinary lowering the caller would otherwise return --
    passed in rather than rebuilt here, so a registered operator override is
    honoured on every path.

    ``eq``/``ne`` RESTRUCTURE the comparison, so an operator the caller
    overrode (``overridden``) is left alone: replacing it would make this
    declaration silently discard the caller's own translation, which is not
    what it declares. ``in`` only gains a presence guard ANDed alongside
    whatever the membership lowered to, which composes with an override rather
    than replacing it.
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
            # A stored COLLECTION, not a literal list: a null element can
            # exist at run time and `null in coll` is TRUE when it does, so
            # the presence guard would exclude exactly the rows CEL allows.
            # The collection's own lowering already handles the null member.
            and isinstance(right, list)
            # A null member already forces the `IS NULL` disjunct, which is
            # definite on its own.
            and not any(member is None for member in right)
        ):
            return and_(left.isnot(None), plain)
    return plain
