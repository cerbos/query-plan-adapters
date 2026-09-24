# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""The walk from a parsed plan condition to a SQLAlchemy boolean expression.

``get_query`` validates its arguments and hands the condition to ``Translator``. Two mutually
recursive entry points do the work: ``predicate`` for a node in a boolean position and
``value`` for a node that produces a value. Operator semantics live in ``_operators``; this
module decides only which operands each operator is handed, in which order, and when a
caller's override, a declared collection or a NULL convention takes precedence.
"""

from collections.abc import Callable
from typing import Any, NoReturn

from sqlalchemy import DateTime, and_, case, false, not_, or_, true
from sqlalchemy.orm import InstrumentedAttribute
from sqlalchemy.sql.expression import ColumnElement

from cerbos_sqlalchemy._null_conventions import (
    EQUALITY_FAMILY,
    NullAttributeRepresentation,
    with_null_conventions,
)
from cerbos_sqlalchemy._operators import (
    FOLDABLE_COLLECTION_OPERATORS,
    MIRRORED_OPERATORS,
    OPERATOR_FNS,
    ORDER_INSENSITIVE_OPERATORS,
    SYMBOLIC_NUMBERS,
    UNARY_VALUE_OPERATORS,
    ConditionalValue,
    arith_over_conditional,
    require_lowerable,
)
from cerbos_sqlalchemy._plan import (
    LAMBDA_BINDING_OPERATORS,
    Expr,
    Operand,
    Value,
    Variable,
    declared_collection_name,
    substitute_lambda_variable,
)
from cerbos_sqlalchemy.collection_storage import (
    INDEXED_VALUE_REFUSAL,
    MEMBERSHIP_REFUSAL,
    CollectionColumn,
    collection_membership,
    collection_size,
    indexed_equality,
    require_index_position,
)
from cerbos_sqlalchemy.errors import UnsupportedPlanError

_BOOLEAN_OPERATORS = frozenset({"and", "or", "not"})
_MEMBERSHIP_OPERATORS = frozenset({"in", "hasIntersection"})
_ORDERING_AND_EQUALITY = frozenset({"eq", "ne", "lt", "le", "gt", "ge"})


def require_boolean(translated: Any, position: str) -> Any:
    """Refuse a translation that is not a boolean SQL expression.

    Every position that CONSUMES a value as a boolean has to make the same check, not just
    the root. A non-boolean root — filter()/map() as the whole condition, or an operator
    override's intermediate value that no enclosing override consumed — must be refused by
    the adapter rather than left for SQLAlchemy's where() coercion to trip over: a value that
    happened to coerce would become a silently-wrong filter. A `filter()` sitting in a
    conjunct would otherwise reach `and_()`, where SQLAlchemy raises its own ArgumentError
    about a WHERE/HAVING role -- fail-closed, but naming SQLAlchemy's coercion rather than
    the mechanism, which is not a rejection this adapter can pin
    (cerbos/query-plan-adapters#387).

    `InstrumentedAttribute` is accepted alongside `ColumnElement` because a bare boolean
    COLUMN is a legitimate condition: `R.attr.aBool` alone plans to a condition that is a
    bare `{"variable": ...}` with no expression wrapper at all
    (`logic/bare-attribute/boolean`). The ORM attribute is a descriptor rather than a Core
    element, so it fails the ColumnElement check while being exactly what `where()` wants
    (cerbos/query-plan-adapters#388). This widens nothing a mapper could not already reach:
    a Core `Column` IS a `ColumnElement`.
    """
    if not isinstance(translated, (ColumnElement, InstrumentedAttribute, bool)):
        raise UnsupportedPlanError(
            f"the plan's {position} translated to {type(translated).__name__!r}, which "
            "is not a boolean SQL expression. filter() and map() return a list, so they "
            "cannot be a condition on their own (only size(filter(...)) has a boolean "
            "meaning), and an operator override returning an intermediate value must be "
            "consumed by an enclosing override before the root"
        )
    return translated


class Translator:
    def __init__(
        self,
        attr_map: dict[str, Any],
        overrides: dict[str, Callable[[Any, Any], Any]],
        null_conventions: dict[str, NullAttributeRepresentation],
        declared_collections: dict[str, CollectionColumn],
    ) -> None:
        self._attr_map = attr_map
        # Never None, and never holding a None entry: `get_query` drops those, so an
        # operator is overridden exactly when it is a key here.
        self._overrides = overrides
        self._null_conventions = null_conventions
        self._declared_collections = declared_collections
        self._declared_names = frozenset(declared_collections)

    # -- leaves and dispatch ---------------------------------------------------------------

    def _resolve_variable(self, variable: str) -> Any:
        try:
            return self._attr_map[variable]
        except KeyError:
            raise KeyError(
                f"Attribute does not exist in the attribute column map: {variable}"
            )

    def _is_explicit_null(self, variable: str) -> bool:
        return self._null_conventions.get(variable) == "explicit"

    def _apply(self, operator: str, left: Any, right: Any) -> Any:
        """Lower one operator: the caller's override when there is one, else the default."""
        override = self._overrides.get(operator)
        if override is not None:
            return override(left, right)
        default = OPERATOR_FNS.get(operator)
        if default is None:
            raise UnsupportedPlanError(f"Unrecognised operator: {operator}")
        require_lowerable(operator, left)
        require_lowerable(operator, right)
        return default(left, right)

    def _resolve(self, operand: Operand) -> Any:
        """Resolve a literal, mapped variable, or nested value expression."""
        if isinstance(operand, Value):
            return operand.value
        if isinstance(operand, Variable):
            return self._resolve_variable(operand.name)
        return self.value(operand)

    # -- collection macros over a literal list ---------------------------------------------

    def _fold_value_list_macro(
        self, operator: str, elements: Any, lambda_operand: Operand
    ) -> Any:
        """Fold a collection macro whose collection operand is a literal value list.

        The planner emits this shape when a known-value collection (typically a
        folded principal attribute) has more than 10 elements — at 10 or fewer
        it unrolls `exists`/`all` into an or/and chain itself
        (cerbos/cerbos#2570, cerbos/cerbos#2817; `maxItems = 10` in the
        planner's struct matcher). Apply the same fold here, uncapped, so the
        translated query does not depend on which side of that threshold the
        collection lands: substitute each element into the lambda body and
        combine the per-element predicates with OR (`exists`) or AND (`all`).

        Each substituted body goes back through the ordinary traversal, so
        comparison semantics — operator overrides, three-valued NULL handling,
        value-first mirroring — are identical to a planner-unrolled chain of
        the same comparisons.
        """
        if operator not in FOLDABLE_COLLECTION_OPERATORS:
            raise UnsupportedPlanError(
                f"{operator} over a literal collection value is not supported. "
                "Only exists() and all() can be folded into a flat filter."
            )
        if not isinstance(elements, list):
            raise UnsupportedPlanError(
                f"{operator} over a literal collection requires a list value"
            )

        if not isinstance(lambda_operand, Expr) or lambda_operand.operator != "lambda":
            raise UnsupportedPlanError(
                f"Second operand of {operator} must be a lambda expression"
            )
        lambda_operands = lambda_operand.operands
        if len(lambda_operands) != 2:
            raise UnsupportedPlanError(
                f"{operator} over a literal collection supports single-variable "
                "lambdas only"
            )
        body, variable = lambda_operands
        if not isinstance(variable, Variable) or not variable.name:
            raise UnsupportedPlanError("Lambda variable must have a name")

        predicates = [
            self.predicate(substitute_lambda_variable(body, variable.name, element))
            for element in elements
        ]
        if not predicates:
            # CEL identity semantics over an empty collection: exists() matches
            # nothing, all() matches everything.
            return false() if operator == "exists" else true()
        return or_(*predicates) if operator == "exists" else and_(*predicates)

    def _try_fold_value_list_macro(
        self, operator: str, operands: tuple[Operand, ...]
    ) -> Any:
        """Return the folded predicate for a value-list macro, else None.

        A literal value list can never be a relation marker or a column, so no
        operator override can meaningfully consume it — fold it here instead.
        """
        if operator not in LAMBDA_BINDING_OPERATORS or len(operands) != 2:
            return None
        collection, lambda_operand = operands
        if not isinstance(collection, Value):
            return None
        return self._fold_value_list_macro(operator, collection.value, lambda_operand)

    # -- declared collection storage -------------------------------------------------------

    def _declared_collection(self, expression: Expr) -> CollectionColumn | None:
        """The declared storage ``expression`` reads, when it is ``size``/``index`` of one."""
        name = declared_collection_name(expression, self._declared_names)
        return None if name is None else self._declared_collections[name]

    def _declared_index(self, operand: Operand) -> tuple[CollectionColumn, int] | None:
        """``(storage, position)`` when ``operand`` indexes a declared collection, else None."""
        if not isinstance(operand, Expr) or operand.operator != "index":
            return None
        declared = self._declared_collection(operand)
        if declared is None:
            return None
        if len(operand.operands) != 2 or not isinstance(operand.operands[1], Value):
            # A dynamic index is not a position the translator can check, and CEL raises for
            # every position it would reject.
            require_index_position(None)
        return declared, require_index_position(operand.operands[1].value)

    def _reads_declared_index(self, operands: tuple[Operand, ...]) -> bool:
        return any(self._declared_index(child) is not None for child in operands)

    def _indexed_comparison(self, operator: str, operands: tuple[Operand, ...]) -> Any:
        """``collection[i] == literal`` or ``!=``: the only comparisons an element supports.

        Anything else over an element is refused. An ordering, a membership or a nested value
        expression would need the element's JSON type and its index error carried through
        SQL that has neither, and only the literal equality below keeps both.
        """
        indexed = [self._declared_index(child) for child in operands]
        position = next(i for i, found in enumerate(indexed) if found is not None)
        declared, index = indexed[position]
        others = [child for i, child in enumerate(operands) if i != position]
        if (
            operator not in ("eq", "ne")
            or len(others) != 1
            or not isinstance(others[0], Value)
        ):
            raise UnsupportedPlanError(INDEXED_VALUE_REFUSAL)
        equality = indexed_equality(declared, index, others[0].value)
        # NOT keeps the CASE's UNKNOWN for an absent element, so a negated comparison still
        # denies the row CEL's index error denies.
        return not_(equality) if operator == "ne" else equality

    def _declared_collection_value(
        self, expression: Expr, declared: CollectionColumn
    ) -> Any:
        """``size()`` of a declared collection, or the refusal for an element read as a value."""
        if expression.operator == "index":
            # Reached only where an element is a VALUE -- not a direct comparison, which
            # `_indexed_comparison` took -- so `_declared_index` has checked the position and
            # there is nothing left to translate.
            self._declared_index(expression)
            raise UnsupportedPlanError(
                f"{INDEXED_VALUE_REFUSAL}; nested value expressions cannot preserve "
                "element types and index errors"
            )
        if len(expression.operands) != 1:
            raise UnsupportedPlanError(
                f"size takes 1 operand, got {len(expression.operands)}"
            )
        return collection_size(declared)

    def _storage_only_collection(self, operand: Operand) -> CollectionColumn | None:
        """The declared storage of an attribute ``attr_map`` does not map, else None.

        Membership reads a declaration only for such an attribute. One ``attr_map`` also maps
        keeps its membership there -- typically a relation marker an override translates -- so
        a declaration added for ``size()`` and ``index`` never changes how membership is read.
        """
        if (
            isinstance(operand, Variable)
            and operand.name in self._declared_collections
            and operand.name not in self._attr_map
        ):
            return self._declared_collections[operand.name]
        return None

    def _declared_membership(self, operator: str, operands: tuple[Operand, ...]) -> Any:
        """``literal in collection`` or ``hasIntersection`` over a declared collection, else None.

        Only a scalar literal element, or a literal list of them, is translated: an element that
        is itself a column or an expression would have to be compared with a JSON element
        whose type it cannot see, so it is refused rather than coerced.
        """
        if operator not in _MEMBERSHIP_OPERATORS or len(operands) != 2:
            return None
        declared = [self._storage_only_collection(operand) for operand in operands]
        if not any(found is not None for found in declared):
            return None
        if operator == "in":
            needle, _collection = operands
            if declared[1] is None or not isinstance(needle, Value):
                raise UnsupportedPlanError(MEMBERSHIP_REFUSAL)
            if isinstance(needle.value, (list, dict)):
                raise UnsupportedPlanError(MEMBERSHIP_REFUSAL)
            return collection_membership(declared[1], [needle.value])
        # hasIntersection is symmetric, and the planner keeps the policy's operand order.
        position = 0 if declared[0] is not None else 1
        values = operands[1 - position]
        if not isinstance(values, Value) or not isinstance(values.value, list):
            raise UnsupportedPlanError(MEMBERSHIP_REFUSAL)
        if any(isinstance(value, (list, dict)) for value in values.value):
            raise UnsupportedPlanError(MEMBERSHIP_REFUSAL)
        return collection_membership(declared[position], values.value)

    @staticmethod
    def _refuse_undeclared_index(collection: Operand | None) -> NoReturn:
        if isinstance(collection, Variable):
            raise UnsupportedPlanError(
                f"Index storage shape is undeclared for '{collection.name}': declare it "
                'in collection_columns with storage "json" or "pgArray"; a relation has '
                "no positional order"
            )
        raise UnsupportedPlanError(
            "Index access requires a collection attribute declared in "
            "collection_columns; a computed collection has no storage to read"
        )

    # -- value positions -------------------------------------------------------------------

    def _ternary(self, operands: tuple[Operand, ...]) -> Any:
        """``if(cond, then, else)``; the condition may be an expression or a bare leaf.

        Three-valued logic: when the condition is UNKNOWN (e.g. a NULL column), CEL raises
        a missing-attribute error — a deny — so the SQL result must be NULL, not the
        else-branch. A CASE with only a WHEN-cond/WHEN-not-cond pair (no ELSE) yields NULL
        for UNKNOWN conditions, keeping the row excluded under BOTH polarities
        (`NOT (NULL > 1)` stays UNKNOWN instead of leaking to TRUE).
        """
        condition = self.predicate(operands[0])
        then_value = self._resolve(operands[1])
        else_value = self._resolve(operands[2])
        if isinstance(then_value, SYMBOLIC_NUMBERS) or isinstance(
            else_value, SYMBOLIC_NUMBERS
        ):
            return ConditionalValue(condition, then_value, else_value)
        return case((condition, then_value), (not_(condition), else_value))

    def _binary_value(self, operator: str, operands: tuple[Operand, ...]) -> Any:
        """add/sub/mult/div/mod, plus any caller override, over two resolved operands.

        Operands are passed in wire (source) order, which is significant for
        non-commutative operators (sub/div) and receiver-style string ops.
        """
        left = self._resolve(operands[0])
        right = self._resolve(operands[1])
        if not (
            isinstance(left, SYMBOLIC_NUMBERS) or isinstance(right, SYMBOLIC_NUMBERS)
        ):
            return self._apply(operator, left, right)
        if operator == "mod":
            # CEL's % is integer-only while Cerbos attribute values are always
            # doubles, so a modulus over this arithmetic is a no-overload error
            # that denies every row at check time. Folding it with Python's %
            # would answer a question CEL refused.
            raise UnsupportedPlanError(
                "modulus over a division whose denominator may be zero is not "
                "supported: CEL's % is integer-only and attribute values are "
                "always doubles, so the condition can never be satisfied by the PDP"
            )
        # A retained ternary (a division that may be non-finite) must keep
        # propagating symbolically through the surrounding arithmetic.
        return arith_over_conditional(
            lambda a, b: self._apply(operator, a, b), left, right
        )

    def value(self, expression: Expr) -> Any:
        """Evaluate a value-producing node, retaining predicate traversal where needed."""
        operator = expression.operator
        operands = expression.operands
        if operator == "if":
            return self._ternary(operands)

        # A declared collection is read from its storage before an override or attr_map is
        # consulted: the declaration names this attribute, an override only its operator.
        if self._reads_declared_index(operands):
            return self._indexed_comparison(operator, operands)
        declared = self._declared_collection(expression)
        if declared is not None:
            return self._declared_collection_value(expression, declared)
        if operator == "index" and "index" not in self._overrides:
            self._refuse_undeclared_index(operands[0] if operands else None)
        membership = self._declared_membership(operator, operands)
        if membership is not None:
            return membership

        if operator in EQUALITY_FAMILY and len(operands) == 2 and _all_leaves(operands):
            # A lambda body is evaluated as a value. Preserve the same per-attribute
            # NULL conventions that apply when this leaf is at the filter root.
            return self.predicate(expression)

        # Boolean combinators can appear nested inside value expressions
        # (e.g. a lambda body of `and(...)`); route them back through the
        # predicate traversal rather than treating them as binary operators.
        if operator in _BOOLEAN_OPERATORS:
            return self.predicate(expression)

        folded = self._try_fold_value_list_macro(operator, operands)
        if folded is not None:
            return folded

        if operator == "hierarchy":
            target = self._resolve(operands[0])
            delimiter = self._resolve(operands[1]) if len(operands) == 2 else None
            return self._apply(operator, target, delimiter)

        if operator in UNARY_VALUE_OPERATORS:
            return self._apply(operator, self._resolve(operands[0]), None)

        if len(operands) < 2:
            # e.g. timestamp(...) — a planner shape with no SQL translation.
            raise UnsupportedPlanError(f"Unrecognised unary operator: {operator}")

        return self._binary_value(operator, operands)

    # -- boolean positions -----------------------------------------------------------------

    def predicate(self, operand: Operand) -> Any:
        """Translate a node in a boolean position."""
        # Bare leaves in a boolean position resolve directly.
        if isinstance(operand, Variable):
            return self._resolve_variable(operand.name)
        if isinstance(operand, Value):
            return operand.value

        operator = operand.operator
        operands = operand.operands

        if operator in _BOOLEAN_OPERATORS:
            branches = [
                require_boolean(self.predicate(o), f"{operator!r} operand")
                for o in operands
            ]
            if operator == "and":
                return and_(*branches)
            if operator == "or":
                return or_(*branches)
            return not_(*branches)
        if operator == "if":
            # A bare boolean-result ternary used directly as a predicate.
            return self.value(operand)
        if operator == "index" or self._declared_collection(operand) is not None:
            # An element or a size in a boolean position is a value, and refused or
            # translated as one.
            return self.value(operand)

        # A literal value list arrives when the planner could not unroll a
        # macro over a known collection (more than 10 elements). Fold it before
        # override dispatch: overrides exist to translate relation/column
        # collections, which a literal can never be.
        folded = self._try_fold_value_list_macro(operator, operands)
        if folded is not None:
            return folded

        if self._reads_declared_index(operands):
            return self._indexed_comparison(operator, operands)
        membership = self._declared_membership(operator, operands)
        if membership is not None:
            return membership

        has_nested_expression = not _all_leaves(operands)

        # An overridden operator over anything but a plain `leaf op leaf` pair (e.g.
        # size(tags) where tags is a collection) gets its operands resolved and handed to
        # the override directly.
        override = self._overrides.get(operator)
        if override is not None and (has_nested_expression or len(operands) != 2):
            resolved = [self._resolve(o) for o in operands]
            if len(resolved) == 1:
                resolved.append(None)
            return override(*resolved)

        # Boolean leaf operators take exactly two operands. Either side may be
        # a nested value-producing expression (arithmetic, cast, ternary, ...).
        if len(operands) == 2 and has_nested_expression:
            left = self._resolve(operands[0])
            right = self._resolve(operands[1])
            return self._apply(operator, left, right)

        # Both operands are variables or literals. Their order is not guaranteed
        # to be variable-first: the planner preserves policy source order
        # (`1 < R.attr.x` arrives value-first).
        left_operand, right_operand = operands
        if isinstance(left_operand, Variable) and isinstance(right_operand, Variable):
            return self._field_to_field(operator, left_operand, right_operand)
        if isinstance(left_operand, Value) and isinstance(right_operand, Value):
            # Both sides constant (rare; the planner usually folds these).
            return self._apply(operator, left_operand.value, right_operand.value)
        if isinstance(left_operand, Value):
            return self._value_first(operator, left_operand, right_operand)
        return self._column_first(operator, left_operand, right_operand)

    def _with_null_conventions(
        self,
        operator: str,
        left: Any,
        right: Any,
        left_explicit: bool,
        right_explicit: bool,
    ) -> Any:
        return with_null_conventions(
            operator,
            left,
            right,
            left_explicit,
            right_explicit,
            plain=self._apply(operator, left, right),
            overridden=operator in self._overrides,
        )

    def _field_to_field(self, operator: str, left: Variable, right: Variable) -> Any:
        """Both sides are columns (`R.attr.a == R.attr.b`).

        Wire order is preserved; SQL three-valued logic keeps rows with a NULL side
        excluded, matching CEL's missing-attribute deny.
        """
        left_column = self._resolve_variable(left.name)
        right_column = self._resolve_variable(right.name)
        if operator in _ORDERING_AND_EQUALITY and any(
            isinstance(getattr(column, "type", None), DateTime)
            for column in (left_column, right_column)
        ):
            raise UnsupportedPlanError(
                "Bare temporal attributes compare RFC 3339 strings in CEL; stored "
                "timestamps lose the original spelling; use timestamp() on both operands"
            )
        # Mixing the two conventions across one comparison has no faithful
        # rendering. The declared side needs a definite answer for its NULL
        # (CEL holds a null VALUE); the undeclared side needs UNKNOWN for its
        # NULL (a missing attribute, which CEL denies under both polarities).
        # A definite predicate returns rows the PDP refuses; a plain one drops
        # rows the PDP allows. Refuse it rather than pick a direction --
        # declare both attributes, or neither.
        left_explicit = self._is_explicit_null(left.name)
        right_explicit = self._is_explicit_null(right.name)
        if left_explicit != right_explicit and operator in ("eq", "ne"):
            raise UnsupportedPlanError(
                f"Cannot translate `{operator}` between two columns under mixed "
                "null conventions: cannot compare an attribute declared "
                "explicit-null with one on the omitted convention: the omitted "
                "side is UNKNOWN for a NULL column while the declared side is "
                "definite, and no single predicate is both. Declare "
                "attribute_null_representation for both attributes, or for "
                "neither."
            )
        return self._with_null_conventions(
            operator, left_column, right_column, left_explicit, right_explicit
        )

    def _value_first(self, operator: str, value: Value, variable: Variable) -> Any:
        column = self._resolve_variable(variable.name)
        if operator in MIRRORED_OPERATORS:
            # Directional: `1 < R.attr.x` means `x > 1`.
            return self._apply(MIRRORED_OPERATORS[operator], column, value.value)
        if operator in ORDER_INSENSITIVE_OPERATORS:
            return self._with_null_conventions(
                operator,
                column,
                value.value,
                self._is_explicit_null(variable.name),
                False,
            )
        # Receiver-sensitive (contains/startsWith/endsWith/...): keep wire
        # order — the value is the receiver, the column the argument.
        return self._apply(operator, value.value, column)

    def _column_first(self, operator: str, variable: Variable, value: Value) -> Any:
        column = self._resolve_variable(variable.name)
        return self._with_null_conventions(
            operator,
            column,
            value.value,
            self._is_explicit_null(variable.name),
            False,
        )


def _all_leaves(operands: tuple[Operand, ...]) -> bool:
    return all(isinstance(operand, (Variable, Value)) for operand in operands)
