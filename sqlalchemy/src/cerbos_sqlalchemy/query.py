# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""``get_query``: the public entry point, its types and argument validation."""

from collections.abc import Callable
from typing import Any, ClassVar, Protocol, TypeVar, overload

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from google.protobuf.json_format import MessageToDict
from sqlalchemy import Column, Table, select
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import (
    BinaryExpression,
    ColumnElement,
    ColumnOperators,
    FromClause,
)

from cerbos_sqlalchemy._null_conventions import (
    NullAttributeRepresentation,
    assert_no_null_comparison_operands,
    validate_representations,
)

# Re-exported as the public read-only map of default handlers.
from cerbos_sqlalchemy._operators import OPERATOR_FNS  # noqa: F401
from cerbos_sqlalchemy._plan import (
    Expr,
    Operand,
    Value,
    assert_no_same_collection_correlation,
    declared_collection_name,
    parse_operand,
)
from cerbos_sqlalchemy._translator import Translator, require_boolean
from cerbos_sqlalchemy.collection_storage import (  # noqa: F401 - re-exported for callers
    INDEXED_VALUE_REFUSAL,
    CollectionColumn,
    collection_size,
    indexed_equality,
    require_index_position,
)
from cerbos_sqlalchemy.errors import UnsupportedPlanError

try:  # SQLAlchemy >= 2.0
    from sqlalchemy.orm import DeclarativeBase
except ImportError:  # SQLAlchemy 1.4 predates the class-based declarative base.

    class DeclarativeBase:  # type: ignore[no-redef]
        """Stand-in so ``GenericTable`` stays constructible under SQLAlchemy 1.4."""


class _MappedClass(Protocol):
    """An ORM model with a mapped ``__table__``.

    A protocol because the two declarative styles share no base class.
    """

    __table__: ClassVar[FromClause]


_ORMModel = TypeVar("_ORMModel", bound=_MappedClass)

# A 2.0-style model's metaclass is not a DeclarativeMeta, hence DeclarativeBase.
GenericTable = Table | DeclarativeMeta | type[DeclarativeBase]
GenericColumn = Column | InstrumentedAttribute
GenericExpression = BinaryExpression | ColumnOperators
OperatorFnMap = dict[str, Callable[[GenericColumn, Any], GenericExpression]]

# Accept both the HTTP and gRPC clients' kinds.
_DENY_KINDS = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_DENIED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_DENIED,
    ]
)
_ALLOW_KINDS = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_ALLOWED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_ALLOWED,
    ]
)

# Built-in traversal that overrides cannot replace.
_UNOVERRIDABLE_OPERATORS = frozenset({"and", "or", "not", "if"})


class _UnhandledRelationError(UnsupportedPlanError, TypeError):
    """Raised when a relation marker is not consumed by any operator override.

    Also a ``TypeError``, which this refusal raised before ``UnsupportedPlanError``.
    """


def _validate_collection_columns(
    collection_columns: dict[str, CollectionColumn] | None,
) -> dict[str, CollectionColumn]:
    declared_collections = dict(collection_columns or {})
    for attribute, declared in declared_collections.items():
        if not isinstance(declared, CollectionColumn):
            raise TypeError(
                "collection_columns values must be CollectionColumn, got "
                f"{type(declared).__name__} for {attribute!r}"
            )
    return declared_collections


def _plan_condition(
    query_plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,
) -> Operand:
    return parse_operand(
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else query_plan.filter.condition.to_dict()
    )


def _table_name(t: GenericTable) -> str:
    try:
        # ORM model
        return t.__table__.name
    except AttributeError:
        # Core `Table` type
        return t.name


def _variables_outside_overrides(
    operand: Operand,
    override_operators: frozenset[str],
    declared: frozenset[str],
    override_owned: bool = False,
) -> frozenset[str]:
    """Find variables that still need an ordinary table mapping.

    An override owns its whole operand subtree, which may use correlated
    subqueries instead of joins. A declared collection operand is skipped because
    its column is validated separately.
    """
    if isinstance(operand, Value):
        return frozenset()
    if not isinstance(operand, Expr):
        return frozenset() if override_owned else frozenset({operand.name})

    owns_children = override_owned or (
        operand.operator in override_operators
        and operand.operator not in _UNOVERRIDABLE_OPERATORS
    )
    children = operand.operands
    if declared_collection_name(operand, declared) is not None:
        children = children[1:]
    variables: frozenset[str] = frozenset()
    for child in children:
        variables |= _variables_outside_overrides(
            child, override_operators, declared, owns_children
        )
    return variables


def _require_table_mapping(
    table: GenericTable,
    attr_map: dict[str, GenericColumn],
    table_mapping: list[tuple[GenericTable, GenericExpression]] | None,
    overrides: dict[str, Any] | None,
    condition: Operand,
    declared_collections: dict[str, CollectionColumn],
) -> None:
    """Refuse a column on a table that is neither queried nor joined in.

    Only columns outside override-owned subtrees are checked, so an unrelated
    override cannot switch the check off. With no overrides, every entry is checked.
    """
    if overrides is None:
        attributes = list(attr_map.items())
    else:
        variables = _variables_outside_overrides(
            condition, frozenset(overrides), frozenset(declared_collections)
        )
        attributes = [
            (variable, attr_map[variable])
            for variable in variables
            if variable in attr_map
        ]
    # Declared collection columns must also be on the queried or a joined table.
    attributes += [
        (attribute, declared.column)
        for attribute, declared in declared_collections.items()
    ]

    required_tables = set()
    for variable, column in attributes:
        column_table = getattr(column, "table", None)
        if column_table is None:
            # A self-contained expression, e.g. a correlated scalar subquery, needs
            # no join. A missing hop makes it NULL, which stays excluded. See #375.
            if isinstance(column, ColumnElement):
                continue
            raise _UnhandledRelationError(
                f"Attribute '{variable}' must be handled by an operator override "
                "or map to a SQLAlchemy column"
            )
        if column_table.name != _table_name(table):
            required_tables.add(column_table.name)

    if not required_tables:
        return
    if table_mapping is None:
        raise TypeError(
            "get_query() missing 1 required positional argument: 'table_mapping'"
        )
    required_tables -= {_table_name(mapped_table) for mapped_table, _ in table_mapping}
    if required_tables:
        raise TypeError(
            "positional argument 'table_mapping' missing mapping for table(s): '{}'".format(
                "', '".join(sorted(required_tables))
            )
        )


# Overloads let an ORM model's row type be inferred. `Select[...]` is quoted because
# it is not subscriptable on early SQLAlchemy 1.4.
@overload
def get_query(
    query_plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: type[_ORMModel],
    attr_map: dict[str, GenericColumn],
    table_mapping: list[tuple[GenericTable, GenericExpression]] | None = ...,
    operator_override_fns: OperatorFnMap | None = ...,
    null_attribute_representation: NullAttributeRepresentation = ...,
    attribute_null_representation: dict[str, NullAttributeRepresentation] | None = ...,
    collection_columns: dict[str, CollectionColumn] | None = ...,
) -> "Select[tuple[_ORMModel]]": ...


# Core `Table`, and 1.4 legacy models whose stubs lack `__table__`.
@overload
def get_query(
    query_plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: GenericTable,
    attr_map: dict[str, GenericColumn],
    table_mapping: list[tuple[GenericTable, GenericExpression]] | None = ...,
    operator_override_fns: OperatorFnMap | None = ...,
    null_attribute_representation: NullAttributeRepresentation = ...,
    attribute_null_representation: dict[str, NullAttributeRepresentation] | None = ...,
    collection_columns: dict[str, CollectionColumn] | None = ...,
) -> "Select[Any]": ...


def get_query(
    query_plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    table: GenericTable,
    attr_map: dict[str, GenericColumn],
    table_mapping: list[tuple[GenericTable, GenericExpression]] | None = None,
    operator_override_fns: OperatorFnMap | None = None,
    null_attribute_representation: NullAttributeRepresentation = "explicit",
    attribute_null_representation: dict[str, NullAttributeRepresentation] | None = None,
    collection_columns: dict[str, CollectionColumn] | None = None,
) -> "Select[Any]":
    """Translate a Cerbos query plan into a SQLAlchemy ``Select``.

    Args:
        query_plan: A plan response from the HTTP or gRPC Cerbos client.
        table: The ORM model or Core ``Table`` to select from.
        attr_map: Maps each plan attribute reference, e.g. ``request.resource.attr.x``,
            to a column or SQL expression.
        table_mapping: ``(table, join_predicate)`` pairs for every other table the
            mapped columns live on.
        operator_override_fns: Per-call handlers that replace the default lowering
            of an operator. An overridden operator owns its operand subtree.
        null_attribute_representation: How a NULL column is sent to ``check()``.
            The plan is the same either way, so the caller must say. ``"explicit"``
            (default) sends a ``null`` attribute, so ``IS NULL`` matches.
            ``"omitted"`` sends no attribute, which CEL denies, so null comparison
            operands are rejected rather than translated.
        attribute_null_representation: The same, per attribute, overriding the
            call-level value. Use it when a policy mixes conventions. An
            ``"explicit"`` attribute renders ``eq``, ``ne`` and ``in`` so they are
            never SQL UNKNOWN. Undeclared attributes are rendered as NOT NULL.
            See #302 and #308.
        collection_columns: How collection attributes are stored. Used for
            ``size()`` and constant ``index`` reads, where it beats ``attr_map``
            and overrides, and for literal ``in``/``hasIntersection`` on attributes
            absent from ``attr_map``. Only ``==``/``!=`` against a scalar literal
            is translated for an index. SQLite and PostgreSQL only. See #227.

    Returns:
        A ``Select`` over ``table`` filtered to the rows the plan allows.

    Raises:
        UnsupportedPlanError: If the plan contains a shape the adapter cannot
            translate faithfully. A subclass of ``ValueError``.
        ValueError: If an option value is invalid.
        KeyError: If the plan references an attribute missing from ``attr_map``.
        TypeError: If a mapped column's table is not joined via ``table_mapping``,
            or a ``collection_columns`` value is not a ``CollectionColumn``.
    """
    # Drop None entries, but keep None distinct from {} for _require_table_mapping.
    overrides = (
        None
        if operator_override_fns is None
        else {
            operator: override
            for operator, override in operator_override_fns.items()
            if override is not None
        }
    )
    null_conventions = validate_representations(
        null_attribute_representation, attribute_null_representation, attr_map
    )
    declared_collections = _validate_collection_columns(collection_columns)

    if query_plan.filter is None or query_plan.filter.kind in _DENY_KINDS:
        return select(table).where(False)

    if query_plan.filter.kind in _ALLOW_KINDS:
        return select(table)

    condition = _plan_condition(query_plan)
    # Always run: an attribute can declare "omitted" under an "explicit" call.
    assert_no_null_comparison_operands(
        condition, null_conventions, null_attribute_representation
    )
    assert_no_same_collection_correlation(condition)
    _require_table_mapping(
        table, attr_map, table_mapping, overrides, condition, declared_collections
    )

    translator = Translator(
        attr_map, overrides or {}, null_conventions, declared_collections
    )
    where = require_boolean(translator.predicate(condition), "condition")
    query = select(table).where(where)

    if table_mapping:
        query = query.select_from(table)
        for join_table, predicate in table_mapping:
            query = query.join(join_table, predicate)

    return query
