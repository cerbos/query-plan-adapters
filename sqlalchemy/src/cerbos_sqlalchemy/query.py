# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""``get_query``: the public entry point, its types, and the validation of its arguments.

The translation itself lives in private modules: ``_plan`` decodes the wire, ``_operators``
holds every default operator lowering, ``_null_conventions`` the NULL-column conventions, and
``_translator`` walks the condition tree.
"""

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

# Re-exported: `cerbos_sqlalchemy.query.OPERATOR_FNS` is the read-only map of default handlers.
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
from cerbos_sqlalchemy.collection_storage import (  # noqa: F401 - historically importable here
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
    """What `get_query` actually needs of an ORM model: a mapped `__table__`.

    Structural rather than nominal because the two declarative styles share no
    base class. Bounding the overload's TypeVar on it keeps unmapped classes out
    — unbounded, `Type[_ORMModel]` would admit any class at all, which is looser
    than the union it replaced.
    """

    __table__: ClassVar[FromClause]


_ORMModel = TypeVar("_ORMModel", bound=_MappedClass)

# A 2.0-style model's metaclass (`DeclarativeAttributeIntercept`) is *not* a
# `DeclarativeMeta`, so the legacy member alone does not admit it.
GenericTable = Table | DeclarativeMeta | type[DeclarativeBase]
GenericColumn = Column | InstrumentedAttribute
GenericExpression = BinaryExpression | ColumnOperators
OperatorFnMap = dict[str, Callable[[GenericColumn, Any], GenericExpression]]

# We support both the legacy HTTP and gRPC clients, so therefore we need to accept both input types
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

# Boolean/ternary traversal is built in and cannot itself be overridden.
_UNOVERRIDABLE_OPERATORS = frozenset({"and", "or", "not", "if"})


class _UnhandledRelationError(UnsupportedPlanError, TypeError):
    """A plan reaching a relation marker no operator override consumes.

    Also a ``TypeError``, which is what this refusal raised before
    :class:`UnsupportedPlanError` existed.
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
        # ORM model — both declarative styles carry the mapped `Table` here
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
    """Find variables that still require an ordinary table mapping.

    An override owns its complete operand subtree: it may turn foreign columns
    or relation markers into a correlated subquery instead of a flat JOIN.
    Variables outside such a subtree retain the normal fail-closed
    ``table_mapping`` requirement. Boolean/ternary traversal is built in and
    cannot itself be overridden, so merely declaring those keys owns nothing.

    A collection read through its ``collection_columns`` declaration needs no
    ``attr_map`` entry at all -- the declared column is validated on its own --
    so that operand is skipped whoever owns the subtree.
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
    """Refuse a column on a table that is neither the queried one nor joined in.

    Inspect columns that the normal translator owns. Override-owned operands
    may legitimately be relation markers or columns translated into
    correlated subqueries, but an unrelated override must never disable the
    ordinary cross-table mapping requirement. Omitting ``operator_override_fns``
    validates every ``attr_map`` entry.
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
    # A declared collection column has to be addressable exactly as a mapped one does: on the
    # queried table, or on one `table_mapping` joins.
    attributes += [
        (attribute, declared.column)
        for attribute, declared in declared_collections.items()
    ]

    required_tables = set()
    for variable, column in attributes:
        column_table = getattr(column, "table", None)
        if column_table is None:
            # A self-contained SQL expression — canonically a correlated scalar
            # subquery — is how a caller reaches a scalar through a to-ONE hop
            # without a join (cerbos/query-plan-adapters#375). It carries its own
            # correlation, so it needs no `table_mapping`, and an absent hop makes
            # it SQL NULL: CEL's missing-path error, excluded under BOTH polarities
            # because NOT NULL is still NULL. Only a value that is neither a column
            # nor an expression — a bare relation marker used outside an override —
            # is a mapping error.
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


# An ORM model class carries its row type; a Core `Table` does not. Overloading on
# that distinction lets callers infer the model rather than annotate the result.
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


# Everything else `GenericTable` admits — a Core `Table`, and a legacy model
# under 1.4, whose stubs do not declare `__table__` so it cannot match the bound
# above. Row type unknown, but the call is still accepted: without this arm the
# overloads would be narrower than the union they replaced.
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

    ``null_attribute_representation`` declares how the caller represents a NULL
    column when building the attributes it sends to ``check()``. The planner
    emits the same ``eq(attr, null)`` node either way, so the plan cannot reveal
    which convention is in use and the adapter has to be told.

    - ``"explicit"`` (default) -- a NULL column is sent as an explicit ``null``
      attribute. CEL compares ``null == null``, so ``IS NULL`` selects exactly
      the rows ``check()`` allows.
    - ``"omitted"`` -- a NULL column sends no attribute at all. CEL then raises a
      missing-attribute error, which Cerbos treats as a deny, so a filter that
      *selects* NULL rows returns rows the PDP denies. Null comparison operands
      are rejected instead of translated.

    ``attribute_null_representation`` declares the same thing PER ATTRIBUTE,
    keyed by the references ``attr_map`` uses. It overrides
    ``null_attribute_representation`` for the attributes it names and asserts
    that their columns can be NULL; an attribute it does not name is treated as
    NOT NULL when rendering a comparison, which is the historical translation.

    It exists because one policy suite can legitimately mix the two conventions
    -- the same column can be mapped twice, sent as an explicit null under one
    attribute name and omitted under another -- which a single call-level
    option cannot express. Declaring an attribute ``"explicit"`` makes the
    equality family (``eq``, ``ne``, ``in``) render so it can never be SQL
    UNKNOWN: CEL holds a null VALUE under that convention, so ``null != "x"``
    is TRUE and ``null == "x"`` is FALSE, both definite, while UNKNOWN excludes
    the row under BOTH polarities. Ordering and string operators are left
    alone, because a null receiver raises a no-overload error in CEL, which
    denies exactly as UNKNOWN does.

    See https://github.com/cerbos/query-plan-adapters/issues/302 and
    https://github.com/cerbos/query-plan-adapters/issues/308.

    ``collection_columns`` declares how a collection attribute is STORED, keyed
    by the same references: a ``CollectionColumn`` naming the column and its
    storage, ``"json"`` or ``"pgArray"``. It is read in exactly two places --
    the operand of ``size()`` and the collection an ``index`` reads -- and in
    both it takes precedence over ``attr_map`` and over any operator override,
    because it is the more specific declaration. For an attribute ``attr_map``
    does not map it also answers literal membership: ``literal in x`` and
    ``hasIntersection(x, [literals])``, each literal matching only an element
    of its own JSON type. Everywhere else the attribute still resolves through
    ``attr_map``, so a relation marker there keeps serving the collection
    macros and membership. An index is translated only as a direct
    ``==``/``!=`` against a scalar literal at a constant non-negative position;
    anything else over a declared collection is refused. The SQL renders on
    SQLite and PostgreSQL. See ``cerbos_sqlalchemy.collection_storage`` and
    https://github.com/cerbos/query-plan-adapters/issues/227.
    """
    # A None entry means no override on every traversal path. Keep None and an
    # explicitly supplied empty mapping distinct for attribute validation below.
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
    # Always: the call-level option is only the fallback now, and an attribute
    # can declare "omitted" while the call declares "explicit".
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
    # The root of the plan must translate to a boolean SQL expression.
    where = require_boolean(translator.predicate(condition), "condition")
    query = select(table).where(where)

    if table_mapping:
        query = query.select_from(table)
        for join_table, predicate in table_mapping:
            query = query.join(join_table, predicate)

    return query
