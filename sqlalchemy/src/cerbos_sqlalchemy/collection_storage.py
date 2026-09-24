# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Declared collection storage for CEL ``size()``, constant indexing and literal membership.

Semantics match the drizzle adapter (#225). The SQL renders per dialect at compile time.
"""

import math
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import Boolean, Integer, literal, literal_column
from sqlalchemy import types as sqltypes
from sqlalchemy.exc import CompileError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from cerbos_sqlalchemy.errors import UnsupportedPlanError

__all__ = ["CollectionColumn", "CollectionStorage"]

#: How a declared collection is stored: a JSON array, or a PostgreSQL native array.
CollectionStorage = Literal["json", "pgArray"]

_STORAGES = ("json", "pgArray")
_MAX_INDEX = 2**31 - 1

#: Refusal for any use of a declared element other than ``==``/``!=`` a literal.
INDEXED_VALUE_REFUSAL = (
    "Indexed values support only direct eq/ne comparisons with scalar literals"
)


@dataclass(frozen=True)
class CollectionColumn:
    """Declares how one collection attribute is stored, for ``size()``, index and membership.

    The adapter never infers storage, since the SQL differs for JSON, arrays and
    relations. Other operators still resolve the attribute through ``attr_map``.
    Supported on SQLite and PostgreSQL only.

    Args:
        column: The column holding exactly the list sent to Cerbos, nulls included.
        storage: ``"json"`` for a JSON array (PostgreSQL ``JSON``/``JSONB`` or a
            SQLite JSON text column), or ``"pgArray"`` for a PostgreSQL array of
            text, varchar, boolean, integer or smallint.

    Raises:
        ValueError: If ``storage`` is not ``"json"`` or ``"pgArray"``.
    """

    column: Any
    storage: CollectionStorage

    def __post_init__(self) -> None:
        if self.storage not in _STORAGES:
            raise ValueError(
                f"CollectionColumn storage must be 'json' or 'pgArray', got {self.storage!r}"
            )


def collection_size(declared: CollectionColumn) -> Any:
    """Return CEL ``size()`` of a declared collection, or NULL when it is absent."""
    return _CollectionSize(_document(declared))


def require_index_position(position: Any) -> int:
    """Return a valid constant index position, or raise.

    CEL errors on a negative or fractional index, so neither is coerced. An integral
    double is accepted because gRPC sends every number as one.
    """
    if (
        isinstance(position, bool)
        or not isinstance(position, (int, float))
        or (isinstance(position, float) and not position.is_integer())
        or not 0 <= position <= _MAX_INDEX
    ):
        raise UnsupportedPlanError(
            "Index access requires a constant non-negative 32-bit integer position"
        )
    return int(position)


def indexed_equality(declared: CollectionColumn, position: int, value: Any) -> Any:
    """Translate ``collection[position] == value`` for a scalar literal.

    The element's JSON type is checked first, as in CEL: ``[true][0] == 1`` is false
    even though SQLite and MySQL store true as 1. An absent element is UNKNOWN; a
    null element is a value. The position is inlined, not bound: it is a validated
    int, it belongs in the cache key, and SQLAlchemy 1.4 cannot repeat a positional bind.
    """
    document = _document(declared)
    index = literal_column(str(require_index_position(position)), Integer)
    if value is None:
        return _ElementIsNull(document, index)
    if isinstance(value, bool):
        return _ElementEqualsBool(document, index, literal(value, Boolean))
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise UnsupportedPlanError(
                "Indexed numeric comparisons require a finite literal"
            )
        return _ElementEqualsNumber(document, index, literal(value))
    if isinstance(value, str):
        return _ElementEqualsString(document, index, literal(value))
    raise UnsupportedPlanError(INDEXED_VALUE_REFUSAL)


#: Refusal for membership in a declared collection against a non-scalar-literal.
MEMBERSHIP_REFUSAL = (
    "Membership in a declared collection supports only scalar literal elements"
)


def collection_membership(declared: CollectionColumn, values: Any) -> Any:
    """Test whether a declared collection holds any of ``values``, keeping JSON types.

    ``"2" in [2]`` is false. A NULL or non-array collection is UNKNOWN, so negation
    still denies it. An empty ``values`` is FALSE for a present collection.
    """
    matches = []
    for value in values:
        if value is None:
            matches.append(_MemberIsNull())
        elif isinstance(value, bool):
            matches.append(_MemberEqualsBool(literal(value, Boolean)))
        elif isinstance(value, (int, float)):
            if not math.isfinite(value):
                raise UnsupportedPlanError(
                    "Membership in a declared collection requires a finite numeric literal"
                )
            matches.append(_MemberEqualsNumber(literal(value)))
        elif isinstance(value, str):
            matches.append(_MemberEqualsString(literal(value)))
        else:
            raise UnsupportedPlanError(MEMBERSHIP_REFUSAL)
    return _CollectionContains(_document(declared), *matches)


def _document(declared: CollectionColumn) -> Any:
    if declared.storage == "pgArray":
        return _PgArrayDocument(declared.column)
    return _JsonDocument(declared.column)


# -- the constructs -------------------------------------------------------------------------


class _JsonDocument(FunctionElement):
    """A column declared ``"json"``, read as a JSON document."""

    name = "cerbos_json_document"
    inherit_cache = True


class _PgArrayDocument(FunctionElement):
    """A column declared ``"pgArray"``, read as a JSON document.

    ``to_jsonb`` keeps null elements and indexes by position, so a non-1 lower bound works.
    """

    name = "cerbos_pg_array_document"
    inherit_cache = True


class _CollectionSize(FunctionElement):
    name = "cerbos_collection_size"
    type = Integer()
    inherit_cache = True


class _ElementIsNull(FunctionElement):
    name = "cerbos_element_is_null"
    type = Boolean()
    inherit_cache = True


class _ElementEqualsBool(FunctionElement):
    name = "cerbos_element_equals_bool"
    type = Boolean()
    inherit_cache = True


class _ElementEqualsNumber(FunctionElement):
    name = "cerbos_element_equals_number"
    type = Boolean()
    inherit_cache = True


class _ElementEqualsString(FunctionElement):
    name = "cerbos_element_equals_string"
    type = Boolean()
    inherit_cache = True


class _CollectionContains(FunctionElement):
    """True if any element passes one of the ``_Member*`` tests after the document."""

    name = "cerbos_collection_contains"
    type = Boolean()
    inherit_cache = True


# Alias of the element `_CollectionContains` iterates. `_Member*` tests render only inside it.
_ELEMENT = "cerbos_element"


class _MemberIsNull(FunctionElement):
    name = "cerbos_member_is_null"
    type = Boolean()
    inherit_cache = True


class _MemberEqualsBool(FunctionElement):
    name = "cerbos_member_equals_bool"
    type = Boolean()
    inherit_cache = True


class _MemberEqualsNumber(FunctionElement):
    name = "cerbos_member_equals_number"
    type = Boolean()
    inherit_cache = True


class _MemberEqualsString(FunctionElement):
    name = "cerbos_member_equals_string"
    type = Boolean()
    inherit_cache = True


# All state lives in clause arguments, never Python attributes, so inherit_cache is safe.
_CONSTRUCTS = (
    _CollectionContains,
    _MemberIsNull,
    _MemberEqualsBool,
    _MemberEqualsNumber,
    _MemberEqualsString,
    _JsonDocument,
    _PgArrayDocument,
    _CollectionSize,
    _ElementIsNull,
    _ElementEqualsBool,
    _ElementEqualsNumber,
    _ElementEqualsString,
)


def _args(element, compiler, **kw):
    return [compiler.process(clause, **kw) for clause in element.clauses.clauses]


def _declared_column(element, compiler):
    """Return a document's column and its dialect storage type, decorators unwrapped."""
    column = element.clauses.clauses[0]
    stored = column.type.dialect_impl(compiler.dialect)
    while isinstance(stored, sqltypes.TypeDecorator):
        stored = stored.load_dialect_impl(compiler.dialect)
    return column, stored


# `str(query)` uses the "default" dialect, so render a readable placeholder there.
# Every other dialect without a renderer below is refused.
def _unsupported(element, compiler, **kw):
    if compiler.dialect.name == "default":
        return f"{element.name}({', '.join(_args(element, compiler, **kw))})"
    raise CompileError(
        "collection_columns storage renders only on SQLite and PostgreSQL, not "
        f"{compiler.dialect.name}"
    )


for _construct in _CONSTRUCTS:
    compiles(_construct)(_unsupported)


# -- SQLite (JSON1) ----------------------------------------------------------------------------


@compiles(_JsonDocument, "sqlite")
def _sqlite_json_document(element, compiler, **kw):
    column, stored = _declared_column(element, compiler)
    if not isinstance(stored, (sqltypes.JSON, sqltypes.String)):
        raise CompileError(
            'collection_columns storage "json" requires a SQLite JSON text column'
        )
    return compiler.process(column, **kw)


@compiles(_PgArrayDocument, "sqlite")
def _sqlite_pg_array_document(element, compiler, **kw):
    raise CompileError(
        'collection_columns storage "pgArray" requires a PostgreSQL array column'
    )


@compiles(_CollectionSize, "sqlite")
def _sqlite_size(element, compiler, **kw):
    (document,) = _args(element, compiler, **kw)
    # json_array_length() is 0 for a non-array, but CEL errors.
    return (
        f"CASE WHEN json_type({document}) = 'array' "
        f"THEN json_array_length({document}) END"
    )


def _sqlite_element(element, compiler, equality, **kw):
    document, index, *value = _args(element, compiler, **kw)
    path = f"'$[{index}]'"
    kind = f"json_type({document}, {path})"
    extracted = f"json_extract({document}, {path})"
    # json_type() is 'null' for a null element and SQL NULL for a missing one.
    return (
        f"CASE WHEN json_type({document}) = 'array' AND {kind} IS NOT NULL "
        f"THEN {equality(kind, extracted, *value)} END"
    )


@compiles(_ElementIsNull, "sqlite")
def _sqlite_is_null(element, compiler, **kw):
    return _sqlite_element(
        element, compiler, lambda kind, _extracted: f"{kind} = 'null'", **kw
    )


@compiles(_ElementEqualsBool, "sqlite")
def _sqlite_equals_bool(element, compiler, **kw):
    # json_extract() returns 1 for both true and 1, so check the type.
    return _sqlite_element(
        element,
        compiler,
        lambda kind, extracted, value: (
            f"({kind} IN ('true', 'false') AND {extracted} = {value})"
        ),
        **kw,
    )


@compiles(_ElementEqualsNumber, "sqlite")
def _sqlite_equals_number(element, compiler, **kw):
    # CEL numbers are doubles, so compare as REAL.
    return _sqlite_element(
        element,
        compiler,
        lambda kind, extracted, value: (
            f"CASE WHEN {kind} IN ('integer', 'real') "
            f"THEN CAST({extracted} AS REAL) = CAST({value} AS REAL) ELSE 0 END"
        ),
        **kw,
    )


@compiles(_ElementEqualsString, "sqlite")
def _sqlite_equals_string(element, compiler, **kw):
    return _sqlite_element(
        element,
        compiler,
        lambda kind, extracted, value: f"({kind} = 'text' AND {extracted} = {value})",
        **kw,
    )


@compiles(_CollectionContains, "sqlite")
def _sqlite_contains(element, compiler, **kw):
    document, *matches = _args(element, compiler, **kw)
    # json_each().type tells true from 1 and "2" from 2, which `value` cannot.
    condition = " OR ".join(f"({match})" for match in matches) or "0"
    return (
        f"CASE WHEN json_type({document}) = 'array' THEN EXISTS "
        f"(SELECT 1 FROM json_each({document}) AS {_ELEMENT} WHERE {condition}) END"
    )


@compiles(_MemberIsNull, "sqlite")
def _sqlite_member_is_null(element, compiler, **kw):
    return f"{_ELEMENT}.type = 'null'"


@compiles(_MemberEqualsBool, "sqlite")
def _sqlite_member_equals_bool(element, compiler, **kw):
    (value,) = _args(element, compiler, **kw)
    return f"{_ELEMENT}.type IN ('true', 'false') AND {_ELEMENT}.value = {value}"


@compiles(_MemberEqualsNumber, "sqlite")
def _sqlite_member_equals_number(element, compiler, **kw):
    (value,) = _args(element, compiler, **kw)
    return (
        f"CASE WHEN {_ELEMENT}.type IN ('integer', 'real') "
        f"THEN CAST({_ELEMENT}.value AS REAL) = CAST({value} AS REAL) ELSE 0 END"
    )


@compiles(_MemberEqualsString, "sqlite")
def _sqlite_member_equals_string(element, compiler, **kw):
    (value,) = _args(element, compiler, **kw)
    return f"{_ELEMENT}.type = 'text' AND {_ELEMENT}.value = {value}"


# -- PostgreSQL ------------------------------------------------------------------------------

# Element types whose SQL value matches what Cerbos sees. Floats are excluded because
# to_jsonb turns NaN and infinities into strings; numeric and bigint may not fit a double;
# an enum is not text.
_PG_ARRAY_ELEMENT_TYPES = (sqltypes.String, sqltypes.Boolean, sqltypes.Integer)
_PG_ARRAY_REFUSED_ELEMENT_TYPES = (sqltypes.Enum, sqltypes.BigInteger)


@compiles(_JsonDocument, "postgresql")
def _postgresql_json_document(element, compiler, **kw):
    column, stored = _declared_column(element, compiler)
    if not isinstance(stored, sqltypes.JSON):
        raise CompileError(
            'collection_columns storage "json" requires a PostgreSQL JSON or JSONB column'
        )
    return f"CAST({compiler.process(column, **kw)} AS JSONB)"


@compiles(_PgArrayDocument, "postgresql")
def _postgresql_pg_array_document(element, compiler, **kw):
    column, stored = _declared_column(element, compiler)
    item = getattr(stored, "item_type", None)
    if (
        not isinstance(stored, sqltypes.ARRAY)
        or not isinstance(item, _PG_ARRAY_ELEMENT_TYPES)
        or isinstance(item, _PG_ARRAY_REFUSED_ELEMENT_TYPES)
    ):
        raise CompileError(
            'collection_columns storage "pgArray" requires a PostgreSQL array of text, '
            "varchar, boolean, integer or smallint"
        )
    return f"to_jsonb({compiler.process(column, **kw)})"


@compiles(_CollectionSize, "postgresql")
def _postgresql_size(element, compiler, **kw):
    (document,) = _args(element, compiler, **kw)
    return (
        f"CASE WHEN jsonb_typeof({document}) = 'array' "
        f"THEN jsonb_array_length({document}) END"
    )


def _postgresql_element(element, compiler, equality, **kw):
    document, index, *value = _args(element, compiler, **kw)
    item = f"({document} -> {index})"
    # `->` is SQL NULL past the end and JSON null for a null element.
    return (
        f"CASE WHEN jsonb_typeof({document}) = 'array' AND {item} IS NOT NULL "
        f"THEN {equality(item, *value)} END"
    )


@compiles(_ElementIsNull, "postgresql")
def _postgresql_is_null(element, compiler, **kw):
    return _postgresql_element(
        element, compiler, lambda item: f"jsonb_typeof({item}) = 'null'", **kw
    )


@compiles(_ElementEqualsBool, "postgresql")
def _postgresql_equals_bool(element, compiler, **kw):
    return _postgresql_element(
        element,
        compiler,
        lambda item, value: f"{item} = to_jsonb(CAST({value} AS BOOLEAN))",
        **kw,
    )


@compiles(_ElementEqualsNumber, "postgresql")
def _postgresql_equals_number(element, compiler, **kw):
    # CASE, not AND: PostgreSQL may evaluate AND in any order, and casting a string
    # element raises. The cast to double matches CEL's double equality.
    return _postgresql_element(
        element,
        compiler,
        lambda item, value: (
            f"CASE WHEN jsonb_typeof({item}) = 'number' "
            f"THEN CAST({item} #>> '{{}}' AS FLOAT(53)) = CAST({value} AS FLOAT(53)) "
            "ELSE false END"
        ),
        **kw,
    )


@compiles(_ElementEqualsString, "postgresql")
def _postgresql_equals_string(element, compiler, **kw):
    return _postgresql_element(
        element,
        compiler,
        lambda item, value: f"{item} = to_jsonb(CAST({value} AS TEXT))",
        **kw,
    )


@compiles(_CollectionContains, "postgresql")
def _postgresql_contains(element, compiler, **kw):
    document, *matches = _args(element, compiler, **kw)
    condition = " OR ".join(f"({match})" for match in matches) or "false"
    return (
        f"CASE WHEN jsonb_typeof({document}) = 'array' THEN EXISTS "
        f"(SELECT 1 FROM jsonb_array_elements({document}) AS {_ELEMENT} "
        f"WHERE {condition}) END"
    )


@compiles(_MemberIsNull, "postgresql")
def _postgresql_member_is_null(element, compiler, **kw):
    return f"jsonb_typeof({_ELEMENT}.value) = 'null'"


@compiles(_MemberEqualsBool, "postgresql")
def _postgresql_member_equals_bool(element, compiler, **kw):
    (value,) = _args(element, compiler, **kw)
    return f"{_ELEMENT}.value = to_jsonb(CAST({value} AS BOOLEAN))"


@compiles(_MemberEqualsNumber, "postgresql")
def _postgresql_member_equals_number(element, compiler, **kw):
    # CASE for the same reason as `_postgresql_equals_number`.
    (value,) = _args(element, compiler, **kw)
    return (
        f"CASE WHEN jsonb_typeof({_ELEMENT}.value) = 'number' "
        f"THEN CAST({_ELEMENT}.value #>> '{{}}' AS FLOAT(53)) = CAST({value} AS FLOAT(53)) "
        "ELSE false END"
    )


@compiles(_MemberEqualsString, "postgresql")
def _postgresql_member_equals_string(element, compiler, **kw):
    (value,) = _args(element, compiler, **kw)
    return f"{_ELEMENT}.value = to_jsonb(CAST({value} AS TEXT))"
