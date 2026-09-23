"""Declared collection storage: CEL ``size()``, constant indexing and literal membership.

A collection attribute has no portable translation until the caller says how it is stored. The
plan names ``R.attr.tags`` and nothing else, and the right SQL for ``size(R.attr.tags)`` differs
for a JSON document, a PostgreSQL array and a related table -- so the adapter never infers it. A
relation in particular has no positional order at all, which is why ``R.attr.tags[0]`` cannot be
read from one. ``CollectionColumn`` is the declaration, and it is read only where a collection's
own storage decides the answer: the operand of ``size()``, the collection an ``index`` reads, and
-- for an attribute ``attr_map`` does not map -- the collection a literal ``in`` or
``hasIntersection`` searches. Every other operator keeps resolving the attribute through
``attr_map``, so a caller can keep a relation marker there for its collection macros and membership
and declare the ordered column beside it.

The semantics are the drizzle adapter's (cerbos/query-plan-adapters#225), which is what makes the
two storage names the same strings in both:

- An absent element, an SQL NULL collection and a JSON value that is not an array all yield SQL
  UNKNOWN. CEL raises an evaluation error for each, and Cerbos denies, so the row must stay
  excluded under negation too.
- A null ELEMENT is a value: ``[null][0] == null`` is true and ``[null][0] != "x"`` is true.
- Comparisons keep JSON's types, as CEL's heterogeneous equality does: a string literal equals
  only a JSON string, a number only a JSON number (compared as doubles), and a boolean only a
  JSON boolean. SQLite and MySQL store a JSON true as 1, so reading the element back as SQL and
  comparing it with the literal would make ``[true][0] == 1`` true; the element's JSON type is
  checked first (the corpus's ``index-bool-list-vs-number`` and ``index-number-list-vs-bool``).
  Membership keeps them the same way: ``"2" in [2]`` and ``"true" in [true]`` are false, and a
  ``hasIntersection`` literal list may mix types, each element matching only its own
  (``in-number-list-vs-string``, ``hasint-number-list-vs-string`` and their boolean mirrors).
- ``size()`` of an empty collection is 0 and of an absent one is UNKNOWN, so ``size(x) == 0``
  selects the empty rows and never the missing ones.

The SQL is dialect-specific and ``get_query`` is never told the dialect, so each construct below
renders itself per dialect at compile time. SQLite (JSON1) and PostgreSQL are the two it renders
on; any other dialect raises ``CompileError`` rather than guessing. Every piece of state lives in
a construct's clause arguments, never in a Python attribute, which is what makes
``inherit_cache = True`` safe: the statement cache keys on exactly what the SQL depends on.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy import Boolean, Integer, literal, literal_column
from sqlalchemy import types as sqltypes
from sqlalchemy.exc import CompileError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

__all__ = ["CollectionColumn", "CollectionStorage"]

#: How a declared collection is stored: a JSON array document, or a PostgreSQL native array.
CollectionStorage = Literal["json", "pgArray"]

_STORAGES = ("json", "pgArray")
_MAX_INDEX = 2**31 - 1

#: The refusal for every use of a declared element other than ``== literal`` / ``!= literal``.
INDEXED_VALUE_REFUSAL = (
    "Indexed values support only direct eq/ne comparisons with scalar literals"
)


@dataclass(frozen=True)
class CollectionColumn:
    """One attribute's ordered collection, stored in one column.

    ``storage`` is ``"json"`` for a JSON array (a PostgreSQL ``JSON``/``JSONB`` column, or a
    SQLite JSON text column) or ``"pgArray"`` for a PostgreSQL array of text, varchar, boolean,
    integer or smallint. The column must hold exactly the list the application sends to Cerbos,
    null elements included: that invariant is the caller's, as it is for every mapping.
    """

    column: Any
    storage: CollectionStorage

    def __post_init__(self) -> None:
        if self.storage not in _STORAGES:
            raise ValueError(
                f"CollectionColumn storage must be 'json' or 'pgArray', got {self.storage!r}"
            )


def collection_size(declared: CollectionColumn) -> Any:
    """CEL ``size()`` of a declared collection: an integer, or NULL when it is absent."""
    return _CollectionSize(_document(declared))


def require_index_position(position: Any) -> int:
    """The constant position an ``index`` reads, or a refusal.

    CEL raises for a negative or fractional index, so neither may be coerced into a valid read:
    ``-1`` is not "the last element" and ``0.5`` is not ``0``. An integral double is accepted,
    because the gRPC transport delivers every plan number as one.
    """
    if (
        isinstance(position, bool)
        or not isinstance(position, (int, float))
        or (isinstance(position, float) and not position.is_integer())
        or not 0 <= position <= _MAX_INDEX
    ):
        raise ValueError(
            "Index access requires a constant non-negative 32-bit integer position"
        )
    return int(position)


def indexed_equality(declared: CollectionColumn, position: int, value: Any) -> Any:
    """``collection[position] == value`` for a scalar literal, keeping JSON's types.

    The position is rendered inline, not bound. It is an integer ``require_index_position``
    has already checked, so there is nothing to inject, and a SQL literal is part of the
    statement cache key where a bound value is not. It is also read more than once per
    statement, and SQLAlchemy 1.4 does not repeat a positional bind rendered twice.
    """
    document = _document(declared)
    index = literal_column(str(require_index_position(position)), Integer)
    if value is None:
        return _ElementIsNull(document, index)
    if isinstance(value, bool):
        return _ElementEqualsBool(document, index, literal(value, Boolean))
    if isinstance(value, (int, float)):
        if not math.isfinite(value):
            raise ValueError("Indexed numeric comparisons require a finite literal")
        return _ElementEqualsNumber(document, index, literal(value))
    if isinstance(value, str):
        return _ElementEqualsString(document, index, literal(value))
    raise ValueError(INDEXED_VALUE_REFUSAL)


#: The refusal for a membership in a declared collection whose other side is not a scalar literal.
MEMBERSHIP_REFUSAL = (
    "Membership in a declared collection supports only scalar literal elements"
)


def collection_membership(declared: CollectionColumn, values: Any) -> Any:
    """Whether a declared collection holds any of ``values``, keeping JSON's types.

    ``x in collection`` is one value and ``hasIntersection(collection, [...])`` a list of them.
    An SQL NULL collection, or a JSON value that is not an array, is UNKNOWN, as it is for
    ``size()``: CEL raises for a missing attribute, so a negated membership must deny it too.
    An empty ``values`` list is FALSE for every present collection.
    """
    matches = []
    for value in values:
        if value is None:
            matches.append(_MemberIsNull())
        elif isinstance(value, bool):
            matches.append(_MemberEqualsBool(literal(value, Boolean)))
        elif isinstance(value, (int, float)):
            if not math.isfinite(value):
                raise ValueError(
                    "Membership in a declared collection requires a finite numeric literal"
                )
            matches.append(_MemberEqualsNumber(literal(value)))
        elif isinstance(value, str):
            matches.append(_MemberEqualsString(literal(value)))
        else:
            raise ValueError(MEMBERSHIP_REFUSAL)
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

    ``to_jsonb`` rather than ``array[i + 1]``: it keeps null elements as JSON nulls and addresses
    POSITIONS, so an array whose lower bound is not 1 reads the same element CEL does.
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
    """Whether any element satisfies one of the ``_Member*`` tests that follow the document."""

    name = "cerbos_collection_contains"
    type = Boolean()
    inherit_cache = True


# The per-value tests `_CollectionContains` ORs together. Each reads the one element the
# enclosing EXISTS is iterating, under the alias below, so it renders only inside that construct.
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
    """A document construct's column, and the type the dialect actually stores for it, with
    variants and decorators unwrapped."""
    column = element.clauses.clauses[0]
    stored = column.type.dialect_impl(compiler.dialect)
    while isinstance(stored, sqltypes.TypeDecorator):
        stored = stored.load_dialect_impl(compiler.dialect)
    return column, stored


# `str(query)` compiles under SQLAlchemy's string dialect, named "default". That is a debugging
# aid rather than a database, so it gets a readable placeholder instead of an error; every real
# dialect other than the two below is refused.
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
    # json_array_length() is 0 for a JSON value that is not an array, which is not CEL's answer.
    return (
        f"CASE WHEN json_type({document}) = 'array' "
        f"THEN json_array_length({document}) END"
    )


def _sqlite_element(element, compiler, equality, **kw):
    document, index, *value = _args(element, compiler, **kw)
    path = f"'$[{index}]'"
    kind = f"json_type({document}, {path})"
    extracted = f"json_extract({document}, {path})"
    # json_type() is the STRING 'null' for a null element and SQL NULL for a missing one, which is
    # the whole distinction between a null value and an index error.
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
    # json_extract() gives a JSON true as 1 and a JSON 1 as 1 too, so the type is what tells
    # them apart; the bound boolean is 1 or 0.
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
    # CEL numbers are doubles on the wire, so an integer element and a double literal compare as
    # the same number.
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
    # json_each() over a correlated column; its `type` column tells a JSON true from a JSON 1
    # and a JSON "2" from a JSON 2, which its `value` column (1, 1, '2', 2) cannot.
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

# Element types whose SQL value is the value the application sends to Cerbos. Floating-point
# arrays admit NaN and infinities, which to_jsonb turns into STRINGS; numeric and bigint can
# differ from the double Cerbos carries; an enum is a named type, not text.
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
    # `->` is SQL NULL for a position past the end and the JSON null for a null element.
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
    # A CASE rather than AND: PostgreSQL does not promise to evaluate an AND left to right, and
    # casting a string element to a float raises. jsonb equality would compare `2` and `2.0` as
    # numerics, which is right, but the double cast keeps it CEL's double equality.
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
    # A CASE for the reason `_postgresql_equals_number` gives: casting a string element raises.
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
