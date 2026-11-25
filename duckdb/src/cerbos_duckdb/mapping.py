# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""How the plan's attribute references reach DuckDB columns, list columns and joined rows."""

import enum
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any


class ValueType(enum.Enum):
    """What a column holds. The plan carries no types, so declare them where it matters."""

    DEFAULT = "default"
    TIMESTAMP = "timestamp"
    BOOL = "bool"
    STRING = "string"
    NUMBER = "number"


class NullConvention(enum.Enum):
    """How the caller sends a NULL in this column to ``check()``."""

    UNSET = "unset"
    EXPLICIT = "explicit"
    OMITTED = "omitted"


class NullRepresentation(enum.Enum):
    """The call-level fallback for attributes that declare no ``NullConvention``."""

    EXPLICIT = "explicit"
    OMITTED = "omitted"


class RestrictOp(enum.Enum):
    EQ = "eq"
    NE = "ne"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"
    IN = "in"
    NOT_IN = "not_in"


@dataclass(frozen=True)
class Restriction:
    """A predicate your application applies when it reads a joined table, such as a soft delete."""

    column: str
    op: RestrictOp = RestrictOp.EQ
    value: Any = None
    values: tuple[Any, ...] = ()


@dataclass(frozen=True)
class Hop:
    """An intermediate table between a joined table and the row that references it."""

    table: str
    child_column: str
    join_column: str
    subquery_filter: tuple[Restriction, ...] = ()


@dataclass(frozen=True)
class Join:
    """A to-one relation: ``<table>.<target_column> = <row>.<source_column>``.

    Declaring it asserts your schema makes the match unique.
    """

    table: str
    source_column: str
    target_column: str
    via: tuple[Hop, ...] = ()
    subquery_filter: tuple[Restriction, ...] = ()


@dataclass(frozen=True)
class ListOf:
    """The elements of a ``LIST`` column: a scalar ``element``, or ``STRUCT`` ``fields``."""

    element: "Entry | None" = None
    fields: Mapping[str, "Entry"] = field(default_factory=dict)


Column = str | tuple[str, ...]


@dataclass(frozen=True)
class Entry:
    """What one attribute reference resolves to.

    ``column`` names a column, or a ``STRUCT`` field path as a tuple. With ``list``, the column is
    a ``LIST``. With ``join``, the column is read from the joined row; a ``join`` without a column
    is the joined row itself.
    """

    column: Column | None = None
    value_type: ValueType = ValueType.DEFAULT
    null_convention: NullConvention = NullConvention.UNSET
    list: ListOf | None = None
    join: Join | None = None
    qualifier: str | None = None

    @property
    def path(self) -> tuple[str, ...]:
        if self.column is None:
            return ()
        return (self.column,) if isinstance(self.column, str) else tuple(self.column)


Mapper = Mapping[str, Entry] | Callable[[str], Entry | None]
