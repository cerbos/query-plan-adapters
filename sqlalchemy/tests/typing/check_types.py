"""Static conformance for `get_query`'s public signature.

Checked by pyright (see `sqlalchemy/pyrightconfig.json`), not by pytest — the
claims in this file are about what a *type checker* infers, so the runtime suite
cannot prove any of them. Every assertion here failed at least one way before
the signature it pins was written.

The negative case carries a `pyright: ignore`, and `reportUnnecessaryTypeIgnore
Comment` is on: if `get_query` ever stops rejecting an unmapped class, the
suppression becomes unnecessary and pyright fails the file. A negative test that
silently stops testing anything is the failure mode this guards.

Run against SQLAlchemy >= 2.0 only. 1.4 ships no `DeclarativeBase` and does not
declare `__table__` on mapped classes, so none of these inferences hold there —
1.4 callers get the untyped `Select[Any]` overload, exactly as before #181.
"""

from typing import Any, Tuple, cast

from cerbos.sdk.model import PlanResourcesResponse
from typing_extensions import assert_type

from cerbos_sqlalchemy import get_query
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.orm import DeclarativeBase, declarative_base
from sqlalchemy.sql import Select

LegacyBase = declarative_base()


class LegacyModel(LegacyBase):
    __tablename__ = "legacy"

    id = Column(Integer, primary_key=True)


class ModernBase(DeclarativeBase):
    pass


class ModernModel(ModernBase):
    __tablename__ = "modern"

    id = Column(Integer, primary_key=True)


core_table = Table("core", MetaData(), Column("id", Integer), Column("name", String))

plan = cast(PlanResourcesResponse, None)


# A 2.0 `DeclarativeBase` model is accepted and keeps its row type (#181).
assert_type(get_query(plan, ModernModel, {}), Select[Tuple[ModernModel]])

# So is a legacy `declarative_base()` model — the two share no base class, which
# is why the bound is structural.
assert_type(get_query(plan, LegacyModel, {}), Select[Tuple[LegacyModel]])

# A Core `Table` carries no row type, so it resolves to the untyped overload.
assert_type(get_query(plan, core_table, {}), Select[Any])

# An unmapped class must NOT be accepted. Before the TypeVar was bounded, an
# unconstrained `Type[_ORMModel]` admitted every class and inferred
# `Select[Tuple[str]]` — looser than the union the overloads replaced.
get_query(plan, str, {})  # pyright: ignore[reportCallIssue, reportArgumentType]
