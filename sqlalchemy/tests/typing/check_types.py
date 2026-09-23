# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Type-checks `get_query`'s signature. Run by pyright, not pytest.

`reportUnnecessaryTypeIgnoreComment` is on, so the negative case fails if it stops
erroring.
SQLAlchemy >= 2.0 only: 1.4 callers get the untyped `Select[Any]` overload.
"""

from typing import Any, cast

from cerbos.sdk.model import PlanResourcesResponse
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.orm import DeclarativeBase, declarative_base
from sqlalchemy.sql import Select
from typing_extensions import assert_type

from cerbos_sqlalchemy import get_query

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


# A 2.0 `DeclarativeBase` model keeps its row type.
assert_type(get_query(plan, ModernModel, {}), Select[tuple[ModernModel]])

# So does a legacy model. The two share no base class, so the bound is structural.
assert_type(get_query(plan, LegacyModel, {}), Select[tuple[LegacyModel]])

# A Core `Table` carries no row type, so it resolves to the untyped overload.
assert_type(get_query(plan, core_table, {}), Select[Any])

# An unmapped class is rejected.
get_query(plan, str, {})  # pyright: ignore[reportCallIssue, reportArgumentType]
