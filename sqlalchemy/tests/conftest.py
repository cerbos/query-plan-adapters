# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Fixtures for the offline suites: an in-memory SQLite schema in both model styles.

No PDP starts here. ``test_adversarial_conformance.py`` starts its own.
"""

from importlib.metadata import version

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    create_engine,
    insert,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

_IS_SQLA_14 = version("sqlalchemy").startswith("1.4")


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)


class Resource(Base):
    __tablename__ = "resource"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    # Camel case to match the policies' attribute names.
    aBool = Column(Boolean)
    aString = Column(String)
    aNumber = Column(Integer)

    ownedBy = Column(String, ForeignKey("user.id"))
    createdBy = Column(String, ForeignKey("user.id"))
    owner = relationship("User", foreign_keys=[ownedBy])
    creator = relationship("User", foreign_keys=[createdBy])


# 2.0-style models on parallel tables with the same rows. They are not
# `DeclarativeMeta` instances, so they take a different arm of `GenericTable`.
try:
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    class ModernBase(DeclarativeBase):
        pass

    class ModernUser(ModernBase):
        __tablename__ = "modern_user"

        id: Mapped[int] = mapped_column(primary_key=True)

    class ModernResource(ModernBase):
        __tablename__ = "modern_resource"

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(String(30))
        aBool: Mapped[bool] = mapped_column(Boolean)
        aString: Mapped[str] = mapped_column(String)
        aNumber: Mapped[int] = mapped_column(Integer)

        ownedBy: Mapped[str] = mapped_column(String, ForeignKey("modern_user.id"))
        createdBy: Mapped[str] = mapped_column(String, ForeignKey("modern_user.id"))

    HAS_DECLARATIVE_BASE = True
except ImportError:  # SQLAlchemy 1.4
    ModernBase = ModernUser = ModernResource = None
    HAS_DECLARATIVE_BASE = False


_RESOURCE_ROWS = [
    {
        "name": "resource1",
        "aBool": True,
        "aString": "string",
        "aNumber": 1,
        "ownedBy": "1",
        "createdBy": "1",
    },
    {
        "name": "resource2",
        "aBool": False,
        "aString": "amIAString?",
        "aNumber": 2,
        "ownedBy": "1",
        "createdBy": "2",
    },
    {
        "name": "resource3",
        "aBool": True,
        "aString": "anotherString",
        "aNumber": 3,
        "ownedBy": "2",
        "createdBy": "2",
    },
]


@pytest.fixture(scope="module")
def engine():
    engine = create_engine("sqlite://")

    Base.metadata.create_all(engine)
    if HAS_DECLARATIVE_BASE:
        ModernBase.metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(
            insert(User.__table__),
            [{"id": "1"}, {"id": "2"}],
        )
        conn.execute(insert(Resource.__table__), _RESOURCE_ROWS)

        if HAS_DECLARATIVE_BASE:
            conn.execute(
                insert(ModernUser.__table__),
                [{"id": "1"}, {"id": "2"}],
            )
            conn.execute(insert(ModernResource.__table__), _RESOURCE_ROWS)

        if not _IS_SQLA_14:
            conn.commit()

    yield engine


@pytest.fixture
def conn(engine):
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def user_table():
    return User


@pytest.fixture
def resource_table():
    return Resource


def _require_declarative_base() -> None:
    """Skip on 1.4, but fail if 2.0 could not build the models.

    Keyed on the installed version so an upstream rename cannot turn into silent skips.
    """
    if _IS_SQLA_14:
        pytest.skip("DeclarativeBase requires SQLAlchemy >= 2.0")
    assert HAS_DECLARATIVE_BASE, (
        "SQLAlchemy >= 2.0 is installed but the DeclarativeBase models failed to "
        "import — the 2.0 declarative tests would otherwise skip silently"
    )


@pytest.fixture
def modern_user_table():
    _require_declarative_base()
    return ModernUser


@pytest.fixture
def modern_resource_table():
    _require_declarative_base()
    return ModernResource
