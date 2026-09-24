# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Fixtures for the offline suites: a throwaway in-memory schema, in both model styles.

No PDP is started anywhere in this test suite. The suites these fixtures serve build their
plans by hand; the ones that read a real planner's output read it from
``conformance/golden/`` (``test_translator.py`` and ``test_adversarial_conformance.py``).
"""

import sys
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
    # Camel case, matching the resource attribute names the corpus policies use
    aBool = Column(Boolean)
    aString = Column(String)
    aNumber = Column(Integer)

    ownedBy = Column(String, ForeignKey("user.id"))
    createdBy = Column(String, ForeignKey("user.id"))
    owner = relationship("User", foreign_keys=[ownedBy])
    creator = relationship("User", foreign_keys=[createdBy])


# The SQLAlchemy 2.0 declarative style, mapped onto parallel tables holding the
# same rows. These are not `DeclarativeMeta` instances, so they exercise a
# different arm of `GenericTable` (#181).
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
    # in-memory database.
    #
    # A SQLite REGEXP function used to be registered here, for the one retired test that
    # EXECUTED a caller-supplied `matches` override. What that override does to the emitted
    # SQL is now asserted in `test_translator.py` from a recorded regex plan, and
    # nothing left in this file executes one, so registering it would be dead setup.
    engine = create_engine("sqlite://")

    # generate tables from sqla metadata
    Base.metadata.create_all(engine)
    if HAS_DECLARATIVE_BASE:
        ModernBase.metadata.create_all(engine)

    # Populate with test data
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
    """Skip on 1.4, but fail loudly if 2.0 could not build the models.

    Keyed on the installed version, not on `HAS_DECLARATIVE_BASE`: keyed on the
    import result, a rename upstream would turn the whole 2.0 leg into silent
    skips and leave CI green with the models never exercised.
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


def pytest_terminal_summary(terminalreporter):
    """Per-tier outcomes of the conformance harness, when it ran."""
    harness = sys.modules.get("test_adversarial_conformance")
    if harness is None or not harness.RESULTS:
        return
    terminalreporter.section("conformance")
    for run, tally in sorted(harness.RESULTS.items()):
        tiers = sorted({tier for tier, _ in tally})
        summary = ", ".join(
            f"{tier} {tally[(tier, 'pass')]}/"
            f"{sum(n for (t, _), n in tally.items() if t == tier)}"
            for tier in tiers
        )
        terminalreporter.write_line(f"{run}: {summary} passed")
