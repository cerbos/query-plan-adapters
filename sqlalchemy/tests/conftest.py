import os

import pytest
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.model import Principal, ResourceDesc

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    create_engine,
    insert,
)
from sqlalchemy.orm import Session, declarative_base, relationship

USER_ROLE = "USER"

Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)


class Resource(Base):
    __tablename__ = "resource"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    # Camel case, as we're being consistent with the attributes created in the base policy files for the shared repo
    aBool = Column(Boolean)
    aString = Column(String)
    aNumber = Column(Integer)

    ownedBy = Column(String, ForeignKey("user.id"))
    createdBy = Column(String, ForeignKey("user.id"))
    owner = relationship("User", foreign_keys=[ownedBy])
    creator = relationship("User", foreign_keys=[createdBy])


@pytest.fixture
def engine():
    # in-memory database
    engine = create_engine("sqlite://")

    # generate tables from sqla metadata
    Base.metadata.create_all(engine)

    # Populate with test data
    engine.execute(
        insert(User.__table__),
        [
            {"id": "1", "name": "user1", "role": "admin"},
            {"id": "2", "name": "user2", "role": "user"},
        ],
    )
    engine.execute(
        insert(Resource.__table__),
        [
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
        ],
    )

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


@pytest.fixture
def principal():
    return Principal(id="1", roles={USER_ROLE})


@pytest.fixture
def resource_desc():
    return ResourceDesc("resource")


@pytest.fixture(scope="module")
def cerbos_container():
    policy_dir = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../..", "policies")
    )

    container = CerbosContainer(image="ghcr.io/cerbos/cerbos:dev")
    container.with_volume_mapping(policy_dir, "/policies")
    container.with_env("CERBOS_NO_TELEMETRY", "1")
    container.with_command("server --set=schema.enforcement=reject")
    container.start()
    container.wait_until_ready()

    yield container

    container.stop()


@pytest.fixture(scope="function")
def cerbos_client(cerbos_container):
    client = CerbosClient(cerbos_container.http_host(), debug=True)
    yield client
    client.close()
