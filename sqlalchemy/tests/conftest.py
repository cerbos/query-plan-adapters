import os
from contextlib import contextmanager

import pytest
from cerbos.engine.v1 import engine_pb2
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.grpc.client import CerbosClient as GrpcCerbosClient
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
from sqlalchemy.orm import declarative_base, relationship

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


@contextmanager
def cerbos_container_host(client_type: str) -> str:
    policy_dir = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../..", "policies")
    )

    container = CerbosContainer(image="ghcr.io/cerbos/cerbos:dev")
    container.with_volume_mapping(policy_dir, "/policies")
    container.with_env("CERBOS_NO_TELEMETRY", "1")
    container.with_command("server --set=schema.enforcement=reject")
    container.start()
    container.wait_until_ready()

    yield container.http_host() if client_type == "http" else container.grpc_host()

    container.stop()


@pytest.fixture(scope="module", params=["http", "grpc"])
def cerbos_client(request):
    client_type = request.param
    with cerbos_container_host(client_type) as host:
        client_cls = CerbosClient if client_type == "http" else GrpcCerbosClient
        with client_cls(host, tls_verify=False) as client:
            yield client


@pytest.fixture
def principal(cerbos_client):
    principal_cls = (
        engine_pb2.Principal
        if isinstance(cerbos_client, GrpcCerbosClient)
        else Principal
    )
    return principal_cls(id="1", roles={USER_ROLE})


@pytest.fixture
def resource_desc(cerbos_client):
    desc_cls = (
        engine_pb2.PlanResourcesInput.Resource
        if isinstance(cerbos_client, GrpcCerbosClient)
        else ResourceDesc
    )
    return desc_cls(kind="resource")
