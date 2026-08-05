import os
import re
from contextlib import contextmanager
from importlib.metadata import version
from typing import Any, Callable, Dict, Generator

import pytest
from cerbos.engine.v1 import engine_pb2
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.grpc.client import CerbosClient as GrpcCerbosClient
from cerbos.sdk.model import Principal, ResourceDesc
from google.protobuf.json_format import ParseDict
from google.protobuf.struct_pb2 import Value

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    String,
    create_engine,
    event,
    insert,
)
from sqlalchemy.orm import declarative_base, relationship

USER_ROLE = "USER"

Base = declarative_base()

_IS_SQLA_14 = None


def _is_sqla_14() -> bool:
    global _IS_SQLA_14
    if _IS_SQLA_14 is not None:
        return _IS_SQLA_14

    _IS_SQLA_14 = version("sqlalchemy").startswith("1.4")
    return _IS_SQLA_14


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
    # in-memory database
    engine = create_engine("sqlite://")

    # SQLite has no built-in REGEXP function; register one so that the
    # adapter's `regexp_match` lowers to a working SQL operator under tests.
    @event.listens_for(engine, "connect")
    def _register_regexp(dbapi_conn, _):
        dbapi_conn.create_function(
            "regexp", 2, lambda pat, s: 1 if re.search(pat, s or "") else 0
        )

    # generate tables from sqla metadata
    Base.metadata.create_all(engine)
    if HAS_DECLARATIVE_BASE:
        ModernBase.metadata.create_all(engine)

    # Populate with test data
    with engine.connect() as conn:
        conn.execute(
            insert(User.__table__),
            [
                {"id": "1", "name": "user1", "role": "admin"},
                {"id": "2", "name": "user2", "role": "user"},
            ],
        )
        conn.execute(insert(Resource.__table__), _RESOURCE_ROWS)

        if HAS_DECLARATIVE_BASE:
            conn.execute(
                insert(ModernUser.__table__),
                [{"id": "1"}, {"id": "2"}],
            )
            conn.execute(insert(ModernResource.__table__), _RESOURCE_ROWS)

        if not _is_sqla_14():
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
    if _is_sqla_14():
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


@contextmanager
def cerbos_container_host(client_type: str) -> Generator[str, None, None]:
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
def principal_with_attr(cerbos_client) -> Callable[[Dict[str, Any]], Any]:
    """Build a principal carrying attributes, for whichever transport is active.

    The gRPC client takes `map<string, google.protobuf.Value>`, so plain Python
    containers have to be parsed into `Value` first; the HTTP client takes them
    as-is.
    """
    is_grpc = isinstance(cerbos_client, GrpcCerbosClient)

    def build(attr: Dict[str, Any]) -> Any:
        if is_grpc:
            return engine_pb2.Principal(
                id="1",
                roles={USER_ROLE},
                attr={k: ParseDict(v, Value()) for k, v in attr.items()},
            )
        return Principal(id="1", roles={USER_ROLE}, attr=attr)

    return build


@pytest.fixture
def resource_desc(cerbos_client):
    desc_cls = (
        engine_pb2.PlanResourcesInput.Resource
        if isinstance(cerbos_client, GrpcCerbosClient)
        else ResourceDesc
    )
    return desc_cls(kind="resource")
