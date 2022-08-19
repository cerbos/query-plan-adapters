import pytest

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine, insert
from sqlalchemy.orm import Session, declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    role = Column(String(255))
    contacts = relationship("Contact")


class Contact(Base):
    __tablename__ = "contact"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    age = Column(Integer)
    user = Column(Integer, ForeignKey("user.id"))


@pytest.fixture
def engine():
    # in-memory database
    engine = create_engine("sqlite://")

    # generate tables from sqla metadata
    Base.metadata.create_all(engine)

    # Populate with test data
    engine.execute(
        insert(User.__table__),
        [{"name": "user1", "role": "admin"}, {"name": "user2", "role": "user"}],
    )
    engine.execute(
        insert(Contact.__table__),
        [
            {"name": "contact1", "age": 32, "user": "user1"},
            {"name": "contact2", "age": 42, "user": "user2"},
            {"name": "contact3", "age": 52, "user": "user1"},
        ],
    )

    yield engine


@pytest.fixture
def conn(engine):
    with engine.connect() as conn:
        yield conn


@pytest.fixture
def session(engine):
    with Session(engine) as session:
        yield session


@pytest.fixture
def contact_table():
    return Contact.__table__
