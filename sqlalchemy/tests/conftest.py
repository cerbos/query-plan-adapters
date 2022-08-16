import pytest

from sqlalchemy import Column, Integer, String, create_engine, insert
from sqlalchemy.orm import Session, declarative_base, relationship

Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    role = Column(String(255))

    contacts = relationship("Contact", back_populates="owner")


class Contact(Base):
    __tablename__ = "contact"

    id = Column(Integer, primary_key=True)
    name = Column(String(30))

    owner = relationship("User", back_populates="contacts")


@pytest.fixture
def engine():
    # in-memory database
    engine = create_engine("sqlite://")

    # generate tables from sqla metadata
    Base.metadata.create_all(engine)

    # Populate with test data
    engine.execute(
        insert(User.__table__),
        [{"name": "sam", "role": "admin"}, {"name": "frankie", "role": "user"}],
    )
    engine.execute(
        insert(Contact.__table__),
        [{"name": "ollie", "owner": "sam"}, {"name": "robin", "owner": "frankie"}],
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
def user_table():
    return User.__table__


@pytest.fixture
def contact_table():
    return Contact.__table__
