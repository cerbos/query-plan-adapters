import sqlite3
import pytest
from pypika import Table


@pytest.fixture(scope="module")
def db():
    """Create in-memory SQLite database with test data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE user (
            id INTEGER PRIMARY KEY
        )
    """)
    
    cursor.execute("""
        CREATE TABLE resource (
            id INTEGER PRIMARY KEY,
            name TEXT,
            aBool INTEGER,
            aString TEXT,
            aNumber INTEGER,
            ownedBy TEXT,
            createdBy TEXT
        )
    """)
    
    cursor.executemany(
        "INSERT INTO user (id) VALUES (?)",
        [(1,), (2,)]
    )
    
    cursor.executemany(
        "INSERT INTO resource (name, aBool, aString, aNumber, ownedBy, createdBy) VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("resource1", 1, "string", 1, "1", "1"),
            ("resource2", 0, "amIAString?", 2, "1", "2"),
            ("resource3", 1, "anotherString", 3, "2", "2"),
        ]
    )
    
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def cursor(db):
    """Provide a database cursor for executing queries."""
    return db.cursor()


@pytest.fixture
def user_table():
    return Table('user')


@pytest.fixture
def resource_table():
    return Table('resource')
