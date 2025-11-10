import os
import sqlite3
from contextlib import contextmanager
from typing import Generator

import pytest
from pypika import Table, Query
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.container import CerbosContainer
from cerbos.sdk.model import (
    PlanResourcesFilter,
    PlanResourcesFilterKind,
    PlanResourcesResponse,
    Principal,
    ResourceDesc,
)

USER_ROLE = "USER"


@pytest.fixture(scope="module")
def db():
    """Create in-memory SQLite database with test data."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE user (
            id INTEGER PRIMARY KEY,
            role TEXT,
            department TEXT
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
        "INSERT INTO user (id, role, department) VALUES (?, ?, ?)",
        [(1, "admin", "engineering"), (2, "user", "marketing")]
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


@pytest.fixture
def user_desc():
    return ResourceDesc(kind="user")


def default_plan_params():
    """Default parameters for PlanResourcesResponse."""
    return {
        "request_id": "1",
        "action": "view",
        "resource_kind": "resource",
        "policy_version": "default",
    }


def create_conditional_plan(operator, operands):
    """Create a conditional plan filter with given operator and operands."""
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.CONDITIONAL,
        "condition": {
            "expression": {
                "operator": operator,
                "operands": operands,
            },
        },
    })
    return PlanResourcesResponse(
        filter=plan_filter,
        **default_plan_params()
    )


def create_simple_plan(kind):
    """Create a simple plan (ALWAYS_ALLOWED or ALWAYS_DENIED)."""
    plan_filter = PlanResourcesFilter.from_dict({"kind": kind})
    return PlanResourcesResponse(
        filter=plan_filter,
        **default_plan_params()
    )


def build_query_with_criterion(criterion, table):
    """Build query from criterion and table."""
    if criterion is None:
        return Query.from_(table).select('*')
    return Query.from_(table).select('*').where(criterion)


@contextmanager
def cerbos_container_host() -> Generator[str, None, None]:
    policy_dir = os.path.realpath(
        os.path.join(os.path.dirname(__file__), "../..", "policies")
    )

    container = CerbosContainer(image="ghcr.io/cerbos/cerbos:dev")
    container.with_volume_mapping(policy_dir, "/policies")
    container.with_env("CERBOS_NO_TELEMETRY", "1")
    container.with_command("server --set=schema.enforcement=reject")
    container.start()
    container.wait_until_ready()

    yield container.http_host()

    container.stop()


@pytest.fixture(scope="module")
def cerbos_client():
    with cerbos_container_host() as host:
        with CerbosClient(host, tls_verify=False) as client:
            yield client


@pytest.fixture
def principal():
    return Principal(id="1", roles={USER_ROLE})


@pytest.fixture
def resource_desc():
    return ResourceDesc(kind="resource")
