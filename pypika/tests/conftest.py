import pytest
from pypika import Table


@pytest.fixture
def user_table():
    return Table('user')


@pytest.fixture
def resource_table():
    return Table('resource')
