def test_fixtures_work(resource_table):
    """Verify test fixtures are set up correctly."""
    assert resource_table is not None
    assert str(resource_table) == '"resource"'


def test_get_query_is_importable():
    """Verify get_query can be imported."""
    from cerbos_pypika import get_query
    assert callable(get_query)
