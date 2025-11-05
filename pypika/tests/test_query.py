def test_fixtures_work(resource_table):
    """Verify test fixtures are set up correctly."""
    assert resource_table is not None
    assert str(resource_table) == '"resource"'
