def test_fixtures_work(resource_table):
    """Verify test fixtures are set up correctly."""
    assert resource_table is not None
    assert str(resource_table) == '"resource"'


def test_get_query_is_importable():
    """Verify get_query can be imported."""
    from cerbos_pypika import get_query
    assert callable(get_query)


def test_always_allow(resource_table):
    """Test ALWAYS_ALLOWED returns unfiltered query."""
    from cerbos.sdk.model import (
        PlanResourcesFilter,
        PlanResourcesFilterKind,
        PlanResourcesResponse,
    )
    from cerbos_pypika import get_query
    
    plan_filter = PlanResourcesFilter.from_dict({
        "kind": PlanResourcesFilterKind.ALWAYS_ALLOWED,
    })
    plan = PlanResourcesResponse(
        filter=plan_filter,
        request_id="1",
        action="view",
        resource_kind="resource",
        policy_version="default",
    )
    
    query = get_query(plan, resource_table, {})
    sql = query.get_sql()
    
    assert "WHERE" not in sql
