from cerbos.sdk.model import PlanResourcesFilter, PlanResourcesFilterKind, PlanResourcesResponse

from cerbos_sqlalchemy.query import get_query, AttributeColumnMap
from sqlalchemy import Column, Integer, MetaData, String, Table

metadata_obj = MetaData()

user_table = Table(
    "user_account",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("name", String(30)),
    Column("fullname", String),
)


class TestCerbosClient:
    def test_always_allow(self, user_table, session):
        plan_resource_resp = PlanResourcesResponse(
            request_id="foo",
            action="read",
            resource_kind="contact",
            policy_version="default",
            filter=PlanResourcesFilter(
                PlanResourcesFilterKind.ALWAYS_ALLOWED,
            ),
        )
        query = get_query(plan_resource_resp, user_table, AttributeColumnMap())
        res = session.execute(query).fetchall()
        assert len(res) == 2

    def test_always_deny(self, user_table, session):
        plan_resource_resp = PlanResourcesResponse(
            request_id="foo",
            action="read",
            resource_kind="contact",
            policy_version="default",
            filter=PlanResourcesFilter(
                PlanResourcesFilterKind.ALWAYS_DENIED,
            ),
        )
        query = get_query(plan_resource_resp, user_table, AttributeColumnMap())
        assert query is None
