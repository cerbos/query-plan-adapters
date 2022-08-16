from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse

from sqlalchemy import Column, Table, select
from sqlalchemy.orm import Query

AttributeColumnMap = dict[str, str]


def get_query(
    query_plan: PlanResourcesResponse, table: Table, attr_map: AttributeColumnMap
) -> Query | None:
    if query_plan.filter.kind == PlanResourcesFilterKind.ALWAYS_ALLOWED:
        return select(table)
    if query_plan.filter.kind == PlanResourcesFilterKind.ALWAYS_DENIED:
        return None
