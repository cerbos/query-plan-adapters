from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse

from sqlalchemy import Table, and_, or_, select
from sqlalchemy.orm import Query


def get_query(
    query_plan: PlanResourcesResponse, table: Table, attr_map: dict[str, str]
) -> Query:
    if (
        query_plan.filter is None
        or query_plan.filter.kind == PlanResourcesFilterKind.ALWAYS_DENIED
    ):
        return select(table).where(False)
    if query_plan.filter.kind == PlanResourcesFilterKind.ALWAYS_ALLOWED:
        return select(table)

    def traverse_and_map_operands(operand: dict):
        if exp := operand.get("expression"):
            return traverse_and_map_operands(exp)

        operator = operand["operator"]
        child_operands = operand["operands"]

        # if `operator` in ["and", "or"], `child_operands` is a nested list of `expression` dicts (handled at the
        # beginning of this closure)
        if operator == "and":
            return and_(*[traverse_and_map_operands(o) for o in child_operands])
        if operator == "or":
            return or_(*[traverse_and_map_operands(o) for o in child_operands])

        # otherwise, they are a list[dict] (len==2), in the form: `[{'variable': 'foo'}, {'value': 'bar'}]`
        variable, value = [next(iter(l.values())) for l in child_operands]

        try:
            column = getattr(table.c, attr_map[variable])
        except KeyError:
            raise KeyError(
                f"Attribute does not exist in the attribute column map: {variable}"
            )
        except AttributeError:
            raise AttributeError(
                f"Table column name does not match key in attribute column map: {attr_map[variable]}"
            )

        # the operator handlers here are the leaf nodes of the recursion
        if operator == "eq":
            return column == value
        if operator == "ne":
            return column != value
        if operator == "lt":
            return column < value
        if operator == "gt":
            return column > value
        if operator == "lte":
            return column <= value
        if operator == "gte":
            return column >= value
        if operator == "in":
            return column.in_(value)

        raise ValueError(f"Unrecognised operator: {operator}")

    return select(table).where(
        traverse_and_map_operands(query_plan.filter.condition.to_dict())
    )
