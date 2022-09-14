from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse

from sqlalchemy import Column, Table, and_, not_, or_, select
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import BinaryExpression, ColumnOperators


def _get_table_name(t: DeclarativeMeta | Table) -> str:
    try:
        # `DeclarativeMeta` type
        return t.__table__.name
    except AttributeError:
        # `Table` type
        return t.name


def get_query(
    query_plan: PlanResourcesResponse,
    table: Table | DeclarativeMeta,
    attr_map: dict[str, Column | InstrumentedAttribute],
    table_mapping: list[
        tuple[Table | DeclarativeMeta, BinaryExpression | ColumnOperators]
    ]
    | None = None,
) -> Select:
    if (
        query_plan.filter is None
        or query_plan.filter.kind == PlanResourcesFilterKind.ALWAYS_DENIED
    ):
        return select(table).where(False)
    if query_plan.filter.kind == PlanResourcesFilterKind.ALWAYS_ALLOWED:
        return select(table)

    # Inspect passed columns. If > 1 origin table, assert that the mapping has been defined
    required_tables = set()
    for c in attr_map.values():
        # c is of type Column | InstrumentedAttribute - both have a `table` attribute returning a `Table` type
        if (n := c.table.name) != _get_table_name(table):
            required_tables.add(n)

    if len(required_tables):
        if table_mapping is None:
            raise TypeError(
                "get_query() missing 1 required positional argument: 'table_mapping'"
            )
        for t, _ in table_mapping:
            required_tables.discard(_get_table_name(t))
        if len(required_tables):
            raise TypeError(
                "positional argument 'table_mapping' missing mapping for table(s): '{0}'".format(
                    "', '".join(required_tables)
                )
            )

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
        if operator == "not":
            return not_(*[traverse_and_map_operands(o) for o in child_operands])

        # otherwise, they are a list[dict] (len==2), in the form: `[{'variable': 'foo'}, {'value': 'bar'}]`
        # The order of the keys `variable` and `value` is not guaranteed.
        d = {k: v for o in child_operands for k, v in o.items()}
        variable = d["variable"]
        value = d["value"]

        try:
            column = attr_map[variable]
        except KeyError:
            raise KeyError(
                f"Attribute does not exist in the attribute column map: {variable}"
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

    q = select(table).where(
        traverse_and_map_operands(query_plan.filter.condition.to_dict())
    )

    if table_mapping:
        q = q.select_from(table)
        for join_table, predicate in table_mapping:
            q = q.join(join_table, predicate)

    return q
