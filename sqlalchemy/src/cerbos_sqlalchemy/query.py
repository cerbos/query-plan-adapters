from types import MappingProxyType
from typing import Any, Callable

from google.protobuf.json_format import MessageToDict
from sqlalchemy import Column, Table, and_, not_, or_, select
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from sqlalchemy.sql import Select
from sqlalchemy.sql.expression import BinaryExpression, ColumnOperators

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse

GenericTable = Table | DeclarativeMeta
GenericColumn = Column | InstrumentedAttribute
GenericExpression = BinaryExpression | ColumnOperators
OperatorFnMap = dict[str, Callable[[GenericColumn, Any], GenericExpression]]


# We want to make the base dict "immutable", and enforce explicit (optional) overrides on
# each call to `get_query` (rather than allowing keys in this dict to be overridden, which
# could wreak havoc if different calls from the same memory space weren't aware of each other's
# overrides)
__operator_fns: OperatorFnMap = {
    "eq": lambda c, v: c == v,  # c, v denotes column, value respectively
    "ne": lambda c, v: c != v,
    "lt": lambda c, v: c < v,
    "gt": lambda c, v: c > v,
    "le": lambda c, v: c <= v,
    "ge": lambda c, v: c >= v,
    "in": lambda c, v: c.in_([v]) if not isinstance(v, list) else c.in_(v),
}
OPERATOR_FNS = MappingProxyType(__operator_fns)

# We support both the legacy HTTP and gRPC clients, so therefore we need to accept both input types
_deny_types = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_DENIED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_DENIED,
    ]
)
_allow_types = frozenset(
    [
        PlanResourcesFilterKind.ALWAYS_ALLOWED,
        engine_pb2.PlanResourcesFilter.KIND_ALWAYS_ALLOWED,
    ]
)


def _get_table_name(t: GenericTable) -> str:
    try:
        # `DeclarativeMeta` type
        return t.__table__.name
    except AttributeError:
        # `Table` type
        return t.name


def get_query(
    query_plan: PlanResourcesResponse | response_pb2.PlanResourcesResponse,
    table: GenericTable,
    attr_map: dict[str, GenericColumn],
    table_mapping: list[tuple[GenericTable, GenericExpression]] | None = None,
    operator_override_fns: OperatorFnMap | None = None,
) -> Select:
    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        return select(table).where(False)

    if query_plan.filter.kind in _allow_types:
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

    def get_operator_fn(op: str, c: GenericColumn, v: Any) -> GenericExpression:
        # Check to see if the client has overridden the function
        if (
            operator_override_fns
            and (override_fn := operator_override_fns.get(op)) is not None
        ):
            return override_fn(c, v)

        # Otherwise, fall back to default handlers
        if (default_fn := OPERATOR_FNS.get(op)) is not None:
            return default_fn(c, v)

        raise ValueError(f"Unrecognised operator: {op}")

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
        return get_operator_fn(operator, column, value)

    cond = (
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else query_plan.filter.condition.to_dict()
    )

    q = select(table).where(traverse_and_map_operands(cond))

    if table_mapping:
        q = q.select_from(table)
        for join_table, predicate in table_mapping:
            q = q.join(join_table, predicate)

    return q
