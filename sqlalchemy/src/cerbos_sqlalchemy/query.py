from types import MappingProxyType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, cast

from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from google.protobuf.json_format import MessageToDict

from sqlalchemy import Column, ColumnElement, Table, and_, literal, not_, or_, select
from sqlalchemy.orm import DeclarativeMeta, InstrumentedAttribute
from sqlalchemy.sql import Select

GenericTable = Union[Table, DeclarativeMeta]
GenericColumn = Union[Column, InstrumentedAttribute]
OperatorFnMap = Dict[str, Callable[[GenericColumn, Any], ColumnElement[bool]]]

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
    t_any = cast(Any, t)
    return getattr(t_any, "__table__", t_any).name


class QueryPlanParser:
    def __init__(
        self,
        attr_map: Dict[str, GenericColumn],
        operator_override_fns: Optional[OperatorFnMap] = None,
    ):
        self.attr_map = attr_map
        self.operator_override_fns = operator_override_fns or {}
        self._scope: List[Dict[str, Any]] = [{}]

    def get_column_or_value(self, variable_name: str) -> Any:
        for scope in reversed(self._scope):
            if variable_name in scope:
                return scope[variable_name]

            # sometimes the variable comes in like `w.tags`
            if "." in variable_name:
                base, path = variable_name.split(".", 1)
                if base in scope and isinstance(scope[base], dict):
                    return scope[base].get(path)

        try:
            return self.attr_map[variable_name]
        except KeyError as e:
            raise KeyError(
                f"Attribute does not exist in the attribute column map: {variable_name}"
            ) from e

    def traverse(self, operand: Union[Dict, Any]) -> Any:
        if isinstance(operand, dict):
            if "expression" in operand:
                return self.traverse(operand["expression"])
            if "variable" in operand:
                return self.get_column_or_value(operand["variable"])
            if "value" in operand:
                return operand["value"]
            if "operator" in operand:
                return self._handle_operator(
                    operand["operator"], operand.get("operands", [])
                )
        return operand

    def _resolve_and_normalise(
        self, op: str, operands: List[Dict]
    ) -> Tuple[Any, Any, str]:
        # we need to make sure the SQLAlchemy column is on the LHS (if relevant)
        lhs = self.traverse(operands[0])
        rhs = self.traverse(operands[1]) if len(operands) > 1 else None

        def is_sqla(x: Any) -> bool:
            return hasattr(x, "compile") or hasattr(x, "in_")

        if is_sqla(rhs) and not is_sqla(lhs):
            lhs, rhs = rhs, lhs
            # TODO(saml) invert operators? Might not be necessary

        return lhs, rhs, op

    def _handle_operator(self, op: str, operands: List[Dict]) -> Any:
        if op == "list":
            return [self.traverse(o) for o in operands]
        if op == "struct":
            return {k: v for o in operands for k, v in self.traverse(o).items()}
        if op == "set-field":
            return {self.traverse(operands[0]): self.traverse(operands[1])}

        if op == "all":
            return self._handle_all(operands)

        if op == "and":
            return and_(*[self.traverse(o) for o in operands])
        if op == "or":
            return or_(*[self.traverse(o) for o in operands])
        if op == "not":
            return not_(*[self.traverse(o) for o in operands])
        if op == "hasIntersection":
            return self._handle_intersection(operands)

        # normal comparison operators (eq, ne, lt, in, etc.)
        lhs, rhs, op = self._resolve_and_normalise(op, operands)

        if op in self.operator_override_fns:
            return self.operator_override_fns[op](lhs, rhs)

        if (default_fn := OPERATOR_FNS.get(op)) is not None:
            return default_fn(lhs, rhs)

        raise ValueError(f"Unrecognised operator: {op}")

    # NOTE: Dialect-Specific Implementations
    # The methods below are indicative (they've been constructed for this SQLite POC to pass the tests).
    # In practice, you'd implement the database specific functionality here (for example, to take advantage
    # of Clickhouse's many builtins).
    def _handle_all(self, operands: List[Dict]) -> ColumnElement[bool]:
        list_data = self.traverse(operands[0])
        lambda_def = operands[1]["expression"]

        if lambda_def["operator"] != "lambda":
            raise ValueError("Second operand of 'all' must be a lambda")

        lambda_body = lambda_def["operands"][0]
        lambda_arg_name = lambda_def["operands"][1]["variable"]

        # construct the literal values and append to the stack
        conditions = []
        for item in list_data:
            self._scope.append({lambda_arg_name: item})
            try:
                conditions.append(self.traverse(lambda_body))
            finally:
                self._scope.pop()

        return and_(*conditions) if conditions else literal(True)

    def _handle_intersection(self, operands: List[Dict]) -> Any:
        val = self.traverse(operands[0])
        col = self.traverse(operands[1])

        clauses = [col.like(f'%"{v}"%') for v in val]
        return or_(*clauses)


def get_query(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],  # type: ignore
    table: GenericTable,
    attr_map: Dict[str, GenericColumn],
    table_mapping: Union[List[Tuple[GenericTable, ColumnElement[bool]]], None] = None,
    operator_override_fns: Union[OperatorFnMap, None] = None,
) -> Select:
    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        return select(table).where(literal(False))

    if query_plan.filter.kind in _allow_types:
        return select(table)

    # Inspect passed columns. If > 1 origin table, assert that the mapping has been defined
    required_tables = set()
    for c in attr_map.values():
        # c is of type Union[Column, InstrumentedAttribute] - both have a `table` attribute returning a `Table` type
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

    cond = (
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else cast(Any, query_plan.filter.condition).to_dict()
    )

    parser = QueryPlanParser(attr_map, operator_override_fns)
    q = select(table).where(parser.traverse(cond))

    if table_mapping:
        q = q.select_from(table)
        for join_table, predicate in table_mapping:
            q = q.join(join_table, predicate)

    return q
