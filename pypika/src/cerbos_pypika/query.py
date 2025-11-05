from types import MappingProxyType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pypika import Table, Field, Query
from pypika.terms import Criterion, ValueWrapper
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from google.protobuf.json_format import MessageToDict

# Type aliases
GenericField = Field
GenericCriterion = Criterion
OperatorFnMap = Dict[str, Callable[[GenericField, Any], GenericCriterion]]
JoinSpec = Tuple[Table, GenericCriterion]

# Operator function map (will be populated incrementally during TDD)
__operator_fns: OperatorFnMap = {
    "eq": lambda field, value: field == value,
    "ne": lambda field, value: field != value,
    "lt": lambda field, value: field < value,
    "gt": lambda field, value: field > value,
    "le": lambda field, value: field <= value,
    "ge": lambda field, value: field >= value,
}
OPERATOR_FNS = MappingProxyType(__operator_fns)

# Filter kind constants
_deny_types = frozenset([
    PlanResourcesFilterKind.ALWAYS_DENIED,
    engine_pb2.PlanResourcesFilter.KIND_ALWAYS_DENIED,
])
_allow_types = frozenset([
    PlanResourcesFilterKind.ALWAYS_ALLOWED,
    engine_pb2.PlanResourcesFilter.KIND_ALWAYS_ALLOWED,
])


def _handle_comparison_operator(operator: str, operands: List[Dict], attr_map: Dict[str, GenericField], operator_override_fns: Optional[OperatorFnMap] = None) -> GenericCriterion:
    """Extract variable and value from operands and apply comparison operator."""
    d = {k: v for o in operands for k, v in o.items()}
    variable = d["variable"]
    value = d["value"]
    
    try:
        field = attr_map[variable]
    except KeyError:
        raise KeyError(f"Attribute does not exist in the attribute column map: {variable}")
    
    operator_fns = operator_override_fns or OPERATOR_FNS
    try:
        operator_fn = operator_fns[operator]
    except KeyError:
        raise ValueError(f"Unknown operator: {operator}")
    
    return operator_fn(field, value)


def _handle_logical_operator(operator: str, operands: List[Dict], attr_map: Dict[str, GenericField], operator_override_fns: Optional[OperatorFnMap] = None) -> GenericCriterion:
    """Handle logical operators (and, or, not) by recursively evaluating operands."""
    if operator == "and":
        criteria = [
            traverse_and_map_operands(o, attr_map, operator_override_fns)
            for o in operands
        ]
        result = criteria[0]
        for c in criteria[1:]:
            result = result & c
        return result
    
    raise ValueError(f"Unknown logical operator: {operator}")


def traverse_and_map_operands(operand, attr_map, operator_override_fns=None):
    """Recursively traverse Cerbos AST and build PyPika criterion."""
    if exp := operand.get("expression"):
        return traverse_and_map_operands(exp, attr_map, operator_override_fns)
    
    operator = operand["operator"]
    child_operands = operand["operands"]
    
    # Handle logical operators
    if operator in ("and", "or", "not"):
        return _handle_logical_operator(operator, child_operands, attr_map, operator_override_fns)
    
    # Handle comparison operators
    return _handle_comparison_operator(operator, child_operands, attr_map, operator_override_fns)


def get_query(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],
    table: Table,
    attr_map: Dict[str, GenericField],
    joins: Optional[List[JoinSpec]] = None,
    operator_override_fns: Optional[OperatorFnMap] = None,
) -> Query:
    """
    Convert a Cerbos query plan into a PyPika query with authorization filters.
    
    Args:
        query_plan: Cerbos PlanResourcesResponse containing the filter AST
        table: PyPika Table instance representing the primary query table
        attr_map: Mapping of Cerbos attribute paths to PyPika Field objects
        joins: Optional list of (table, join_condition) tuples for multi-table queries
        operator_override_fns: Optional custom operator implementations
        
    Returns:
        PyPika Query instance with authorization filters applied
        
    Raises:
        KeyError: If an attribute in the query plan is not in attr_map
        ValueError: If an unrecognized operator is encountered
    """
    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        return Query.from_(table).select('*').where(ValueWrapper(1) == ValueWrapper(0))
    
    if query_plan.filter.kind in _allow_types:
        return Query.from_(table).select('*')
    
    cond_dict = query_plan.filter.condition.to_dict()
    criterion = traverse_and_map_operands(cond_dict, attr_map, operator_override_fns)
    
    query = Query.from_(table).select('*').where(criterion)
    return query
