from types import MappingProxyType
from typing import Any, Callable, Dict, List, Optional, Union

from pypika import Field
from pypika.terms import Criterion, ValueWrapper
from cerbos.sdk.model import PlanResourcesFilterKind, PlanResourcesResponse
from cerbos.engine.v1 import engine_pb2
from cerbos.response.v1 import response_pb2
from google.protobuf.json_format import MessageToDict

# Type aliases
GenericField = Field
GenericCriterion = Criterion
OperatorFnMap = Dict[str, Callable[[GenericField, Any], GenericCriterion]]

# We want to make the base dict "immutable", and enforce explicit (optional) overrides on
# each call to `cerbos_plan_criterion` (rather than allowing keys in this dict to be overridden,
# which could wreak havoc if different calls from the same memory space weren't aware of each
# other's overrides)
__operator_fns: OperatorFnMap = {
    "eq": lambda f, v: f == v,  # f, v denotes field, value respectively
    "ne": lambda f, v: f != v,
    "lt": lambda f, v: f < v,
    "gt": lambda f, v: f > v,
    "le": lambda f, v: f <= v,
    "ge": lambda f, v: f >= v,
    "in": lambda f, v: f.isin(v if isinstance(v, list) else [v]),
}
OPERATOR_FNS = MappingProxyType(__operator_fns)

# Support both SDK and gRPC response types from Cerbos
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
    operand_dict = {k: v for o in operands for k, v in o.items()}
    variable_path = operand_dict["variable"]
    comparison_value = operand_dict["value"]
    
    try:
        field = attr_map[variable_path]
    except KeyError:
        raise KeyError(f"Attribute does not exist in the attribute column map: {variable_path}")
    
    # Check to see if the client has overridden the function
    if operator_override_fns and (override_fn := operator_override_fns.get(operator)) is not None:
        return override_fn(field, comparison_value)
    
    # Otherwise, fall back to default handlers
    if (default_fn := OPERATOR_FNS.get(operator)) is not None:
        return default_fn(field, comparison_value)
    
    raise ValueError(f"Unrecognised operator: {operator}")


def _handle_logical_operator(operator: str, operands: List[Dict], attr_map: Dict[str, GenericField], operator_override_fns: Optional[OperatorFnMap] = None) -> GenericCriterion:
    """Handle logical operators (and, or, not) by recursively evaluating operands."""
    if operator in ("and", "or"):
        criteria = [
            _traverse_and_map_operands(o, attr_map, operator_override_fns)
            for o in operands
        ]
        result = criteria[0]
        combinator = (lambda a, b: a & b) if operator == "and" else (lambda a, b: a | b)
        for c in criteria[1:]:
            result = combinator(result, c)
        return result
    
    if operator == "not":
        if not operands:
            raise ValueError("NOT operator requires exactly one operand")
        criterion = _traverse_and_map_operands(operands[0], attr_map, operator_override_fns)
        return criterion.negate()
    
    raise ValueError(f"Unknown logical operator: {operator}")


def _traverse_and_map_operands(operand: Dict[str, Any], attr_map: Dict[str, GenericField], operator_override_fns: Optional[OperatorFnMap] = None) -> GenericCriterion:
    """Recursively traverse Cerbos AST and build PyPika criterion."""
    if exp := operand.get("expression"):
        return _traverse_and_map_operands(exp, attr_map, operator_override_fns)
    
    operator = operand["operator"]
    child_operands = operand["operands"]
    
    # Handle logical operators
    if operator in ("and", "or", "not"):
        return _handle_logical_operator(operator, child_operands, attr_map, operator_override_fns)
    
    # Handle comparison operators
    return _handle_comparison_operator(operator, child_operands, attr_map, operator_override_fns)


def cerbos_plan_criterion(
    query_plan: Union[PlanResourcesResponse, response_pb2.PlanResourcesResponse],  # type: ignore (https://github.com/microsoft/pyright/issues/1035)
    attr_map: Dict[str, GenericField],
    operator_override_fns: Optional[OperatorFnMap] = None,
) -> Optional[GenericCriterion]:
    """
    Convert a Cerbos query plan into a PyPika Criterion for use in WHERE clauses.
    
    This is the PyPika-idiomatic way to apply Cerbos authorization filters. The returned
    Criterion can be used directly in `.where()` clauses, combined with other criteria
    using `&` (AND) and `|` (OR), or negated with `.negate()`.
    
    Args:
        query_plan: Cerbos PlanResourcesResponse containing the filter AST
        attr_map: Mapping of Cerbos attribute paths to PyPika Field objects
        operator_override_fns: Optional custom operator implementations
        
    Returns:
        PyPika Criterion representing the authorization filter, or None for special cases
        
    Raises:
        KeyError: If an attribute in the query plan is not in attr_map
        ValueError: If an unrecognized operator is encountered
        
    Example:
        >>> from cerbos_pypika import cerbos_plan_criterion
        >>> from pypika import Query, Table
        >>> 
        >>> resources = Table('resources')
        >>> criterion = cerbos_plan_criterion(plan, attr_map)
        >>> query = Query.from_(resources).select('*').where(criterion)
        >>> 
        >>> # Can combine with other criteria:
        >>> custom_filter = resources.active == True
        >>> query = Query.from_(resources).select('*').where(criterion & custom_filter)
    """
    # Handle special filter kinds
    if query_plan.filter is None or query_plan.filter.kind in _deny_types:
        # ALWAYS_DENIED: return criterion that's always false
        return ValueWrapper(1) == ValueWrapper(0)
    
    if query_plan.filter.kind in _allow_types:
        # ALWAYS_ALLOWED: return None (no filtering needed)
        return None
    
    # Convert condition to dict (handle both SDK and gRPC response types)
    cond_dict = (
        MessageToDict(query_plan.filter.condition)
        if isinstance(query_plan, response_pb2.PlanResourcesResponse)
        else query_plan.filter.condition.to_dict()
    )
    
    return _traverse_and_map_operands(cond_dict, attr_map, operator_override_fns)
