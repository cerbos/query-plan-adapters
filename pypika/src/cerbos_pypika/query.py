from types import MappingProxyType
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pypika import Table, Field, Query
from pypika.terms import Criterion
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
__operator_fns: OperatorFnMap = {}
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
