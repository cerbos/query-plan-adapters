import importlib.metadata
from cerbos_pypika.query import cerbos_plan_criterion, OPERATOR_FNS

try:
    __version__ = importlib.metadata.version(__package__ or __name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "dev"
    
__all__ = ["cerbos_plan_criterion", "OPERATOR_FNS"]
