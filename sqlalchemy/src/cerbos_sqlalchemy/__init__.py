import importlib.metadata

from cerbos_sqlalchemy.query import get_query
from cerbos_sqlalchemy.relations import require_hops

__version__ = importlib.metadata.version(__package__ or __name__)

__all__ = ["get_query", "require_hops"]
