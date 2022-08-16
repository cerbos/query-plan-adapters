import importlib.metadata

# TODO ensure consistency with PyPI name
__version__ = importlib.metadata.version(__package__ or __name__)
