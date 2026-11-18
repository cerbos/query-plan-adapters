"""Print the SQL a domain mapping emits for one golden: show_sql.py <dialect> <style> <golden path>."""

import importlib
import json
import sys

import harness
from sqlalchemy.dialects import mysql, postgresql, sqlite

from cerbos_sqlalchemy import get_query

dialect_name, style, path = sys.argv[1:4]
with open(path) as f:
    golden = json.load(f)
module = importlib.import_module(golden["domain"])
query = get_query(
    harness.plan_for(golden), module.ROOT, **module.mapping(style, dialect_name)
)
dialect = {"sqlite": sqlite, "postgresql": postgresql, "mysql": mysql}[dialect_name].dialect()
print(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
