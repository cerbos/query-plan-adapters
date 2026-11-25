# Changelog

## Unreleased

- First release: `translate()` turns a `PlanResources` response from either Cerbos SDK client into a
  DuckDB `WHERE` fragment and its `?` parameters. Collections are `LIST` columns, to-one relations
  are `Join`s, and every shape DuckDB cannot be asked exactly raises `UnsupportedPlanError`.
- List operations run as `list_filter` lambdas over the `LIST` value instead of correlated `unnest`
  joins, an order of magnitude faster on large tables; `unnest` remains only where the predicate
  reads a `Join`.
