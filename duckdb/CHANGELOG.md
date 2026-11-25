# Changelog

## Unreleased

- First release: `translate()` turns a `PlanResources` response from either Cerbos SDK client into a
  DuckDB `WHERE` fragment and its `?` parameters. Collections are `LIST` columns, to-one relations
  are `Join`s, and every shape DuckDB cannot be asked exactly raises `UnsupportedPlanError`.
