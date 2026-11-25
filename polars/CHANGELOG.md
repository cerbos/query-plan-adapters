# Changelog

## Unreleased

### Added

- First release: `get_filter(plan, schema, attr_map)` translates a Cerbos query plan into one Polars
  expression for `DataFrame.filter` and `LazyFrame.filter`, eager or on the streaming engine. Every
  shape it cannot translate exactly raises `UnsupportedPlanError`. Needs Python 3.11+ and
  Polars 2.2+.
