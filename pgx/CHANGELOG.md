# Changelog

## Unreleased

## v0.1.1

- Fixed an over-grant on PostgreSQL: a timestamp literal with digits past the microsecond was rounded to the stored microsecond grid when bound, so `>=` and `<` could admit the row on the boundary.
- A `filter` or `map` in boolean position is translated before it becomes UNKNOWN, so a shape inside it the adapter cannot read is refused.
