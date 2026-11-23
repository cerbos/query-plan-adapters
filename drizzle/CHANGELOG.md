# Changelog

## Unreleased

## v0.6.0

### Added

- Translates `hasAll`, `hasAny`, `hasOnly` and `hasExactly` (CEL list functions from Cerbos
  0.58) against a literal list, as the `hasIntersection` and `isSubset` the adapter already
  lowers. A shape that rewrites to one the adapter refuses is refused the same way.

### Fixed

- `int()` over a string holding a NUL (`"-7\u0000x"`) is an error on SQLite, as in CEL. SQLite's `substr()` and `length()` stop at the NUL, so the part before it was converted: an over-grant under negation.
- `+`, `-` and `*` over a stored number are computed in doubles, as CEL computes them. On PostgreSQL an integer column at its 32-bit maximum overflowed the integer addition and failed the whole query.
- `+`, `-` and `*` over `int()` of an integer column are computed in `bigint` on PostgreSQL, as CEL computes ints in 64 bits. A column at its 32-bit bounds (`int(aNumber) - 5` at -2147483648) overflowed and failed the whole query.

## v0.5.0

### Breaking

- Every relation subquery now ranges under its own alias, named by its path (`cerbos_<relation>`). A `subqueryFilter` or `transform` is rebound to the alias through its drizzle columns, but one that names the related table in raw SQL no longer reaches the related row. Write it with drizzle columns instead (#750).

### Fixed over-grants

- Two relations into the same table (`origin.country == destination.country`) compared the inner row with itself, and a relation from a table to itself (`manager.id == "e02"`) was not correlated with the outer row. Both returned rows the PDP denies (#750).
- MySQL with `HIGH_NOT_PRECEDENCE` in `sql_mode`: a negated comparison, `LIKE` or `IN` was emitted as `not x < ?`, which that mode parses as `(not x) < ?`. Every `not` now wraps its operand in parentheses (#757).
- A bare attribute compared with `==` or `!=` to `timestamp(...)` is decided by type, as CEL does: `==` is false and `!=` is true where the value is present. A comparison against a non-literal timestamp is refused (#755).

### Other changes

- A drizzle `alias()` table declared as a relation's `table` now works (#750).
- A `map` or `filter` in boolean position is checked for unknown operators before it becomes UNKNOWN, so they are refused (#756).
- On MySQL, `sql_mode` must not include `NO_BACKSLASH_ESCAPES` when the driver interpolates parameters on the client, as `mysql2`'s `query` does. The README now says so (#758).
- The package no longer ships the test suites or the conformance harness (#751).
