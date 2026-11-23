# Changelog

## Unreleased

## v0.7.0

### Added

- Translates `hasAny` and `hasAll` (CEL list functions from Cerbos 0.58) against a literal list,
  as `hasIntersection`. `hasOnly` and `hasExactly` lower to `isSubset`, which the adapter does not
  translate, so they are refused.

### Fixed

- MySQL: division read its column operands as single-precision floats (MySQL 8.0.17+ renders `CAST(x AS FLOAT)` that way), so `aDouble / 2.5 > 0.120000000000001` granted rows holding 0.3. Division now casts to `DOUBLE`.
- `relation_overrides()`: `hasIntersection(relation.map(...), [])` keeps the projection's error guard, so under a negation a row whose projection errors is denied, as CEL denies it, instead of granted.

## v0.6.2

### Changes

- An operator Cerbos plans do not contain is refused with `UnsupportedPlanError` before its operands are resolved (#756).
- `exists`, `all`, `exists_one`, `filter` and `map` without a lambda as the second operand, and a binary operator that does not have exactly two operands, are refused with `UnsupportedPlanError` (#756).
- The sdist no longer ships the test suites or the conformance harness (#751).
