# Changelog

## Unreleased

### Fixed

- `relation_overrides()`: `hasIntersection(relation.map(...), [])` keeps the projection's error guard, so under a negation a row whose projection errors is denied, as CEL denies it, instead of granted.

## v0.6.2

### Changes

- An operator Cerbos plans do not contain is refused with `UnsupportedPlanError` before its operands are resolved (#756).
- `exists`, `all`, `exists_one`, `filter` and `map` without a lambda as the second operand, and a binary operator that does not have exactly two operands, are refused with `UnsupportedPlanError` (#756).
- The sdist no longer ships the test suites or the conformance harness (#751).
