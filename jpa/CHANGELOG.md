# Changelog

## Unreleased

### Added

- Translates `hasAll`, `hasAny`, `hasOnly` and `hasExactly` (CEL list functions from Cerbos
  0.58) against a literal list, as the `hasIntersection` and `isSubset` the adapter already
  lowers. A shape that rewrites to one the adapter refuses is refused the same way.

### Fixed

- `hasIntersection(collection.map(t, t.field), [])` keeps its UNKNOWN guard, so under a negation a row with a missing `field` is denied, as CEL's map() error denies it, instead of granted.
- Every read of a to-one association reuses one LEFT join instead of joining it again. A condition reading the parent many times reached MySQL's 61-table join limit and failed the whole query.
- A comparison of an `Integer` attribute against a whole constant outside the `Integer` range (`R.attr.count < 3000000000`) is translated, comparing the column as a long; Hibernate threw a `CoercionException` building the predicate.
