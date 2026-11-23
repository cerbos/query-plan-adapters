# Changelog

## Unreleased

### Fixed

- `hasIntersection(collection.map(t, t.field), [])` keeps its UNKNOWN guard, so under a negation a row with a missing `field` is denied, as CEL's map() error denies it, instead of granted (jpa fix).
- Every read of a to-one association reuses one LEFT join instead of joining it again. A condition reading the parent many times reached MySQL's 61-table join limit and failed the whole query (jpa fix).
- A comparison of an `Integer` attribute against a whole constant outside the `Integer` range (`R.attr.count < 3000000000`) is translated, comparing the column as a long; Hibernate threw a `CoercionException` building the predicate (jpa fix).
