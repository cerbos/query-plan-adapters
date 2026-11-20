# Changelog

## Unreleased

### Fixed

- `hasIntersection(collection.map(t, t.field), [])` keeps its UNKNOWN guard, so under a negation a row with a missing `field` is denied, as CEL's map() error denies it, instead of granted.
