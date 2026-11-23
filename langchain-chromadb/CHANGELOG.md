# Changelog

## Unreleased

- Translates `hasAll`, `hasAny`, `hasOnly` and `hasExactly` (CEL list functions from Cerbos
  0.58) against a literal list, as the `hasIntersection` and `isSubset` the adapter already
  lowers. A shape that rewrites to one the adapter refuses is refused the same way.
## v0.2.1

### Fixed over-grants

- A fractional literal compared with integer metadata (`x == 0.3`, `x > 2.5`, `x in [0.3]`) matched as if truncated toward zero, because the Chroma server truncates the literal and the client stores whole-valued doubles as integers. The literal is now bounded by the integers on either side of it (#732).

### Other changes

- Anything in place of a lambda that is not a `lambda` is refused with `UnsupportedOperatorError` (#756).
- The package no longer ships the test suites or the conformance harness (#751).
