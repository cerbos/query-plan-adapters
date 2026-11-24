# Changelog

## Unreleased

## v4.2.1

- Skips a full scan of the plan for null literals when translating with `nullAttributeRepresentation: "explicit"`.

## v4.2.0

- Translates `hasAll`, `hasAny`, `hasOnly` and `hasExactly` (CEL list functions from Cerbos
  0.58) against a literal list, as the `hasIntersection` and `isSubset` the adapter already
  lowers. A shape that rewrites to one the adapter refuses is refused the same way.
## v4.1.2

### Fixed over-grants

- `matches()` on a number or boolean field has no CEL overload, so it errors and is false either way. It used to be lowered to `$regex`, so the negation `!x.matches(...)` returned rows the PDP denies (#755).

### Fixed under-grants

- `&&` and `||` used as a value (`(name && flag) == false`) required every operand to be a boolean. They now follow CEL's absorption: `&&` is false once any operand is false, and `||` is true once any is true, whatever the others raise (#759).

### Other changes

- An operator Cerbos plans do not contain is refused with `UnsupportedQueryPlanError` before translation (#756).
- The package no longer ships the test suites or the conformance harness (#751).
