# Changelog

## Unreleased

## v4.1.2

### Fixed over-grants

- `matches()` on a number or boolean field has no CEL overload, so it errors and is false either way. It used to be lowered to `$regex`, so the negation `!x.matches(...)` returned rows the PDP denies (#755).

### Fixed under-grants

- `&&` and `||` used as a value (`(name && flag) == false`) required every operand to be a boolean. They now follow CEL's absorption: `&&` is false once any operand is false, and `||` is true once any is true, whatever the others raise (#759).

### Other changes

- An operator Cerbos plans do not contain is refused with `UnsupportedQueryPlanError` before translation (#756).
- The package no longer ships the test suites or the conformance harness (#751).
