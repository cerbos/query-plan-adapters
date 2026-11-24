# Changelog

## Unreleased

## v0.2.1

- Reduced allocations when translating conditional plans by removing a copy of the expression tree.

## v0.2.0

- Translates `hasAll`, `hasAny`, `hasOnly` and `hasExactly` (CEL list functions from Cerbos
  0.58) against a literal list, as the `hasIntersection` and `isSubset` the adapter already
  lowers. A shape that rewrites to one the adapter refuses is refused the same way.
- Requires `github.com/cerbos/cerbos/api/genpb` v0.58.0 (was v0.57.0), and is tested against
  Cerbos 0.58.0 and 0.57.0.
- **Breaking:** an ordering between two attributes neither of which declares a `ValueType` is
  refused. PostgreSQL ordered them under the columns' own collation, which under a linguistic one
  (ICU `en-US`) puts "one" before "One", an over-grant. Declare string columns `ValueString`.

## v0.1.1

- Fixed an over-grant on PostgreSQL: a timestamp literal with digits past the microsecond was rounded to the stored microsecond grid when bound, so `>=` and `<` could admit the row on the boundary.
- A `filter` or `map` in boolean position is translated before it becomes UNKNOWN, so a shape inside it the adapter cannot read is refused.
