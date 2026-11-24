# Changelog

## Unreleased

### Changed

- Conditional plans now resolve string indexes and list functions in one traversal, reducing translation overhead.

### Added

- Translates `hasAny` and `hasAll` (CEL list functions from Cerbos 0.58) against a literal list,
  as `hasIntersection`, in positive position only. `hasOnly` and `hasExactly` lower to `isSubset`, which the adapter does not
  translate, so they are refused.
- `ScalarType.INTEGER`, for a number in an `integer`, `short` or `byte` field. A comparison against a constant outside the 32-bit range (`R.attr.count < 3000000000`) is refused with `UnsupportedPlanShapeException`, where Elasticsearch rejected the query; such an element of an `in` list is dropped, as no document holds it. A `switch` over `ScalarType` needs the new case.

### Breaking

- `hasIntersection` against a scalar literal, and a field-first `in` against one, are refused with `UnsupportedPlanShapeException`. Both are CEL errors that deny every row; the terms query read the scalar as a one-element list and granted the rows holding it.
