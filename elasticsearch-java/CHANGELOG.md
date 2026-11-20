# Changelog

## Unreleased

### Breaking

- `hasIntersection` against a scalar literal, and a field-first `in` against one, are refused with `UnsupportedPlanShapeException`. Both are CEL errors that deny every row; the terms query read the scalar as a one-element list and granted the rows holding it.
