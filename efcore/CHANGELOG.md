# Changelog

## Unreleased

- Translating a conditional plan now reuses the parts of its protobuf expression tree that need no set-function rewrite.

- Translates `hasAll`, `hasAny`, `hasOnly` and `hasExactly` (CEL list functions from Cerbos
  0.58) against a literal list, as the `hasIntersection` and `isSubset` the adapter already
  lowers. A shape that rewrites to one the adapter refuses is refused the same way.
