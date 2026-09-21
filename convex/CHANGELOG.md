# Changelog

## 0.3.0

- **Breaking:** `QueryPlanToConvexResult` now requires a `path` discriminator on conditional
  results: `db` requires `filter`, `post` requires `postFilter`, and `split` requires both.
  Unconditional results carry neither function. Existing destructuring remains supported.
- Reuse the adversarial document schema for table validation, mutation arguments and stored types.
