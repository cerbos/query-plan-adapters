# Changelog

## Unreleased

- **Breaking:** `%` over an attribute, a comparison decided by the sign of an infinity from a zero
  column denominator, and `string()` over a `ValueNumber` column outside `==`/`!=` against a
  string constant (or against `"0"`/`"-0"`) now return an error wrapping `ErrUnsupported` instead of
  a filter that disagreed with CEL. `string(number) == "1e+06"` now lowers to a numeric comparison
  with the double CEL spells that way, and a list literal compared with a declared scalar column is
  unequal instead of failing in the driver.
- `string()` over a column declared `ValueBool` now translates, to
  `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END` cast to text, instead of
  returning an error wrapping `ErrUnsupported`. The `CASE` spells CEL's `"true"` and `"false"` on
  SQLite, PostgreSQL and MySQL alike, where a `CAST` renders a stored boolean as `"1"` on two of
  them (#418).
- **Breaking:** `Translate` rejects unknown `WithDialect` values with a configuration error.
  Use `dialect.SQLite` (the default), `dialect.Postgres` or `dialect.MySQL`; aliases and empty
  strings are no longer accepted.
