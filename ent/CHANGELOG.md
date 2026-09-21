# Changelog

## Unreleased

- `string()` over a column declared `ValueBool` now translates, to
  `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END` cast to text, instead of
  returning an error wrapping `ErrUnsupported`. The `CASE` spells CEL's `"true"` and `"false"` on
  SQLite, PostgreSQL and MySQL alike, where a `CAST` renders a stored boolean as `"1"` on two of
  them (#418).
- **Breaking:** `Translate` rejects unknown `WithDialect` values with a configuration error.
  Use `dialect.SQLite` (the default), `dialect.Postgres` or `dialect.MySQL`; aliases and empty
  strings are no longer accepted.
