# Changelog

## Unreleased

- **Breaking:** `Translate` rejects unknown `WithDialect` values with a configuration error.
  Use `dialect.SQLite` (the default), `dialect.Postgres` or `dialect.MySQL`; aliases and empty
  strings are no longer accepted.
