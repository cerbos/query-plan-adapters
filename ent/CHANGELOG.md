# Changelog

## Unreleased

- **Breaking:** a bare attribute mapped `ValueTimestamp` compared with a `timestamp()` value
  (`R.attr.createdAt < now() - duration("24h")`) is answered as CEL answers it: CEL holds the bare
  attribute as its RFC 3339 string, so an ordering is a no-overload error (UNKNOWN, the row is
  excluded under both polarities), `==` is false and `!=` true. It used to compare the stored
  instants and returned rows the PDP denies (`type-mismatch/less-than/string-field-against-timestamp`).
  Wrap the attribute in `timestamp()` to compare instants.
- **Breaking:** a list or map literal that still reaches a plain value position fails closed. It
  used to be bound as one opaque parameter, which is how `"x" in (R.attr.flag ? ["x"] : [])` matched
  no row at all (`composition/derived-role/runtime-effective-derived-roles`); that shape now
  translates, with the ternary lifted above the membership.
- `!("x" in R.attr.list)` over a relation whose element is declared `NullConventionExplicit` keeps
  a row whose list holds a null and no `"x"`: CEL's `"x" == null` is false, where SQL's `=` was
  UNKNOWN and dropped the row (`membership/in/negated-literal-in-resource-list`).
- Newly translated, each proved by the corpus: a ternary yielding a list, lifted above the
  comparison or membership it feeds; `+` between list literals, and `x in R.attr.list + [...]`
  as a disjunction of memberships; `x in [e1, …]` and `exists()`/`all()` over a list of computed
  elements, UNKNOWN unless every element evaluates; constant map literals, and
  `{"k": v, …}[x] == c`; `m["key"]` as the mapped member `m.key`; `isSubset()` of a stored list
  against a literal list; `except(a, b) == []` and `intersect(a, b) == []`; `c in` the result of
  `filter()` or `map()`; `upperAscii()`, as a `REPLACE` per ASCII letter so no collation's Unicode
  case folding applies; and `duration()`, `timestamp() ± duration` and `timeSince()`, folded into
  the constant side of the comparison. `timeSince()` reads the clock at translation, as `check()`
  reads it at evaluation.
- `x == null` and `x != null` over an attribute declared `NullConventionOmitted` now translate, to
  `CASE WHEN x IS NULL THEN NULL ELSE FALSE END` (`ELSE TRUE` for `!=`), instead of returning an
  error wrapping `ErrUnsupported`. A NULL column is CEL's missing-attribute error, which the `CASE`
  keeps UNKNOWN under any negation, and a column read through a to-one `ScalarRelation` renders the
  same way. Every other null operand against such an attribute is still refused, and the
  call-level `NullOmitted` is unchanged (#551).
- **Breaking:** `string()` over a numeric constant, directly or as a ternary branch
  (`string(R.attr.flag ? 1000000 : 0)`), now returns an error wrapping `ErrUnsupported`. The plan
  ships an int and a double constant as the same number, which CEL renders differently
  (`"1000000"` and `"1e+06"`), and SQLite's CAST says `"1000000.0"`, so the comparison denied the
  rows the PDP allowed and its negation returned them. A to-one relation used as a value
  (`"k" in R.attr.parent`) is refused at translation instead of emitting SQL that fails to run
  (#554).
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
- `string()` over a boolean-valued expression (`string(R.attr.n > 3)`), or over a column declared
  `ValueBool` and read through a to-one `ScalarRelation`, is spelled through the same `CASE` as a
  plain `ValueBool` column. It used to emit a plain `CAST`, which SQLite and MySQL render as
  `"1"`/`"0"`, so a negated comparison with `"true"` returned rows the PDP denies (#470).
- `string()` over a ternary whose arms are all boolean (`string(R.attr.n > 3 ? R.attr.flag : false)`)
  is spelled through the same `CASE`, instead of a plain `CAST` of the ternary that SQLite and MySQL
  render as `"1"`/`"0"` (#538).
- **Breaking:** `Translate` rejects unknown `WithDialect` values with a configuration error.
  Use `dialect.SQLite` (the default), `dialect.Postgres` or `dialect.MySQL`; aliases and empty
  strings are no longer accepted.
