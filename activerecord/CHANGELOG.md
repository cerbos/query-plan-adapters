## [Unreleased]

### Added

- `Cerbos::ActiveRecord.query_plan_to_relation` to translate a Cerbos `PlanResources` response into an `ActiveRecord::Relation`, with `Cerbos::ActiveRecord.field` and `Cerbos::ActiveRecord.relation` to map plan attributes onto the model ([#370](https://github.com/cerbos/query-plan-adapters/pull/370))

  Shapes the adapter cannot express faithfully raise a `Cerbos::ActiveRecord::Error` instead of emitting a filter.

- `string()` over a boolean column, translated as a `CASE` that spells `'true'` and `'false'` on every dialect ([#468](https://github.com/cerbos/query-plan-adapters/pull/468))

  It previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`, because `CAST(col AS TEXT)` gives `"1"` on SQLite and MySQL.

- Support for Ruby 4.0 ([#508](https://github.com/cerbos/query-plan-adapters/pull/508))

### Changed

- The conformance suite runs on PostgreSQL and MySQL as well as SQLite, and the fixes below are what those stores exposed ([#500](https://github.com/cerbos/query-plan-adapters/issues/500))

  Filters get narrower or stop failing; nothing that translated now raises.

  - `+`, `-` and `*` read an integer or decimal column as a double, as CEL reads every attribute number. PostgreSQL and MySQL computed `aNumber * 0.1` in exact decimal, so `aNumber * 0.1 == 0.3` held for 3, where CEL computes `0.30000000000000004`.
  - `%` by a column that is zero is UNKNOWN (`NULLIF`), as CEL's error is. PostgreSQL raised `division by zero` and failed the whole query.
  - `string()` of a string column casts to `CHAR` on MySQL, where `CAST(... AS VARCHAR)` was a syntax error.
  - A whole number in a plan beyond the int64 range (`R.attr.aDouble > -1e19`) is bound as a double, where ActiveRecord refused to bind it on PostgreSQL.
  - `ancestorOf`, `descendentOf` and `overlaps` over `hierarchy()` of a number or boolean column are UNKNOWN, as CEL's type error is. PostgreSQL refused the `LIKE` on a number.

- **Breaking:** `size()`, `contains`, `startsWith` and `endsWith` raise `Cerbos::ActiveRecord::UnsupportedOperatorError` for a numeric or boolean column ([#458](https://github.com/cerbos/query-plan-adapters/pull/458))

  See [#414](https://github.com/cerbos/query-plan-adapters/issues/414).

- **Breaking:** a comparison between raw temporal columns raises `Cerbos::ActiveRecord::UnsupportedOperatorError` unless both operands are wrapped in `timestamp()`, because SQL discards the RFC 3339 spelling that CEL compares ([#458](https://github.com/cerbos/query-plan-adapters/pull/458))

  See [#414](https://github.com/cerbos/query-plan-adapters/issues/414).

- **Breaking:** `in` with a list or map element raises `Cerbos::ActiveRecord::UnsupportedOperatorError` before SQL rendering ([#458](https://github.com/cerbos/query-plan-adapters/pull/458))

  See [#414](https://github.com/cerbos/query-plan-adapters/issues/414).

- Negated scalar-list macros, omitted scalar membership, hierarchy prefix shortcuts and NaN ordering preserve CEL's null and error behaviour through negation ([#458](https://github.com/cerbos/query-plan-adapters/pull/458))

  See [#414](https://github.com/cerbos/query-plan-adapters/issues/414).

- Comparisons between known different scalar types follow CEL equality and missing-value rules instead of letting SQL coerce (for example, `"0"` to a number) ([#458](https://github.com/cerbos/query-plan-adapters/pull/458))

  Two declared explicit nulls still compare equal.
  See [#414](https://github.com/cerbos/query-plan-adapters/issues/414).

- Constant NaN ordering follows Cerbos 0.55 ([#467](https://github.com/cerbos/query-plan-adapters/pull/467))

  An unordered comparison is false, so its negation is true.
  Under Cerbos 0.54 it was an evaluation error and stayed denied under negation.
  Missing attributes and other evaluation errors are unchanged.

- `in` and `hasIntersection` drop every literal whose CEL type differs from the elements' ([#505](https://github.com/cerbos/query-plan-adapters/pull/505))

  This applies to a relation mapped by `member_field` and to the `IN` list for a scalar column: `"2" in aNumberList` is false, where SQLite's REAL affinity read `'2'` as the number 2 and matched, and `aNumber in ["5", 2]` is `a_number IN (2)`.
  Filters get narrower; nothing that translated now raises.

- **Breaking:** `%` raises `Cerbos::ActiveRecord::UnsupportedOperatorError` unless both operands are `int()` results, `size()` results or whole constants

  Every number in a request attribute is a double, and CEL's `%` has no double overload, so `R.attr.n % 2` errors on every row where SQL computed a remainder.

- **Breaking:** `string()` over a numeric column is translated only in `==` and `!=` against a string literal, which compare the column with the double CEL spells that way; any other use, and a `"0"` or `"-0"` literal, raises `Cerbos::ActiveRecord::UnsupportedOperatorError`

  `CAST(col AS TEXT)` spells `2.0` as `"2.0"` and `1000000.0` as `"1000000.0"`, where CEL writes `"2"` and `"1e+06"`. `string(int(col))` still casts.

- `hierarchy()` accepts the empty delimiter, which splits a path into one segment per character, as Cerbos does

- Comparing a scalar with a list literal is false (UNKNOWN for a missing attribute), as in CEL, instead of failing with a `TypeError`

### Removed

- Support for Ruby 3.2 ([#508](https://github.com/cerbos/query-plan-adapters/pull/508))

[Unreleased]: https://github.com/cerbos/query-plan-adapters/commits/main/activerecord
