## [Unreleased]

### Added

- `Cerbos::ActiveRecord.query_plan_to_relation` to translate a Cerbos `PlanResources` response into an `ActiveRecord::Relation`, with `Cerbos::ActiveRecord.field` and `Cerbos::ActiveRecord.relation` to map plan attributes onto the model ([#370](https://github.com/cerbos/query-plan-adapters/pull/370))

  Shapes the adapter cannot express faithfully raise a `Cerbos::ActiveRecord::Error` instead of emitting a filter.

- `string()` over a boolean column, translated as a `CASE` that spells `'true'` and `'false'` on every dialect ([#468](https://github.com/cerbos/query-plan-adapters/pull/468))

  It previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`, because `CAST(col AS TEXT)` gives `"1"` on SQLite and MySQL.

- Support for Ruby 4.0 ([#508](https://github.com/cerbos/query-plan-adapters/pull/508))

- `==` and `!=` between a field attribute and `null` under the `:omitted` NULL convention, rendered as `CASE WHEN col IS NULL THEN NULL ELSE FALSE END` (`ELSE TRUE` for `!=`) ([#551](https://github.com/cerbos/query-plan-adapters/issues/551))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. A NULL column is a missing attribute, which CEL answers with an error, so the comparison is UNKNOWN and stays UNKNOWN under `not`. It applies to a root column and to one reached through a to-one path such as `parent.tag`. A null in an `in` or `hasIntersection` list, and a null given to an operator override of `eq` or `ne`, still raise.

- `contains`, `startsWith`, `endsWith` and `size()` over a number or a boolean (a numeric or boolean column, a computed number or boolean, or a constant), translated as SQL `NULL` ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. CEL has no such overload, so every row is an error, decided by the declared type rather than the row's value. UNKNOWN denies under both polarities as the error does, and an operator applied to it (`==`, `in`, `string()` and the rest) is UNKNOWN too. A ternary over such an error, and these functions over a temporal column, still raise.

- A collection where a boolean belongs (`filter()`, `map()` or a mapped association as a condition, a conjunct or the operand of `!`), translated as SQL `NULL` ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  It previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. CEL's logical operators take only a boolean, so a list there is an error on every row.

- Arithmetic of an `int()` result beside an operand CEL certainly holds as something else (an attribute column, a computed double, a fractional constant), and `%` over such an operand, translated as SQL `NULL` ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. CEL has no overload mixing an int with a double, and `%` has no double overload, so every row is an error. Beside an operand whose type the plan does not settle, such as a ternary of whole constants, they still raise.

- `int()` over a double column, truncated toward zero inside CEL's range (-2^63, 2^63) and UNKNOWN outside it ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  It previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`, because PostgreSQL and MySQL round a `CAST`. The fraction is now dropped first (`TRUNC`, `TRUNCATE`; SQLite's `CAST` already truncates). `int()` over a decimal column still raises: CEL truncates the double nearest the stored value, which can differ.

- `==` and `!=` between two columns under mixed NULL conventions ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. The `:explicit` side's NULL is compared definitely as a null value, and a NULL on the other side, a missing attribute, makes the comparison UNKNOWN. `in` over a list mixing conventions still raises.

- Map literals and nested lists of constants, compared by CEL equality: `==` and `!=` against a column (always FALSE / TRUE, guarded for a missing attribute), `in` and `hasIntersection` with a list or map element, and a macro over a list of maps reading `m.field` ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. A map or list literal holding a column, one given to any other operator or to an operator override, and a ternary with a list or map arm still raise. A field a map does not hold is a CEL error, UNKNOWN. `x in map` tests the map's keys, as CEL does.

- `filter()` and `map()` over a list of constants, and `except()` of a list of constants or a scalar relation, evaluated element by element: `size()` of a filtered list or a difference, and `in` over a projected list ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. An element whose body errors makes the whole list an error (UNKNOWN), as `filter()` and `map()` never ignore one, and a missing attribute inside `except()`'s right list errors the call. `except` follows Cerbos's `exceptList`, keeping duplicates. Any other use of these lists still raises.

- `matches()` over a string column with a constant pattern, lowered to exact string predicates without the store's regex engine ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  It previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError` for every pattern. A pattern is lowered when its matches are a finite set of literals under its anchors, every character from a small set, or a prefix and suffix around non-newline characters; `(?i)` folds as RE2 does. A pattern RE2 rejects is UNKNOWN, as CEL's error is. Any other pattern, a pattern held in a column, and a non-string-column receiver still raise. The lowering is drizzle's, ported. An operator override for `matches` now replaces this built-in translation.

- `+`, `-` and `*` over a division that may be NaN or Infinity (`x / x + 1`), carried into the division's branches ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  They previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`. A non-finite constant is computed in Ruby with IEEE-754, and NaN beside a double stays NaN wherever that value is present. An Infinity beside a column, which might hold the opposite Infinity, and arithmetic between two such values still raise.

### Changed

- `== null`, `!= null` and `in` over a list holding `null` treat a NULL computed value (a connective, a quantifier, arithmetic, a `CASE`) as the CEL error it is, so the row is UNKNOWN ([#577](https://github.com/cerbos/query-plan-adapters/issues/577))

  CEL never holds a computed value as `null`. `(R.attr.aNumber > 1 && R.attr.aBool) in [false, null]` returned a row whose `aNumber` is NULL, because `IS NULL` read the error as a null value. Filters only get narrower: a ternary whose arm is an `:explicit` column that is NULL, compared with `null`, is now UNKNOWN where CEL would allow it.

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

- `string()` over any boolean-valued operand spells `"true"`/`"false"`, as `string()` over a boolean column already did: a comparison, a logical operator, `in`, a string predicate, a collection macro, and a `member_field` element held in a boolean column ([#471](https://github.com/cerbos/query-plan-adapters/issues/471))

  They previously went through `CAST(... AS TEXT)`, which gives `"1"`/`"0"` on SQLite and MySQL, so `!(string(R.attr.n > 3) == "true")` returned rows the policy denies.
  A NULL or missing operand still leaves the row out under both polarities.

- **Breaking:** a division whose operands are both CEL ints, at least one an `int()` result or int arithmetic on one, is int division truncating toward zero, as CEL's is ([#545](https://github.com/cerbos/query-plan-adapters/issues/545))

  `int(R.attr.n) / 2 == 1` used to divide as doubles, so `int(3) / 2` was `1.5`, the row was dropped, and the negation returned it though the PDP denies it. The division is now `/` over integers (`DIV` on MySQL). Its divisor must be a non-zero constant: CEL's int division by zero is an error that denies the row, where PostgreSQL aborts the query, so any other int divisor raises `Cerbos::ActiveRecord::UnsupportedOperatorError`.

- **Breaking:** arithmetic between an `int()` result and an operand that is not certainly an int (a column, a fractional constant), and `string()` over a ternary of whole-number constants whose int and double spellings differ, raise `Cerbos::ActiveRecord::UnsupportedOperatorError` ([#554](https://github.com/cerbos/query-plan-adapters/issues/554))

  CEL has no overload mixing int and double, so `int(R.attr.n) + R.attr.d > 0.0` is an error that denies every row, where SQL added the two and its negation returned rows the PDP denies. The plan carries `1000000` and `1000000.0` as the same number, which CEL's `string()` spells `"1000000"` and `"1e+06"`; `string(R.attr.flag ? 1000000 : 0)` used to cast the int. A ternary with an `int()` arm fixes its other arm as an int and still translates.

- **Breaking:** `in` against a list literal holding a column compares each element under each column's own null convention, as `==` does, and raises `Cerbos::ActiveRecord::UnsupportedOperatorError` when the needle and a member column are under different conventions ([#574](https://github.com/cerbos/query-plan-adapters/issues/574))

  `a in [b]` added a both-NULL branch from the call's convention, never either column's declaration, and wrapped an `:explicit` needle in an `IS NOT NULL` guard meant for a list of constants. So two `:explicit` NULLs did not match, `!(a in [b])` returned that row and missed a value beside a NULL, and two `:omitted` NULLs matched under the call's default `:explicit`. A NULL `:omitted` column now makes the whole membership UNKNOWN, so `a in [b, 2]` no longer grants `a = 2` when `b` is missing.

- `value in R.attr.<relation>` compares an `:explicit` value against the related rows' member column under that declaration, whatever the call's `null_attribute_representation` says ([#591](https://github.com/cerbos/query-plan-adapters/issues/591))

  The both-NULL branch came from the call's convention, so under a call-level `:omitted` a NULL `:explicit` value never matched a NULL member, and `!(value in R.attr.<relation>)` returned that row though the PDP denies it.

### Removed

- Support for Ruby 3.2 ([#508](https://github.com/cerbos/query-plan-adapters/pull/508))

[Unreleased]: https://github.com/cerbos/query-plan-adapters/commits/main/activerecord
