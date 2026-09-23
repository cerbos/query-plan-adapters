## [Unreleased]

### Added

- `Cerbos::ActiveRecord.query_plan_to_relation` to translate a Cerbos `PlanResources` response into an `ActiveRecord::Relation`, with `Cerbos::ActiveRecord.field` and `Cerbos::ActiveRecord.relation` to map plan attributes onto the model ([#370](https://github.com/cerbos/query-plan-adapters/pull/370))

  Shapes the adapter cannot express faithfully raise a `Cerbos::ActiveRecord::Error` instead of emitting a filter.

- `string()` over a boolean column, translated as a `CASE` that spells `'true'` and `'false'` on every dialect ([#468](https://github.com/cerbos/query-plan-adapters/pull/468))

  It previously raised `Cerbos::ActiveRecord::UnsupportedOperatorError`, because `CAST(col AS TEXT)` gives `"1"` on SQLite and MySQL.

- Support for Ruby 4.0

### Changed

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

### Removed

- Support for Ruby 3.2

[Unreleased]: https://github.com/cerbos/query-plan-adapters/commits/main/activerecord
