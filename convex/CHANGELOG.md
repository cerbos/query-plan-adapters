# Changelog

## Unreleased

- **Breaking:** under `nullAttributeRepresentation: "omitted"`, a mapper entry that does not
  declare `nullable` is treated as `nullable: true`, and `postFilter` reads a stored `null` as a
  missing attribute. Comparisons over such an entry are answered by `postFilter`, so they need
  `allowPostFilter: true`; the old pushed-down `q.neq(...)` and negations matched documents the
  field was missing from, which `check()` denies. Declare `nullable: false` on an entry that is
  always stored and never null to keep it on Convex's filter engine. `"explicit"` output is
  unchanged ([#493](https://github.com/cerbos/query-plan-adapters/issues/493)).

- An ordering (`<`, `<=`, `>`, `>=`) against a literal is pushed to Convex only inside a guard
  confining the field to the literal's type, and `not` is pushed inward so the guard is never
  negated. Convex orders values across types, so a non-nullable field compared with a literal of
  another type used to match every document of the lower type (and, negated, of the higher one);
  CEL denies both. An ordering against null, a list or a map is now a constant false (#516).

- A shape the adapter cannot translate now throws `UnsupportedQueryPlanError`, an exported subclass
  of `Error`. What it translates is unchanged; an unmapped reference and a missing
  `allowPostFilter` opt-in stay a plain `Error`.
- **Breaking:** `%` now throws `UnsupportedQueryPlanError` unless each operand is a field read or
  certainly a CEL int (an integral constant, `int()`, `size()`, or int arithmetic over those). A
  field read as an operand now evaluates to CEL's no-such-overload error, since every attribute
  number is a double, and so does an int modulus by zero; JavaScript's `%` used to answer both.
- `int()` accepts a leading `+` and keeps an int beyond 2^53 exact, as Go's `strconv.ParseInt`
  does; `string()` over a double prints Go's `%g` spelling (`1e+06`, `-0`) instead of refusing
  outside a measured magnitude band; and division by a zero read from the document yields the
  signed infinity CEL does instead of throwing.

## 0.3.0

- **Breaking:** `QueryPlanToConvexResult` now requires a `path` discriminator on conditional
  results: `db` requires `filter`, `post` requires `postFilter`, and `split` requires both.
  Unconditional results carry neither function. Existing destructuring remains supported.
- Reuse the adversarial document schema for table validation, mutation arguments and stored types.
