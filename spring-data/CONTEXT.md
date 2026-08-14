# Domain glossary — spring-data adapter

Terms used by this adapter's code, tests, and reviews. Architecture vocabulary
(module / interface / seam / depth) follows the codebase-design convention.

- **Error→deny contract** — the adapter's semantic target: the filtered row set
  equals what per-resource `check()` calls would allow. CEL evaluation errors
  (null/missing attribute without a null overload) deny, so their SQL
  translation must evaluate UNKNOWN — never FALSE — under every polarity.
- **TriPredicate** — the tri-state predicate algebra module enforcing that
  contract structurally: it owns the UNKNOWN constant, the junction-barriered
  negation (Hibernate 6 collapses `cb.not(cb.not(p))`), and the macro truth
  tables. Inputs consumed in more than one polarity are `Supplier`s, so
  "translate fresh per occurrence" cannot be violated by callers. `cb.not` has
  exactly one call site: inside this module.
- **ComparisonTranslator / Resolved** — the single comparison-translation seam.
  Every binary leaf comparison resolves each operand to a typed `Resolved` case
  (`Constant`, `Field`, `ConstantAdd`, `FieldPlusConstant`, `Arithmetic`,
  `Opaque`) and dispatches on the pair. New operand types (e.g. `timestamp()`)
  are one resolver case + dispatch pairings — see the extension recipe in the
  module Javadoc. Classification is structural; conversion is lazy, because
  which error fires is part of the pinned interface.
- **NormalizedBinary** — planner operands arrive in policy source order
  (`1 < R.attr.x` is value-first); this normalizes field-first and mirrors
  directional operators (`lt`↔`gt`). Receiver-sensitive operators
  (`contains`/`startsWith`/`endsWith`) are exempt — the receiver's position is
  meaning, not noise. Overrides observe the mirrored operator name.
- **Scope / Resolution** — the single variable-resolution seam. `resolve(var)` is
  total: every plan variable lands in `ResolvedScalar` (the mapping it was
  resolved through, plus a column when this scope's `From` holds one — a Field
  behind a relation prefix is scalar with no column here) or `ResolvedRelation`
  (the join chain plus its **owner**), or throws naming why. Chain walking,
  lambda delegation and owner anchoring live only here; `path(var)` is a
  narrowing over it, not a second resolver.
- **Owner anchoring** — a `ResolvedRelation` carries the scope that HOLDS its
  first hop, not the scope that resolved it. `R.attr.tags` referenced inside a
  `categories.exists(c, …)` body is owned by the root, so its subquery
  correlates the root's `From`. Getting this wrong is silent rather than loud:
  the element entity can carry a collection of the same name, so the query
  still builds and returns the wrong rows.
- **ChainSubquery** — the one correlated-subquery skeleton. It anchors
  correlation at the scope that owns the relation and joins through every hop
  of a multi-hop chain; all collection operators compose over it.
- **Differential oracle** — the adversarial conformance suite: hostile policy
  shapes planned against a real PDP, translated, executed on H2, and the id set
  compared row-by-row against `check()` with attributes mirroring the DB rows
  exactly. DB NULL is a *missing* attribute on the check side. No
  hand-computed expectations; a degeneracy guard prevents vacuous passes.
- **Double space** — all numeric work happens in IEEE doubles, because Cerbos
  attribute numbers are CEL doubles and the wire plan erases `1` vs `1.0`.
  Constants fold in Java; columns get a real `CAST(... AS DOUBLE)`.
- **Golden expectation** — the SQL this adapter is pinned to emit for one
  corpus action, in `golden/expectations.json`: the root joins and the `WHERE`
  clause on each of the three dialects CI executes, with criteria literals
  inlined. It records what the differential oracle cannot see — two queries can
  agree on all 21 seeds and disagree on the row a consumer has. Regenerated
  with `gradle goldenUpdate`, never by CI, and reviewed as a diff.
- **The renderer as an input** — a golden expectation is the adapter's Criteria
  tree *plus* Hibernate's rendering of it, so the asset declares the Hibernate
  minor that wrote it. `hibernate-core` is `compileOnly`, meaning a consumer
  brings their own; which one produced these bytes has to be answerable from
  the file rather than from the classpath.
