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
