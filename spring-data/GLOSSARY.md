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
  exactly one call site: inside this module — `LeafTranslator.definiteEquality`
  negates through it too, since `tri.not(p)` IS `cb.not(cb.and(p))`.
- **ComparisonTranslator / Resolved** — the single comparison-translation seam.
  Every binary leaf comparison resolves each operand to a typed `Resolved` case
  (`Constant`, `Field`, `ConstantAdd`, `FieldPlusConstant`, `Arithmetic`,
  `Opaque`) and dispatches on the pair. New operand types (e.g. `timestamp()`)
  are one resolver case + dispatch pairings — see the extension recipe in the
  module Javadoc. Classification is structural; conversion is lazy, because
  which error fires is part of the pinned interface. Two of its pipeline steps
  are collaborators of their own because their shapes are not operand
  resolutions: **TernaryTranslator** (the CEL `if` rewrite, on the RAW operands
  before mirroring) and **SizeTranslator** (`size(x) op N` as LENGTH, COUNT or
  the strict `size(filter(...))` count). **ArithmeticTranslator** is where the
  `Arithmetic` case lowers (see *Double space*); it reaches back to the seam for
  the constant fold and the raw expression comparison so each is spelled once.
- **PlanWalker** — the one walk over the plan tree. It lowers `and`/`or`/`not`
  and the bare boolean variable, owns the macro-depth counter (`enterMacro`),
  and dispatches every other operator by name: the collection macros to
  **CollectionTranslator**, `in`/`hasIntersection` to **MembershipTranslator**,
  `if` to TernaryTranslator, the hierarchy operators to HierarchyTranslator, and
  everything else to ComparisonTranslator. Scope is a walk parameter; polarity is
  NOT — negation is junction-barriered in TriPredicate and applied around a
  built predicate, and pushing it down the walk would change the emitted SQL.
  One instance per Specification evaluation, built by the public facade.
- **LeafTranslator** — the scalar leaf: one mapped column against one plan
  constant. The default lowering of each leaf operator, the one place a
  registered `OperatorFunction` override is consulted (`withOverride`), and the
  two rules of the explicit-null convention (`isExplicitNull`,
  `definiteEquality`).
- **ParsedLambda** — a `lambda(body, var)` operand unpacked once; the three
  operator families that unpack one keep their own wording for the same three
  wire-contract violations, so the messages are caller-supplied.
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
- **ChainSubquery / ChainSubqueries** — the one correlated-subquery skeleton
  (the record: the subquery, its tail join, the rebased outer scope) and the
  module that builds it and every shape over it — EXISTS, the COUNT seed, the
  tri-state macro score, the strict match counter with its undetermined-poison
  term, and the leading-hop guards. It anchors correlation at the scope that owns
  the relation and joins through every hop of a multi-hop chain; all collection
  operators compose over it, and the SELECT-only guard fires here. A body is
  handed in as a **SubqueryBodyBuilder**, never a Predicate, because the macro
  shapes consume it in both polarities.
- **Conformance replay** — the conformance suite: every golden plan the pinned
  PDPs recorded for the shared corpus, translated with one mapping, executed on
  H2, PostgreSQL or MySQL, and the id set compared with the `check()` decisions
  recorded beside it. DB NULL is a *missing* attribute on the check side unless
  the mapping declares the attribute EXPLICIT. The cases this adapter cannot
  pass are listed with reasons in `conformance-ledger.json`.
- **Double space** — all numeric work happens in IEEE doubles, because Cerbos
  attribute numbers are CEL doubles and the wire plan erases `1` vs `1.0`.
  Constants fold in Java; columns get a real `CAST(... AS DOUBLE)`. Owned by
  **ArithmeticTranslator**, including the zero-divisor story (`NULLIF` guard,
  IEEE-arm rewrite) and the MySQL cast probe (**IeeeDoubleCast**).
- **Options** — the one immutable record holding everything a caller tells the
  adapter: the mapping, the operator overrides, the call-level NULL convention,
  and the macro-depth bound. Collections are copied on construction and each
  `with…` returns a new instance, so it can be built once and shared. The
  positional `toSpecification` overloads are this record with the rest at its
  defaults. Macro depth has a precedence: a value declared here wins, the
  `maxMacroDepth` system property applies when none is, the default when neither.
- **Refusal** — the adapter declining to translate, as opposed to translating
  wrongly: a wrong filter is an authorization bug, a throw is a bug report. Every
  refusal goes through the package-private `Refusals` factory, which makes its
  classification a property of the walk site rather than of the message text. A
  branch only an adapter bug can reach (a switch default under a guard that
  already enumerated its cases) is `Refusals.internal`, an
  `IllegalStateException`, and deliberately not a refusal.
- **UnsupportedPlanShapeException / UnmappedAttributeException /
  MalformedPlanException** — the three refusal types, all extending the
  documented `IllegalArgumentException` base. *Unsupported*: a well-formed plan
  the Criteria API has no faithful shape for (a cast, `mod`, a macro past the
  depth bound); the fix is the policy, an override, or a per-row `check()`.
  *Unmapped*: the plan uses an attribute in a way the mapping does not cover — an
  unknown variable, a Relation where a scalar is needed or a Field where a
  collection is, a temporal column whose Java type does not pin an instant, two
  sides of one comparison under different NULL conventions; the fix is a
  declaration. *Malformed*: the planner's wire contract violated — arity, a
  lambda without a variable, a literal CEL would reject; no planner output
  produces one, which `RefusalTypesTest` pins over the ledger's refused cases.
