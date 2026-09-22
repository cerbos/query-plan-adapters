# Conformance corpus

The shared adversarial corpus every adapter is proved against: one hostile policy suite, one set of
hostile seed rows, one derived-field table, one classification ledger (`actions.json`) and golden
planner wire fixtures. Each adapter's harness plans against a real PDP loaded with `policies/`,
executes the translated query against its real store, and compares the returned ids with per-row
`check()` decisions. The PDP is the oracle for both sides, so there are no hand-written
expectations. Adapters consume this directory; none keeps its own copy of the policy, seeds or
action list (extracted from spring-data's `AdversarialConformanceTest`, #263).

**The invariant: a shape an adapter cannot express must throw, never emit a filter.** A wrong
filter returns rows the PDP denies; a throw is a bug report.

```bash
conformance/scripts/validate-corpus.sh           # corpus integrity, offline; runs in every adapter's CI
conformance/scripts/regenerate-wire-fixtures.sh  # after a policy edit or a PDP bump (Docker, curl, jq)
conformance/scripts/check-evaluation-modes.sh    # engine probes in both strictEvaluation modes (Docker)
# Each adapter's harness: npm run test:adversarial, pdm run test, go test ./..., gradle test (see CLAUDE.md)
```

Common tasks:

- **Add or fix a shape** — [Adding a new hostile shape](#adding-a-new-hostile-shape), then
  [Golden expectations](#golden-expectations).
- **Onboard an adapter** — [Adding a new adapter](#adding-a-new-adapter).
- **Bump the PDP** — [Regenerating wire fixtures after a Cerbos version bump](#regenerating-wire-fixtures-after-a-cerbos-version-bump).
- **A harness fails a vacuity check** — [The degeneracy guard](#the-degeneracy-guard).

## Contents

Read by section, not front to back.

- [Why this exists](#why-this-exists)
- [Layout](#layout)
- [The oracle recipe](#the-oracle-recipe)
- [Adding a new hostile shape](#adding-a-new-hostile-shape)
- [Golden expectations](#golden-expectations)
- [Adding a new adapter](#adding-a-new-adapter)
- [Evaluation modes and the 0.55 baseline](#evaluation-modes-and-the-055-baseline)
- [Regenerating wire fixtures after a Cerbos version bump](#regenerating-wire-fixtures-after-a-cerbos-version-bump)

## Why this exists

Every adapter re-derives properties of the planner's wire output by hand: operand source order,
mirroring a directional operator on a value-first comparison, `in` normalization,
receiver-sensitive string operators, three-valued logic under negation. The same wrong assumption
has shipped to more than one adapter at once (value-first inversion in prisma and sqlalchemy:
#258, #259) because nothing shared enforced the rule. This corpus is that shared enforcement.

## Layout

- `policies/adversarial.yaml` — the hostile policy suite and the corpus of record. One resource kind
  (`adversarial`), one role (`USER`), one action per hostile shape, no adapter-specific content.
- `seeds.json` — the hostile seed rows (NULLs, empty strings and collections, negatives, LIKE
  metacharacters `% _ \`, unicode, duplicate and mirrored names) plus the fixed principal. Each
  harness persists these into its own schema **and** mirrors them into `check()` calls. Every key
  except `note` must be consumed by every harness, and that is asserted (see "Seed, principal and
  derived-field coverage"). `parentSeedId` is the one key that resolves against another row (see
  "The real to-one relation").
- `derived-fields.json` — six attributes derived from each seed (`createdBy`, `aDouble`,
  `createdAt`, `updatedAt`, `scope`, `labels`), materialised once per seed id. Harnesses read this
  file; `scripts/validate-corpus.sh` re-derives the rule-based fields and fails on drift. See
  "Deterministic derived fields".
- `actions.json` — every action in the policy, grouped:
  - `adapters` — the canonical roster every other per-adapter key is checked against;
  - `conformance` — must match the `check()` oracle exactly;
  - `adapterUnsupported` — per adapter, conformance actions its query language genuinely cannot
    express (LIKE-wildcard escaping, relation-count thresholds, cross-model column comparisons).
    The adapter must throw, and its harness asserts the throw instead of the oracle match;
  - `expectedUnsupported` — planner shapes the Spring reference adapter rejects; every other
    adapter must also fail loudly unless listed in `adapterSupportedExpected`;
  - `adapterSupportedExpected` — per-adapter exceptions that translate a reference-unsupported
    shape through a documented database capability;
  - `nullRepresentationOmitted` — `== null` probes against an attribute the oracle omits for NULL
    columns; every adapter translates them with its NULL representation set to omitted and must
    reject them (see "NULL conventions");
  - `degenerateOracles` — actions whose oracle is empty or total by construction (see "The
    degeneracy guard");
  - `knownDivergences` — an action plus the adapters excluded from its oracle run, with a reason.
    Currently only `p-has`, a planner bug.
- `wire-fixtures/*.json` — one golden `PlanResources` response per action, captured against the
  pinned PDP. They pin planner wire shape independently of any adapter: after a PDP bump, the
  fixture diff shows exactly what the planner changed.
- `wire-fixtures-strict/*.json` — the same actions captured from a separate PDP started with
  `engine.strictEvaluation=true`. Generated independently, never copied from the default capture,
  even where the two agree.
- `CERBOS_VERSION` — the exact PDP tag the fixtures were captured against. Pinned, not `latest`, so
  a fixture diff comes only from a deliberate bump.
- `CERBOS_IMAGE_DIGEST` — the digest that tag resolves to; a tag can be re-pushed. Every harness
  and workflow composes `ghcr.io/cerbos/cerbos:$CERBOS_VERSION@$CERBOS_IMAGE_DIGEST` from the two
  files, and `scripts/validate-corpus.sh` asserts both halves wherever either is restated.
- `scripts/regenerate-wire-fixtures.sh` — regenerates both fixture directories from a pinned PDP.
  Review the diff and commit it with the change that caused it.
- `evaluation-modes/` — engine probes run by `scripts/check-evaluation-modes.sh` (see "Evaluation
  modes and the 0.55 baseline").

**Not here:** the filter each adapter is pinned to emit. Those are per-adapter golden expectations
in the adapter's own directory (see "Golden expectations").

`validate-corpus.sh` holds `actions.json`'s entry schemas closed: unknown keys, missing required
fields and empty or wrongly typed metadata fail. Optional `relatedIssue` values must be non-empty
strings, not `null`, and every adapter a known divergence names must be on the roster. When you
move an action between buckets, use the destination bucket's fields.

## The oracle recipe

Each harness implements this against its own ORM:

1. **Seed** the adapter's schema from `seeds.json`, in whatever native shape the ORM needs.
2. **Plan**: call `PlanResources` against a real PDP for each `conformance` action, translate the
   response through the adapter, execute the native query, and collect the returned ids
   (`adapterFilteredIds`).
3. **Oracle**: for each seed row, call `check()` against the same PDP and action, with attributes
   mirroring that row exactly (`oracleAllowedIds`).
4. **Compare**: `adapterFilteredIds(action)` must equal `oracleAllowedIds(action)` for every
   `conformance` action. Translation must throw for every `expectedUnsupported` action unless the
   adapter is listed for it in `adapterSupportedExpected`, in which case it runs through the same
   oracle comparison. A throw must carry the message the corpus pins for that adapter (see "Pinned
   throw messages").

### Case sensitivity is two invariants, not one

CEL string comparison is exact, and a store can satisfy that for one operator and not the other:

- **`cs-eq`** proves `=`. Collation governs it, and a byte-exact collation is sufficient —
  case-sensitive is not (see below).
- **`cs-contains` / `cs-startswith` / `cs-endswith`** prove string matching, which collation does
  not govern on every engine. SQLite's `LIKE` is case-insensitive for ASCII whatever the column's
  collation; only the per-connection `PRAGMA case_sensitive_like = ON` changes it.

`c1` (`aString` "One") is the witness in all four and the only seed differing from another by case
alone, so a case-insensitive store adds exactly one row.

A harness whose store needs configuring must configure it, and the adapter's README must state the
whole lever. ent, sqlalchemy and prisma set the SQLite pragma; drizzle needs none because it lowers
string matching to `REPLACE` rather than `LIKE`. Prisma's README once named only the collation,
which satisfied the invariant on paper and violated it in fact.

#### Case-sensitive is not byte-exact

`h6` carries a SOFT HYPHEN (U+00AD) in both strings: `aString` is `"o­ne"` and
`aOptionalString` is `"s­et"`. CEL tells them apart from `"one"` and `"set"`; a Unicode
Collation Algorithm collation does not, because UCA gives a default-ignorable code point no weight.
MySQL's `utf8mb4_0900_as_cs` is case- and accent-sensitive and still UCA, so it passes `c1` and
fails `h6`: `cs-eq` and every `in` over the principal's teams (`in-single`, `pv-in`, …) over-grant
it, and `nary-and`'s `aString != "one"` and `pv-not-exists` under-grant it
([#474](https://github.com/cerbos/query-plan-adapters/issues/474)). Existing actions are its
witnesses, so the row needed no new action.

`utf8mb4_bin` is byte-exact but PAD SPACE (`'a' = 'a '`), so the one MySQL collation matching CEL
on case, accent, ignorables and trailing spaces is `utf8mb4_0900_bin` (8.0.17+), which every MySQL
leg pins. Measured on the pinned `mysql:8.4`:

| probe | `_0900_ai_ci` | `_0900_as_cs` | `utf8mb4_bin` | `_0900_bin` |
|---|---|---|---|---|
| `'One' = 'one'` | TRUE | FALSE | FALSE | FALSE |
| `'o­ne' = 'one'` | TRUE | TRUE | FALSE | FALSE |
| `'one ' = 'one'` | FALSE | FALSE | TRUE | FALSE |
| `'o­ne' LIKE 'one'` | FALSE | FALSE | FALSE | FALSE |

`LIKE` compares per character, so `h6` never reaches it; the witness is `=` and `IN`.

`h6` also found a bug no collation governs: drizzle rendered CEL `size()` as `length()`, which
counts bytes on MySQL, so `string-size`'s `size(aString) > 4` admitted the 4-character, 5-byte
`"o­ne"`. The same byte count positioned the `substr()` behind drizzle's `startsWith` and
`endsWith`, which counts characters, so any multi-byte needle mis-sliced. `h7` witnesses that: its
`aOptionalString` `"é"` (2 bytes, 1 character) both starts and ends its `aString` `"é-x-é"`, so
`f2f-startswith` and `f2f-endswith` under-grant it and `not-startswith` over-grants it under a
byte-counting slice. drizzle now renders `char_length()` over a MySQL column for all three.

### NULL conventions

By default a DB `NULL` (or a missing element field, such as a NULL tag name) becomes a **missing
attribute** on the check side. CEL's `!=` and macro bodies then raise a missing-attribute error,
which Cerbos treats as a deny — the same three-valued logic SQL applies to `NULL` (`UNKNOWN`,
excluded from both a predicate and its negation). `NOT (NULL = x)` is still `UNKNOWN`.

The `in-null-elem-*` and `in-var-var*` probes exercise the other planner convention: `owner`
aliases the `aOptionalString` column but is sent as an **explicit null** when the column is NULL,
and `tagNames` is the scalar projection of `tags[].name` with NULL names kept as explicit null
elements. This pins `null in [null]`, `null in tagNames` and variable-in-variable membership.
Object-valued `tags` still omit a NULL `name`, so lambda bodies still hit missing-attribute errors.
Every harness must implement both representations exactly.

#### `nullRepresentationOmitted`: the two conventions are indistinguishable on the wire

The planner emits the same `eq(attr, null)` node under both conventions: `null-eq` (explicit-null
`owner`) and `null-eq-missing` (default-convention `aOptionalString`) have byte-identical wire
fixtures apart from the variable name. Their oracles differ:

| action | attribute convention | `check()` allows | a NULL-selecting filter returns |
|---|---|---|---|
| `null-eq` | explicit null | `a2 a4 a8 c2 e1` | the same 5 — aligned |
| `null-eq-missing` | omitted | **nothing** | those 5 — **over-grants** |

Under the omitted convention CEL errors for every NULL row and compares `"set" == null` false for
every other, so `check()` denies all 29 seeds. An adapter cannot recover the convention from the
plan, so every adapter that can emit a NULL-selecting predicate takes a
`nullAttributeRepresentation` option, defaulting to `explicit` (the historical translation; #302).

`null-eq-missing` lives in its own group rather than `conformance`, because a rejected shape has no
filter to compare. Each harness translates the group with its representation set to omitted,
asserts the rejection, **and** asserts why the rejection is needed, so it cannot pass by throwing
for an unrelated cause. That second assertion depends on where the adapter's NULL lives:

- **prisma, drizzle, sqlalchemy, spring-data** — a SQL `NULL` is a stored value, so the default
  translation returns the five rows the PDP denies. The harnesses pin that over-grant.
- **mongoose** — `nullable: true` on a mapper entry means "a stored null is a missing Cerbos
  attribute" and makes `eq(field, null)` contradictory. The harness asserts the empty result *and*
  that `owner` (same column, no `nullable`) still returns its five explicit-null documents.
- **convex** — the harness omits the field entirely; because `aOptionalString` is `nullable: true`,
  the adapter refuses the push-down and evaluates in its JavaScript post-filter, where the absent
  path raises the same CEL error `check()` denied on. Same paired `owner` assertion as mongoose.
  Alignment comes from the storage layout, not from a Convex `q.eq(field, null)`, which never runs
  for either action (#327).
- **langchain-chromadb, elasticsearch-java** — need no option: neither store distinguishes an
  explicit null from a missing key, so every null-selecting direction fails closed under both
  conventions. The harnesses assert the rejection regardless, as a tripwire for a future null
  sentinel.

These oracles are empty by construction, so the actions are declared in `degenerateOracles` (see
"The degeneracy guard") and carry this "why" assertion as their anti-vacuity check.

#### The other side of the same option: an explicit null against a non-null constant

Against null, SQL's `IS NULL` and CEL agree. Against a non-null constant they do not: CEL holds a
null *value* under the explicit convention, so `null != "x"` is TRUE and `null == "x"` FALSE, while
SQL answers UNKNOWN and excludes the row under both polarities. Every SQL-backed adapter returned
fewer rows than the PDP allowed (#308). `optional-ne` does not reach this: it uses the
omitted-convention `aOptionalString`, where UNKNOWN and a missing-attribute error agree.

The direction is safe (narrower, never wider) but the id sets differ. Five actions pin it:
`null-value-ne-const`, `null-value-not-eq-const`, `null-value-not-in-const`, `null-value-f2f` and
`null-value-pv-not-exists`.

**A call-level option cannot fix it, so the convention is declared per attribute**
([ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md)). The corpus maps
the same column twice — `owner` sends an explicit null, `aOptionalString` sends nothing — because
real applications do. Told `explicit`, an adapter breaks `optional-ne`; told `omitted`, it refuses
the null-comparison shapes. So each mapper carries a per-attribute declaration, and the call-level
`nullAttributeRepresentation` is its default. Declaring nothing means "treat this column as NOT
NULL" (the historical rendering), so the fix is opt-in per column.

The declaration changes only the **equality family** — `eq`, `ne`, `in` — the operators CEL
evaluates to a definite boolean over null. `lt`/`le`/`gt`/`ge` and the string operators raise a
no-overload error on a null receiver, which denies under both polarities exactly as UNKNOWN does.

`coOwner` is the second explicit-null attribute, added for `null-value-f2f`. It aliases **`scope`**
rather than `aOptionalString`, because a column compared with itself is TRUE for all 29 seeds and
the degeneracy guard forbids a total oracle. Against `scope`, only `e1` has both sides NULL, so the
oracle is exactly that row — the one the naive translation loses.

Per adapter:

- **prisma, drizzle, sqlalchemy, spring-data, ent, pgx** translate the declaration; all five actions
  are oracle-compared.
- **mongoose, convex** need none: they store the value the caller sent, so a stored null already
  compares as CEL does. (Mongoose refuses `null-value-pv-not-exists` for an unrelated reason — the
  value-list fold puts a collection macro under a negation.)
- **langchain-chromadb** refuses all five: its metadata model has no null, so `$ne`/`$nin` match
  documents missing the key.
- **elasticsearch-java** takes the declaration in order to **refuse** ("cannot distinguish an
  explicit null value from a missing field without an indexed null-value sentinel"). The guard used
  to key off a null literal in the plan, so it never fired for these shapes. Every Query DSL
  spelling of `!= "x"` either requires the field (dropping the row) or matches every document
  missing it.

#### The absent to-one parent

`mainCategory` is a to-one parent on the check side. A seed with no `subCategoryNames` sends **no
`mainCategory` attribute**, so CEL raises a missing-path error and `check()` denies. An adapter
reaches the same data through a join chain from the resource row, where an absent parent and a
childless parent both produce an empty result:

| shape | `check()` | a chain that does not require the hop |
|---|---|---|
| `mainCategory.subCategories.exists(s, …)` | deny | no rows → false → deny — **agrees, for the wrong reason** |
| `size(mainCategory.subCategories) > 0` | deny | count 0 → deny — **agrees, for the wrong reason** |
| `mainCategory.subCategories.all(s, …)` | deny | no rows → vacuously TRUE → **over-grants** |
| `!mainCategory.subCategories.exists(s, …)` | deny | no rows → `!false` → **over-grants** |
| `size(mainCategory.subCategories) == 0` | deny | count 0 → **over-grants** |
| `size(mainCategory.subCategories) >= 0` | deny | count 0 → **over-grants** |
| `!("finance" in mainCategory.subNames)` | deny | no rows → `!false` → **over-grants** |
| `!hasIntersection(mainCategory.subNames, […])` | deny | no rows → `!false` → **over-grants** |
| `!(size(mainCategory.subCategories) > 0)` | deny | count 0 → `!false` → **over-grants** |
| `("finance" in mainCategory.subNames) ? … : …` | deny | no rows → `!false` → else-branch → **over-grants** |
| `size(mainCategory.subCategories) <= 1.5` | deny | count 0 ≤ 1 → **over-grants** |

Only a universal, a negation, a zero/lower-bound count or a ternary's false branch discriminates,
which is why `w1-exists-chain`, `w1-size-chain` and `w1-in-chain` passed everywhere while the bug was
live (#309). The 9 that discriminate: `w1-all-chain`, `w1-not-exists-chain`, `w1-size-zero-chain`,
`w1-size-nonneg-chain`, `w1-not-in-chain`, `w1-not-hasint-chain`, `w1-not-size-chain`,
`w1-ternary-chain-cond` and `w1-size-frac-le-chain`.

`w1-size-frac-chain` does **not** probe this hazard: `>= 1.5` rounds up to `>= 2`, which a count of
zero fails either way. It pins that the adapter *rounds* rather than truncates over a chain
(`cr-size-frac-ge` pins it over a direct relation); truncating to `>= 1` returns `a1/a6/a8/c1`
against an empty oracle.

The fix is in translation: **a chained collection must require its intermediate hop to exist**, so
an absent parent stays excluded under both polarities. `w1-size-zero-chain`, `w1-not-size-chain` and
`w1-size-frac-chain` have empty oracles by construction (no seed holds a parent with zero children,
or with two or more) and are declared in `degenerateOracles`; the other `w1-*` discriminators have
non-degenerate oracles and carry the group's anti-vacuity assertion.

**Put the guard in the shared relation-scope construction, not in each operator.** Guarding only the
collection macros (#309) left sibling operators on the same chain unguarded: membership and
`hasIntersection` (#315), and `!(size > 0)`, which the planner emits verbatim rather than
normalising to `size == 0` (#316). ent and pgx needed no change: their membership routes through the
same guarded tri-state existence construction. Adapters with no UNKNOWN to represent (Prisma
filters, Mongo query documents) require the hops **outside** the negation, so the negation cannot
flip the requirement.

**Reuse one negation.** A ternary's false branch is "the condition is definitively FALSE" — the same
three-valued negation the `not` handler computes. Prisma spelled it a second time as a bare `NOT`,
so the #315/#316 hop requirement never reached it and the else branch was selected for every
parentless row (#334). The fix is delegation: one negation with the guard inside, reused wherever a
condition is falsified.

Mongoose does not share that defect — its ternary is a single `$cond` — but has a latent one: `$cond`'s
`if` treats a missing field path as falsy, selecting the else branch for an absent parent. No
collection action reaches it; probing it needs a chained **scalar** attribute (see "The real to-one
relation").

**The fractional-threshold collapse is the one branch the corpus cannot reach.** CEL rejects
`==`/`!=` between `int` and `double` ("found no matching overload for `_==_` applied to
`(int, double)`"), so no policy can plan a fractional equality against `size()`, and the ordering
spellings (`>= 1.5`, `<= 1.5`) round to an integer threshold. An adapter that folds the fractional
equality to a constant must still guard it — `hops AND constant` is two-valued and readmits
parentless rows under a negation — and only its unit tests can prove that (#333). This is kind 1 in
`CLAUDE.md`, "What a translator unit test may pin".

### Shapes that live only in a unit test

`CLAUDE.md` ("What a translator unit test may pin") admits three kinds of material into a
per-adapter unit test. This is the registry of what each adapter parks there today, so a reader can
tell a permanent entry from a bridge. The banners in each test class are the source; this list is
kept by hand, and an entry no test carries any more is stale, not a licence.

**elasticsearch-java** (`ElasticsearchQueryPlanAdapterTest`; regex probes in
`ElasticsearchSurfaceTest`):

- **Kind 1 — a branch CEL itself cannot reach.** Permanent. Each test quotes the checker error that
  stops the shape being planned:
  - an unknown operator (`unsupported_op`) — `undeclared reference to 'unsupported_op' (in
    container '')`;
  - `isSet` (#261) — `undeclared reference to 'isSet'`; existence is `R.attr.x != null`, which the
    corpus carries as `null-ne`;
  - a lambda body naming an unbound variable — `undeclared reference to 'x'`;
  - a collection macro over a scalar literal — `expression of type 'string' cannot be range of a
    comprehension (must be list, map, or dynamic)`;
  - a `timestamp()` literal outside strict RFC 3339 or CEL's range — CEL's `timestamp()` rejects
    each (e.g. `parsing time "2024-02-30T00:00:00Z": day out of range`). The adapter validates it
    anyway, because it decides whether the emitted `term` or `range` is well-formed.
- **Kind 2 — a caller-supplied argument the corpus structurally cannot vary.** Permanent.
  `actions.json` classifies against one `Options` per adapter, so these have no corpus spelling: an
  unmapped reference (refused, not used verbatim), an `OperatorFunction` override and which
  polarities it reaches, the two lowerings that borrow a shape without its override (hierarchy →
  `prefix`/`terms`, `^literal` → `prefix`), a collection macro over an undeclared nested path, a
  value-list macro needing no nested path, `size()` over a declared flat collection versus an
  undeclared field, `Options` immutability and the convenience overloads delegating to it, and the
  three typed refusals (`UnsupportedPlanShapeException`, `UnmappedAttributeException`,
  `MalformedPlanException`).
- **Kind 3 — a corpus gap wearing a unit test.** Policy-reachable, pinned here alone; each is a
  bridge tracked by [#414](https://github.com/cerbos/query-plan-adapters/issues/414), deleted when
  its corpus action lands, and opens with *Corpus gap.*:
  - `anUnfoldableMacroOverAValueListIsRefusedByName` — direct boolean-root `filter` and `map`
    results; the new actions use computed collections as operands instead;
  - `exceptIsRefusedByNameWhereverItAppears` — directly negated and nested-lambda positions; root,
    size and equality positions are now corpus actions;
  - `everySpellingOfNonEmptinessIsTheSameCheck` — direct flat-collection `size != 0` and its
    negation; `size-ge-one` covers the inclusive threshold;
  - `hasIntersectionWithANullElementIsRefusedWhicheverPositionCarriesIt` — intersection inside a
    nested lambda; the flat and projected operand orders are now corpus actions;
  - `aNonScalarLiteralWhereAScalarIsExpectedIsRefused` — raw protobuf structured values in ordering
    and string operations. The new equality actions cover the planner's `list`/`struct`
    representation, which is a distinct wire shape.

The numeric decoder's exact signed-long boundaries and non-finite protobuf values remain wire
contracts; the pinned PDP cannot serialize a non-finite literal (see below). The regex surface tests
remain mechanism tests against Lucene: they show why the regex actions are refused, which a refusal
assertion cannot measure. The empty hierarchy delimiter is now covered by `hier-empty-delim`.

**spring-data** (`SpringDataQueryPlanAdapterTest`; banners are the source, every kind-3 test opens
with *Corpus gap.*):

- **Kind 1.** Permanent. An operator CEL does not have (`isSet`), a comparison the type checker
  rejects (fractional `size()` equality, a timestamp against a number), an operand shape the planner
  never emits (wrong arity, a bare string where `timestamp()` always wraps one, a leaf with a third
  operand), and constant-only sub-expressions the planner folds before the wire — proved by the
  corpus's own fixtures: `p-startswith-concat` arrives with `"100" + "%"` folded and `in-empty`
  arrives as `ALWAYS_DENIED`.
- **Kind 2.** Permanent. An `OperatorFunction` override on every scalar-leaf path, the macro-depth
  bound (the `Options` value and its system-property fallback), call-level and per-attribute
  `NullAttributeRepresentation`, mappings the corpus does not use (`OffsetDateTime` or
  `LocalDateTime` columns, an unmapped reference), the bulk-delete guard, the null-predicate
  contract with Spring Data, defensive copies, and — in `RefusalTypesTest` and `OptionsTest` — the
  three typed refusals and `Options` immutability.
- **Kind 3.** Bridges tracked by [#414](https://github.com/cerbos/query-plan-adapters/issues/414),
  grouped as the banners group them: `size(collection)` against arbitrary, fractional and
  out-of-int-range thresholds; empty-list intersection over a direct scalar, relation or map
  projection (the new action covers an absent to-one parent); value-first and relation structured
  comparisons; suffix and integral `add` solve forms; CEL primitive and minor-operator shapes;
  collection-macro composition; value-first operand orders beyond the corpus's; the ternary
  rewrite's nested, negated and value-first forms; SQL Server `[` escaping (a *store*-dimension gap —
  no leg runs SQL Server); constant-receiver string matches; arithmetic as a comparison operand;
  constant NaN and infinity ordering; and the `timestamp(field)` operator cells the corpus does not
  reach.

Error-message context, list cardinality and value redaction remain translator contracts even where a
corpus action proves the refusal.

**prisma** (`translator.test.ts`): the caller-crafted nested-map boolean-body contract is a
permanent kind-1 test. The pinned PDP rejects `R.attr.tags.all(t, R.attr.tags.map(x, x.name))` with
`expected type 'bool' but found 'list(dyn)'`; the test exercises the defensive fallback anyway,
proving an inner map's nullable projection cannot leak into the outer lambda scope (#430).

### Issue #414 port and planner evidence

The port adds 67 actions, giving the original families corpus spellings: wildcard needles, 10 regex
patterns beyond a literal prefix, the three arrival positions of two-list `except`, `size-ge-one`,
`in-numbers`, the four empty-list macro identities, principal struct projections, variable
shadowing, negated principal macros, `root-not-bool`, and literal membership inside a lambda. `h4`
carries the string `"0"` beside numeric `0`, testing heterogeneous equality against SQLite's
numeric-string coercion. `h1` (`a{q}*?b`), `h2` (`a\nb`) and `h3` (`ab`) together distinguish literal
wildcard escaping, RE2's newline rule, a literal brace and a real regex match. The principal struct
fixtures contain the `list`/`struct`/`set-field` expressions the pinned planner actually emits; do
not replace them with assumed protobuf value-list fixtures.

The September 18 follow-up adds heterogeneous equality and string-operation probes, omitted
variable membership, unsolvable concatenation under negation, a hierarchy prefix whose list still
reads a missing attribute, empty intersection through an absent parent, a nested divisor, and raw
temporal equality. `type-string-number` uses principal `zero: 0`, so MySQL coercing a non-numeric
string to zero is observable. The type probes have empty oracles by design — they catch a store
matching values CEL cannot compare — and the empty macro identities and non-scalar literal probes
are empty or total too; all are declared in `degenerateOracles` and asserted to be exactly that.

`not-nan-ord-le` distinguishes the ternary arms under Cerbos 0.55 / CEL 0.30. The true arm compares
`1 <= 2`, so negation denies it. The false arm compares `0.5 <= NaN`, which is false, so **negation
allows it** (under 0.54 that comparison raised an error and stayed denied). The finite arm was
changed during the upgrade because `1 <= 0.5` made the oracle total. See "Evaluation modes and the
0.55 baseline".

One requested spelling has no fixture: `R.attr.aNumber / (0.0 / 0.0) > 0` compiles, but
`PlanResources` returns HTTP 500 with `proto: google.protobuf.Value.number_value: invalid NaN
value`. It is excluded from the manifest because no adapter receives a plan — an upstream
serialization limitation, not an adapter refusal. `div-by-division` carries the nested finite-divisor
case.

The Java unit-test registry above stays authoritative for finer operator cells these actions did not
replace. A broad family action does not cover every refusal location, operand order or caller
contract; a surviving *Corpus gap.* label is still pending port work.

### Issue #396 regex, indexing and conversion probes

11 actions cover the remaining #396 mechanisms. `h5` carries `"ab\n"` and `h3` `"ab"`:
`regex-final-newline` distinguishes RE2's absolute-end `$` from an engine that also matches before a
final newline. `h5` also carries the derived `createdBy = "not-a-timestamp"`, exercising conversion
failure in `p-timestamp` and the new `cast-not-timestamp` negation. The derivation checker records
that exception; harnesses read the materialised value.

Cerbos 0.55 rejects a **literal** `a(?=b)` at compile time. `regex-lookahead` selects the same string
through the principal's `context` attribute, deferring validation to evaluation and keeping the
original `matches` wire node; `scripts/check-evaluation-modes.sh` separately asserts the literal's
compile rejection in both modes. A PCRE engine accepting it would allow `h3` and `h5`, which the
checker denies. `regex-eq-true` pins the retained `eq(matches(...), true)` rather than assuming the
planner folds the wrapper.

`index-negative` and `index-fractional` keep `-1` and `0.5` in their wire nodes; both raise during
CEL list access. These two and `regex-lookahead` include an independent `aNumber == 5` branch that
allows `a1`, keeping the oracle non-empty while an invalid access or foreign regex engine can still
over-grant other rows. `index-not-oob` reads index 1 under negation; `a6` supplies an in-bounds
unequal value, and shorter lists must stay denied rather than making a missing element unequal.

`cast-not-int` and `cast-not-double` have the numeric string `h4` as an allowed witness; malformed
numeric strings still deny under negation. `cast-not-string-missing` and `cast-not-string-null`
distinguish an omitted attribute from an explicit null through `aOptionalString` and `owner`.
Neither conversion error may become an allow under `not`. All 11 actions have non-empty, non-total
oracles and sit in each adapter's compared or refusal-liveness guard according to its observed
classification (#401).

### Number and boolean list elements

`index-scalar-list` and its companions read `tagNames`, a list of strings, so they never ask whether
an adapter keeps an element's JSON type for a number or boolean literal. Two seed fields exist for
that: `aNumberList` and `aBoolList`, homogeneous scalar lists on every seed. Most rows hold `[]`,
where every position is an index error and the PDP denies under both polarities. Eight rows
discriminate:

| seed | `aNumberList` | `aBoolList` | what it witnesses |
| --- | --- | --- | --- |
| `a1` | `[2]` | `[true]` | the match, and the `aNumber == 5` branch below |
| `a3` | `[2, 3]` | `[false]` | a match with a longer list; false leading |
| `a4` | `[3, 2]` | `[null, true]` | the value at the wrong position; a null element |
| `a5` | `[-2]` | `[false, true]` | the wrong sign; false leading |
| `a6` | `[null, 2]` | `[]` | a null element, which is a value: `null == 2` is false, its negation true |
| `a7` | `[20]` | `[]` | the value a text comparison would take for a prefix |
| `b4` | `[1]` | `[true]` | 1 and true, which SQLite and MySQL both store as 1 |
| `c1` | `[0]` | `[true, false]` | a zero a NULL could be mistaken for |

Six actions read them, each non-degenerate in both evaluation modes: `index-number-list`
(`[0] == 2`: `a1 a3`) and its negation (`a4 a5 a6 a7 b4 c1`), `index-bool-list` (`[0] == true`:
`a1 b4 c1`) and its negation (`a3 a4 a5`), and two cross-type probes. `index-bool-list-vs-number`
(`aBoolList[0] == 1`) and `index-number-list-vs-bool` (`aNumberList[0] == true`) are false for every
row in CEL, whose equality is heterogeneous; an adapter comparing a JSON element as SQL returns
`b4` and `c1`, or `b4`, because SQLite and MySQL store JSON true as 1. Each carries the
`aNumber == 5` branch, as `index-negative` does, so the oracle is `a1` rather than empty.

Every harness declares and consumes both fields. An adapter with no positional list read refuses all
six, as it refuses `index-scalar-list`.

### The real to-one relation

The corpus carries exactly one **real** to-one join: `parent`, and `parent.inner` one hop further.
It sits beside `obj.inner`, which looks identical in a policy but is not a join — every harness maps
`obj.inner` to the row's own `aString`, as the spring-data reference does for `p-struct`. A reader
should be able to tell which dotted attribute joins.

`mainCategory` is also a chain, but its tail is a collection, and scalars reach different hazards.
This relation is the chained scalar "The absent to-one parent" asks for to probe mongoose's `$cond`.
See [ADR 0005](../docs/adr/0005-the-conformance-corpus-carries-a-real-to-one-relation.md).

**One seed key, resolved against another row.** `parentSeedId` names the seed whose four scalars —
`aBool`, `aNumber`, `aString`, `aOptionalString` — a row's `parent` carries; that seed's own
`parentSeedId` names what `parent.inner` carries, and the chain is **cut there** (no
`parent.inner.inner`). `null` means no parent. Seed rows stay one line each, and the parent values
are the corpus's own hostile strings (LIKE metacharacters, unicode, empty string, case traps, NULL
optionals) rather than a second curated set.

**Do not write the nested object into `seeds.json`.** Harnesses materialise it, as they materialise
`mainCategory` from `subCategoryNames`.

**The parent is a copy, not a pointer.** Each harness creates a fresh parent (and inner) row per
resource rather than pointing at the named seed's row. With shared rows, a filter returning the
parent instead of the child could agree with the oracle.

| store | how the two levels are materialised |
|---|---|
| prisma, drizzle, sqlalchemy, spring-data, ent, pgx | two owned tables, unique foreign key per level — a real join |
| mongoose, convex, elasticsearch-java | two nested objects embedded in the document |
| langchain-chromadb | both levels flattened onto dotted metadata keys (`parent.aString`, `parent.inner.aString`) |

How a store spells "this level is absent" is its own business (a missing row, a missing key, or a
stored null under that harness's NULL convention). The **check side** must agree everywhere: an
absent level sends no `parent` (or no `parent.inner`) attribute, so CEL raises a missing-path error
and `check()` denies — the scalar counterpart of "The absent to-one parent".

**How each adapter spells the hop.** The 15 `rel-*` actions proved that reaching a scalar through a
to-one hop was a shape several translators lacked. In every case an absent hop must be UNKNOWN, not
false, because `NOT UNKNOWN` is still UNKNOWN:

| adapter | how the hop is reached | what makes an absent hop UNKNOWN |
|---|---|---|
| prisma | `is:` on an optional relation | the hop required as a conjunct OUTSIDE the negation |
| drizzle | correlated `EXISTS` | `CASE WHEN <hop exists> THEN … END`, no `ELSE` |
| sqlalchemy, ent, pgx | correlated SCALAR subquery | no correlated row IS already SQL NULL |
| spring-data | `LEFT JOIN` through the association | an unmatched join row is SQL NULL |
| mongoose, convex, elasticsearch-java | a nested object path | the path is simply absent from the document |
| langchain-chromadb | flattened dotted metadata keys | nothing does — a missing key MATCHES `$ne`, so the negated and null-comparison shapes fail closed |

A correlated scalar subquery needs no hop guard: the collection chains need one because `EXISTS` is
two-valued and collapses "absent parent" onto "no matching child", which a scalar projection never
does. An inner join does not work either — it removes the row from the whole query rather than
making one branch unknown, which shows only under a disjunction. `rel-hop2-or-exists` discriminates
it and is the one action in the group whose failure is an under-grant.

**Every harness pins the fixture directly.** Each reads both hops back out of its store (through a
real join where it has one) and compares them with the corpus rather than counting rows — a count
cannot tell an inner row carrying the corpus's values from one carrying the root's own columns.
`scripts/validate-corpus.sh` asserts every `parentSeedId` names a seed, no row is its own parent, and
all three depths (no parent, parent without inner, parent with inner) are non-empty.

### The primary key as a filterable attribute

Every action but the six `id-*` actions filters on a resource attribute. Those six filter on
`request.resource.id`, which the planner leaves symbolic (PlanResources is asked about a kind, not a
row), so the operand arrives as a `variable` named `request.resource.id`, not
`request.resource.attr.id`.

The key is the one column whose mapping differs **structurally** per adapter: an ObjectId in
mongoose, the primary key in the SQL adapters, the document identifier in ChromaDB, `_id` metadata
in Elasticsearch. An adapter that resolves references by stripping `request.resource.attr.` never
reaches this name, and fails silently — the variable looks like an unmapped attribute.

Two harness rules:

- **A store whose key is not a queryable field must mirror it into one.** ChromaDB's `where` filters
  metadata only, and Elasticsearch addresses `_id` with the `ids` query, not a term query. Both
  harnesses index the corpus id as an ordinary field and map the key onto it. Leaving it unmapped
  makes the group throw for a harness reason (#326); leaving it unindexed is worse — the filter is
  well-formed and matches nothing.
- **One key mapping cannot be two types.** Three of the six compare the key against a string
  column, so mongoose maps it to the string field holding the corpus id. The ObjectId coercion is a
  caller-supplied `valueParser`, pinned against the `id-eq-const` fixture in the translator unit
  test instead.

`f1` is the witness: its `aString` equals its own id and its `aOptionalString` is that id prefixed,
so the field-to-field and concatenation oracles are non-degenerate. `principal.attr.context` carries
the same id for the value-first and hierarchy shapes.

### Casts and concatenation are store-dependent in opposite directions

`string()` and CEL's string `+` are the two shapes where a correct-looking lowering is right on some
stores and wrong on others.

`cast-string-double` agrees everywhere: CEL formats the shortest round-tripping decimal, and so does
every store the corpus runs — PostgreSQL 17 renders `CAST((-0.6)::float8 AS TEXT)` as `-0.6`, as do
SQLite and MySQL (measured on the pinned images; `-0.60000000000000009` appears only under
`extra_float_digits = 3`, and PostgreSQL 12 made shortest round-trip the default).

`cast-string-bool` diverges. SQLite and MySQL store booleans as 1/0, so `CAST(a_bool AS TEXT)` is
`"1"` where CEL and PostgreSQL say `"true"`. An adapter spanning both cannot lower it through a
`CAST` ([#418](https://github.com/cerbos/query-plan-adapters/issues/418)):

- **activerecord, sqlalchemy, ent, pgx and drizzle** lower it through
  `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END`, which spells CEL's words
  on every engine and keeps a NULL column UNKNOWN.
- **spring-data** compares the constant in Java: `"true"` and `"false"` become `col = true` and
  `col = false`; any other constant matches no row.
- **mongoose and convex** lower it directly (`$toString` and JavaScript render a bool as CEL does).
  **prisma, langchain-chromadb and elasticsearch-java** refuse it: none has a computed string
  operand.

The `CASE` has a hazard the action does not reach. Its words are literals, so MySQL compares them in
the *connection's* collation, and a driver's default is case-insensitive: `string(flag) == "TRUE"`
would match every true row. drizzle renders the literals `COLLATE utf8mb4_0900_bin` and ent keeps its
binary-collation `CAST` around the `CASE`, so both are byte-exact on MySQL; activerecord and
sqlalchemy run no MySQL leg and state the requirement in their READMEs; spring-data never compares
text. The action only compares with `"true"` and `aBool` is never NULL, so neither the collation nor
the `IS NULL` arm is oracle-proved yet — both are pinned in unit tests and golden expectations until
the corpus carries a probe ([#469](https://github.com/cerbos/query-plan-adapters/issues/469)).

`id-concat` is the same lesson for `add`. The corpus's `add` is otherwise numeric, and a string
concatenation sent to SQL `+` is a hard error on PostgreSQL, an under-grant on SQLite, and a silent
**over-grant** on MySQL, which coerces both operands to 0 and matched 18 of the 21 seeds against a
one-row oracle. Adapters that know their engine render `||` or `CONCAT`; adapters that do not know
their dialect refuse it.

#### A constant is what tells the two `+` overloads apart

`id-concat` and `id-concat-vf` carry a string **literal**, and CEL has no mixed-type `+`, so one
string operand proves the expression is a concatenation. `concat-f2f`
(`R.attr.aString + R.attr.aOptionalString`) removes it. A plan names no operand types, so two
variables cannot tell concatenation from arithmetic.

Guessing arithmetic fails in the dangerous direction: a hard error on PostgreSQL, zero rows on
SQLite, and on MySQL **16 of the 21 seeds against a one-row oracle**, because both text operands and
the string constant coerce to 0.

The corpus does not prescribe a resolution; the adapters split four ways:

- **convex** and **sqlalchemy** need nothing. Convex concatenates in JavaScript (CEL's semantics);
  SQLAlchemy renders through the column's declared type, so its dialect picks `||` or `CONCAT`.
- **ent** and **pgx** take a declaration — `ValueType: ValueString` on the mapper entry, like
  `ValueBool` for `string()`. Declared, they concatenate; undeclared, they fail closed.
- **prisma**, **drizzle**, **langchain-chromadb**, **elasticsearch-java** and **spring-data** refuse
  it through limitations they already had.
- **mongoose** refuses it, after being taught to: it sent `$add` to the server, which aborted the
  query rather than returning a wrong row set.

There is no `ValueNumber`. Every numeric `add` the planner emits carries a constant operand, so a
declaration for the both-columns numeric case would be read by nothing — the same reason there is no
`CastInt` (#319). A corpus action reaching that case should introduce it.

### Root position and bare operand forms

Eight actions — `not-lt`, `not-gt`, `gt-bare`, `le-bare`, `root-bare-bool`, `root-or`,
`or-eq-exists`, `or-eq-in` — pin **positions**, not hazards. They came from set-differencing the node
shapes the wire fixtures produce (`operator(child,child)`, children in source order) against those
the policies could reach; each was an accidental hole (#388).

`not(lt)`, `or(V,lt)`, `or(V,exists)` and `or(V,in)` are node shapes nothing else produces.
`not-gt`, `gt-bare`, `le-bare` and `root-bare-bool` are not new shapes, but every existing
occurrence was reached through another mechanism:

- `gt(V,K)` and `le(V,K)` exist only via `rel-gt-hop`/`rel-le-hop`, behind the to-one join walk — and
  behind no walk at all for an adapter that refuses a hop.
- a bare variable at the root exists only via `rel-bool-hop`.
- `not(gt)` exists only over a `size()` or a ternary (`w1-not-size-chain`, `ternary-negated`,
  `p-not-ternary-null`) — never over an indexable operand.

Most adapters translated all eight. Two did not:

- **sqlalchemy** refused `root-bare-bool`. Its root guard (written to reject `filter()`/`map()`,
  which return lists) tested for a Core `ColumnElement`, but a bare column resolves to an ORM
  descriptor. The same operand was accepted as an `and`/`or`/`not` child, so the position was
  deciding, not the shape.
- **langchain-chromadb** fails closed on `or-eq-exists` and `or-eq-in`, correctly: its Where model
  has no nested-expression form for the branch it cannot express.

`or-eq-exists` also matters because the only other subquery under a disjunction,
`rel-hop2-or-exists`, carries a two-level hop, so an adapter refusing hops never executed the
disjunction.

`root-or`'s second disjunct is `R.attr.aNumber < 0`, not the specified `aString != "one"`: `aString`
is never NULL and the one `"one"` seed has `aBool` true, so that spelling allowed all 29 seeds — a
total oracle that would pass against any filter.

### Hazard classes the corpus missed

12 actions — `not-and`, `not-contains`, `not-startswith`, `arith-mod`, `index-scalar-list`,
`map-eq-list`, `vf-lt`, `vf-size`, `vf-hasint`, `pv-exists-unrolled`, `pv-all-unrolled` and
`filter-as-conjunct` — came from the same set-differencing (#387). Each pins a place where CEL and a
store's query language are known to disagree, or where the same bug has already shipped to several
adapters:

- **`not-and`** is the De Morgan branch; the corpus negated every other connective but `and`. It is
  also byte for byte the shape a DENY rule composes to.
- **`not-contains` / `not-startswith`** negate a LIKE against a **column** needle, so the negation
  meets the NULL-needle rows. A NULL needle must be UNKNOWN, not FALSE, or `NOT` flips it and the row
  leaks; and metacharacter escaping is tested in the under-granting direction no positive LIKE sees.
- **`arith-mod`** is `int(R.attr.aNumber) % 2 == 1`, not the specified `== 0`: `x % 2 == 0` is
  sign-invariant, so truncated (CEL, Go, SQL, JavaScript) and floored (Python) modulo agree. `== 1`
  makes the `-5` seed the witness, and the failure is an over-grant.
- **`index-scalar-list`** indexes a scalar list directly — a bare `index(V,K)` operand; `p-index`
  reaches its rejection through a `get-field` projection. The `index-scalar-list-not-eq` and
  `index-scalar-list-null` companions distinguish an invalid position from an explicit null element:
  empty lists stay denied under negation, while the null first elements in `b5`, `b6` and `e1`
  satisfy the null comparison.
- **`map-eq-list`** compares a projection to a literal list; `map` was previously only fed into
  `hasIntersection` or left bare.
- **`vf-lt` / `vf-size` / `vf-hasint`** complete the value-first family, the canonical bug class
  (#258/#259). `hasIntersection` is commutative, so an inversion is invisible in the answer but not in
  the emitted query.
- **`pv-exists-unrolled` / `pv-all-unrolled`** plan the other side of the planner's unroll cliff.
  `manyTeams` holds 11 elements, so `pv-exists`/`pv-all` produce the value-list form; `fewTeams` is
  the same witness set at 3, which the planner unrolls into an or/and chain — what most real
  principals produce.
- **`pv-in` / `pv-in-unrolled`** ([#411](https://github.com/cerbos/query-plan-adapters/issues/411))
  test direct membership of the omitted-convention `aOptionalString` in `manyTeams` and `fewTeams`.
  The planner emits `in(variable, value-list)` for both sizes; direct membership does not cross the
  unroll boundary. They share oracles with `pv-exists` / `pv-exists-unrolled`, the lists discriminate
  `same`, and missing attributes must stay denied.
- **`filter-as-conjunct`** puts a `filter()` one level below the root. `filter-as-condition` pins
  the rejection at the root; an adapter can reject there and still walk a macro in a conjunct.

The group found, and the adapters now fix:

- emitted filters where a throw was required — drizzle's mirrored `hasIntersection` became a bare
  `FALSE`; sqlalchemy passed a bare Python `False` to `where()`;
- root-only guards that `filter-as-conjunct` walked around — convex's post-filter read the held list
  through `asBoolean()` (denying every row, so agreeing with the empty oracle by accident);
  sqlalchemy raised SQLAlchemy's coercion error instead of one naming the mechanism;
- ent and pgx binding a held collection as a query parameter, refused only by the driver at
  execution time;
- elasticsearch-java reading `0 < size(c)` as `size(c) < 0`;
- an **over-grant** in spring-data: a column-needle LIKE rendered as
  `needle IS NOT NULL AND haystack LIKE pattern` is definite FALSE for a NULL needle, which `NOT`
  flips to TRUE, so every negated column-needle match returned the NULL-needle rows the PDP denies.

`filter-as-conjunct`'s oracle is empty by construction, so it is in `degenerateOracles` with its own
anti-vacuity assertion in every harness: the other conjunct is `R.attr.aBool` (what `root-bare-bool`
spells alone), so an adapter that dropped the untranslatable half would return 14 rows the PDP
denies. The assertion pins that, not merely that a rejection happens.

The #430 audit adds `projection-exists-eq` and `projection-exists-not-eq` for a scalar projection
lambda's positive and negated bodies (negating the whole macro, as `lambda-in-literal-neg` does, is a
different branch). The pair distinguishes an explicit null element from a missing object attribute:
`null != "public"` is true, while reading a missing attribute raises. `rel-not-eq-hop`,
`rel-not-contains-hop` and `rel-not-hierarchy-hop` test negative scalar predicates through the
to-one parent; parentless rows must stay excluded. Every harness guards these for non-empty,
non-total oracles on its compared or refusal side.

### The degeneracy guard

The step-4 comparison passes vacuously if the oracle is trivial. An empty oracle still catches an
over-grant; a **total** oracle catches nothing. A harness whose PDP connection or policy load
silently failed would pass every comparison against a deny-everything oracle.

So every harness asserts, for **every** action it oracle-compares, that the oracle is neither empty
nor the full seed set (`!ids.isEmpty() && ids.size() < seeds.size()`), on the oracle the comparison
already computed. (It once covered a sample, leaving under half the compared actions guarded on some
harnesses — [#490](https://github.com/cerbos/query-plan-adapters/issues/490).)

**`degenerateOracles` in `actions.json` is the only exemption.** It lists every action whose oracle
is empty or total *by construction*, with `"oracle": "empty"` or `"total"` and a reason (a type
error, a planner fold, an IEEE identity, a seed set with no witness). It is corpus data because the
oracle is a property of the PDP and corpus, not of an adapter. Every harness asserts it both ways:

- a listed action's oracle is **exactly** what it declares (`[]`, or every seed id), so an entry that
  starts discriminating fails instead of standing as a blanket exemption;
- every harness asserts every entry, whether or not it compares the action.

`validate-corpus.sh` holds the schema closed and rejects an entry naming an unclassified action, a
`knownDivergences` action (never compared, so exempting it exempts nothing) or a duplicate. List an
action only after watching its oracle against a live PDP in both evaluation modes. A degenerate
oracle on a new action is far more often a missing discriminating seed than a genuine identity; fix
the seed.

Each harness also keeps **liveness-only probes**: shapes the adapter refuses, kept because the group
has no compared member for that adapter and a non-degenerate oracle still proves the PDP and policy
are live. Each entry is asserted *not* to be in the adapter's oracle set (a newly supported shape
moves to the sweep) and *not* to be in `degenerateOracles` (a trivial oracle proves no liveness).
**Derive the list per adapter; never copy another harness's** — a copied list drifts into naming
shapes the adapter compares (#324).

### Pinned throw messages

A bare "it threw" assertion is satisfied by a mapper typo, an unrelated validation or a transport
error, none of which reaches the mechanism the `reason` names. elasticsearch-java found this out: an
unmapped `categories` field had six actions throwing "Unknown attribute" — a harness gap (#326).

So every throwing classification carries the substring the adapter's error must contain:

- `adapterUnsupported[<adapter>][].message`;
- `expectedUnsupported[].messages[<adapter>]` — one per adapter that must reject the shape
  (generalising the old spring-data-only `springDataMessage`);
- `nullRepresentationOmitted[].messages[<adapter>]` — the same, for the group every adapter rejects.

`scripts/validate-corpus.sh` enforces all three: every `adapterUnsupported` entry has a non-empty
`message`; every `expectedUnsupported` entry's `messages` keys are *exactly* the roster minus the
adapters that promoted it into `adapterSupportedExpected`; every `nullRepresentationOmitted` entry's
keys are the whole roster. A missing key leaves a harness nothing to assert; a stray one is a message
nothing reads.

Each harness resolves its messages while deriving the classification and **fails the run if one is
absent**, and unit-tests that guard directly so it cannot go inert.

The assertion is `contains`, not equality — a deliberate weakening for spring-data, which used
`assertEquals` on `springDataMessage`. One meaning across every harness is worth more, and several
messages carry a runtime value (`Timestamp value exceeds millisecond precision: <now()-24h>`).
Rewording the mechanism still fails every suite; appending to a message no longer fails spring-data.

Some messages are shared across many actions: Chroma answers 86 of its 126 throwing shapes with
"Nested expressions are not supported by ChromaDB filters". Those 86 `reason` strings name different
upstream limitations (no count function, no relation model, no temporal type) that converge on one
rejection: every Chroma operand must be a bare field or literal. The message discriminates the
mechanism, not the action, and a rejection from anywhere else still fails. Pin what the adapter says.

The message and the `reason` must name the same mechanism. When they disagree, work out which
limitation fires first and correct the `reason`; do not loosen the pin. Pinning surfaced several
reasons that named what a maintainer had in mind rather than what the walk reaches (prisma's
`p-deep-nest` hits the LIKE-metacharacter needle before the cross-model comparison;
elasticsearch-java's `p-ternary-under-all` rejects the positive `all` before reaching the
conditional).

### Known divergences still need a tripwire

A `knownDivergences` action is excluded from the oracle run, so it is exercised nowhere unless the
harness says so. Every harness therefore pins the `p-has` planner over-grant directly: the plan folds
to `KIND_ALWAYS_ALLOWED`, the oracle is non-empty and non-total (it denies seeds whose attribute is
missing), and the adapter returns every row. When upstream fixes the fold the assertion fails —
move the action back into the oracle run.

### Deterministic derived fields

Six resource attributes are derived from each seed. **The values live in `derived-fields.json`, one
entry per seed id, and every harness reads them from there.** Hand-transcribing them per harness
hid errors: the same copy feeds the stored row and the oracle, so a wrong value makes both sides
agree (#318).

`scripts/validate-corpus.sh` asserts one entry per seed id with exactly the declared fields,
re-derives `createdBy`, `aDouble` and `createdAt` from `seeds.json` using the rules below, and diffs
`scope` and `labels` (which have no rule) against its own restatement of the tables. It is a
checker, never an input to a harness, so it can only fail loudly.

The rules the file materialises:

- `createdBy`: `h5 = "not-a-timestamp"`; otherwise
  `aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z"`.
- `aDouble`: `a1 = -0.6`, `a2 = 0.25`, `a3 = NULL`/missing, `g1 = -9.5e18` (the int64-saturation
  witness for `double-huge-lt`/`double-huge-gt`), otherwise `aNumber + 0.3`.
- `createdAt`: `a1 = 2020-03-15T10:30:00Z`, `a2 = 2037-01-01T00:00:00Z`, `a3 = NULL`/missing,
  `a4 = 2024-06-01T00:00:00Z`, `a5 = 2020-03-15T10:30:00.123456Z`; otherwise use
  `2036-06-06T06:06:06Z` when `aNumber >= 2`, or `2021-05-05T05:05:05Z`.
- `updatedAt`: `a1 = 2020-03-15T10:30:00.000Z`, `a4 = 2024-06-01T00:00:00Z`; otherwise
  NULL/missing. `a1` equals `createdAt` as an instant but differs as an RFC 3339 string, while
  `a4` is equal under both readings. Oracle attributes must preserve these original strings;
  parsing and re-serializing the instant would erase the witness.
- third-level `labels[].name`: `a1 = ["gold", "silver"]`, `a6 = [missing, "silver"]`,
  `a8 = ["silver"]`, `c1 = ["Gold"]`, otherwise empty.
- `scope`: `a1=dept`, `a2=dept.eng`, `a3=dept.eng.platform`,
  `a4=dept.eng.platform.obs`, `a5=dept.engineering`, `a6=dept.sales`, `a7=NULL`,
  `a8=""`, `a9=50%`, `b1=50%:a_b:x`, `b2=50x:a_b:y`, `b3=50%:aXb:y`,
  `b4=50%:a_b`, `b5=dept.eng.platform2`, `b6=50%.a_b`, `c1=Dept.Eng`,
  `c2=dept.eng.`, `d1=[env]:prod:eu`, `d2=e:prod:eu`; all other seeds use NULL.

Do not replace these with adapter-specific fixtures, and do not recompute them in a harness.

### Seed, principal and derived-field coverage

A seed key a harness does not consume is dropped from the stored row **and** the oracle at once, so
the differential still agrees and the field tests nothing. Every harness therefore declares the exact
seed key set it consumes and asserts equality against the JSON — both directions, since a key the
corpus stops carrying would otherwise decode to its zero value on both sides. `note` is the one
exclusion. The same assertion covers the elements of `tags[]`, the one nested object array.

It also covers `derived-fields.json`: each harness declares the six fields it consumes and fails if
the file's `fields` list or any entry's key set differs. In Go that is `DisallowUnknownFields` plus a
key-set assertion; in Java, records without `@JsonIgnoreProperties(ignoreUnknown = true)` plus a
key-set assertion; in TypeScript and Python, an explicit `assertKeys`. TypeScript harnesses that
rebuild each seed field by field (mongoose, langchain-chromadb) assert against the *raw* JSON — a
rebuilt object only reports keys the parser already names.

The **principal** is guarded the same way, and it is where the trap actually fired: an attribute
dropped on the way in vanishes from plan and oracle together, the plan folds to `ALWAYS_DENIED`, the
oracle agrees, and the action tests nothing. Every harness declares `{id, roles, attr}` and the
attribute names inside `attr`, and asserts equality in both directions. `id` and `roles` are inside
the guard: a dropped role changes every decision at once.

Attribute *values* are asserted too: string scalars, the numeric `zero`, string lists (including
`emptyTeams`), and three struct lists — `manyStructs` (a string `name` on every element),
`nullableStructs` (an explicit null `name`) and `missingStructs` (empty objects), each with 11
elements to cross the planner's unrolling threshold. The guards validate each nested key and value
shape so an SDK cannot erase a missing/null distinction.

Construction stays verbatim pass-through; the guard keeps it that way.
`scripts/regenerate-wire-fixtures.sh` copies `.principal` wholesale with `jq` and needs no guard.

Adding a seed field or principal attribute must fail every harness loudly. That is the acceptance
test for these guards; run it before trusting them.

## Adding a new hostile shape

Any change to how a shape is translated starts here, not in one adapter (see `CLAUDE.md`, "Changing
how a condition is translated").

1. Add the action and condition to `policies/adversarial.yaml`, with a comment saying what it probes
   and which seed rows discriminate it (follow the existing style).
2. Add the action to `actions.json` — `conformance`, `expectedUnsupported`, or
   `nullRepresentationOmitted` if it probes `== null` against an attribute the oracle omits for NULL
   columns.
3. If the shape needs new seed data to be non-degenerate, add a seed to `seeds.json` with a `note`
   saying what it witnesses (see `a9`, `b1`–`b6`), and its `derived-fields.json` entry in the same
   commit; `scripts/validate-corpus.sh` names the expected values when it fails. A new seed *field*
   or **principal attribute** must also be added to every harness's declared key set — the guards
   in "Seed, principal and derived-field coverage" fail every harness until it is.
4. Run `scripts/regenerate-wire-fixtures.sh` and commit the new fixture with the policy change.
   Confirm the diff adds only the new action.
5. Run every adapter's harness; each picks the action up from `actions.json`. Triage each divergence
   into a fix, an `adapterUnsupported` entry, or a `knownDivergences` entry (see "Adding a new
   adapter", step 5) — never a special case in the harness. A fail-closed classification needs the
   message the adapter actually raises pinned beside it (see "Pinned throw messages"); every harness
   refuses to run with one missing.
6. Bump the tripwires deliberately. Every harness pins the corpus size and its throwing-action
   count; convex, langchain-chromadb and elasticsearch-java also pin oracle counts. The convex
   harness also pins which actions its filter engine decides alone under each of its two mappers,
   because its README quotes those counts
   ([#327](https://github.com/cerbos/query-plan-adapters/issues/327)); name the new action in one of
   those buckets.
7. Confirm the action cannot pass vacuously. Every compared action is swept for a non-empty,
   non-total oracle, so a translated shape needs nothing added. Where an adapter throws on it and the
   group has no compared member there, add it to that harness's liveness-only list (see "The
   degeneracy guard"). If the oracle is empty or total, add a discriminating seed; declare it in
   `degenerateOracles` only when it is degenerate *by construction*.

   Check that no harness projects corpus data into a narrower local shape. langchain-chromadb once
   rebuilt the principal from an attribute allowlist; when `pv-exists` added
   `principal.attr.manyTeams`, the projection dropped it, the plan folded to `ALWAYS_DENIED`, and
   the oracle — built from the same projection — agreed. Pass corpus data through verbatim.

   A `nullRepresentationOmitted` action is empty by construction, so it goes in `degenerateOracles`
   and needs the anti-vacuity assertion that pins *why* the rejection is required (see
   "`nullRepresentationOmitted`: the two conventions are indistinguishable on the wire"), plus a
   `messages` entry per adapter.
8. Regenerate the **golden expectations** of every adapter with a translator unit test and read the
   added entry. Those suites fail until the action is accounted for (see "Golden expectations").
9. Update the affected adapters' README `Conformance contract` tables in the same commit.

## Golden expectations

A **golden expectation** is the database-native filter one adapter is pinned to emit for one corpus
action. It is the central assertion of a *translator unit test* — the offline suite that reads its
plans from `wire-fixtures/` and needs no PDP and no store
([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)). That
suite also pins plan kinds, refusals and caller-supplied contracts (`CLAUDE.md`, "What a translator
unit test may pin").

**Expectations are not corpus data and never live here.** Every adapter workflow triggers on
`conformance/**`, so one adapter re-pinning one filter would re-run every other adapter for nothing
— the same argument that keeps service image pins out of the corpus
([ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md)). Only the *format* lives here.

### The file

One file per adapter, at `<adapter>/golden/expectations.json`:

```jsonc
{
  "adapter": "drizzle",
  "regenerate": "npm run golden:update",
  // Optional, and only where the value needs it — see "When the generator is an input" below.
  "sqlalchemy": "2.x",
  "expectations": {
    "<action>": { "note": "optional, human, preserved across regeneration", /* … */ }
  }
}
```

- **`adapter`** is checked by the loader. A copy from another adapter would otherwise parse cleanly
  and be compared against the wrong translator.
- **`regenerate`** is the command that rewrites the file.
- **`expectations`** is keyed by corpus action and **sorted** (asserted), so a translator change reads
  as the list of shapes it moved.
- **`note`** is the one reserved key inside an entry: never compared, carried across by the
  regenerator. Everything else is the adapter's filter document.

An adapter may add its own header key only when the value depends on something other than the plan
(see below). The loader checks it like `adapter`.

### What the entry holds is per adapter, and has to be

A Prisma `where` input, a Mongoose query document, a Drizzle `SQL` tree and an Elasticsearch query
share no type, so the *value* schema is the adapter's, documented in its README. Only the layout is
shared: a JSON object keyed by action, one entry per accounted-for fixture.

An adapter whose output is not data still fits. Convex emits a **function** of the query builder
plus an in-memory post-filter, so its entry records the calls that function makes against a
recording builder and which half answers the query. The rule is "the observable the translator
produced", not "the query text"; where the adapter has a boundary the corpus cannot see, pin that
boundary.

Two rules constrain the value:

- **It must round-trip as JSON.** Normalise what JSON cannot hold and pin it in code: drizzle records
  `0` where the adapter binds `-0` and asserts the list of actions that bind one separately.
- **It is a filter, never a row set.** Which rows a filter returns is the oracle's answer; writing
  it down would freeze an authorization decision.

### When the generator is an input

Most adapters record their translator's return value, whose only input is the plan. Where the
recorded value passes through something whose version can change the bytes, the file must say which
version, or a toolchain change reads as a translation change.

sqlalchemy is the case: its entry records the emitted expression *compiled* — a `WHERE` clause per
dialect plus the bound parameters, asserted to be shared across dialects — and the two SQLAlchemy
majors render some trees differently. The file carries `"sqlalchemy": "2.x"`, and an adapter in the
same position copies all three consequences:

1. **The loader checks the key**, like `adapter`.
2. **Regeneration refuses under any other version**, so a toolchain swap cannot pose as a
   translation change.
3. **The other version asserts a pinned divergence list** instead of the bytes, in *both*
   directions, so a shape that stops diverging fails as loudly as one that starts.

spring-data declares `"hibernate": "6.6"`: its entry records Hibernate's rendering of the emitted
JPA `Specification`. Rules 1 and 2 apply, and so does rule 3 — the `ADAPTER_TEST_ORM=next` leg
(Hibernate 7 / Spring Data JPA 4) asserts a pinned divergence list in both directions. The header
matters more there because `hibernate-core` is `compileOnly`: a consumer brings their own renderer,
so which one wrote the bytes must be answerable from the file.

Do not add a key per environment difference. A key is justified only when the difference is
*outside* the adapter and *inside* the recorded value; a dialect is a dimension of the value (it goes
in the entry), and a Node version changes nothing.

### A throwing action carries no entry

If `actions.json` says the adapter must refuse an action, its message is already corpus data. The
translator unit test reads `adapterUnsupported[adapter]`, `expectedUnsupported` and
`nullRepresentationOmitted` as the harness does and asserts the throw against that message. Copying
it into the asset would give one string two homes, and would make an adapter that refuses most of the
corpus (langchain-chromadb, elasticsearch-java) carry a file of restatements.

### The completeness guard

ADR 0006 requires every wire fixture to be accounted for **exactly once** per adapter carrying this
test:

```
keys(golden/expectations.json)  ∪  throwing actions from actions.json  ==  wire-fixtures/*.json
```

Total, so a fixture with neither fails; disjoint, so an action with both is caught. Add per-bucket
count tripwires beside it.

### Regeneration is a deliberate act, and the diff is the review

Each adapter ships a command that rewrites its file from what the translator emits today (`npm run
golden:update` on drizzle), like `scripts/regenerate-wire-fixtures.sh`. **CI never regenerates**, so
a translator change that moves a filter fails there whatever anyone ran locally, and the diff is what
the reviewer reads.

Regeneration cannot protect a property nobody wrote down — it will happily record a filter that
collapses a NULL to FALSE. Keep the *rules* as assertions beside the pinned bytes; they survive
regeneration and hold for actions not yet added.

### Language neutrality

The layout is JSON, the guard is set arithmetic, enumeration is iterating keys — pytest
`@pytest.mark.parametrize`, JUnit `@MethodSource`, Go `t.Run` in a loop. The loader is not shared:
per ADR 0007 each adapter writes its own, idiomatically.

## Adding a new adapter

A new adapter joins by proving itself against the corpus. Work in this order: the classification is
an *output* of the harness. Declaring an action unsupported before watching it fail is how a
translatable shape gets permanently skipped.

1. **Implement translation.** Follow the closest existing adapter. Spring Data is the reference:
   when a shape is ambiguous its behaviour defines the answer, and whether it translates a shape
   decides `conformance` vs `expectedUnsupported`.

2. **Write the differential harness**, implementing the oracle recipe against the adapter's own
   store. Never hand-write expected id sets. Derive the classification from `actions.json` at
   runtime:

   ```
   oracleActions   = conformance - adapterUnsupported[me] + adapterSupportedExpected[me]
   throwingActions = adapterUnsupported[me] + (expectedUnsupported - adapterSupportedExpected[me])
   nullOmitted     = nullRepresentationOmitted            (translated with the option flipped)
   skipped         = knownDivergences where adapters contains me
   ```

   Resolve each throwing action's message (`.message` on `adapterUnsupported`, `.messages[me]` on
   `expectedUnsupported`) while deriving, and fail the run when one is absent.
   `drizzle/src/adversarial.test.ts` is the cleanest example. Each adapter's key in `actions.json` is
   its **directory name** (`langchain-chromadb`, `elasticsearch-java`).

   **Read every group, and derive the manifest from the same expressions.** The "each action
   classified exactly once" assertion catches a forgotten group only if the group feeds both sides.
   Harnesses that re-validate `actions.json` into a local record (mongoose, langchain-chromadb) must
   parse each group explicitly; a group the parser does not name vanishes from every count at once.

3. **Persist the seeds exactly**, including the NULL conventions, and read derived fields from
   `derived-fields.json`. The NULL `aOptionalString` and `tags[].name` values are what the
   three-valued-logic probes discriminate on. Declare the seed keys, principal keys and derived
   fields the harness consumes and assert set equality (see "Seed, principal and derived-field
   coverage").

4. **Assert the degeneracy guard** and pin the corpus size: sweep every compared action, assert
   every `degenerateOracles` entry, and derive the liveness-only list from this adapter's own
   refusals — one lifted from another harness will name shapes this adapter compares. Pin every
   `knownDivergences` action (see "Known divergences still need a tripwire").

5. **Run it and let it fail.** Triage every divergence into exactly one of:
   - a translation bug — fix it;
   - a shape the query language genuinely cannot express — add it to
     `adapterUnsupported[<adapter>]` with a **specific** reason naming the mechanism, and make the
     adapter throw. "Cannot express this shape faithfully" is not a reason; "emits LIKE without an
     ESCAPE clause, so `%` cannot be matched literally" is. Pin the message the adapter actually
     raises and check it names the mechanism the reason declares;
   - an upstream planner bug — add it to `knownDivergences` with the affected adapters and a reason.

   **An inexpressible shape must throw before its filter can be used.** Never degrade one operator
   into a weaker one (`exists_one` into `exists`) to make a test pass.

6. **Register in `actions.json`**: add the adapter to `adapters`, and give every
   `expectedUnsupported` entry it does not promote a `messages` key. `scripts/validate-corpus.sh`
   checks that every `adapterUnsupported` entry names a real `conformance` action and carries a
   message, every `adapterSupportedExpected` entry names a real `expectedUnsupported` one, and each
   `messages` key set is exactly the roster minus the promotions.

7. **Write the example application.** `demo/scripts/validate-demo.sh` reads the same `adapters`
   roster and fails for any adapter without a runnable `<adapter>/example/run.sh`, so steps 6 and 7
   land together. The example implements the demo domain's five usage shapes against the **packed
   artifact** ([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)) — the published
   surface and composed usage no harness exercises. There is no opt-out bucket
   ([ADR 0001](../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)); a shape needing a
   carve-out belongs back in this corpus. Read [demo/README.md](../demo/README.md), "What an example
   must do", first.

8. **Wire CI.** Copy an existing adapter workflow. It must:
   - read the PDP tag from `CERBOS_VERSION` and digest from `CERBOS_IMAGE_DIGEST`, never hardcoded;
   - run `scripts/validate-corpus.sh` in every job that replays the corpus **or** hardcodes the PDP
     image (a job interpolating the two files at runtime cannot drift);
   - trigger on `conformance/**` and `demo/**` as well as the adapter's directory;
   - run the adversarial suite **inside the same job as the regular tests** — a separate job costs
     runner minutes for no coverage;
   - pin every service image it starts by tag **and** digest (see "Pinning service images");
   - run `demo/scripts/validate-demo.sh` and `demo/scripts/run-example.sh <adapter>` in an example
     job **in this adapter's own workflow**. `renovate.json` automerges non-major bumps, so an ORM
     bump arrives as one PR touching the manifest and the example's lockfile; the example job on that
     PR is what blocks a breaking automerge. A nightly or standalone workflow fails only after merge.

9. **Document the contract** in the adapter's README: a `Conformance contract` table
   (oracle-tested / fail-closed / known divergence counts) and a `Mapping hazards` table (see
   below). Each adapter is published independently, so its README must stand alone.

### Pinning service images

Every container a test or workflow starts is written `repository:tag@sha256:<64 hex>`. The tag says
which release; the digest says which build a green run proved. Tags are re-pushed (`postgres:16`,
`docker.elastic.co/elasticsearch/elasticsearch:8.15.3`). `validate-corpus.sh` enforces both halves:

- **The PDP** is corpus-wide, so its halves live here in `CERBOS_VERSION` and `CERBOS_IMAGE_DIGEST`.
  Every restatement in the repository must match both — a right tag with another build's digest
  reads as pinned and is not ([#322](https://github.com/cerbos/query-plan-adapters/issues/322)).
- **Everything else** (databases, search and vector stores) is pinned *per harness*, in one constant
  that adapter's suites share, **not** under `conformance/`: a change here re-runs every adapter
  workflow. `validate-corpus.sh` holds a list of image repositories, requires every occurrence to
  carry a tag and digest, and requires each `repo:tag` to resolve to one digest repo-wide. **Adding a
  new service means adding its repository to that list**; an unscanned repository is unpinned.
- Markdown is out of scope for both scans: a README telling a consumer how to start their own PDP is
  about their environment.

Renovate's Docker managers are off (`docker:disable`), so a `Dockerfile` or compose file is bumped by
hand with whatever re-verification it needs. The `*_IMAGE` files are the exception: a regex custom
manager in `renovate.json` proposes tag and digest bumps for them, automerging non-major ones and
leaving majors to a maintainer.

### Vendored code stays byte-identical

ent and pgx each vendor the translator under `internal/queryplan`, so a consumer pulls in only one
module. A semantic fix can therefore land in one copy alone, and the corpus notices only if some
action exercises the fixed shape ([#319](https://github.com/cerbos/query-plan-adapters/issues/319)).

`validate-corpus.sh` diffs `ent/internal/queryplan` against `pgx/internal/queryplan` and fails on any
difference; both workflows run it and trigger on `conformance/**`, so a one-sided edit fails
whichever side it lands on. Byte-identical, with no allowlist: anything genuinely per-engine goes in
that module's `render.go`, outside the shared tree.

`ent/translate_test.go` and `pgx/translate_test.go` share test names, section order and shapes. Keep
them in step when you add an invariant to either.

### Mapping hazards: the rows the subquery sees

Everything above proves the **plan** side. The other half of the contract is the **mapping**, which
no policy action can express:

> **The rows an adapter's subquery sees must equal the rows the application put into the resource
> attributes.**

When they differ, the filter returns rows the PDP denies and no corpus action notices, because the
oracle reads the attributes and the adapter reads the store. Each hazard below violates that
sentence, and each was a real over-grant (#314, found while building the ActiveRecord adapter):

| Hazard | What goes wrong | Where it shows up |
|---|---|---|
| A **filtered association** | the application's association applies a predicate the subquery does not, so the subquery matches rows the application never serialised | ActiveRecord `has_many …, -> { where(visible: true) }`; SQLAlchemy `relationship(primaryjoin=…)`; Hibernate `@Where`/`@Filter`; a Prisma client extension or middleware injecting `where` |
| A **default scope on the target model** | the subquery reads the table directly and skips the scope every application read applies | ActiveRecord `default_scope`; Hibernate `@Where` on the entity; a soft-delete filter |
| **Subtype discrimination** | the association also filters on a type/discriminator column; the bare table holds the other subtypes too | ActiveRecord STI; JPA `@DiscriminatorValue`; a Mongoose discriminator, though only for the model the caller hands to `find()` |
| A **to-one relation used as a collection** | nothing makes the database enforce one row, so the application sees one and the subquery examines all of them | ActiveRecord `has_one`; any unindexed FK-back-reference |
| A **composite association key** | a multi-column key becomes one quoted identifier and the query fails, or worse joins on the wrong column | ActiveRecord 7.1+ composite keys; any two-column FK |
| An **absent to-one parent** | see the section above — this one *is* expressible, and `w1-all-chain` and friends pin it | every relational adapter |

**1. Decide about each hazard explicitly.** For a hazard that can arise in an adapter there are three
sanctioned outcomes:

- **Reproduced** — the mapping carries the store-side predicate, so the subquery reads what the
  application reads. Class 1 adapters (below) take an optional relation predicate for this.
- **Rejected** — the adapter refuses the mapping with an error naming the mechanism, or its mapper
  type cannot express the hazardous mapping. Every adapter whose relation mapping takes a single
  source column rejects a composite key this way: a compile error, not a wrong join.
- **Declared caller-owned** — the README states the caller must hold the invariant and names the
  exact ORM feature to check. Available **only** where the adapter cannot detect the hazard from its
  mapper (it cannot see a client extension, a soft-delete convention or a discriminator from a table
  and two columns). Where it can see the hazard, it must reproduce or reject. A row saying only
  "caller-owned" does not pass review.

A best-effort subquery is forbidden. **Not applicable** is allowed only when the hazard structurally
cannot arise and the row says why, backed by a test: class 3 adapters write it for the five subquery
hazards because they build no subquery, and mongoose's harness asserts that — the day it grows a
`$lookup`, those rows would become over-grants.

**2. Say so in the adapter's README**, next to the `Conformance contract` table: one row per hazard
above, all six, in this order, even when most are inapplicable:

```
| Hazard | Position | Mechanism to check |
```

The absent to-one parent's row records that it is *proved by the corpus* (`w1-all-chain` and
siblings). An adapter may append rows for a hazard only its store has, saying so in the prose above
the table, with the six shared rows first. A hazard two adapters could hit belongs in this shared
list instead. The one such row today is elasticsearch-java's **analyzed (`text`) field mapping**:
Elasticsearch tokenizes a stored string before comparing, so a field mapped `text` widens every
string comparison. No other store transforms a value between write and comparison.

The adapters fall into three classes:

| Class | Adapters | What the store applies to the subquery |
|---|---|---|
| **1 — bare-table subquery** | drizzle, ent, pgx, prisma, activerecord | nothing |
| **2 — ORM-association subquery** | spring-data, sqlalchemy | Hibernate applies `@SQLRestriction`/`@Where` — on the entity and on the joined collection — and the single-table discriminator; SQLAlchemy applies `primaryjoin` and the single-table discriminator *only* when the caller's override goes through a mapped `relationship()` |
| **3 — no subquery** | mongoose, convex, langchain-chromadb, elasticsearch-java | n/a — relations are paths inside the same document |

Prisma names a relation but is class 1: it has no `@Where` equivalent, so nothing store-side reaches
the nested `some`/`every`/`none`.

Class 1 adapters expose an **optional** relation predicate; declaring none emits exactly the old
filter. Class 2 adapters do **not** — re-declaring a filter the ORM already applies would apply it
twice, removing rows the PDP permits.

activerecord is class 1 by what reaches its subquery, but it **rejects** rather than reproduces. It is
handed an association *name*, so `reflect_on_association` exposes the scope, the target's
`default_scope`, the STI discriminator and the composite key directly. With the hazard visible,
caller-owned is not available, and a caller-supplied predicate would be a second home for the same
truth; each is a refusal naming the reflection that carries it.

The precedent for a contract with no policy action is `nullRepresentationOmitted`. If a hazard turns
out to be expressible as a plan shape — as the absent to-one parent was — move it into the policy
suite and classify it.

### Gotchas worth knowing up front

- **Do not trust a local pass that depends on gitignored generated state.** Convex's harness imports
  `convex/_generated/`, which exists only after `npx convex codegen` against a live backend; Prisma's
  generated clients are the same. Stale local artifacts can pass where CI fails.
- **Java harnesses read `../conformance/`**, so containerised runs must mount the repository root.
  See the recipe in `CLAUDE.md`.
- **Wire fixtures are not consumed by adversarial harnesses**, which plan against a live PDP. The
  `Conformance Corpus` workflow replans them against the pinned PDP and fails on drift. They *are*
  consumed by translator unit tests
  ([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md);
  `prisma/src/translator.test.ts` is the reference), which account for every fixture exactly once —
  so **adding a corpus action fails every adapter that has one** until its golden expectation is
  recorded.
- **A dialect the harness does not run is not covered.** Collation, LIKE metacharacters and parameter
  typing differ per dialect. ent and spring-data run three dialects each, and so do drizzle and prisma
  (SQLite, PostgreSQL, MySQL, chosen with `ADAPTER_TEST_DB`;
  [#320](https://github.com/cerbos/query-plan-adapters/issues/320) for PostgreSQL,
  [#340](https://github.com/cerbos/query-plan-adapters/issues/340) for MySQL); the other TypeScript
  harnesses are single-store. Those legs found four mechanisms SQLite could not see:
  - drizzle, `$1 IS NULL` over a bound constant — untypeable on PostgreSQL, a hard error rather than
    a redundancy (`cr-contains`, `like-underscore`, the five `cr-div-*` shapes). Fixed in the
    translator.
  - drizzle, a numeric constant typed from the column instead of the value — `aNumber >= 1.5` against
    an `integer`, `size(aString) > 4294967296` against `length()`'s `integer` (`double-threshold`,
    `p-double-frac`, `cr-size-frac-ge`, `size-huge-gt`, `size-huge-lt`, `cr-div-neg-zero`,
    `cr-div-other-column`). Read as SQL `numeric` rather than `float(53)` it is silent:
    `aNumber * 0.1 == 0.3` is exact in decimal and admits a row CEL's binary floating point denies.
    Fixed in the translator.
  - prisma, `like-backslash` — `\` is the default `LIKE` escape on PostgreSQL and MySQL and literal on
    SQLite. Prisma emits no `ESCAPE` clause, so it is `adapterUnsupported` and throws.
  - drizzle, `cast-string-double` — the cast *target*: `CAST(… AS TEXT)` is a syntax error on MySQL,
    which spells it `CHAR`, and `CHAR` on PostgreSQL is `character(1)`. With no known dialect there is
    no portable spelling, so `string()` is `adapterUnsupported` there; ent translates it because
    `WithDialect` tells its renderer the target.

  Collation cost, measured on the MySQL legs: under MySQL's default `utf8mb4_0900_ai_ci`, **61 of
  drizzle's 236** oracle-tested actions disagree with the PDP, and under Prisma's
  `utf8mb4_unicode_ci` **58 of prisma's 172** do, `cs-eq` among them. Both legs pin
  `utf8mb4_0900_bin` (not `utf8mb4_0900_as_cs`, which `h6` showed is not byte-exact — see
  "Case-sensitive is not byte-exact") and both READMEs state the requirement. Prisma's migration
  engine writes `COLLATE utf8mb4_unicode_ci` into every `CREATE TABLE`, ignoring the server default,
  so its tables must be converted after `db push`.

  The same applies to a hosted store replaced by a local build: convex runs against a pinned
  self-hosted `convex-backend`, never Convex Cloud, and most of its corpus is decided by the
  adapter's own JavaScript post-filter rather than a filter engine
  ([#327](https://github.com/cerbos/query-plan-adapters/issues/327)). Each adapter's README names the
  stores its contract is proved on and how much of the corpus each executes.

## Evaluation modes and the 0.55 baseline

The baseline is Cerbos **0.55.0**, under both `engine.strictEvaluation=false` (the PDP default) and
`true`. Every live adapter suite accepts `ADAPTER_TEST_STRICT_EVALUATION=false|true`, defaults to
`false`, rejects other values and sets the engine flag explicitly. CI runs both modes inside the
existing adversarial jobs, keeping the database/ORM dimensions and the baseline Node gate. Each run
compares against `check()` from the **same** PDP mode, never across modes.

`scripts/regenerate-wire-fixtures.sh` captures both modes independently — `wire-fixtures/` (default)
and `wire-fixtures-strict/` (strict) — and publishes neither until both succeed.
`validate-corpus.sh` checks action coverage, response identity, plan kinds and timestamp
normalization in each. Offline translator tests consume the default fixtures; live suites run both.
The two sets currently match; that is observed, not a reason to copy one over the other or to assume
their decisions agree. The classification ledger is shared because classifications currently agree
in both modes; a future difference must be measured and represented, not skipped.

Strict evaluation denies an affected action when a rule condition errors; variable errors affect
referencing actions, and derived-role errors affect rules using that role. The adapter corpus mostly
exercises single conditions, so [`evaluation-modes/`](evaluation-modes/README.md) defines **engine
contract probes** against dedicated resource kinds in the same `policies/` tree, run by
`scripts/check-evaluation-modes.sh`: a matching ALLOW beside an erroring DENY, missing attributes,
type errors, a referenced variable, a derived role, an unrelated action that must stay allowed, and a
valid-input control. Known principal inputs make these plans unconditional, so the probes assert
exact Check decisions and Plan kinds in both modes without an adapter or weakening the
non-degeneracy guards.

The 0.55 upgrade exposed three changes:

- **Invalid literal regexes fail compilation.** `regex-lookahead` uses a principal-selected pattern
  to keep the hostile plan; the engine probes pin the literal compile failure.
- **Compile-time non-finite arithmetic cannot be serialized in a plan.** The five NaN/infinity
  actions include `now() == now()`: Cerbos captures one timestamp per evaluation, so it is true, and
  an expression containing `now()` bypasses constant folding, keeping the original division subtrees
  and their adapter coverage. The engine probes require the unguarded plans to fail with their actual
  HTTP 500 diagnostics while checking their decisions; if upstream fixes serialization, those probes
  fail and prompt removing the workaround.
- **NaN ordering now yields false, including beneath negation.** The wire plan is unchanged; Check
  decisions changed. Adapters that fold these comparisons must preserve false under negation rather
  than treating NaN as an error. Missing attributes and SQL NULL keep their own error/unknown
  semantics. This is a consumer-visible change: updated adapters target 0.55 and must not claim
  unchanged 0.54 compatibility for these expressions.

Two actions protect the migration fixes. `not-ternary-parent` distinguishes an unselected missing
relation from a selected missing/null attribute under ternary negation; negating the whole relation
predicate can either deny the former or allow the latter. `not-nan-order-string` distinguishes a
finite-number/string type error from NaN/string ordering (false under CEL 0.30), and also catches a
SQL dialect inferring an all-NULL conditional as text where a boolean UNKNOWN is required. Both are
classified from live runs, with refusal messages where required, and sit in the non-degeneracy
guards.

The `p-has` divergence stays pinned in both modes. Review new SDK/renderer goldens only after
same-mode live oracle checks pass. A future PDP upgrade must review plan diffs **and** decision
changes: identical wire output does not establish semantic compatibility.

## Regenerating wire fixtures after a Cerbos version bump

```bash
# edit CERBOS_VERSION first, then resolve the digest the new tag points at:
#   docker buildx imagetools inspect ghcr.io/cerbos/cerbos:$(cat CERBOS_VERSION) \
#     --format '{{.Manifest.Digest}}' > CERBOS_IMAGE_DIGEST
./scripts/regenerate-wire-fixtures.sh
git diff -- wire-fixtures wire-fixtures-strict  # from conformance/: review both modes
./scripts/check-evaluation-modes.sh # Check/Plan error scoping and planner limitations
./scripts/validate-corpus.sh        # both fixture sets and every pin restatement
```

Requires `docker`, `curl`, and `jq`.
