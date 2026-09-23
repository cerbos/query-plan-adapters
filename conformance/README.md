# Conformance corpus

The shared adversarial corpus every adapter is proved against. Each adapter's harness plans against
a real PDP loaded with `policies/`, runs the translated query against its real store, and compares
the returned ids with per-row `check()` decisions. The PDP is the oracle for both sides, so there are
no hand-written expectations, and no adapter keeps its own copy of the policy, seeds or action list.

**The invariant: a shape an adapter cannot express must throw, never emit a filter.** A wrong filter
returns rows the PDP denies; a throw is a bug report.

The corpus exists because adapters re-derive the planner's wire contract by hand, and the same wrong
assumption has shipped to several adapters at once (value-first inversion in prisma and sqlalchemy,
#258, #259).

```bash
conformance/scripts/validate-corpus.sh           # corpus integrity, offline; runs in every adapter's CI
conformance/scripts/regenerate-wire-fixtures.sh  # after a policy edit or a PDP bump (Docker, curl, jq)
conformance/scripts/check-evaluation-modes.sh    # engine probes in both strictEvaluation modes (Docker)
# Each adapter's harness: npm run test:adversarial, pdm run test, go test ./..., ./gradlew test (see CLAUDE.md)
```

Common tasks:

- **Add or fix a shape** — [Adding a new hostile shape](#adding-a-new-hostile-shape), then
  [Golden expectations](#golden-expectations).
- **Onboard an adapter** — [Adding a new adapter](#adding-a-new-adapter).
- **Bump the PDP** — [Regenerating wire fixtures after a Cerbos version bump](#regenerating-wire-fixtures-after-a-cerbos-version-bump).
- **A harness fails a vacuity check** — [The degeneracy guard](#the-degeneracy-guard).

## Contents

- [Layout](#layout)
- [The oracle recipe](#the-oracle-recipe) — and the hazards the corpus probes
- [Adding a new hostile shape](#adding-a-new-hostile-shape)
- [Golden expectations](#golden-expectations)
- [Adding a new adapter](#adding-a-new-adapter)
- [Evaluation modes and the 0.55 baseline](#evaluation-modes-and-the-055-baseline)
- [Regenerating wire fixtures after a Cerbos version bump](#regenerating-wire-fixtures-after-a-cerbos-version-bump)

## Layout

| Path | What it is |
|---|---|
| `policies/adversarial.yaml` | The hostile policy suite. One resource kind (`adversarial`), one role (`USER`), one action per shape, one rule per action except the `compose-*` family (see "Rule composition"). `adversarial-compose-roles.yaml` holds that family's derived role. |
| `seeds.json` | Hostile seed rows (NULLs, empty strings and collections, negatives, LIKE metacharacters `% _ \`, unicode, near-duplicate names) and the fixed principal. Every key except `note` must be consumed by every harness. |
| `derived-fields.json` | Six attributes derived per seed (`createdBy`, `aDouble`, `createdAt`, `updatedAt`, `scope`, `labels`). Harnesses read it; never recompute. |
| `actions.json` | The classification ledger (below). |
| `wire-fixtures/`, `wire-fixtures-strict/` | One golden `PlanResources` response per action, captured from the pinned PDP in default and strict evaluation mode. Each set is captured independently, never copied. |
| `CERBOS_VERSION`, `CERBOS_IMAGE_DIGEST` | The PDP tag and the digest it resolves to. Everything composes `ghcr.io/cerbos/cerbos:$CERBOS_VERSION@$CERBOS_IMAGE_DIGEST`; never hardcode either. |
| `scripts/` | `validate-corpus.sh`, `regenerate-wire-fixtures.sh`, `check-evaluation-modes.sh`, `verify-cerbos-digest.sh`. |
| `evaluation-modes/` | Engine probes (see "Evaluation modes and the 0.55 baseline"). |

The per-adapter filters are **not** here — they live in each adapter's `golden/expectations.json`.

`actions.json` groups every action:

- `adapters` — the roster every per-adapter key is checked against.
- `conformance` — must match the `check()` oracle exactly.
- `adapterUnsupported` — per adapter, conformance actions its query language genuinely cannot express.
  The adapter must throw; the harness asserts the throw.
- `expectedUnsupported` — shapes the Spring reference adapter rejects. Every adapter must fail loudly
  unless listed in `adapterSupportedExpected`.
- `adapterSupportedExpected` — per-adapter exceptions that translate a reference-unsupported shape
  through a documented database capability.
- `nullRepresentationOmitted` — `== null` probes every adapter must reject (see "NULL conventions").
- `degenerateOracles` — actions whose oracle is empty or total by construction (see "The degeneracy
  guard").
- `knownDivergences` — upstream planner bugs, excluded from the oracle run. Currently only `p-has`.

`validate-corpus.sh` keeps these schemas closed: unknown keys, missing fields, `null` for an optional
`relatedIssue`, or an off-roster adapter all fail. When you move an action between groups, use the
destination group's fields.

## The oracle recipe

Each harness implements this against its own ORM:

1. **Seed** the store from `seeds.json` and `derived-fields.json`.
2. **Plan** each `conformance` action against a real PDP, translate, execute, and collect the ids.
3. **Oracle**: `check()` each seed row against the same PDP and action, with attributes mirroring
   the row exactly.
4. **Compare**: the two id sets must be equal. `expectedUnsupported` actions must throw (unless the
   adapter is in `adapterSupportedExpected`), and every throw must carry the pinned message (see
   "Pinned throw messages").

The sections below describe the hazards the corpus probes and what each adapter learned from them.

### Case sensitivity is two invariants, not one

CEL string comparison is exact. A store can get that right for one operator and wrong for another:

- **`cs-eq`** proves `=`, which collation governs. The collation must be byte-exact, not merely
  case-sensitive.
- **`cs-contains` / `cs-startswith` / `cs-endswith`** prove string matching, which collation does not
  govern everywhere: SQLite's `LIKE` is ASCII case-insensitive unless the connection sets
  `PRAGMA case_sensitive_like = ON`.

`c1` (`aString` "One") is the witness. A harness whose store needs configuring must configure it,
and the adapter's README must state the whole requirement. ent, sqlalchemy and prisma set the SQLite
pragma; drizzle lowers string matching to `REPLACE` and needs none.

#### Case-sensitive is not byte-exact

`h6` contains a SOFT HYPHEN (U+00AD): `aString` is `"o­ne"`, `aOptionalString` is `"s­et"`. CEL treats
them as different from `"one"` and `"set"`; any Unicode Collation Algorithm collation ignores the
hyphen. So MySQL's `utf8mb4_0900_as_cs` passes `c1` but fails `h6`, over-granting `cs-eq` and every
`in` over the principal's teams ([#474](https://github.com/cerbos/query-plan-adapters/issues/474)).
`utf8mb4_bin` is byte-exact but PAD SPACE. **Every MySQL leg pins `utf8mb4_0900_bin`** (8.0.17+),
the only collation matching CEL on case, accents, ignorables and trailing spaces. Measured on the
pinned `mysql:8.4`:

| probe | `_0900_ai_ci` | `_0900_as_cs` | `utf8mb4_bin` | `_0900_bin` |
|---|---|---|---|---|
| `'One' = 'one'` | TRUE | FALSE | FALSE | FALSE |
| `'o­ne' = 'one'` | TRUE | TRUE | FALSE | FALSE |
| `'one ' = 'one'` | FALSE | FALSE | TRUE | FALSE |
| `'o­ne' LIKE 'one'` | FALSE | FALSE | FALSE | FALSE |

Multi-byte strings are also why string length must count characters, not bytes: MySQL `length()`
counts bytes. `h7` (`"é-x-é"` with needle `"é"`) witnesses `size()`, `startsWith` and `endsWith`;
drizzle renders `char_length()` on MySQL.

### NULL conventions

By default a DB `NULL` (or a NULL tag name) becomes a **missing attribute** on the check side. CEL
then raises a missing-attribute error, which Cerbos treats as deny — matching SQL's three-valued
logic, where `NULL` is excluded from both a predicate and its negation.

Some attributes use the other convention and send an **explicit null**: `owner` aliases the
`aOptionalString` column, and `tagNames` projects `tags[].name` keeping NULL names as null elements.
The `in-null-elem-*` and `in-var-var*` probes pin this. Object-valued `tags` still omit a NULL `name`.
Every harness must implement both conventions exactly.

#### `nullRepresentationOmitted`: the two conventions are indistinguishable on the wire

The planner emits the same `eq(attr, null)` node under both conventions, but the oracles differ:

| action | convention | `check()` allows | a NULL-selecting filter returns |
|---|---|---|---|
| `null-eq` | explicit null | `a2 a4 a8 c2 e1` | the same 5 — aligned |
| `null-eq-missing` | omitted | **nothing** | those 5 — **over-grants** |

An adapter cannot recover the convention from the plan, so adapters that can emit a NULL-selecting
predicate take a `nullAttributeRepresentation` option (default `explicit`, #302). Each harness
translates this group with the option set to omitted, asserts the rejection, **and** asserts why the
rejection is needed, so it cannot pass by throwing for an unrelated reason:

- **prisma, drizzle, sqlalchemy, spring-data** — pin that the default translation returns the five
  denied rows.
- **mongoose, convex** — pin the empty result, plus that `owner` (same column, explicit convention)
  still returns its five rows.
- **langchain-chromadb, elasticsearch-java** — neither store distinguishes null from missing, so both
  conventions fail closed; the rejection is asserted as a tripwire.

These oracles are empty by construction, so the actions are in `degenerateOracles`.

#### The other side of the same option: an explicit null against a non-null constant

Under the explicit convention CEL holds a null *value*: `null != "x"` is TRUE. SQL answers UNKNOWN
and drops the row under both polarities, so SQL adapters under-granted (#308). Five actions pin it:
`null-value-ne-const`, `null-value-not-eq-const`, `null-value-not-in-const`, `null-value-f2f` and
`null-value-pv-not-exists`.

**The convention is declared per attribute**
([ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md)), because the
corpus — like real applications — maps one column both ways. The call-level
`nullAttributeRepresentation` is only the default; declaring nothing means "treat as NOT NULL". The
declaration affects only `eq`, `ne` and `in`; ordering and string operators error on a null receiver,
which denies exactly as UNKNOWN does. `coOwner` (explicit null, aliasing `scope`) exists so
`null-value-f2f` has a one-row oracle (`e1`).

| adapters | behaviour |
|---|---|
| prisma, drizzle, sqlalchemy, spring-data, ent, pgx | translate the declaration; all five compared |
| mongoose, convex | store the value sent, so a stored null already compares as CEL does |
| langchain-chromadb | refuses all five: no null in its metadata model |
| elasticsearch-java | takes the declaration in order to refuse — no Query DSL spelling of `!= "x"` handles a missing field correctly |

#### The absent to-one parent

`mainCategory` is a to-one parent. A seed with no `subCategoryNames` sends **no `mainCategory`**, so
`check()` denies. An adapter reaching it through a join chain sees an absent parent and a childless
parent the same way — an empty result. That agrees with the PDP for `exists` and `size() > 0`, but
**over-grants** for anything that turns "no rows" into true: `all`, negations, `size() == 0`,
`size() >= 0`, `size() <= 1.5`, and a ternary's false branch (#309).

The discriminating actions are `w1-all-chain`, `w1-not-exists-chain`, `w1-size-zero-chain`,
`w1-size-nonneg-chain`, `w1-not-in-chain`, `w1-not-hasint-chain`, `w1-not-size-chain`,
`w1-ternary-chain-cond` and `w1-size-frac-le-chain`. `w1-size-frac-chain` pins rounding (`>= 1.5` is
`>= 2`), not this hazard.

The rules the fixes established:

- **A chained collection must require its intermediate hop to exist**, so an absent parent stays
  excluded under both polarities.
- **Put the guard in the shared relation-scope construction**, not per operator. Guarding only the
  macros left membership, `hasIntersection` and `!(size > 0)` exposed (#315, #316). Adapters with no
  UNKNOWN (Prisma filters, Mongo documents) require the hop *outside* the negation.
- **Reuse one negation.** Prisma spelled the ternary's false branch as a second, unguarded `NOT` (#334).
- Mongoose's `$cond` treats a missing path as falsy — a latent hazard only a chained scalar can reach
  (see "The real to-one relation").
- A fractional `size()` *equality* is a CEL type error, but **`dyn()` reaches it**: the planner drops
  the wrapper, so `size(x) != dyn(1.5)` arrives as `ne(size(x), 1.5)`. An adapter that folds it must
  still guard it (#333). `size-frac-ne-not`/`size-frac-eq-not` carry the string-length half; the chain
  and `size(filter(...))` halves are unit-tested only, as corpus gaps (kind 3 in `CLAUDE.md`).

**A statically decided `size(string)` must stay UNKNOWN for a NULL column.** Out-of-int-range
thresholds and fractional equalities are decided for every *present* string, so adapters fold them.
A NULL column is a missing attribute that denies under both polarities, but a fold to `IS NOT NULL`
negates to `IS NULL` and a constant FALSE negates to TRUE — both readmit the NULL rows.
`size-huge-gt`/`-lt` cannot see this (`aString` is never NULL), so the six `size-*-not` actions ask
it over `aOptionalString`, OR-ed with `aNumber > 10` to keep the oracle non-degenerate: a2/a4/a8 are
the rows an unguarded fold readmits.

### Shapes that live only in a unit test

`CLAUDE.md` ("What a translator unit test may pin") admits three kinds of material into a per-adapter
unit test. This is the hand-kept registry of what each adapter parks there; the banners in each test
class are the source of truth.

**elasticsearch-java** (`ElasticsearchQueryPlanAdapterTest`; regex probes in `ElasticsearchSurfaceTest`):

- **Kind 1** (CEL cannot reach it; permanent). Each test quotes the checker error:
  - unknown operator `unsupported_op` — `undeclared reference to 'unsupported_op' (in container '')`;
  - `isSet` (#261) — `undeclared reference to 'isSet'` (existence is `null-ne` in the corpus);
  - a lambda body naming an unbound variable — `undeclared reference to 'x'`;
  - a macro over a scalar literal — `expression of type 'string' cannot be range of a comprehension
    (must be list, map, or dynamic)`;
  - a `timestamp()` literal outside RFC 3339 or CEL's range (e.g. `parsing time
    "2024-02-30T00:00:00Z": day out of range`), validated because it decides whether `term`/`range`
    is well-formed.
- **Kind 2** (caller-supplied; permanent): an unmapped reference, `OperatorFunction` overrides and the
  polarities they reach, the lowerings that borrow a shape without its override (hierarchy →
  `prefix`/`terms`, `^literal` → `prefix`), macros over an undeclared nested path, value-list macros
  needing no nested path, `size()` over a declared flat collection versus an undeclared field,
  `Options` immutability and its convenience overloads, and the three typed refusals
  (`UnsupportedPlanShapeException`, `UnmappedAttributeException`, `MalformedPlanException`).
- **Kind 3** (corpus gap; bridge tracked by [#509](https://github.com/cerbos/query-plan-adapters/issues/509),
  each test opens with *Corpus gap.*):
  - `anUnfoldableMacroOverAValueListIsRefusedByName` — direct boolean-root `filter`/`map` results;
  - `exceptIsRefusedByNameWhereverItAppears` — directly negated and nested-lambda positions;
  - `everySpellingOfNonEmptinessIsTheSameCheck` — flat-collection `size != 0` and its negation;
  - `hasIntersectionWithANullElementIsRefusedWhicheverPositionCarriesIt` — inside a nested lambda;
  - `aNonScalarLiteralWhereAScalarIsExpectedIsRefused` — raw protobuf structured values in ordering and
    string operations.

The numeric decoder's signed-long boundaries and non-finite values remain wire contracts, and the
regex surface tests remain mechanism tests against Lucene.

**spring-data** (`SpringDataQueryPlanAdapterTest`; every kind-3 test opens with *Corpus gap.*):

- **Kind 1.** `isSet`; type-checker rejections (a timestamp against a number); operand shapes the
  planner never emits (wrong arity, a bare string where `timestamp()` always wraps one, a third
  operand); and constant sub-expressions the planner folds (proved by
  `p-startswith-concat` and `in-empty`'s fixtures).
- **Kind 2.** `OperatorFunction` overrides on every scalar-leaf path, the macro-depth bound and its
  system-property fallback, call-level and per-attribute `NullAttributeRepresentation`,
  `OffsetDateTime`/`LocalDateTime` columns, unmapped references, the bulk-delete guard, the
  null-predicate contract with Spring Data, defensive copies, and (in `RefusalTypesTest`/`OptionsTest`)
  the typed refusals and `Options` immutability.
- **Kind 3.** Bridges tracked by [#509](https://github.com/cerbos/query-plan-adapters/issues/509):
  `size()` against arbitrary, fractional and out-of-int-range thresholds, and fractional `size()`
  equality over a collection, a chain and `size(filter(...))` (reachable via `dyn()`); empty-list
  intersection over a direct scalar, relation or map projection; value-first and relation structured
  comparisons;
  suffix and integral `add` solve forms; CEL primitive and minor-operator shapes; macro composition;
  further value-first operand orders; nested, negated and value-first ternary rewrites; SQL Server `[`
  escaping (no leg runs SQL Server); constant-receiver string matches; arithmetic as a comparison
  operand; constant NaN and infinity ordering; and uncovered `timestamp(field)` operator cells.

Error-message context, list cardinality and value redaction stay translator contracts.

**prisma** (`translator.test.ts`): **kind 1** — the nested-map boolean-body fallback. The PDP rejects
`R.attr.tags.all(t, R.attr.tags.map(x, x.name))` with `expected type 'bool' but found 'list(dyn)'`;
the test proves an inner map's nullable projection cannot leak into the outer lambda (#430).

### Regex, indexing, conversion and type probes

- **#414 port.** Wildcard needles, regex patterns beyond a literal prefix, `except` in three
  positions, empty-list macro identities, principal struct projections, variable shadowing and more.
  `h1`–`h4` distinguish literal wildcard escaping, RE2's newline rule, a literal brace, and `"0"`
  against numeric `0`. The principal-struct fixtures hold the `list`/`struct`/`set-field` expressions
  the planner actually emits; do not replace them with assumed protobuf value lists.
- **#396 probes.** `regex-final-newline` (`h5` = `"ab\n"`) distinguishes RE2's absolute-end `$`.
  `regex-lookahead` selects `a(?=b)` through the principal's `context` because 0.55 rejects it as a
  literal. `index-negative`, `index-fractional` and the `cast-not-*` actions check that an invalid
  access or failed conversion never becomes an allow under negation. `h5`'s `createdBy` is
  deliberately not a timestamp.
- **Unserialisable NaN.** `R.attr.aNumber / (0.0 / 0.0) > 0` makes `PlanResources` return HTTP 500,
  so it has no fixture; `div-by-division` covers the finite case.
- The type probes (`type-string-number` etc.) have empty oracles by design and are declared in
  `degenerateOracles`.

### Number and boolean list elements

`aNumberList` and `aBoolList` test whether an adapter keeps a JSON element's type. Most seeds hold
`[]`; eight rows discriminate:

| seed | `aNumberList` | `aBoolList` | what it witnesses |
| --- | --- | --- | --- |
| `a1` | `[2]` | `[true]` | the match, and the `aNumber == 5` branch below |
| `a3` | `[2, 3]` | `[false]` | a match with a longer list; false leading |
| `a4` | `[3, 2]` | `[null, true]` | the value at the wrong position; a null element |
| `a5` | `[-2]` | `[false, true]` | the wrong sign; false leading |
| `a6` | `[null, 2]` | `[]` | a null element is a value: `null == 2` is false, its negation true |
| `a7` | `[20]` | `[]` | the value a text comparison would take for a prefix |
| `b4` | `[1]` | `[true]` | 1 and true, which SQLite and MySQL both store as 1 |
| `c1` | `[0]` | `[true, false]` | a zero a NULL could be mistaken for |

Six actions read them: `index-number-list` and `index-bool-list`, their negations, and two cross-type
probes (`index-bool-list-vs-number`, `index-number-list-vs-bool`) that are false in CEL but match `b4`
and `c1` on a store comparing JSON as SQL. The cross-type probes add `|| aNumber == 5` so the oracle
is `a1`, not empty. An adapter with no positional list read refuses all six.

### Cross-type membership and comparison

CEL equality is heterogeneous: `"2" == 2` is false and `!=` is true. Stores that coerce one side
disagree — Elasticsearch, Mongoose, SQLite, H2 and MySQL all did, and PostgreSQL errors.

| action | condition | oracle |
| --- | --- | --- |
| `in-number-list` | `2 in aNumberList` | `a1 a3 a4 a6` — matching-type baseline |
| `in-number-list-vs-string` | `"2" in aNumberList \|\| aNumber == 5` | `a1` |
| `in-bool-list-vs-string` | `"true" in aBoolList \|\| aNumber == 5` | `a1` |
| `hasint-number-list-vs-string` | `hasIntersection(aNumberList, ["2", 3])` | `a3 a4` |
| `hasint-bool-list-vs-string` | `hasIntersection(aBoolList, ["true"]) \|\| aNumber == 5` | `a1` |
| `eq-number-vs-string` | `aNumber == "5" \|\| aNumber == 2` | `a3 a8`; coercion adds `a1` |
| `ne-number-vs-string` | `aNumber != "5" && aNumber > 3` | every `aNumber > 3`, `a1` included; coercion drops it |
| `eq-bool-vs-string` | `aBool == "true" \|\| aNumber == 5` | `a1`; coercion adds every true row |
| `eq-string-vs-number` | `aString == 0 \|\| aNumber == 5` | `a1`; `h4`'s `aString` is `"0"` |
| `in-scalar-number-vs-string` | `aNumber in ["5", 2]` | `a3 a8` |
| `exists-tag-name-vs-number` | `tags.exists(t, t.name == 0) \|\| aNumber == 5` | `a1` |
| `hasint-map-vs-number` | `hasIntersection(tags.map(t, t.name), ["public", 0])` | rows with a `public` tag |
| `null-in-number-list` | `null in aNumberList` | `a6` |
| `not-null-in-number-list` | `!(null in aNumberList)` | every row but `a6` |

A correct translation answers as CEL does (drop the mistyped member, answer false, keep `!=` true) or
refuses with a pinned message.

### The real to-one relation

The corpus has exactly one **real** to-one join: `parent`, and `parent.inner` one hop further. It
looks like `obj.inner`, which is not a join — every harness maps `obj.inner` to the row's own
`aString`. See [ADR 0005](../docs/adr/0005-the-conformance-corpus-carries-a-real-to-one-relation.md).

- **`parentSeedId`** names the seed whose `aBool`, `aNumber`, `aString` and `aOptionalString` a row's
  `parent` carries; that seed's own `parentSeedId` gives `parent.inner`. The chain stops there. `null`
  means no parent. **Do not write the nested object into `seeds.json`** — harnesses build it.
- **The parent is a copy, not a pointer.** Each harness creates fresh parent and inner rows per
  resource, so a filter returning the parent instead of the child cannot agree by accident.
- **An absent level sends no attribute**, so `check()` denies.

| store | materialised as |
|---|---|
| prisma, drizzle, sqlalchemy, spring-data, ent, pgx | two owned tables, unique foreign key per level |
| mongoose, convex, elasticsearch-java | two nested embedded objects |
| langchain-chromadb | flattened dotted keys (`parent.aString`, `parent.inner.aString`) |

An absent hop must be UNKNOWN, not false, because `NOT UNKNOWN` is still UNKNOWN (the 15 `rel-*`
actions):

| adapter | how the hop is reached | what makes an absent hop UNKNOWN |
|---|---|---|
| prisma | `is:` on an optional relation | the hop required as a conjunct outside the negation |
| drizzle | correlated `EXISTS` | `CASE WHEN <hop exists> THEN … END`, no `ELSE` |
| sqlalchemy, ent, pgx | correlated scalar subquery | no row is already SQL NULL |
| spring-data | `LEFT JOIN` | an unmatched join is SQL NULL |
| mongoose, convex, elasticsearch-java | nested object path | the path is absent |
| langchain-chromadb | dotted keys | nothing — a missing key matches `$ne`, so negated shapes fail closed |

An inner join is wrong: it drops the row from the whole query, which shows under a disjunction
(`rel-hop2-or-exists`, the one under-grant in the group). Every harness also reads both hops back from
its store and compares them with the corpus; `validate-corpus.sh` checks every `parentSeedId`
resolves, none is self-referential, and all three depths occur.

### The primary key as a filterable attribute

The six `id-*` actions filter on `request.resource.id`, which arrives as a `variable` named
`request.resource.id` — not `request.resource.attr.id`. An adapter that resolves names by stripping
`request.resource.attr.` treats it as unmapped.

- **A store whose key is not a queryable field must mirror it into one.** ChromaDB filters metadata
  only and Elasticsearch addresses `_id` with the `ids` query, so both harnesses index the id as an
  ordinary field (#326).
- **One key mapping cannot be two types.** mongoose maps the key to a string field; the ObjectId
  `valueParser` is pinned in its unit test instead.

`f1` is the witness (its `aString` equals its id).

### Casts and concatenation are store-dependent in opposite directions

- **`cast-string-double`** agrees on every store the corpus runs (all render shortest round-trip).
- **`cast-string-bool`** does not: SQLite and MySQL store booleans as 1/0, so `CAST` yields `"1"`
  ([#418](https://github.com/cerbos/query-plan-adapters/issues/418)).
  - activerecord, sqlalchemy, ent, pgx and drizzle emit
    `CASE WHEN col IS NULL THEN NULL WHEN col THEN 'true' ELSE 'false' END`.
  - spring-data compares the constant in Java (`"true"` → `col = true`; anything else matches nothing).
  - mongoose and convex lower it directly; prisma, langchain-chromadb and elasticsearch-java refuse it.
  - On MySQL the `CASE` literals compare in the connection collation, so drizzle adds
    `COLLATE utf8mb4_0900_bin` and ent keeps a binary `CAST`. Neither this nor the `IS NULL` arm is
    oracle-proved yet ([#469](https://github.com/cerbos/query-plan-adapters/issues/469)).
- **`id-concat`**: string `+` sent to SQL `+` errors on PostgreSQL, under-grants on SQLite and
  over-grants on MySQL (18 of 21 seeds). Adapters that know their dialect render `||` or `CONCAT`;
  the rest refuse.

#### A constant is what tells the two `+` overloads apart

`concat-f2f` (`aString + aOptionalString`) has no literal, and a plan carries no types, so two
variables cannot tell concatenation from arithmetic. Guessing arithmetic over-grants on MySQL (16 of
21 seeds). Adapters split four ways:

- **convex, sqlalchemy** need nothing (JavaScript semantics; SQLAlchemy's column type picks the operator).
- **ent, pgx** need `ValueType: ValueString` on the mapper entry; undeclared, they fail closed.
- **prisma, drizzle, langchain-chromadb, elasticsearch-java, spring-data** refuse through existing
  limitations.
- **mongoose** refuses explicitly (it used to send `$add`).

There is no `ValueNumber`: every numeric `add` the planner emits has a constant operand (#319).

### Root position and bare operand forms

`not-lt`, `not-gt`, `gt-bare`, `le-bare`, `root-bare-bool`, `root-or`, `or-eq-exists` and `or-eq-in`
pin node **positions** that no other action reached directly (#388). They caught sqlalchemy refusing a
bare boolean column at the root. `root-or` uses `aNumber < 0` because the obvious spelling gave a
total oracle.

### Hazard classes the corpus missed

These actions pin places where CEL and a store's query language are known to disagree (#387):

- **`not-and`** — De Morgan over `and`.
- **`not-contains` / `not-startswith`** — a negated LIKE against a column needle: a NULL needle must
  be UNKNOWN, not FALSE, or `NOT` leaks the row. This caught a spring-data over-grant.
- **`arith-mod`** — `% 2 == 1` so the `-5` seed separates truncated from floored modulo.
- **`index-scalar-list`** and companions — a bare index, and an invalid position versus a null element.
- **`map-eq-list`** — a projection compared to a literal list.
- **`vf-lt` / `vf-size` / `vf-hasint`** — the rest of the value-first family (#258/#259).
- **`pv-exists-unrolled` / `pv-all-unrolled`** — `fewTeams` (3 elements) makes the planner unroll into
  an or/and chain; `manyTeams` (11) gives the value-list form. `pv-in` / `pv-in-unrolled` (#411) pin
  direct membership at both sizes.
- **`filter-as-conjunct`** — a `filter()` below the root, past root-only guards. Its oracle is empty
  by construction; the anti-vacuity assertion pins that dropping the untranslatable half would return
  the 14 `aBool` rows.
- **`projection-exists-eq` / `-not-eq`** and **`rel-not-*-hop`** (#430) — explicit null versus missing
  inside a projection lambda, and negated scalar predicates through the to-one parent.

### A macro nested over the same collection

`nest-same-exists`, `nest-same-not-exists` and `nest-same-all`
([#509](https://github.com/cerbos/query-plan-adapters/issues/509)) nest a macro over the collection
the enclosing macro iterates, reading the outer element:
`R.attr.tags.exists(t, R.attr.tags.exists(u, u.name != t.name))`. One table name, alias or scope for
both subqueries compares each tag with itself: `a6` (two names) is dropped by the positive form and
returned by the negated one; `a3` (one name, two ids) is dropped by `nest-same-all`, which also pins
that the id is correlated, not only the name. `b6` (a NULL-name tag) stays denied under every
polarity. The family found drizzle (bare table name, every store) and spring-data (Hibernate 7
gives a correlated root's joins the outer element's navigable path) comparing a tag with itself.

### Rule composition

Real policies compose rules, and the planner builds the root: a conditional DENY beside an ALLOW plans
to `and(not(Y), X)`, several ALLOWs to `or(...)`, a derived role to a conjunction. The nine `compose-*`
actions make the planner assemble these roots ([#487](https://github.com/cerbos/query-plan-adapters/issues/487)):

| action | rules | pinned plan root |
|---|---|---|
| `compose-allow-deny` | ALLOW + DENY | `and(not(gt), eq)` |
| `compose-multi-allow` | two ALLOW `all`s | `or(and, and)` |
| `compose-multi-allow-deny` | two ALLOW `all`s + DENY | `and(not(gt), or(and, and))` |
| `compose-or-not` | ALLOW + ALLOW `none` | `or(gt, not(eq))` |
| `compose-deny-only` | unconditional ALLOW + DENY | `not(lt)` |
| `compose-two-deny` | ALLOW + two DENYs | `and(not(or(eq, gt)), ge)` |
| `compose-derived-role` | ALLOW on a derived role with a resource condition | `and(lt, eq)` |
| `compose-derived-deny` | ALLOW + unconditional DENY on that derived role | `and(not(eq), lt)` |
| `compose-variable` | ALLOW + DENY, each through a policy variable | `and(not(gt), in)` |

- They share the `adversarial` resource kind. `validate-corpus.sh` rejects a repeated action outside
  `compose-`.
- **Every DENY is a plain comparison on a never-NULL column.** A DENY over a NULL-bearing column makes
  default-mode `check()` allow rows that no filter faithful to the plan can return (strict mode
  agrees with the plan). That is a planner/check disagreement, tracked separately.
- The derived role `compose_flagged` reads the resource (`R.attr.aBool == true`), so it survives into
  the plan.

### The degeneracy guard

A comparison against a trivial oracle passes vacuously — a total oracle catches nothing, and a
harness whose PDP silently failed would pass against deny-everything. So **every harness asserts,
for every action it compares, that the oracle is neither empty nor the full seed set**
([#490](https://github.com/cerbos/query-plan-adapters/issues/490)).

**`degenerateOracles` in `actions.json` is the only exemption.** Each entry has `"oracle": "empty"` or
`"total"` and a reason (a type error, a planner fold, an IEEE identity, no witness seed). Every harness
asserts every entry is *exactly* as degenerate as declared, whether or not it compares the action.
`validate-corpus.sh` rejects entries for unclassified, `knownDivergences` or duplicate actions.
Declare an entry only after watching the oracle in both evaluation modes — a degenerate oracle is
usually a missing discriminating seed, so fix the seed first.

**Liveness-only probes** are shapes an adapter refuses, kept because its group has no compared member
there. Each is asserted not to be compared and not to be in `degenerateOracles`. **Derive the list per
adapter; never copy another harness's** (#324).

### Pinned throw messages

A bare "it threw" is satisfied by a mapper typo or transport error (#326). So every throwing
classification pins a substring the adapter's error must contain:

- `adapterUnsupported[<adapter>][].message`
- `expectedUnsupported[].messages[<adapter>]` — keys exactly the roster minus `adapterSupportedExpected`
- `nullRepresentationOmitted[].messages[<adapter>]` — keys exactly the roster

`validate-corpus.sh` checks the key sets, and every harness fails the run if a message is missing. The
assertion is `contains`, since some messages carry runtime values. One message may cover many actions
(Chroma answers most refusals with "Nested expressions are not supported by ChromaDB filters").

**The message and the `reason` must name the same mechanism.** When they disagree, find which
limitation fires first and fix the `reason`; do not loosen the pin.

### Known divergences still need a tripwire

`knownDivergences` actions are excluded from the oracle run, so every harness pins `p-has` directly:
the plan folds to `KIND_ALWAYS_ALLOWED`, the oracle is non-empty and non-total, and the adapter returns
every row. When upstream fixes the fold that assertion fails — move the action back into the oracle
run.

### Deterministic derived fields

Six attributes are derived per seed. **The values live in `derived-fields.json`, one entry per seed
id, and every harness reads them from there** — hand copies hid errors because the same copy feeds
both sides (#318). `validate-corpus.sh` re-derives `createdBy`, `aDouble` and `createdAt` from the
rules below and diffs `scope` and `labels` against its own copy of the tables.

- `createdBy`: `h5 = "not-a-timestamp"`; otherwise
  `aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z"`.
- `aDouble`: `a1 = -0.6`, `a2 = 0.25`, `a3 = NULL`/missing, `g1 = -9.5e18` (the int64-saturation
  witness for `double-huge-lt`/`double-huge-gt`), otherwise `aNumber + 0.3`.
- `createdAt`: `a1 = 2020-03-15T10:30:00Z`, `a2 = 2037-01-01T00:00:00Z`, `a3 = NULL`/missing,
  `a4 = 2024-06-01T00:00:00Z`, `a5 = 2020-03-15T10:30:00.123456Z`; otherwise use
  `2036-06-06T06:06:06Z` when `aNumber >= 2`, or `2021-05-05T05:05:05Z`.
- `updatedAt`: `a1 = 2020-03-15T10:30:00.000Z`, `a4 = 2024-06-01T00:00:00Z`; otherwise
  NULL/missing. `a1` equals `createdAt` as an instant but not as a string; keep the original strings,
  since re-serialising erases the witness.
- third-level `labels[].name`: `a1 = ["gold", "silver"]`, `a6 = [missing, "silver"]`,
  `a8 = ["silver"]`, `c1 = ["Gold"]`, otherwise empty.
- `scope`: `a1=dept`, `a2=dept.eng`, `a3=dept.eng.platform`,
  `a4=dept.eng.platform.obs`, `a5=dept.engineering`, `a6=dept.sales`, `a7=NULL`,
  `a8=""`, `a9=50%`, `b1=50%:a_b:x`, `b2=50x:a_b:y`, `b3=50%:aXb:y`,
  `b4=50%:a_b`, `b5=dept.eng.platform2`, `b6=50%.a_b`, `c1=Dept.Eng`,
  `c2=dept.eng.`, `d1=[env]:prod:eu`, `d2=e:prod:eu`; all other seeds use NULL.

### Seed, principal and derived-field coverage

A field a harness does not consume drops out of the stored row **and** the oracle at once, so the
comparison still agrees and the field tests nothing. Every harness therefore declares the exact keys
it consumes and asserts set equality, in both directions, against:

- the `seeds.json` row keys (except `note`) and the `tags[]` element keys;
- `derived-fields.json`'s `fields` list and every entry's keys;
- the principal: `{id, roles, attr}`, the attribute names inside `attr`, and their value shapes
  (string scalars, numeric `zero`, string lists, and the 11-element `manyStructs`, `nullableStructs`
  and `missingStructs`).

Harnesses that rebuild seeds field by field (mongoose, langchain-chromadb) assert against the raw
JSON. Pass corpus data through verbatim; adding a seed field or principal attribute must fail every
harness until it is declared.

## Adding a new hostile shape

Any change to how a shape is translated starts here (see `CLAUDE.md`, "Changing how a condition is
translated").

1. Add the action to `policies/adversarial.yaml`, with a comment saying what it probes and which
   seeds discriminate it.
2. Add it to `actions.json` — `conformance`, `expectedUnsupported`, or `nullRepresentationOmitted`.
3. If it needs new seed data, add a seed with a `note` and its `derived-fields.json` entry in the same
   commit. A new seed *field* or principal attribute must also be added to every harness's declared
   key set.
4. Run `scripts/regenerate-wire-fixtures.sh` and confirm the diff adds only the new action.
5. Run every adapter's harness and triage each divergence into a fix, an `adapterUnsupported` entry
   or a `knownDivergences` entry — never a special case in a harness. Pin the message the adapter
   actually raises.
6. Bump the tripwires: every harness pins corpus size and throwing count; convex, langchain-chromadb
   and elasticsearch-java also pin oracle counts, and convex pins which actions its filter engine
   decides alone ([#327](https://github.com/cerbos/query-plan-adapters/issues/327)).
7. Confirm it cannot pass vacuously. Compared actions are swept automatically. Where an adapter throws
   and the group has no compared member, add a liveness-only entry. If the oracle is empty or total,
   add a seed; use `degenerateOracles` only for degeneracy by construction. A
   `nullRepresentationOmitted` action also needs the "why" assertion and a message per adapter.
8. Regenerate every adapter's golden expectations and read the added entry.
9. Update the affected adapters' README `Conformance contract` tables in the same commit.

## Golden expectations

A **golden expectation** is the filter one adapter is pinned to emit for one corpus action — the core
assertion of its translator unit test, which reads plans from `wire-fixtures/` and needs no PDP or
store ([ADR 0006](../docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)).

**Expectations never live under `conformance/`**, because a change there re-runs every adapter's CI
([ADR 0007](../docs/adr/0007-adapters-share-data-not-code.md)). Only the format is defined here.

### The file

One file per adapter, at `<adapter>/golden/expectations.json`:

```jsonc
{
  "adapter": "drizzle",
  "regenerate": "npm run golden:update",
  // Optional, only where the value needs it — see "When the generator is an input".
  "sqlalchemy": "2.x",
  "expectations": {
    "<action>": { "note": "optional, human, preserved across regeneration", /* … */ }
  }
}
```

- **`adapter`** is checked by the loader, so a copied file cannot pass against the wrong translator.
- **`regenerate`** is the command that rewrites the file.
- **`expectations`** is keyed by action and sorted (asserted).
- **`note`** is never compared and survives regeneration.

The *value* schema is the adapter's own, documented in its README. It is whatever the translator
observably produced: convex records the calls its function makes against a recording query builder
and which half answers the query. The value must round-trip as JSON (normalise anything else and pin
it in code), and it is a filter, never a row set.

### When the generator is an input

If the recorded value passes through something whose version can change the bytes, the file declares
that version — otherwise a toolchain change reads as a translation change. sqlalchemy declares
`"sqlalchemy": "2.x"` (compiled SQL per dialect) and spring-data `"hibernate": "6.6"` (the rendered
JPA `Specification`; `hibernate-core` is `compileOnly`, so consumers bring their own). Such a key means:

1. the loader checks it, like `adapter`;
2. regeneration refuses under any other version;
3. the other version's CI leg asserts a pinned divergence list in both directions.

Add a key only when the difference is outside the adapter and inside the recorded value. A dialect is
part of the value; a Node version changes nothing.

### Rules for the file

- **A throwing action carries no entry** — its message is already in `actions.json`, and the unit
  test asserts it from there.
- **Completeness:** golden keys ∪ throwing actions == `wire-fixtures/*.json`, with no overlap. Add
  per-group count tripwires beside it.
- **CI never regenerates.** A translator change fails CI until someone regenerates, and the diff is
  the review. Regeneration will happily record a wrong filter, so keep the *rules* as assertions too.
- **Loaders are not shared** (ADR 0007); each adapter writes its own idiomatically.

## Adding a new adapter

Classification is an *output* of the harness: declaring an action unsupported before watching it fail
is how a translatable shape gets permanently skipped.

1. **Implement translation.** Spring Data is the reference; its behaviour decides ambiguous shapes
   and `conformance` versus `expectedUnsupported`.
2. **Write the differential harness** from the oracle recipe. Derive the classification from
   `actions.json` at runtime (the adapter's key is its directory name):

   ```
   oracleActions   = conformance - adapterUnsupported[me] + adapterSupportedExpected[me]
   throwingActions = adapterUnsupported[me] + (expectedUnsupported - adapterSupportedExpected[me])
   nullOmitted     = nullRepresentationOmitted            (translated with the option flipped)
   skipped         = knownDivergences where adapters contains me
   ```

   Resolve each throwing message while deriving and fail if one is missing. Parse every group
   explicitly so none vanishes from the counts. `drizzle/src/adversarial.test.ts` is the cleanest
   example.
3. **Persist the seeds exactly**, including both NULL conventions, read `derived-fields.json`, and add
   the key-set guards (see "Seed, principal and derived-field coverage").
4. **Add the degeneracy guard**, the `degenerateOracles` assertions, a liveness-only list derived from
   this adapter's own refusals, the corpus-size pin, and the `p-has` tripwire.
5. **Run it and let it fail.** Triage each divergence into a translation bug (fix it), a genuinely
   inexpressible shape (`adapterUnsupported` with a reason naming the mechanism — "emits LIKE without
   an ESCAPE clause", not "cannot express faithfully" — and a throw), or an upstream planner bug
   (`knownDivergences`). Never degrade one operator into a weaker one to pass.
6. **Register in `actions.json`**: add the adapter to `adapters` and give every `expectedUnsupported`
   entry it does not promote a `messages` key.
7. **Write the example application** against the packed artifact
   ([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)); `validate-demo.sh` fails for
   a rostered adapter without `<adapter>/example/run.sh`. There is no opt-out
   ([ADR 0001](../docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)). Read
   [demo/README.md](../demo/README.md), "What an example must do", first.
8. **Wire CI** by copying an adapter workflow. It must read the PDP pin from the two corpus files,
   run `validate-corpus.sh`, trigger on `conformance/**` and `demo/**`, run the adversarial suite in
   the same job as the unit tests, pin every service image (below), and run the example job in this
   adapter's own workflow so a Renovate ORM bump is blocked when it breaks the example.
9. **Document the contract** in the adapter's README: a `Conformance contract` table and a
   `Mapping hazards` table (below). The README must stand alone.

### Pinning service images

Every container a test or workflow starts is written `repository:tag@sha256:<64 hex>` — tags get
re-pushed. `validate-corpus.sh` enforces:

- **The PDP**: every restatement must match both `CERBOS_VERSION` and `CERBOS_IMAGE_DIGEST`
  ([#322](https://github.com/cerbos/query-plan-adapters/issues/322)).
- **Every other image** is pinned per harness in one constant that adapter's suites share — **not**
  under `conformance/`, which would re-run every adapter's CI. `validate-corpus.sh` lists the image
  repositories it scans, requires tag and digest on each occurrence, and requires one digest per
  `repo:tag` repository-wide. **Adding a new service means adding its repository to that list.**
- Markdown is not scanned.

Renovate proposes bumps only for the `*_IMAGE` files (automerging non-major); Docker managers are off
otherwise.

### Vendored code stays byte-identical

ent and pgx each vendor the translator under `internal/queryplan`. `validate-corpus.sh` fails on any
difference between the two trees
([#319](https://github.com/cerbos/query-plan-adapters/issues/319)); per-engine code goes in each
module's `render.go`. Keep `ent/translate_test.go` and `pgx/translate_test.go` in step.

### Mapping hazards: the rows the subquery sees

The corpus proves the plan side. The mapping side has one rule no policy can express:

> **The rows an adapter's subquery sees must equal the rows the application put into the resource
> attributes.**

Each hazard below breaks it, and each was a real over-grant (#314):

| Hazard | What goes wrong | Where it shows up |
|---|---|---|
| A **filtered association** | the application's association applies a predicate the subquery does not | ActiveRecord `has_many …, -> { where(visible: true) }`; SQLAlchemy `relationship(primaryjoin=…)`; Hibernate `@Where`/`@Filter`; a Prisma extension injecting `where` |
| A **default scope on the target model** | the subquery skips a scope every application read applies | ActiveRecord `default_scope`; Hibernate `@Where` on the entity; a soft-delete filter |
| **Subtype discrimination** | the bare table holds other subtypes too | ActiveRecord STI; JPA `@DiscriminatorValue`; a Mongoose discriminator (only for the model handed to `find()`) |
| A **to-one relation used as a collection** | the application sees one row; the subquery examines all | ActiveRecord `has_one`; any unindexed FK back-reference |
| A **composite association key** | a multi-column key becomes one identifier and fails, or joins the wrong column | ActiveRecord 7.1+ composite keys; any two-column FK |
| An **absent to-one parent** | expressible — `w1-all-chain` and friends pin it | every relational adapter |

For each hazard an adapter must choose, explicitly:

- **Reproduced** — the mapping carries the store-side predicate (class 1 adapters take an optional
  relation predicate).
- **Rejected** — the adapter refuses the mapping, or its mapper type cannot express it (e.g. a single
  source column makes a composite key a compile error).
- **Declared caller-owned** — only where the adapter cannot see the hazard; the README names the exact
  ORM feature to check.
- **Not applicable** — only where it structurally cannot arise, backed by a test.

A best-effort subquery is forbidden. The README lists all six hazards in this order as
`| Hazard | Position | Mechanism to check |`, with store-specific extras after them (e.g.
elasticsearch-java's analyzed `text` fields).

| Class | Adapters | What the store applies to the subquery |
|---|---|---|
| **1 — bare-table subquery** | drizzle, ent, pgx, prisma, activerecord | nothing |
| **2 — ORM-association subquery** | spring-data, sqlalchemy | Hibernate applies `@SQLRestriction`/`@Where` and the single-table discriminator; SQLAlchemy applies `primaryjoin` and the discriminator only through a mapped `relationship()` |
| **3 — no subquery** | mongoose, convex, langchain-chromadb, elasticsearch-java | n/a — relations are paths in the document |

Class 1 adapters expose an optional relation predicate; class 2 adapters must not, or the filter is
applied twice. activerecord is class 1 but **rejects** instead: it reflects on the association and can
see every hazard directly. If a hazard turns out to be expressible as a plan shape, move it into the
policy suite.

### Gotchas worth knowing up front

- **A local pass can depend on stale gitignored state** — convex's `_generated/` and Prisma's
  generated clients.
- **Java harnesses read `../conformance/`**, so containerised runs must mount the repository root.
- **Adversarial harnesses plan live; translator unit tests read the wire fixtures.** Adding a corpus
  action fails every adapter with a unit test until its golden expectation is recorded.
- **A dialect the harness does not run is not covered.** drizzle and prisma run SQLite, PostgreSQL and
  MySQL (`ADAPTER_TEST_DB`; [#320](https://github.com/cerbos/query-plan-adapters/issues/320),
  [#340](https://github.com/cerbos/query-plan-adapters/issues/340)), as do ent and spring-data. Those
  legs found untypeable `$1 IS NULL` on PostgreSQL, numeric constants typed from the column, `\` as the
  default `LIKE` escape (prisma refuses `like-backslash`), and `CAST … AS TEXT` being invalid on MySQL.
- **Collation matters.** Under MySQL's default collation 61 of drizzle's 236 compared actions
  disagree with the PDP, and 58 of prisma's 172 under Prisma's `utf8mb4_unicode_ci`. Both pin
  `utf8mb4_0900_bin` (see "Case-sensitive is not byte-exact"); Prisma's tables must be converted after
  `db push` because its migration engine writes its own collation.
- **convex runs against a pinned self-hosted backend**, never Convex Cloud, and most of its corpus is
  decided by its JavaScript post-filter (#327).

## Evaluation modes and the 0.55 baseline

The baseline is Cerbos **0.55.0**, under both `engine.strictEvaluation=false` (the default) and
`true`.

- Every live adapter suite takes `ADAPTER_TEST_STRICT_EVALUATION=false|true` (default `false`) and
  compares against `check()` from the same mode, never across modes. CI runs both.
- `regenerate-wire-fixtures.sh` captures `wire-fixtures/` (default) and `wire-fixtures-strict/`
  independently. Unit tests use the default set. The two currently match — that is observed, not a
  reason to copy one over the other. A future difference in classification must be measured and
  represented.
- [`evaluation-modes/`](evaluation-modes/README.md) holds engine probes for how errors scope under each
  mode, run by `scripts/check-evaluation-modes.sh`.

What 0.55 changed:

- **Invalid literal regexes fail compilation.** `regex-lookahead` uses a principal-selected pattern.
- **Non-finite arithmetic folded at compile time cannot be serialised in a plan.** The five NaN and
  infinity actions add a `now() == now()` guard, which blocks constant folding. The engine probes
  assert the unguarded plans still fail; when upstream fixes serialisation they will fail and prompt
  removing the workaround.
- **NaN ordering now yields false, including under negation.** Adapters that fold these comparisons
  must keep false under negation. This is a consumer-visible change: updated adapters target 0.55.

`not-ternary-parent` and `not-nan-order-string` guard the migration fixes. `not-nan-ord-le`'s false arm
(`0.5 <= NaN`) is allowed under negation in 0.55 but was denied in 0.54.

On a future PDP upgrade, review decision changes as well as plan diffs: identical wire output does not
mean identical semantics.

## Regenerating wire fixtures after a Cerbos version bump

Requires `docker`, `curl` and `jq`. From `conformance/`:

```bash
# edit CERBOS_VERSION first, then resolve the digest the new tag points at:
#   docker buildx imagetools inspect ghcr.io/cerbos/cerbos:$(cat CERBOS_VERSION) \
#     --format '{{.Manifest.Digest}}' > CERBOS_IMAGE_DIGEST
./scripts/regenerate-wire-fixtures.sh
git diff -- wire-fixtures wire-fixtures-strict  # review both modes
./scripts/check-evaluation-modes.sh             # Check/Plan error scoping and planner limitations
./scripts/validate-corpus.sh                    # both fixture sets and every pin restatement
```
