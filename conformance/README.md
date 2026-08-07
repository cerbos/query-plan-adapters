# Conformance corpus

Shared hostile-shape corpus for the adversarial differential harness pattern, extracted from the
spring-data adapter's `AdversarialConformanceTest` (see cerbos/query-plan-adapters#263). Every
adapter's differential test should consume this directory rather than maintaining its own copy of
the policy, seed data, or action list.

## Why this exists

Each adapter re-derives certain properties of the Cerbos planner's wire output by hand: operand
source-order preservation, directional-operator mirroring on value-first comparisons, `in`
normalization, receiver-sensitive string operators, three-valued logic under negation. A bug in
one of these assumptions has historically shipped identically to more than one adapter (the
value-first inversion in prisma and sqlalchemy: #258, #259) because nothing shared enforced the
rule. This corpus is that shared enforcement: one hostile policy suite, one set of hostile seed
rows, and one oracle recipe that every adapter's harness implements against its own ORM.

## Layout

- `policies/adversarial.yaml` — the hostile policy suite. One resource kind (`adversarial`), one
  role (`USER`), one action per hostile shape. Pure Cerbos policy YAML — no adapter-specific
  content. Edit this file to add a new hostile shape; it is the corpus of record.
- `seeds.json` — the hostile seed rows (NULLs, empty strings/collections, negatives, LIKE
  metacharacters `% _ \`, unicode, duplicate/mirrored names) plus the fixed principal used
  throughout. This is the single source of truth an adapter's harness persists into its own
  schema (SQL rows, Prisma records, whatever) AND mirrors into check() oracle calls — see
  "The oracle recipe" below. Every key except `note` must be consumed by every harness; that is
  asserted, not assumed (see "Deterministic derived fields").
- `derived-fields.json` — the five attributes derived from each seed (`createdBy`, `aDouble`,
  `createdAt`, `scope`, `labels`), materialised once per seed id. Every harness reads this file
  instead of restating the rules; `scripts/validate-corpus.sh` re-derives the rule-based fields
  from `seeds.json` and fails on drift. See "Deterministic derived fields" below.
- `actions.json` — every action in `policies/adversarial.yaml`, grouped into `adapters` (the
  canonical adapter roster, which every other per-adapter key is checked against), `conformance`
  (must match the check() oracle exactly), `adapterUnsupported` (per-adapter lists of conformance
  actions that adapter's query language genuinely cannot express — LIKE-wildcard escaping,
  relation-count thresholds, cross-model column comparisons; the adapter must THROW for these,
  never emit a silently-wrong filter, and its harness asserts the throw instead of the oracle
  match), `expectedUnsupported` (planner shapes rejected by the Spring reference adapter; other
  adapters must also fail loudly unless listed in `adapterSupportedExpected`),
  `adapterSupportedExpected` (per-adapter exceptions that intentionally translate a
  reference-unsupported shape through a documented database capability), `nullRepresentationOmitted`
  (actions probing `== null` against an attribute the oracle OMITS for NULL columns; every adapter
  must translate these with its NULL representation set to omitted and reject them — see "NULL
  conventions" below), and `knownDivergences` (an action plus the affected adapters intentionally
  excluded from the oracle run, with a reason — currently only `p-has`, excluded because of a
  planner bug, not an adapter bug).
- `wire-fixtures/*.json` — one golden `PlanResources` response per action, captured against the
  pinned Cerbos version in `CERBOS_VERSION`. These pin planner *wire shape* independent of any
  adapter or database — a `diff` against a freshly-regenerated fixture after bumping
  `CERBOS_VERSION` shows exactly what the planner's output changed for a given hostile shape,
  which is a much smaller signal than "an adapter test failed."
- `CERBOS_VERSION` — the exact Cerbos PDP version the wire fixtures were captured against.
  Deliberately pinned rather than `latest`: a fixture diff should come from a deliberate version
  bump, not silently from whatever `latest` resolved to on a given day.
- `CERBOS_IMAGE_DIGEST` — the digest of the image that version's tag resolves to. The tag says
  which release; the digest says which build, and a tag can be re-pushed. Every harness and
  workflow composes `ghcr.io/cerbos/cerbos:$CERBOS_VERSION@$CERBOS_IMAGE_DIGEST` from the two
  files, and `scripts/validate-corpus.sh` asserts both halves everywhere either is restated.
- `scripts/regenerate-wire-fixtures.sh` — regenerates `wire-fixtures/` from a running (pinned)
  Cerbos container. Run it after bumping `CERBOS_VERSION`, review the diff, commit both together.

## The oracle recipe

The differential harness pattern (implemented per-adapter, since translation and query execution
are necessarily language/ORM-specific):

1. **Seed** the adapter's own schema from `seeds.json`, in whatever native shape the ORM needs
   (rows, documents, whatever `tags`/`subCategoryNames` map onto for that adapter).
2. **Plan**: call `PlanResources` against a real PDP for each `conformance` action in
   `actions.json`, translate the response through the adapter under test, execute the resulting
   native query, and collect the returned id set (`adapterFilteredIds`).
3. **Oracle**: for each seed row, call `check()` against the *same* PDP and action, with Cerbos
   attributes built to mirror that row exactly (`oracleAllowedIds`). No hand-computed
   expectations — the PDP is the oracle for both sides.
4. **Compare**: `adapterFilteredIds(action)` must equal `oracleAllowedIds(action)` for every
   `conformance` action. Translation must throw for every `expectedUnsupported` action unless the
   adapter is listed for that action in `adapterSupportedExpected`; declared exceptions must run
   through the same oracle comparison as conformance actions. A throw must carry the message the
   corpus pins for that adapter — see "Pinned throw messages" below.

### NULL conventions

A DB `NULL` (or a missing element field, e.g. a NULL tag name) must become a **missing attribute**
on the check side by default. CEL's `!=`/macro bodies raise a missing-attribute evaluation error,
which Cerbos treats as a deny — the same three-valued logic SQL applies when a `NULL` participates
in a comparison (`UNKNOWN`, excluded from both a predicate and its negation). In particular,
`NOT (NULL = x)` is still `UNKNOWN`, not `TRUE`.

The `in-null-elem-*` and `in-var-var*` probes deliberately exercise the other planner convention:
`owner` aliases the `aOptionalString` column but is sent as an **explicit null** when the column is
NULL, and `tagNames` is the scalar projection of `tags[].name` with NULL names retained as explicit
null list elements. This pins `null in [null]`, `null in tagNames`, and variable-in-variable
membership. Object-valued `tags` still omit a NULL `name`, so collection lambda bodies continue to
exercise missing-attribute errors. Each harness must implement both representations exactly.

#### `nullRepresentationOmitted`: the two conventions are indistinguishable on the wire

The planner emits the same `eq(attr, null)` node under both conventions — `null-eq` (against the
explicit-null `owner`) and `null-eq-missing` (against the default-convention `aOptionalString`)
have byte-identical wire fixtures apart from the variable name. Their oracles do not agree:

| action | attribute convention | `check()` allows | a NULL-selecting filter returns |
|---|---|---|---|
| `null-eq` | explicit null | `a2 a4 a8 c2 e1` | the same 5 — aligned |
| `null-eq-missing` | omitted | **nothing** | those 5 — **over-grants** |

Under the omitted convention CEL raises a missing-attribute error for every NULL row and compares
`"set" == null` false for every other, so `check()` denies all 20 seeds. An adapter cannot recover
the caller's convention from the plan, so it has to be told: every adapter that can emit a
NULL-selecting predicate takes a `nullAttributeRepresentation` option, defaulting to `explicit`
(the historical translation). See cerbos/query-plan-adapters#302.

`null-eq-missing` lives in its own `actions.json` group rather than in `conformance`, because a
rejected shape has no filter to compare against the oracle. Each harness translates the group's
actions with its adapter's representation set to omitted and asserts the rejection — and asserts
the *reason* the rejection is needed, so the test cannot pass by throwing for an unrelated cause.
What that second assertion looks like depends on where the adapter's NULL lives:

- **prisma, drizzle, sqlalchemy, spring-data** — a SQL `NULL` is a stored value, so the default
  translation genuinely returns the five rows the PDP denies. The harnesses pin that over-grant.
- **mongoose** — already discriminates per attribute: `nullable: true` on a mapper entry means "a
  stored null is a missing Cerbos attribute" and makes `eq(field, null)` contradictory. Its
  harness asserts the aligned empty result *and* that `owner` (same column, no `nullable`) still
  returns its five explicit-null documents, so the empty set is the flag talking.
- **convex** — a document store, so the seeded shape mirrors the convention directly: the harness
  omits the field entirely, and because `aOptionalString` is `nullable: true` in the mapper the
  adapter refuses the push-down and evaluates the predicate in its own JavaScript post-filter,
  where the absent path raises the same CEL missing-attribute error that made `check()` deny. Same
  paired assertion as mongoose — `owner` is the same seed field stored as an explicit null and
  returns its five documents through that same evaluator. Alignment here comes from the storage
  layout, not from the plan, and *not* from a Convex `q.eq(field, null)`: that code path never runs
  for either action (cerbos/query-plan-adapters#327).
- **langchain-chromadb, elasticsearch-java** — need no option at all: neither store can represent
  an explicit null distinguishably from a missing key, so every null-selecting direction already
  fails closed under both conventions. Their harnesses assert the rejection happens regardless,
  which is also the tripwire for a future null sentinel introducing a representation dependency.

Because the oracle for these actions is empty by construction, they must **not** join the
degeneracy guard below — that guard asserts a non-empty, non-total oracle, which is exactly what
this shape cannot have.

#### The absent to-one parent

The other representation mismatch the corpus pins is a *path* that is absent rather than a value
that is null. `mainCategory` is a to-one parent on the check side: a seed with no
`subCategoryNames` sends **no `mainCategory` attribute at all**, so CEL raises a missing-path
error and `check()` denies. An adapter reaches the same data through a join chain rooted at the
resource row, where an absent parent and a childless parent produce the same empty result set:

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

Only a universal, a negation, a zero/lower-bound count or a ternary's false-branch discriminates
them, which is why `w1-exists-chain`, `w1-size-chain` and `w1-in-chain` passed everywhere while the
bug was live (cerbos/query-plan-adapters#309). `w1-all-chain`, `w1-not-exists-chain`,
`w1-size-zero-chain`, `w1-size-nonneg-chain`, `w1-not-in-chain`, `w1-not-hasint-chain`,
`w1-not-size-chain`, `w1-ternary-chain-cond` and `w1-size-frac-le-chain` are the nine that
discriminate.

`w1-size-frac-chain` is the one `w1-*` action that does **not** probe this hazard, and it is worth
being clear about why it is still here: `>= 1.5` rounds up to `>= 2`, which a count of zero fails
whether or not the hop is required, so guarded and unguarded agree. It pins the other property of
a fractional threshold — that the adapter ROUNDS rather than truncates — over a chain, which
`cr-size-frac-ge` only pins over a direct relation. An adapter that truncates to `>= 1` returns
`a1/a6/a8/c1` against an empty oracle.

The fix is in the translation, not in the classification: **a chained collection must require its
intermediate hop to exist**, so an absent parent stays excluded under both polarities instead of
collapsing onto the empty-collection case. `w1-size-zero-chain`, `w1-not-size-chain` and
`w1-size-frac-chain` have empty oracles by construction (no seed holds a parent with zero children,
nor one with two or more) and therefore stay out of the degeneracy guard; `w1-all-chain`,
`w1-not-exists-chain`, `w1-size-nonneg-chain`, `w1-not-in-chain`, `w1-not-hasint-chain`,
`w1-ternary-chain-cond` and `w1-size-frac-le-chain` all have non-degenerate oracles and carry the
anti-vacuity assertion for the group.

**Put the guard in the shared relation-scope construction, not in each operator.** The #309 round
guarded the collection macros, which left every sibling operator reached through the same chain
unguarded — membership and `hasIntersection` (#315), and the negated count spelling `!(size > 0)`,
which takes a different branch from `size == 0` because the planner emits the negation verbatim
rather than normalising it (#316). ent and pgx needed no change in any round: their membership
routes through the same guarded tri-state existence construction as everything else. Adapters with
no UNKNOWN to represent (Prisma filters, Mongo query documents) get the same result by requiring
the hops **outside** the negation rather than inside it, so the negation cannot flip the
requirement along with the predicate.

**A private copy of "negate this" is how the ternary escaped both rounds.** A ternary rewrites into
guarded branches, and its false-branch is "the condition is definitively FALSE" — the same
three-valued negation the `not` handler already computes. Prisma spelled that a second time as a
bare `NOT`, so the hop requirement #315/#316 added never reached it and the else-branch was selected
for every parentless row (#334). The repair is delegation, not another patch: one negation with the
guard inside it, reused wherever a condition has to be falsified.

Mongoose is the other adapter with no UNKNOWN to represent, and it does **not** share that defect:
its ternary is a single `$cond` inside an aggregation expression, so there is no second negation to
diverge. It has a *different* latent hazard in the same place — `$cond`'s `if` treats a missing
field path as falsy, which selects the else-branch for an absent parent — but no corpus action
reaches it, because every chained operand the corpus carries is a collection and membership has no
aggregation-expression form there. Probing it needs a chained **scalar** attribute, which is a new
seed field.

**The fractional-threshold collapse is the one branch the corpus cannot reach.** CEL rejects
`==`/`!=` between an `int` and a `double` ("found no matching overload for `_==_` applied to
`(int, double)`"), so no policy can make the planner emit a fractional equality against `size()`,
and the ordering spellings the corpus does pin (`>= 1.5`, `<= 1.5`) round to an integer threshold
and travel the ordinary count path. An adapter that folds the fractional equality to a constant
must still guard it — `hops AND constant` is two-valued and readmits parentless rows under a
negation — but only that adapter's unit tests can prove it (#333). This is the documented exception
to "a per-adapter unit test is not a substitute for a corpus action": there is no corpus action to
substitute for.

### The degeneracy guard

The comparison in step 4 can pass vacuously if the oracle itself is trivial (e.g. the PDP denies
every row, or allows every row, regardless of what the adapter does). Every harness must assert,
for at least a handful of representative actions, that the oracle result is neither empty nor the
full seed set (`!ids.isEmpty() && ids.size() < seeds.size()`). This guards the guard: without it, a
harness whose PDP connection or policy load silently failed would still pass every comparison.

**Derive the list per adapter; never copy another harness's.** The guard protects an oracle
*comparison*, so an entry naming a shape that adapter never compares — because it sits in that
adapter's `adapterUnsupported` set, or in the global `expectedUnsupported` — protects nothing. A
copied list drifts into exactly that as classifications diverge, and the drift is invisible:
nothing fails, the list simply stops meaning what it says (cerbos/query-plan-adapters#324). Each
harness therefore keeps two lists and asserts they are complements of its own oracle set:

- **the guard proper** — a representative sample of the actions the adapter *does* oracle-compare,
  one per hostile group it can express. Each entry is asserted to be in the adapter's oracle set,
  so moving an action into `adapterUnsupported` fails the guard instead of quietly emptying it.
- **liveness-only probes** — shapes the adapter refuses to translate, kept because the group has no
  compared member for that adapter and the non-degenerate oracle still proves the PDP and policy
  are live. Each entry is asserted *not* to be in the oracle set, so a shape the adapter later
  gains support for has to be promoted into the guard proper rather than staying a weaker probe.

Both lists still assert the non-empty, non-total oracle. The exclusion for an action whose oracle
is empty *by construction* is unchanged: it belongs in neither list (see
`nullRepresentationOmitted` above, and
`w1-size-zero-chain`/`w1-not-size-chain`/`w1-size-frac-chain`/`in-empty`/the string casts).

Adapters differ widely in what they can express — `langchain-chromadb` compares 15 of the 136
conformance actions where `ent` and `pgx` compare all of them — so the lists are expected to look
different per harness. That is the point.

### Pinned throw messages

A fail-closed classification is only proven when the throw it rests on is the throw it claims. A
bare "it threw" assertion is satisfied just as happily by a mapper typo, an unrelated validation, or
a transport error — and every one of those makes the action pass while never reaching the mechanism
its `reason` names. The elasticsearch-java harness found this the expensive way: an unmapped
`categories` field had six of its actions throwing "Unknown attribute" — a harness gap — while
never reaching the mechanism their `reason` claimed (cerbos/query-plan-adapters#326).

So every throwing classification carries the substring that adapter's error must contain:

- `adapterUnsupported[<adapter>][].message` — the entry is already per-adapter, so the message sits
  on it directly.
- `expectedUnsupported[].messages[<adapter>]` — one entry per adapter that must reject the shape.
  This generalises the old `springDataMessage`, which pinned the reference and left the other nine
  asserting nothing.
- `nullRepresentationOmitted[].messages[<adapter>]` — the same, for the group every adapter rejects.

`scripts/validate-corpus.sh` enforces all three: every `adapterUnsupported` entry has a non-empty
`message`; every `expectedUnsupported` entry's `messages` key set is *exactly* the `adapters` roster
minus the adapters that promoted the shape into `adapterSupportedExpected`; and every
`nullRepresentationOmitted` entry's key set is the whole roster, since no adapter can translate one.
A missing key is an adapter whose harness would have nothing to assert; a stray one is a message
nothing reads.

Each harness resolves its own message when it derives the classification and **fails the run if one
is absent**, so adding a throwing action without pinning its message is a loud failure rather than a
silent downgrade to a bare throw. Every harness also unit-tests that guard directly, so it cannot go
inert against a corpus that already satisfies it.

The assertion is `contains`, not equality. That is deliberate and it is a **weakening for
spring-data**, which previously asserted `assertEquals` on its own `springDataMessage`: one field
with one meaning across ten harnesses is worth more than byte-exactness in the reference alone, and
several messages carry a runtime value (`Timestamp value exceeds millisecond precision:
<now()-24h>`) that equality could never pin. Rewording the mechanism still fails every suite;
appending to a message no longer fails spring-data's.

Some messages are deliberately shared across many actions — Chroma answers 86 of its 126 throwing
shapes with "Nested expressions are not supported by ChromaDB filters". Those 86 `reason` strings
name different *upstream* limitations (no count function, no relation model, no temporal type), but
they converge on one rejection: every Chroma comparison operand must be a bare field or literal, and
a nested expression is not. Pin what the adapter says. The message discriminates the *mechanism*, not
the action, and a rejection from anywhere else — an unmapped field, a transport error — still fails.
Making those 86 messages individually specific would mean rewriting the adapter's error strings, not
the corpus.

The message and the entry's `reason` must name the same mechanism. Where they disagree, the fix is
to work out which limitation actually fires first and correct the `reason` — not to loosen the pin.
Several `reason` strings named the limitation a maintainer had in mind rather than the one the walk
reaches (prisma's `p-deep-nest` reaches the LIKE-metacharacter needle before the cross-model
comparison; elasticsearch-java's `p-ternary-under-all` rejects the positive `all` before it ever
looks at the conditional inside it), and pinning the messages is what surfaced them.

### Known divergences still need a tripwire

An action in `knownDivergences` is excluded from the oracle run, which leaves it exercised on
neither side unless the harness says something about it explicitly. Every harness therefore pins
the `p-has` planner over-grant directly: the plan folds to `KIND_ALWAYS_ALLOWED`, the check()
oracle is non-empty and non-total (it denies the seeds whose attribute is missing), and the adapter
consequently returns every row. When the upstream fold is fixed the assertion fails, which is the
prompt to move the action back into the oracle run.

### Deterministic derived fields

The corpus keeps raw relational rows compact; five resource attributes and stored columns are
derived from each seed. **The values live in `derived-fields.json`, one entry per seed id, and
every harness reads them from there.** They used to be hand-transcribed once per harness, which is
how a transcription error becomes invisible: the same copy feeds the stored row *and* the check()
oracle, so a wrong value makes both sides of the differential agree for the wrong reason and
nothing downstream can catch it (#318).

`scripts/validate-corpus.sh` asserts that the file carries exactly one entry per seed id and that
every entry carries exactly the fields it declares, re-derives `createdBy`, `aDouble` and
`createdAt` from `seeds.json` using the rules below, and diffs `scope` and `labels` — which have no
rule to re-derive from — against a restatement of their tables. That check is the only independent
statement of these values, and it is a checker, never an input to a harness: unlike the ten copies
it replaced it can only fail loudly, never make both sides of a differential agree.

The rules the file materialises:

- `createdBy`: `aNumber >= 2 ? "2024-06-01T00:00:00Z" : "2026-06-01T00:00:00Z"`.
- `aDouble`: `a1 = -0.6`, `a2 = 0.25`, `a3 = NULL`/missing, otherwise `aNumber + 0.3`.
- `createdAt`: `a1 = 2020-03-15T10:30:00Z`, `a2 = 2037-01-01T00:00:00Z`, `a3 = NULL`/missing,
  `a4 = 2024-06-01T00:00:00Z`, `a5 = 2020-03-15T10:30:00.123456Z`; otherwise use
  `2036-06-06T06:06:06Z` when `aNumber >= 2`, or `2021-05-05T05:05:05Z`.
- third-level `labels[].name`: `a1 = ["gold", "silver"]`, `a6 = [missing, "silver"]`,
  `a8 = ["silver"]`, `c1 = ["Gold"]`, otherwise empty.
- `scope`: `a1=dept`, `a2=dept.eng`, `a3=dept.eng.platform`,
  `a4=dept.eng.platform.obs`, `a5=dept.engineering`, `a6=dept.sales`, `a7=NULL`,
  `a8=""`, `a9=50%`, `b1=50%:a_b:x`, `b2=50x:a_b:y`, `b3=50%:aXb:y`,
  `b4=50%:a_b`, `b5=dept.eng.platform2`, `b6=50%.a_b`, `c1=Dept.Eng`,
  `c2=dept.eng.`, `d1=[env]:prod:eu`, `d2=e:prod:eu`; all other seeds use NULL.

These are part of the shared contract. Do not replace them with adapter-specific fixtures, and do
not recompute them in a harness — read `derived-fields.json`.

### Seed and derived-field coverage

The projection trap this README documents for `actions.json` applies to the seeds too, and it is
worse there: a seed key a harness does not consume is dropped from the stored row **and** the
check() oracle simultaneously, so the differential still agrees and the new field tests nothing.
Every harness therefore declares the exact seed key set it consumes and asserts equality against
the JSON — not merely that unknown keys are rejected, because that direction says nothing about a
key the corpus stops carrying, which would decode to its zero value on both sides. `note` is the
one permitted exclusion: it is corpus prose no harness reads. The same assertion covers `tags[]`,
the one nested object array a seed carries; a key added inside an element is dropped just as
silently as a top-level one.

The same assertion covers `derived-fields.json`: each harness declares the five fields it consumes
and fails if the file's `fields` list, or any entry's key set, differs. Concretely this is
`DisallowUnknownFields` plus a key-set assertion in Go, records without
`@JsonIgnoreProperties(ignoreUnknown = true)` plus a key-set assertion in Java, and an explicit
`assertKeys` in the TypeScript and Python harnesses. The TypeScript harnesses that rebuild each
seed field by field (mongoose, langchain-chromadb) assert against the *raw* JSON — a rebuilt object
can only ever report the keys the parser already names, so asserting on it would pass vacuously.

Adding a field to a seed must fail every harness loudly. That is the acceptance test for this
guard; run it before trusting it.

## Adding a new hostile shape

1. Add the action + condition to `policies/adversarial.yaml`.
2. Add the action name to `actions.json` — `conformance` or `expectedUnsupported`, or
   `nullRepresentationOmitted` if it probes `== null` against an attribute the oracle omits for
   NULL columns — with a comment in the policy explaining what it probes and which seed rows
   discriminate it (follow the existing comment style).
3. If the shape needs new seed data to be non-degenerate, add a seed to `seeds.json` with a `note`
   explaining what it witnesses (see `a9`, `b1`-`b6` for examples), and add its `derived-fields.json`
   entry in the same commit — `scripts/validate-corpus.sh` names the expected values when it fails.
   A seed field that is genuinely new (rather than a new row) has to be added to every harness's
   consumed key set too; the guard described above makes that a loud failure, not a silent drop.
4. Run `scripts/regenerate-wire-fixtures.sh` and commit the new fixture alongside the policy change.
5. Every adapter harness picks up the new action automatically from `actions.json` on next run;
   triage any divergence into a per-adapter fix issue rather than special-casing it in the harness.
   An action that ends up fail-closed for an adapter needs that adapter's throw message pinned
   alongside the classification — see "Pinned throw messages" above. Run the adapter first and pin
   what it actually says; the harness refuses to run with a message missing, so there is no way to
   forget one.
6. Each harness pins the corpus size AND its throwing-action count as tripwires (e.g.
   `expect(MANIFEST_ACTIONS.size).toBe(146)` and `expect(THROWING_ACTIONS).toHaveLength(50)` in
   `prisma/src/adversarial.test.ts`; the oracle counts too in the convex, langchain-chromadb and
   elasticsearch-java harnesses). Bump them deliberately — those assertions exist so a new action
   cannot slip past an adapter unnoticed. The convex harness additionally pins WHICH actions its
   filter engine decides on its own, under each of its two mappers, because its README quotes those
   counts as the coverage the differential actually buys
   ([#327](https://github.com/cerbos/query-plan-adapters/issues/327)) — a new action lands in one
   of those buckets and has to be named.
7. Add the action to each harness's degeneracy-guard list so it cannot pass vacuously — to that
   harness's *compared* list where the adapter translates the shape, and to its liveness-only list
   where it does not, per "The degeneracy guard" above. Adding it to the compared list of an
   adapter that throws on it fails immediately, which is the intended feedback rather than an
   obstacle. Also check that no harness projects the corpus into a narrower local shape.
   `langchain-chromadb` used to
   rebuild the principal from a hardcoded attribute allowlist; when `pv-exists` added
   `principal.attr.manyTeams`, the projection dropped it, the plan folded to `ALWAYS_DENIED`, and
   the oracle — built from the same projected principal — agreed. The action passed on both sides
   while testing nothing. Pass corpus data through verbatim.

   The one exception is a `nullRepresentationOmitted` action: its oracle is empty *by
   construction*, which the degeneracy guard asserts against, so it must stay out of that list.
   It needs a different anti-vacuity assertion instead — assert why the rejection is required,
   not merely that one happens. See the `nullRepresentationOmitted` section above for the form
   that takes in each adapter. It pins a message like any other rejection (see "Pinned throw
   messages"), so a new one needs a `messages` entry per adapter.

## Adding a new adapter

The corpus is the contract; a new adapter joins by proving itself against it. Work in this order —
the classification is an *output* of the harness, not an input to it. Declaring an action
unsupported before you have watched it fail is how a translatable shape gets permanently skipped.

1. **Implement translation.** Follow the closest existing adapter. Spring Data is the reference
   implementation: when a shape is ambiguous, its behaviour defines the answer, and whether it
   translates a shape at all decides `conformance` vs `expectedUnsupported`.

2. **Write the differential harness**, implementing the oracle recipe above against the adapter's
   own store. Never hand-write expected id sets — the PDP is the oracle for both sides. Derive
   the classification from `actions.json` at runtime rather than copying it:

   ```
   oracleActions   = conformance - adapterUnsupported[me] + adapterSupportedExpected[me]
   throwingActions = adapterUnsupported[me] + (expectedUnsupported - adapterSupportedExpected[me])
   nullOmitted     = nullRepresentationOmitted            (translated with the option flipped)
   skipped         = knownDivergences where adapters contains me
   ```

   Each throwing action also carries the message the throw must contain — `.message` on an
   `adapterUnsupported` entry, `.messages[me]` on an `expectedUnsupported` one. Resolve it while
   deriving the classification and fail the run when it is absent, so a shape cannot join the throw
   suite with nothing but a bare throw behind it (see "Pinned throw messages" above).

   `drizzle/src/adversarial.test.ts` is the cleanest example of this wiring. Every adapter's key
   in `actions.json` is its **directory name** (`langchain-chromadb`, `elasticsearch-java`).

   **Read every group, and derive the manifest from the same expressions.** The manifest assertion
   ("each action classified exactly once") is what catches a group you forgot — but only if the
   group feeds both sides. Harnesses that re-validate `actions.json` into a local record
   (mongoose, langchain-chromadb) must parse each group explicitly: a field the parser does not
   name is dropped silently, and a dropped group makes its actions vanish from every count and
   every parameterised case at once. That is the projection trap, and it passes vacuously.

3. **Persist the seeds exactly**, including the NULL conventions, and read the derived fields from
   `derived-fields.json` rather than recomputing them. `aOptionalString` is NULL for several seeds
   and `tags[].name` is NULL for others — those are not incidental, they are what the
   three-valued-logic probes discriminate on. Getting them wrong makes the oracle agree with the
   adapter for the wrong reason.

   Declare the seed keys and derived fields the harness consumes and assert set equality against
   the JSON, as "Seed and derived-field coverage" above describes. A harness without that guard
   silently drops the next corpus field from both sides of its own differential.

4. **Assert the degeneracy guard** (see above) and pin the corpus size, so a silently broken PDP
   connection or a newly added action cannot pass vacuously. Derive the guard's compared list from
   the adapter's own oracle set and assert per-entry membership — a list lifted from the nearest
   existing harness will name shapes this adapter does not translate, and those entries guard
   nothing. Pin every `knownDivergences` action the same way (see above): excluded from the oracle
   run means exercised nowhere unless the harness says so explicitly.

5. **Run it and let it fail.** Triage every divergence into exactly one of:
   - a translation bug in the adapter — fix it;
   - a shape the query language genuinely cannot express — add it to
     `adapterUnsupported[<adapter>]` with a **specific** reason naming the real mechanism, and
     make the adapter throw. "Cannot express this shape faithfully" is not a reason; "emits LIKE
     without an ESCAPE clause, so `%` cannot be matched literally" is. Pin the message the adapter
     actually raises on the same entry, and check it names the mechanism the reason declares;
   - an upstream planner bug — add to `knownDivergences` with the affected adapters and a reason.

   The invariant is absolute: **an inexpressible shape must throw before its filter can be used.**
   A wrong filter is an authorization bug that returns rows the PDP denies. A throw is a bug
   report. Never degrade one operator into a weaker one (`exists_one` into `exists`) to make a
   test pass.

6. **Register in `actions.json`** — add the adapter to the `adapters` roster, and give every
   `expectedUnsupported` entry it does not promote a `messages` key. Run
   `scripts/validate-corpus.sh`: it enforces that every `adapterUnsupported` entry names a real
   `conformance` action and carries a message, that every `adapterSupportedExpected` entry names a
   real `expectedUnsupported` one, and that each `messages` key set is exactly the roster minus the
   promotions — so onboarding an adapter without pinning its messages fails there rather than in
   the new harness alone.

7. **Wire CI.** Copy an existing adapter workflow. It must: read the PDP version from
   `CERBOS_VERSION` and its digest from `CERBOS_IMAGE_DIGEST` (never hardcode either), run
   `scripts/validate-corpus.sh` in every job that replays the corpus **or** hardcodes the PDP
   image — a job that interpolates the two files at runtime cannot drift and does not need it,
   but a hardcoded pin can, and that assertion is `validate-corpus.sh`'s job — trigger on
   `conformance/**` as well as the adapter's own directory, and run the adversarial suite
   **inside the same job as the regular tests**, not as a separate job: the corpus discriminates
   the translator and the datastore, so a separate job costs runner minutes for no extra
   coverage. Pin every service image the harness or the workflow starts, by tag **and** digest —
   see below.

#### Pinning service images

Every container a test or a workflow starts is written as `repository:tag@sha256:<64 hex>`. The
tag says which release a reader is looking at; the digest says which build a green run actually
proved. A tag alone records an intent, not a build — `postgres:16` and
`docker.elastic.co/elasticsearch/elasticsearch:8.15.3` are both re-pushed — so a suite pinned only
by tag cannot answer "what did this pass against", and a suite pinned only by digest cannot answer
"which version is this". `validate-corpus.sh` enforces both halves:

- **The PDP** is corpus-wide, so its two halves live here: `CERBOS_VERSION` and
  `CERBOS_IMAGE_DIGEST`. Every restatement anywhere in the repository is asserted to match both.
  A reference that carries the right tag and a digest from some other build reads as pinned and is
  not, which is why the two are checked together rather than the tag alone
  ([#322](https://github.com/cerbos/query-plan-adapters/issues/322)).
- **Everything else** — the databases, the search and vector stores — is pinned *per harness*, in
  one constant that both that adapter's suites read. It deliberately does **not** live under
  `conformance/`: a change here re-runs all ten adapter workflows, so a shared file would make
  bumping mongoose's server cost nine irrelevant CI runs. What is shared is the rule.
  `validate-corpus.sh` holds a list of image repositories, scans the repository for each, requires
  every occurrence to carry a tag and a digest, and requires a given `repo:tag` to resolve to
  exactly one digest repo-wide — so two harnesses cannot claim the same nominal version while
  running different builds. **Adding a new service means adding its repository to that list**; a
  repository nothing scans is a repository nothing keeps pinned.
- Markdown is out of scope for both scans. A README telling a *consumer* how to start a PDP of
  their own is prose about their environment, not something this repository runs.

Renovate is configured with `docker:disable`, so nothing proposes these bumps: they are made by
hand, deliberately, alongside whatever re-verification the bump needs. That is the accepted
trade — reproducibility now, staleness to be watched for — and re-enabling Docker updates is a
maintainer decision, not a drive-by one.

8. **Document the contract** in the adapter's README with a `Conformance contract` table
   (oracle-tested / fail-closed / known divergence counts). Each adapter is published
   independently, so its README must stand alone — a consumer should not need this monorepo to
   understand what the adapter guarantees.

### Mapping hazards: the rows the subquery sees

Everything above proves the **plan** side — given a policy shape, does the adapter's filter return
the rows `check()` allows. The other half of the contract is the **mapping**, and the corpus
cannot express it with a policy action because the policy is irrelevant to it:

> **The rows an adapter's subquery sees must equal the rows the application put into the resource
> attributes.**

When they differ, the filter returns rows the PDP denies and no corpus action notices, because the
oracle is computed from the attributes and the adapter reads the store. Every hazard below is a
violation of exactly that one sentence, and every one of them was a real over-grant
(cerbos/query-plan-adapters#314, found while building the ActiveRecord adapter):

| Hazard | What goes wrong | Where it shows up |
|---|---|---|
| A **filtered association** | the application's association applies a predicate the subquery does not, so the subquery matches rows the application never serialised | ActiveRecord `has_many …, -> { where(visible: true) }`; SQLAlchemy `relationship(primaryjoin=…)`; Hibernate `@Where`/`@Filter`; a Prisma client extension or middleware injecting `where` |
| A **default scope on the target model** | the subquery reads the table directly and skips the scope every application read applies | ActiveRecord `default_scope`; Hibernate `@Where` on the entity; a soft-delete filter |
| **Subtype discrimination** | the association also filters on a type/discriminator column; the bare table holds the other subtypes too | ActiveRecord STI; JPA `@DiscriminatorValue`; a Mongoose discriminator, though only for the model the caller hands to `find()` |
| A **to-one relation used as a collection** | nothing makes the database enforce one row, so the application sees one and the subquery examines all of them | ActiveRecord `has_one`; any unindexed FK-back-reference |
| A **composite association key** | a multi-column key becomes one quoted identifier and the query fails, or worse joins on the wrong column | ActiveRecord 7.1+ composite keys; any two-column FK |
| An **absent to-one parent** | see the section above — this one *is* expressible, and `w1-all-chain` and friends pin it | every relational adapter |

Two things follow for an adapter author.

**1. Decide about each hazard explicitly.** For a hazard that *can arise* in an adapter, there are
exactly three sanctioned outcomes:

- **Reproduced** — the mapping carries the store-side predicate, so the subquery reads the rows the
  application reads. Class 1 adapters (below) take an optional relation predicate for this.
- **Rejected** — the adapter refuses the mapping with an error naming the real mechanism, or the
  mapper type makes the hazardous mapping unexpressible in the first place. A composite association
  key is rejected this way by every adapter whose relation mapping takes a single source column: the
  caller gets a compile error, not a wrong join.
- **Declared caller-owned** — the adapter states that holding the invariant is the caller's job, and
  the README names the ORM feature the caller has to go and check.

A best-effort subquery is the one outcome the invariant forbids.

A fourth answer is available, and it is not one of the three because it is not a decision: **not
applicable** — the hazard cannot arise, so there is nothing to decide. It is only honest when the
row can say *structurally* why, and the structural reason is load-bearing enough to be worth a test
of its own. Class 3 adapters (below) write it for the five subquery hazards because they build no
subquery, and mongoose's harness asserts exactly that — the day it grows a `$lookup`, five "not
applicable" rows silently become over-grants. "Not applicable" with no mechanism behind it is a
best-effort subquery wearing a label, and does not pass review.

**Declared caller-owned is available only where the adapter cannot detect the hazard from the mapper
it is given.** That restriction is what stops it being a loophole, because "reject" presupposes a
detection that mostly does not exist: an adapter handed a table and two column references cannot see
a client extension, a soft-delete convention or a discriminator. Where the adapter *can* see the
hazard, caller-owned is not available and the position must be reproduced or rejected.

The second guard is a positive obligation: **a caller-owned row must name the exact ORM feature the
caller must check.** A row that only says "caller-owned" does not pass review, for the same reason
"cannot express this shape faithfully" is not an acceptable `adapterUnsupported` reason.

**2. Say so in the adapter's README**, next to the `Conformance contract` table, as a table with one
row per hazard above — six rows, in the same order:

```
| Hazard | Position | Mechanism to check |
```

Six rows even when most of them are inapplicable, because a reader diffing the adapter's table
against this one should find every hazard accounted for rather than having to work out which
omissions were deliberate. The absent to-one parent's row records that it is *proved by the corpus*
(`w1-all-chain` and its siblings) rather than merely documented — it is what a closed hazard looks
like, and it is the row that makes the difference visible.

An adapter may append **additional** rows below those six for a hazard only its store has, and must
say in the prose above the table that it has done so — the six shared rows stay first and in order,
so the diff against this list still reads cleanly. A hazard is adapter-specific only when no other
store can reach it; anything two adapters could hit belongs here, in the shared list, so all ten
have to record a position on it. The one such row today is elasticsearch-java's **analyzed (`text`)
field mapping**: Elasticsearch rewrites a stored string into tokens before comparing it, so a field
mapped `text` widens every string comparison the adapter emits. No other store in this repository
transforms a value between write and comparison, so there is nothing for the other nine to answer.

The three classes the ten adapters fall into determine most of the answers:

| Class | Adapters | What the store applies to the subquery |
|---|---|---|
| **1 — bare-table subquery** | drizzle, ent, pgx, prisma | nothing |
| **2 — ORM-association subquery** | spring-data, sqlalchemy | Hibernate applies `@SQLRestriction`/`@Where` — on the entity and on the joined collection — and the single-table discriminator; SQLAlchemy applies `primaryjoin` and the single-table discriminator *only* when the caller's override goes through a mapped `relationship()` |
| **3 — no subquery** | mongoose, convex, langchain-chromadb, elasticsearch-java | n/a — relations are paths inside the same document |

Prisma names a relation, so it looks like class 2. It is class 1: Prisma has no `@Where` equivalent,
so nothing store-side reaches the nested `some`/`every`/`none`.

Class 1 adapters expose an **optional** relation predicate the caller attaches to the mapping;
declaring nothing emits exactly the filter the adapter emitted before the field existed. Class 2
adapters deliberately do **not** expose one — a caller who re-declared a filter the ORM already
applies would have it applied twice, silently removing rows the PDP permits.

The precedent for handling this without a policy action is `nullRepresentationOmitted`: a
per-adapter contract asserted by each harness rather than a shape in `adversarial.yaml`. If a
hazard turns out to be expressible as a plan shape — as the absent to-one parent was — move it into
the policy suite and classify it like anything else.

### Gotchas worth knowing up front

- **Do not trust a local pass that depends on gitignored generated state.** Convex's harness
  imports `convex/_generated/`, which only exists after `npx convex codegen` against a live
  backend; a type-check that passes locally can fail in CI purely because your tree has stale
  artifacts. The same applies to Prisma's generated clients.
- **Java harnesses read `../conformance/`**, so containerised runs must mount the repository
  root, not the adapter directory. See the recipe in the repo's `CLAUDE.md`.
- **The wire fixtures are not consumed by adapter harnesses.** They pin planner shape
  independently and are enforced by the `Conformance Corpus` workflow, which replans against the
  pinned PDP and fails on drift.
- **A dialect the harness does not exercise is not covered.** Collation, LIKE metacharacter
  handling and parameter typing all differ per dialect, and the READMEs treat them as part of the
  policy contract for exactly this reason. `ent` and `spring-data` run three dialects each;
  `drizzle` and `prisma` run SQLite and PostgreSQL, chosen with `ADAPTER_TEST_DB`
  ([#320](https://github.com/cerbos/query-plan-adapters/issues/320)); the remaining TypeScript
  harnesses are still single-store. That leg is not a formality. Adding it turned up three live
  mechanisms SQLite could not see, none of them visible in `actions.json` afterwards because two
  were fixed in the translator:
  - `drizzle`, `$1 IS NULL` over a bound constant — untypeable on PostgreSQL, so a hard error
    rather than the redundancy it is on SQLite (`cr-contains`, `like-underscore`, and the five
    `cr-div-*` shapes).
  - `drizzle`, a numeric constant typed from the column instead of the value — `aNumber >= 1.5`
    against an `integer`, `size(aString) > 4294967296` against `length()`'s `integer`
    (`double-threshold`, `p-double-frac`, `cr-size-frac-ge`, `size-huge-gt`, `size-huge-lt`,
    `cr-div-neg-zero`, `cr-div-other-column`). Read as SQL `numeric` rather than `float(53)`
    this one is silent, not loud: `aNumber * 0.1 == 0.3` is exact decimal arithmetic and admits
    a row CEL's binary floating point denies.
  - `prisma`, `like-backslash` — `\` is the default `LIKE` escape character on PostgreSQL and
    MySQL and literal on SQLite, so one needle meant two things. Not expressible without an
    `ESCAPE` clause Prisma does not emit, so it is now `adapterUnsupported` and throws.

  The same caution applies to a *hosted* store the harness substitutes a local build for: `convex`
  runs against a pinned self-hosted `convex-backend` container, never Convex Cloud, and most of its
  corpus is decided by the adapter's own JavaScript post-filter rather than by any filter engine at
  all ([#327](https://github.com/cerbos/query-plan-adapters/issues/327)).

  Each adapter's README names the stores its contract is actually proved on, and how much of the
  corpus each one actually executes.

## Regenerating wire fixtures after a Cerbos version bump

```bash
# edit CERBOS_VERSION first, then resolve the digest the new tag points at:
#   docker buildx imagetools inspect ghcr.io/cerbos/cerbos:$(cat CERBOS_VERSION) \
#     --format '{{.Manifest.Digest}}' > CERBOS_IMAGE_DIGEST
./scripts/regenerate-wire-fixtures.sh
git diff conformance/wire-fixtures   # review exactly what the planner's wire output changed
./scripts/validate-corpus.sh         # fails if any restatement still names the old tag or digest
```

Requires `docker`, `curl`, and `jq`.
