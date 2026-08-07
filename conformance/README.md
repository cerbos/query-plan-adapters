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
- `actions.json` — every action in `policies/adversarial.yaml`, grouped into `conformance`
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
   through the same oracle comparison as conformance actions.

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
  omits the field entirely and `q.eq(field, null)` does not match an absent field. Same paired
  assertion as mongoose. Alignment here comes from the storage layout, not from the plan.
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

Only a universal, a negation, or a zero/lower-bound count discriminates them, which is why
`w1-exists-chain`, `w1-size-chain` and `w1-in-chain` passed everywhere while the bug was live
(cerbos/query-plan-adapters#309). `w1-all-chain`, `w1-not-exists-chain`, `w1-size-zero-chain`,
`w1-size-nonneg-chain`, `w1-not-in-chain`, `w1-not-hasint-chain` and `w1-not-size-chain` are the
seven that discriminate.

The fix is in the translation, not in the classification: **a chained collection must require its
intermediate hop to exist**, so an absent parent stays excluded under both polarities instead of
collapsing onto the empty-collection case. `w1-size-zero-chain` and `w1-not-size-chain` have empty
oracles by construction (no seed holds a parent with zero children) and therefore stay out of the
degeneracy guard; `w1-all-chain`, `w1-not-exists-chain`, `w1-size-nonneg-chain`,
`w1-not-in-chain` and `w1-not-hasint-chain` all have non-degenerate oracles and carry the
anti-vacuity assertion for the group.

**Put the guard in the shared relation-scope construction, not in each operator.** The #309 round
guarded the collection macros, which left every sibling operator reached through the same chain
unguarded — membership and `hasIntersection` (#315), and the negated count spelling `!(size > 0)`,
which takes a different branch from `size == 0` because the planner emits the negation verbatim
rather than normalising it (#316). ent and pgx needed no change in either round: their membership
routes through the same guarded tri-state existence construction as everything else. Adapters with
no UNKNOWN to represent (Prisma filters, Mongo query documents) get the same result by requiring
the hops **outside** the negation rather than inside it, so the negation cannot flip the
requirement along with the predicate.

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
`nullRepresentationOmitted` above, and `w1-size-zero-chain`/`w1-not-size-chain`/`in-empty`/the
string casts).

Adapters differ widely in what they can express — `langchain-chromadb` compares 15 of the 133
conformance actions where `ent` and `pgx` compare all of them — so the lists are expected to look
different per harness. That is the point.

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
6. Each harness pins the corpus size as a tripwire (e.g. `expect(MANIFEST_ACTIONS.size).toBe(143)`
   in `prisma/src/adversarial.test.ts`, and the oracle/throwing counts in the convex and
   langchain-chromadb harnesses). Bump them deliberately — that assertion exists so a new action
   cannot slip past an adapter unnoticed.
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
   that takes in each adapter.

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
     without an ESCAPE clause, so `%` cannot be matched literally" is;
   - an upstream planner bug — add to `knownDivergences` with the affected adapters and a reason.

   The invariant is absolute: **an inexpressible shape must throw before its filter can be used.**
   A wrong filter is an authorization bug that returns rows the PDP denies. A throw is a bug
   report. Never degrade one operator into a weaker one (`exists_one` into `exists`) to make a
   test pass.

6. **Register in `actions.json`** and run `scripts/validate-corpus.sh` — it enforces that every
   `adapterUnsupported` entry names a real `conformance` action and every
   `adapterSupportedExpected` entry names a real `expectedUnsupported` one.

7. **Wire CI.** Copy an existing adapter workflow. It must: read the PDP version from
   `CERBOS_VERSION` (never hardcode it), run `scripts/validate-corpus.sh` in every job that
   replays the corpus **or** hardcodes the PDP image — a job that interpolates `CERBOS_VERSION`
   at runtime cannot drift and does not need it, but a hardcoded pin can, and that assertion is
   `validate-corpus.sh`'s job — trigger on `conformance/**` as well as the adapter's own
   directory, and run the adversarial suite **inside the same job as the regular tests**, not as
   a separate job: the corpus discriminates the translator and the datastore, so a separate job
   costs runner minutes for no extra coverage. Pin any service image by digest.

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
- **A dialect the harness does not exercise is not covered.** Most TypeScript harnesses run on
  SQLite only; collation and LIKE metacharacter behaviour differ on MySQL and SQL Server, and the
  READMEs treat collation as part of the policy contract for exactly this reason.

## Regenerating wire fixtures after a Cerbos version bump

```bash
# edit CERBOS_VERSION first
./scripts/regenerate-wire-fixtures.sh
git diff conformance/wire-fixtures   # review exactly what the planner's wire output changed
```

Requires `docker`, `curl`, and `jq`.
