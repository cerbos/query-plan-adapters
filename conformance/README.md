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
  "The oracle recipe" below.
- `actions.json` — every action in `policies/adversarial.yaml`, grouped into `conformance`
  (must match the check() oracle exactly), `adapterUnsupported` (per-adapter lists of conformance
  actions that adapter's query language genuinely cannot express — LIKE-wildcard escaping,
  relation-count thresholds, cross-model column comparisons; the adapter must THROW for these,
  never emit a silently-wrong filter, and its harness asserts the throw instead of the oracle
  match), `expectedUnsupported` (planner shapes rejected by the Spring reference adapter; other
  adapters must also fail loudly unless listed in `adapterSupportedExpected`),
  `adapterSupportedExpected` (per-adapter exceptions that intentionally translate a
  reference-unsupported shape through a documented database capability), and `knownDivergences`
  (an action plus the affected adapters intentionally excluded from the oracle run, with a
  reason — currently only `p-has`, excluded because of a planner bug, not an adapter bug).
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
### The degeneracy guard

The comparison in step 4 can pass vacuously if the oracle itself is trivial (e.g. the PDP denies
every row, or allows every row, regardless of what the adapter does). Every harness must assert,
for at least a handful of representative actions, that the oracle result is neither empty nor the
full seed set (`!ids.isEmpty() && ids.size() < seeds.size()`). This guards the guard: without it, a
harness whose PDP connection or policy load silently failed would still pass every comparison.

### Deterministic derived fields

The corpus keeps raw relational rows compact; these resource attributes and stored columns are
derived from each seed exactly as follows:

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

These are part of the shared contract. Do not replace them with adapter-specific fixtures.
## Adding a new hostile shape

1. Add the action + condition to `policies/adversarial.yaml`.
2. Add the action name to `actions.json` (`conformance` or `expectedUnsupported`, with a comment
   in the policy explaining what it probes and which seed rows discriminate it — follow the
   existing comment style).
3. If the shape needs new seed data to be non-degenerate, add a seed to `seeds.json` with a `note`
   explaining what it witnesses (see `a9`, `b1`-`b6` for examples).
4. Run `scripts/regenerate-wire-fixtures.sh` and commit the new fixture alongside the policy change.
5. Every adapter harness picks up the new action automatically from `actions.json` on next run;
   triage any divergence into a per-adapter fix issue rather than special-casing it in the harness.
6. Each harness pins the corpus size as a tripwire (e.g. `expect(MANIFEST_ACTIONS.size).toBe(126)`
   in `prisma/src/adversarial.test.ts`, and the oracle/throwing counts in the convex and
   langchain-chromadb harnesses). Bump them deliberately — that assertion exists so a new action
   cannot slip past an adapter unnoticed.
7. Add the action to each harness's degeneracy-guard list so it cannot pass vacuously, and check
   that no harness projects the corpus into a narrower local shape. `langchain-chromadb` used to
   rebuild the principal from a hardcoded attribute allowlist; when `pv-exists` added
   `principal.attr.manyTeams`, the projection dropped it, the plan folded to `ALWAYS_DENIED`, and
   the oracle — built from the same projected principal — agreed. The action passed on both sides
   while testing nothing. Pass corpus data through verbatim.

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
   skipped         = knownDivergences where adapters contains me
   ```

   `drizzle/src/adversarial.test.ts` is the cleanest example of this wiring. Every adapter's key
   in `actions.json` is its **directory name** (`langchain-chromadb`, `elasticsearch-java`).

3. **Persist the seeds exactly**, including the NULL conventions and the derived fields above.
   `aOptionalString` is NULL for several seeds and `tags[].name` is NULL for others — those are
   not incidental, they are what the three-valued-logic probes discriminate on. Getting them wrong
   makes the oracle agree with the adapter for the wrong reason.

4. **Assert the degeneracy guard** (see above) and pin the corpus size, so a silently broken PDP
   connection or a newly added action cannot pass vacuously.

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
   `CERBOS_VERSION` (never hardcode it), run `scripts/validate-corpus.sh`, trigger on
   `conformance/**` as well as the adapter's own directory, and run the adversarial suite as its
   own job. Pin any service image by digest.

8. **Document the contract** in the adapter's README with a `Conformance contract` table
   (oracle-tested / fail-closed / known divergence counts). Each adapter is published
   independently, so its README must stand alone — a consumer should not need this monorepo to
   understand what the adapter guarantees.

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
