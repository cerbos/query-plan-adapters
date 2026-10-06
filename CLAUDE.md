# CLAUDE.md

Multi-language ORM adapters that translate Cerbos query plan responses into database-native filters. Each adapter is an independent package with its own build/test cycle.

## Adapters

| Adapter | Language | Package | ORM/DB |
|---------|----------|---------|--------|
| prisma | TypeScript | `@cerbos/orm-prisma` | Prisma v5/v6/v7 |
| mongoose | TypeScript | `@cerbos/orm-mongoose` | Mongoose v9 |
| drizzle | TypeScript | `@cerbos/orm-drizzle` | Drizzle ORM |
| convex | TypeScript | `@cerbos/orm-convex` | Convex |
| langchain-chromadb | TypeScript | `@cerbos/langchain-chromadb` | ChromaDB |
| sqlalchemy | Python | `cerbos-sqlalchemy` | SQLAlchemy |
| activerecord | Ruby | `cerbos-activerecord` | ActiveRecord 7.1–8.x |
| ent | Go | `github.com/cerbos/query-plan-adapters/ent` | Ent |
| pgx | Go | `github.com/cerbos/query-plan-adapters/pgx` | pgx / PostgreSQL |
| elasticsearch-java | Java | `cerbos-elasticsearch` | Elasticsearch |
| spring-data | Java | `cerbos-spring-data` | Spring Data JPA |

## Commands

Run from the adapter directory. No adapter suite starts a PDP: the conformance harness replays
plans and decisions recorded under `conformance/golden/` ("Conformance", below).

To run an adapter's conformance harness with its store started and torn down, use
`conformance/scripts/run-harness.sh <adapter> [store]`: `--list` names each adapter's store legs,
and `--all` runs every adapter one leg at a time. Run legs one at a time; several harnesses at once
overload a laptop into timeouts.

Each adapter's README "Development" section (spring-data: "Build" and "Testing") lists its
commands, suites and the store each one needs. What those READMEs leave implicit:

### TypeScript adapters
```bash
npm run typecheck         # covers src/ AND *.test.ts; `npm run build` emits lib/ and skips the tests
npm test                  # offline unit tests (translator.test.ts): no store, no PDP
npm run test:adversarial  # the conformance harness (adversarial.test.ts) against a real store
```

Convex's harness imports `convex/_generated`, so it needs `npm run convex:up`, a deploy and
`npx convex codegen` first. Drizzle and Prisma also replay the corpus on PostgreSQL and MySQL via
testcontainers (`npm run test:adversarial:postgres` / `…:mysql`, plus `:v6` / `:v7` on Prisma),
selected by `ADAPTER_TEST_DB`; an unknown value fails. Every MySQL leg pins `utf8mb4_0900_bin` and
every PostgreSQL leg initialises with `--lc-collate=C`, because CEL compares strings byte-exactly and
orders them by code point. Why each weaker collation over-grants, and the
`ADAPTER_TEST_POSTGRES_INITDB_ARGS` override that reproduces it: conformance/README.md, "The dataset".

### Python (SQLAlchemy)

`pdm` commands, the `./pw` wrapper and `scripts/test-ci-sqlite.sh` (CI's older SQLite):
`sqlalchemy/README.md`, "Development". CI fails on any diff `pdm run format` or `pdm run lint` leaves.

### Ruby (ActiveRecord)

Everything runs in Docker through `activerecord/scripts/` (`test.sh`, `lint.sh`, `docs.sh`), so no
local Ruby is needed: `activerecord/README.md`, "Development". The Gemfile pins each CI leg to one
minor series: a floating `~> 7.1` resolves to the newest 7.x, and the leg named 7.1 would quietly
become 7.2.

### Go (Ent, pgx)

Commands, including the Docker-free `go test -skip TestAdversarialConformance ./...`:
`ent/README.md` / `pgx/README.md`, "Development".

Both Go modules are standalone: each vendors its own translator under `internal/queryplan` and
depends on nothing else in this repository, so a consumer only ever pulls in the one module. The two
vendored trees are held **byte-identical** and diffed by `validate-corpus.sh` — a semantic fix has to
land in both copies, and anything genuinely per-engine goes in that module's `render.go`, outside the
shared tree. Their unit suites (`translate_test.go`, `render_test.go`) mirror each other for the same
reason.

`./...` stops at a nested `go.mod`, so neither command reaches `ent/example/` or `pgx/example/`.
Each is its own module on purpose: a directory holding a `go.mod` is excluded from its parent's
zip, which keeps the example's dependencies out of a consumer's build. Lint it from its own directory
with `golangci-lint run --config=../.golangci.yaml ./...` (hence the `gomoddirectives` exclusion
scoped to `^example/go\.mod$`) and run it with `demo/scripts/run-example.sh <adapter>`.

### Java (Elasticsearch, Spring Data)

`./gradlew build` from the adapter directory, in a checkout of the whole repository: the harnesses
read `../conformance/`. Suites, the Docker-backed ones and spring-data's `ADAPTER_TEST_DB` /
`ADAPTER_TEST_ORM` legs: each adapter's README.

## Testing

Every adapter has two kinds of suite:

- **The conformance harness** replays the shared corpus against the adapter's real store
  ("Conformance", below). It needs the store, never a PDP.
- **Unit tests** cover what the corpus cannot ask: caller-supplied options, plans the planner
  cannot produce, and the adapter's refusal type. They start no service (an in-memory SQLite or H2
  at most). They do **not** re-assert what
  a corpus case already proves ("What a translator unit test may pin", below).

**`conformance/cases/` is the repository's only policy source for semantics.** The generator
builds `conformance/policies/conformance.yaml` from it. The other policy suites in the repository
prove **plumbing**, not semantics, and neither is a place to put a new shape: `demo/policies/`
feeds every example application, and `spring-data/example/policies/` is that adapter's onboarding
artifact. A shape worth proving is a case
([ADR 0008](docs/adr/0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md)).

## Conformance

`conformance/` is the shared adversarial corpus every adapter is proved against. It exists because
the same semantic bug — value-first operand inversion, LIKE metacharacter leaks, three-valued logic
under negation — has historically shipped identically to more than one adapter. Its cases,
dataset, pinned PDPs and goldens: [conformance/README.md](conformance/README.md), "Layout".

Every adapter carries `<adapter>/conformance-ledger.json`, listing only the cases it cannot pass:
`unsupported` (translating throws the adapter's refusal type) or `divergent` (a known wrong result,
with an `issue`). No entry means the harness asserts the returned ids equal `allowed` exactly. The
harness asserts no counts and pins no messages. The full contract is in
[conformance/README.md](conformance/README.md), "The harness contract".

**The invariant: a shape an adapter cannot express must throw, never emit a filter.** A wrong
filter is an authorization bug that returns rows the PDP denies; a throw is a bug report.

```bash
go -C conformance/generator run .           # rebuild policy, resources.json and goldens (Docker)
go -C conformance/generator run . -check    # CI: fail if anything committed is stale
conformance/scripts/validate-corpus.sh      # offline: ledgers, pins, vendored Go tree, README tables; every adapter's CI
conformance/scripts/bump-pdp.sh [tag]       # run locally: current -> previous, re-record (default: latest release)
```

A disagreement between the plan and `check()`, which no adapter can pass, is declared **once**, as
`plannerDivergence` on the case, never in a ledger. An empty or total oracle needs `degenerate` on
the case. Both: conformance/README.md, "Golden files".

Where `resources.json` omits an attribute on a row (a NULL column, or an absent `parent` hop), an
adapter that has a null convention declares that attribute *omitted* in its harness mapping. A
`null` literal compared against it is then a missing-attribute error that CEL denies, so the adapter
throws rather than emit `IS NULL`, unless its store can tell missing from null
([ADR 0004](docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md)).

The PDP pin lives in `conformance/pdp-versions.json` and nowhere else. Files that cannot read JSON
(Compose files, the Go modules' `cerbos/api/genpb`) restate `current`, and `validate-corpus.sh`
asserts every restatement agrees on **both** tag and digest. Bumping it: conformance/README.md,
"Bumping the PDP".

Every other service image is pinned per harness, in one constant file that adapter's suites share
(`<adapter>/<SERVICE>_IMAGE`), as `repo:tag@sha256:...`. Not a corpus file: `conformance/**` re-runs
every adapter workflow. `validate-corpus.sh` enforces the rule; add a new service's repository to its
`IMAGE_REPOSITORIES`.

**Read [conformance/README.md](conformance/README.md) before changing corpus behaviour**, and
[ADR 0010](docs/adr/0010-conformance-replays-recorded-pdp-decisions.md) for why it works this way.

## The demo domain

`demo/` is the repository's second shared corpus, and it proves a different property.
`conformance/` proves **semantics** — that a translated filter returns exactly the rows the PDP
allows — with hostile shapes and a per-adapter ledger. `demo/` proves **plumbing** — that the
adapter installs, imports, and composes with its ORM's real query methods — with realistic shapes
and **no per-adapter exceptions at all**
([ADR 0001](docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).

Each adapter's `example/` installs the packed artifact and exercises five usage shapes (why, and
which: demo/README.md). The examples are the one place a live PDP still runs:
`demo/docker-compose.yml`, pinned to `current` in `conformance/pdp-versions.json`.

```bash
demo/scripts/run-example.sh <adapter>   # pack, install, run, diff against demo/expected.json
demo/scripts/validate-demo.sh           # demo integrity; runs in every adapter's example job
```

Both scripts take the adapter roster from the directories holding a `conformance-ledger.json`. Two
rules that are easy to get wrong:

- **A shape needing a carve-out for one adapter is wrong for `demo/`.** There is no ledger here and
  adding one is exactly what ADR 0001 rules out; the argument belongs in `conformance/`.
- **Each example's job must stay inside that adapter's own workflow.** `renovate.json` automerges
  non-major bumps, so an ORM bump arrives as one PR touching both the adapter manifest and the
  example's committed lockfile — the example job on that PR is what blocks the automerge when the
  new ORM breaks real usage. A nightly or standalone workflow silently restores the gap.

**Read [demo/README.md](demo/README.md) before changing the demo domain.**

## Code style, commits and pull requests

Commits use Conventional Commits scoped by adapter name (`feat(prisma):`, `fix(mongoose):`,
`chore(deps):`). The review-time rules for style, commits and pull requests are in
[CODING_STANDARDS.md](CODING_STANDARDS.md).

## CI and releases

Read [docs/ci.md](docs/ci.md) before editing anything under `.github/workflows/` or cutting a
release: workflow triggers (and the trailing `!**/*.md`), which matrix legs buy coverage, the cache
keys shared with `warm-caches.yaml`, and the release tags.

## Changing how a condition is translated

**Any change to how an operator, condition, or expression shape is translated starts in the
shared corpus, not in one adapter.** A fix proven only against the adapter you happened to be
looking at leaves the identical bug live in every other adapter.

1. **Add or edit a case** in `conformance/cases/<area>.yaml` (conformance/README.md, "Changing the
   corpus"). A new column or principal attribute goes in `seeds.json` / `derived-fields.json`, with
   its projection in `generator/resources.go`.
2. **Run the generator** and read the golden diff. A new case needs a discriminating oracle: add a
   seed that tells a right translation from the wrong one it targets. An unrelated golden changing
   means the edit perturbed an existing shape.
3. **Run every adapter's harness and triage each failure** into exactly one of: a translation bug
   (fix it), a shape that store genuinely cannot express (make it throw the adapter's refusal type
   and add an `unsupported` ledger entry whose `reason` names the real mechanism), or a known wrong
   result tracked by an issue (`divergent`). A plan/`check()` disagreement is `plannerDivergence` on
   the case, not a ledger entry.
4. **The ledger is an output of the run, not an input.** Declaring a case unsupported before
   watching it fail is how a translatable shape gets permanently skipped.
5. **Update the affected READMEs' `Conformance contract` tables** in the same commit.

### What a translator unit test may pin

**For a shape a policy can reach, a unit test is not a substitute for a case.** Only a case proves
the emitted filter returns the rows the PDP allows, and only the corpus asks the same question of
every other adapter. A unit test must not re-assert a corpus case's output — no pinned filter for a
case, no pinned refusal message, no count of how many cases throw. A pinned filter proves the
adapter still emits what it emitted yesterday, not that it was ever right, and it turns every
harmless rewrite into a diff to approve.

What a unit test *does* pin is what the adapter can be asked **without a store** that no case can
state. Three kinds of material live only there, and they are not equal:

1. **A branch CEL itself cannot reach.** An operator CEL does not have (`isSet`) cannot come from
   any policy. Prove the branch cannot be planned (compile the shape and quote the error) before
   pinning it; do not infer unreachability from the adapter's own code. A type-checker error alone
   is not that proof: `dyn()` defers the check to runtime and the planner drops the wrapper. Try the
   `dyn()` spelling first. Plans the planner cannot produce at all (an unknown kind, a malformed
   operand list) belong here too. Permanent.
2. **A caller-supplied argument the corpus structurally cannot vary.** Each harness uses *one*
   mapping, so an operator override, a second mapper form, `allowPostFilter`, a per-call
   `nullAttributeRepresentation`, or `maxMacroDepth` has no case spelling. The adapter's refusal
   *type* belongs here too. Permanent.
3. **A corpus gap wearing a unit test** — policy-reachable, and the corpus simply does not carry it
   yet. This one is a **bridge, not a home**: a shape parked here is asked of one adapter and none of
   the others, which is the condition every bug this repository exists to stop was living in. Each
   instance must say at the test that it is a corpus gap, name the issue tracking the port
   ([#509](https://github.com/cerbos/query-plan-adapters/issues/509)), and be deleted when the case
   lands. `ElasticsearchQueryPlanAdapterTest` and `SpringDataQueryPlanAdapterTest` are the worked
   examples: a `KIND 3` banner over the block and a `Corpus gap.` lead on every test under it.

## Working with Adapters

- On the TypeScript adapters edit `src/`: `lib/` is `tsc` output and gitignored (activerecord's `lib/` is its source tree)
- `conformance/` affects all adapters: a change there re-runs every adapter's CI, and a new case runs in every adapter's harness
- `demo/` likewise re-runs every adapter's example job, and adding a usage shape means implementing it in every example — there is no ledger to opt out with
- Never edit `policies/conformance.yaml`, `resources.json` or anything under `golden/` by hand: the generator writes them, and CI fails if they are stale
- Adapters share data, not code: the corpus loader each adapter carries (`<adapter>/src/corpus.ts`, `sqlalchemy/tests/corpus.py`, `activerecord/spec/support/conformance_corpus.rb`, the Java `Corpus.java` files, `ent/corpus_test.go`, `pgx/corpus_test.go`) is duplicated **deliberately**, so every adapter stays standalone. Do not extract a shared loader, and do not add a drift check between the copies. That is the opposite of the byte-identical rule on the vendored Go *translator* trees. See [ADR 0007](docs/adr/0007-adapters-share-data-not-code.md)
- A harness passes corpus data through verbatim: one mapping for every case, no per-case options, no hand-projected subset of the dataset
- Write "every adapter" / "every harness" / "every example" wherever prose spans the roster — in docs, test-file comments and JSON `description`s alike. The roster is the set of directories holding a `conformance-ledger.json`, so the phrasing stays true when it changes. Genuine counts of something else (cases, seed rows) go in digits
- When an adapter cannot express a shape, make it throw its refusal type with a message naming the real mechanism — never emit a best-effort filter

## Agent skills

### Issue tracker

GitHub Issues on `cerbos/query-plan-adapters`, via the `gh` CLI. Tag every affected adapter with its
per-adapter label, or `conformance` for corpus-wide work. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, mapped to this tracker's label strings in `docs/agents/triage-labels.md` (`ready-for-agent` is `ready-for-implementation` here).

### Domain docs

Single-context: `GLOSSARY.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
