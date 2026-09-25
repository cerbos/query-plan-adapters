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
| mongodb-ruby | Ruby | `cerbos-mongodb` | MongoDB Ruby driver, Mongoid 9 |
| ent | Go | `github.com/cerbos/query-plan-adapters/ent` | Ent |
| pgx | Go | `github.com/cerbos/query-plan-adapters/pgx` | pgx / PostgreSQL |
| elasticsearch-java | Java | `cerbos-elasticsearch` | Elasticsearch |
| spring-data | Java | `cerbos-spring-data` | Spring Data JPA |

## Commands

Run from the adapter directory. No adapter suite starts a PDP: the conformance harness replays
plans and decisions recorded under `conformance/golden/` ("Conformance", below).

### TypeScript adapters
```bash
npm install
npm run build             # tsc --build -> lib/ (published surface only; test files are excluded)
npm run typecheck         # tsc -p tsconfig.typecheck.json — noEmit, covers src/ AND *.test.ts
npm test                  # Jest — offline unit tests (translator.test.ts): no store, no PDP
npm run test:adversarial  # the conformance harness (adversarial.test.ts) against a real store
```

The harness needs its store: SQLite in-process (drizzle, prisma), `npm run mongo` (mongoose),
`npm run chroma` (langchain-chromadb), or `npm run convex:up` plus a deploy and `npx convex codegen`
(convex, whose harness imports `convex/_generated`). Drizzle and Prisma also replay the corpus on
PostgreSQL and MySQL via testcontainers: `npm run test:adversarial:postgres` / `…:mysql` (plus
`:v6` / `:v7` on Prisma), selected by `ADAPTER_TEST_DB`; an unknown value fails. The MySQL legs pin
the byte-exact collation `utf8mb4_0900_bin`: MySQL's default makes `=` case-insensitive, and
`utf8mb4_0900_as_cs` still ignores a soft hyphen
([#474](https://github.com/cerbos/query-plan-adapters/issues/474)). Every PostgreSQL leg initialises
with `--lc-collate=C`, because CEL orders strings by code point and a linguistic collation does not
([#489](https://github.com/cerbos/query-plan-adapters/issues/489)); `ADAPTER_TEST_POSTGRES_INITDB_ARGS`
overrides it to reproduce the over-grant.

### Python (SQLAlchemy)
```bash
pdm install -G :all    # or ./pw install: pyprojectx fetches pdm, ruff and isort into .pyprojectx/
pdm run test           # pytest: unit suites and the conformance harness
pdm run format         # isort + ruff format
pdm run lint           # ruff check --fix; CI fails on any diff format or lint leaves
```

Every `pdm` command also runs through the pyprojectx wrapper, `./pw` (`pw.bat` on Windows), as in
cerbos-sdk-python: `./pw test`, `./pw format`, `./pw lint`, `./pw pdm build`.

`tests/test_adversarial_conformance.py` is the conformance harness: every case on SQLite (sync and
async), PostgreSQL and MySQL (`sqlalchemy/POSTGRES_IMAGE`, `sqlalchemy/MYSQL_IMAGE`, testcontainers,
Docker), and the cases that read a `collection_columns` declaration once more on PostgreSQL with the
collections stored as native arrays. The other suites start nothing. CI runs the whole harness
under both SQLAlchemy 1.4 and 2.x.

### Ruby (ActiveRecord)
```bash
# Everything runs in Docker.
cd activerecord
./scripts/test.sh                                      # all the specs
./scripts/test.sh spec/conformance_spec.rb             # the conformance harness
ADAPTER_TEST_DB=postgres ./scripts/test.sh spec/conformance_spec.rb   # or mysql; default sqlite
RUBY_VERSION=3.3 ACTIVERECORD_VERSION=7.1 ./scripts/test.sh
./scripts/lint.sh                                      # RuboCop on Standard, via `rake lint`
./scripts/docs.sh                                      # YARD, failing on a warning or an undocumented object
```

`ADAPTER_TEST_DB` selects the conformance store; an unknown value fails. `scripts/test.sh` starts
PostgreSQL or MySQL (`utf8mb4_0900_bin`) from `docker-compose.yaml`, with the images pinned in
`activerecord/POSTGRES_IMAGE` and `activerecord/MYSQL_IMAGE`. CI runs the corpus on all three stores
under ActiveRecord 8.0 and 7.1; every other suite runs on SQLite.
`spec/adapter_contract_spec.rb` is the caller-supplied contract. The Gemfile pins each CI leg to one
minor series: a floating `~> 7.1` resolves to the newest 7.x, and the leg named 7.1 would quietly
become 7.2.

### Ruby (MongoDB driver)
```bash
cd mongodb-ruby                                        # Ruby and Bundler from the host; MongoDB in Docker
./scripts/test.sh                                      # all the specs
./scripts/test.sh spec/adapter_contract_spec.rb spec/mongoid_spec.rb   # offline: no MongoDB
ADAPTER_TEST_MONGO_IMAGE_FILE=MONGO_NEXT_IMAGE ./scripts/test.sh spec/conformance_spec.rb
./scripts/lint.sh                                      # standardrb
```

`spec/conformance_spec.rb` replays every case twice, through the driver and through Mongoid via
`Cerbos::MongoDB::Mongoid.criteria`, on the server `scripts/test.sh` starts from `MONGO_IMAGE` (or
`MONGO_NEXT_IMAGE`).

### Go (Ent, pgx)
```bash
go test ./...             # includes the conformance harness; starts its own store containers
go test -skip TestAdversarialConformance ./...   # unit suites only, no Docker
golangci-lint run ./...   # config mirrors github.com/cerbos/cerbos
golangci-lint fmt ./...
```

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
```bash
# Run from the adapter directory, in a checkout of the whole repository: the harnesses read the
# shared corpus at ../conformance/. Each adapter commits its own Gradle wrapper; JDK 17+.
./gradlew build
```

Spring-data runs every suite on H2; CI adds an `ADAPTER_TEST_ORM=next` leg (Hibernate 7 / Spring
Data JPA 4), and runs the conformance suite on PostgreSQL and MySQL (`ADAPTER_TEST_DB`,
testcontainers; MySQL with client- and server-side prepared statements) under both ORM sets. On elasticsearch-java, `ElasticsearchAdversarialConformanceTest` and
`ElasticsearchSurfaceTest` need Docker. The surface test measures the store facts most of that
adapter's ledger reasons cite (an empty array or a JSON null is not indexed; an analyzed field is
compared per token), since a harness only ever sees the refusal, never the mechanism.

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
under negation — has historically shipped identically to more than one adapter. It holds:

- **cases** (`cases/<area>.yaml`), one Cerbos action each, named `<area>/<operator>/<variant>`,
  each with a **tier** (`core`, `extended`, `adversarial`), an intent and a trap;
- **one dataset**: `seeds.json` and `derived-fields.json`, projected into `resources.json`;
- **two pinned PDPs** in `pdp-versions.json`: `current` (N) and `previous` (N-1), tag and digest;
- **goldens** (`golden/<tag>/<case id>.json`), written by the generator: for each PDP, the plan it
  returned and the seed ids `check()` allowed. `golden/CHANGES.md` is the previous → current diff.

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
conformance/scripts/validate-corpus.sh      # offline: ledgers, pins, vendored Go tree; every adapter's CI
conformance/scripts/bump-pdp.sh [tag]       # run locally: current -> previous, re-record (default: latest release)
```

A disagreement between the plan and `check()`, which no adapter can pass, is declared **once**, as
`plannerDivergence` on the case, whether it is a planner bug or the two calls answering different
questions (an omitted attribute is unknown to the planner and absent to `check()`), optionally scoped to PDP tags, and every harness skips it for that
tag. An empty or total oracle is only legal when the case declares `degenerate` with its reason; the
generator fails on an undeclared one, which usually means a discriminating seed is missing.

Where `resources.json` omits an attribute on a row (a NULL column, or an absent `parent` hop), an
adapter that has a null convention declares that attribute *omitted* in its harness mapping. A
`null` literal compared against it is then a missing-attribute error that CEL denies, so the adapter
throws rather than emit `IS NULL`, unless its store can tell missing from null
([ADR 0004](docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md)).

The PDP pin lives in `conformance/pdp-versions.json` and nowhere else. Files that cannot read JSON
(Compose files, the Go modules' `cerbos/api/genpb`) restate `current`; `validate-corpus.sh`
asserts every restatement agrees on **both** tag and digest, and `verify-cerbos-digest.sh` asserts
each pinned digest is what its tag resolves to. A bump is done locally with `bump-pdp.sh` and opened as a PR
with `golden/CHANGES.md` as its body.

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

Every conformance harness imports its adapter from source, which leaves the published surface —
`exports` maps, type declarations, `files` allowlists, peer ranges, POM scopes — executed nowhere.
Each adapter's `example/` installs the packed artifact
([ADR 0002](docs/adr/0002-examples-install-the-packed-artifact.md); the Go examples use a `replace`
directive and prove usage shapes only) and exercises five usage shapes, of which the load-bearing one
is the adapter's filter composed with an application-owned filter. The examples are the one place a
live PDP still runs: `demo/docker-compose.yml`, pinned to `current` in
`conformance/pdp-versions.json`, which `validate-demo.sh` asserts.

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

## Code Style

- TypeScript: 2-space indent, camelCase functions, PascalCase types, ESM-friendly
- Python: Black (88 cols, 4-space), isort-controlled imports
- Java: 4-space indent, Java 17+, sealed interfaces, pattern matching
- Tests: co-located as `*.test.ts` in `src/` (TS), `tests/test_*.py` (Python), or `src/test/` (Java)

## Commits & Pull Requests

Conventional Commits: `feat(prisma):`, `fix(mongoose):`, `chore(deps):`. Scope is the adapter name. Keep commits focused, and regenerate build artifacts within the same commit when they change.

For pull requests: give a concise summary, note the affected adapters, link related Cerbos issues, and attach logs for significant behaviour changes. Confirm the relevant build and test commands pass, and call out any services a reviewer needs to reproduce locally. When a change alters what an adapter can translate, say so explicitly and document it as a breaking change — a shape that used to return a filter and now throws is a consumer-visible break, even when the old filter was wrong.

## CI

Each adapter has its own GitHub Actions workflow triggered by changes in its directory or `/conformance/` — plus `/demo/` where that adapter has an example. Every one of those workflows, `conformance.yaml` included, ends its `paths` with `!**/*.md`: no script, harness or build reads Markdown, so a prose-only change runs no CI. Keep that negation last in any new workflow, since a later positive pattern re-includes what it excluded, and drop it for good if a script ever starts reading a Markdown file.

Every adapter workflow runs `validate-corpus.sh` and its conformance harness **inside the same job as the regular tests**, and no adapter workflow starts a PDP. Convex is the one exception to the single job, and not by choice: its harness imports `convex/_generated`, which only exists once a live backend has been deployed to, so the corpus leg lives in the job that does the deploy and the codegen. On the TypeScript adapters the harness is gated to the baseline Node leg (`if: matrix.node-version == '22'`), because the corpus discriminates the translator and the datastore, not the Node runtime. The other matrix dimensions divide into two kinds:

- **The datastore is one.** Drizzle, Prisma and ActiveRecord run the corpus once per `ADAPTER_TEST_DB` store (SQLite, PostgreSQL, MySQL), and SQLAlchemy's harness runs all three in one `pdm run test` — collation, LIKE escaping, cast targets and parameter typing are translator behaviour, so a store the workflow does not execute is a store the adapter does not cover. MongoDB server version is the mongoose and mongodb-ruby equivalent, and it exists only on the baseline Node or Ruby leg.
- **The client engine is not, on its own.** Prisma's v6/v7 dimension crosses with the store dimension, giving six conformance runs per Prisma workflow, all on Node 22. Spring-data's ORM set (`baseline`, `next`) and ActiveRecord's version (8.0, 7.1) cross with their store dimensions the same way, since each renders the SQL the store executes.

Adding a store leg buys coverage; adding a Node leg does not. The PDP is not a dimension of any adapter workflow: every harness replays both pinned PDPs' goldens in one run. `conformance.yaml` is the only workflow that starts a PDP: it runs `validate-corpus.sh`, `verify-cerbos-digest.sh`, vets and `gofmt`-checks the generator, and runs `go -C conformance/generator run . -check`.

Every PR-triggered workflow declares a `concurrency` group that cancels a pull request's superseded run; give a new workflow the same block. Adapter workflows never run on `main`, and a cache written from a pull request is visible to that pull request alone, so `warm-caches.yaml` writes the npm, Go and Gradle caches on `main` for every pull request to restore. It can only do that under the keys the jobs look up, so a job's `cache-dependency-path` (and, for Go, its `go-version-file`) must match the entry in `warm-caches.yaml` — the generator's included. Gradle jobs cache through `setup-java`'s `cache: gradle` with `setup-gradle`'s own cache disabled, because `setup-gradle` keys its entries by job id and writes them only on `main`.

Npm releases use `<package-name>@v<version>` tags (for example, `@cerbos/orm-prisma@v5.0.0`),
as declared in each `*-publish.yaml` workflow. The publish workflow calls the adapter's test
workflow at the tagged commit and publishes only after its full matrix, conformance suite and
packaged example succeed. Adapter test workflows run directly on pull requests and through
`workflow_call` for releases, so a release runs the checks once. Keep the publish workflow
filenames stable: npm trusted publishing is configured against them.

Other release tags: `sqla/v*` -> PyPI, `activerecord/v*` -> RubyGems; `ent/v*` and `pgx/v*` are Go
module tags resolved directly from the repository. `elasticsearch-java/v*` and `spring-data/v*` only run that adapter's CI
workflow: neither build configures a Maven Central release (both are `publishToMavenLocal` only, and their `publishing` blocks
say what wiring a release still needs), so no Maven Central publish is wired yet.

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
- Adapters share data, not code: the corpus loader each adapter carries (`<adapter>/src/corpus.ts`, `sqlalchemy/tests/corpus.py`, `activerecord/spec/support/conformance_corpus.rb`, `mongodb-ruby/spec/support/conformance_corpus.rb`, the Java `Corpus.java` files, `ent/corpus_test.go`, `pgx/corpus_test.go`) is duplicated **deliberately**, so every adapter stays standalone. Do not extract a shared loader, and do not add a drift check between the copies. That is the opposite of the byte-identical rule on the vendored Go *translator* trees. See [ADR 0007](docs/adr/0007-adapters-share-data-not-code.md)
- A harness passes corpus data through verbatim: one mapping for every case, no per-case options, no hand-projected subset of the dataset
- Write "every adapter" / "every harness" / "every example" wherever prose spans the roster — in docs, test-file comments and JSON `description`s alike. The roster is the set of directories holding a `conformance-ledger.json`, so the phrasing stays true when it changes. Genuine counts of something else (cases, seed rows) go in digits
- Changing what an adapter can translate means updating its `conformance-ledger.json` and its README contract table in the same commit
- When an adapter cannot express a shape, make it throw its refusal type with a message naming the real mechanism — never emit a best-effort filter

## Agent skills

### Issue tracker

GitHub Issues on `cerbos/query-plan-adapters`, via the `gh` CLI. Tag every affected adapter with its
per-adapter label, or `conformance` for corpus-wide work. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, mapped to this tracker's label strings in `docs/agents/triage-labels.md` (`ready-for-agent` is `ready-for-implementation` here).

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
