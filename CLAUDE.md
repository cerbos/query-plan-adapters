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

Run from the adapter directory:

### TypeScript adapters
```bash
npm install
npm run build    # tsc --build -> lib/ (published surface only; test files are excluded)
npm run typecheck # tsc -p tsconfig.typecheck.json — noEmit, covers src/ AND *.test.ts
npm test         # Jest — the translator unit test, offline (no sidecar, no store)
npm run test:adversarial  # differential suite against the shared conformance corpus
```

On prisma, mongoose, drizzle, convex and langchain-chromadb, `npm test` is the **translator unit
test**: it reads its plans from `conformance/wire-fixtures/`, asserts the emitted filter and the
rest of what an adapter can be asked offline ("What a translator unit test may pin", below), and
needs no sidecar, no database and no generated client ([ADR 0006](docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)).
On prisma it is engine-agnostic, so it has no v5/v6/v7 split; the Prisma major is still a dimension of
`npm run typecheck` and of the adversarial legs. On mongoose it never opens a connection, so the
MongoDB server dimension applies to the adversarial leg alone. On convex it needs neither a Convex
backend nor `convex/_generated`, which is why the mapper it shares with the harness lives in
`convex/convex/adversarialMapper.ts` rather than beside the backend functions that import the
generated API. On langchain-chromadb it needs no ChromaDB container, so that server is started for
the adversarial leg alone.

On drizzle, convex, langchain-chromadb, sqlalchemy, activerecord, spring-data and elasticsearch-java the expected
filters are **golden expectations** — static data in `<adapter>/golden/expectations.json`, rewritten
by that adapter's `golden:update` command and reviewed as a diff — which is the format
[#379](https://github.com/cerbos/query-plan-adapters/issues/379) piloted
and the remaining adapters copy; prisma and mongoose keep their inline expectations until they are
retrofitted. Convex is the case that generalised the format: it emits a *function*, so its entry
records the calls that function makes against a recording query builder plus which half of the
output — Convex's filter engine or the adapter's in-memory post-filter — answers the query.
Langchain-chromadb is the opposite extreme and the cheapest instance: a Chroma `Where` clause is
already JSON, so its entry is the translator's whole result verbatim, and most corpus shapes carry
no entry because it refuses them. Sqlalchemy is the case that showed the value
need not be the translator's return type at all: it emits a Python expression object, so its entry
records that object *compiled* — the `WHERE` clause on SQLite and on PostgreSQL, plus the parameters
it binds, which the two dialects are asserted to share. That also makes the ORM version an input to
the asset rather than only to the tests: SQLAlchemy 1.4 and 2.x render some trees differently, so the
file declares the major it was generated under, `golden:update` refuses to run under the other one,
and the other CI leg asserts a pinned list of exactly which shapes diverge. Spring-data is the same
case in another language and shows what happens when the build has only ONE version of that
generator: it emits a JPA `Specification`, so its entry records that Specification rendered — the
root joins and the `WHERE` clause on H2, PostgreSQL and MySQL, all three of which its CI executes —
with criteria literals inlined so the operands are in the asset rather than behind a `?`. The file
declares `"hibernate": "6.6"` and the suite asserts the running Hibernate matches, but there is no
second leg and so no divergence list; the header is load-bearing because `hibernate-core` is a
`compileOnly` dependency and a consumer brings their own renderer. Elasticsearch-java is the second
adapter, after langchain-chromadb, whose value needs no rendering at all: the Query DSL IS JSON and
the adapter emits a `Map<String, Object>` of plain JDK values with no client library on the
classpath, so its entry is the translator's return value verbatim — the plan kind, plus the query
for a conditional plan — and it declares no generator. Object keys are sorted on the way in, because
the adapter builds its queries with `Map.of`, whose iteration order is randomised per JVM run; a
suite assertion pins that no library type ever reaches the asset, which is what keeps "no generator"
true. Most corpus shapes carry no entry because it refuses them. The schema is in
`conformance/README.md`, "Golden expectations"; the principle is
[ADR 0007](docs/adr/0007-adapters-share-data-not-code.md).

Drizzle and Prisma also replay the corpus against real PostgreSQL and real MySQL servers
(testcontainers, so Docker is required): `npm run test:adversarial:postgres` and
`…:adversarial:mysql`, each with a `…:v6` / `…:v7` split on Prisma. The store is chosen with
`ADAPTER_TEST_DB` (`sqlite` by default); an unknown value fails rather than falling back. The
MySQL legs pin a case- and accent-sensitive collation, because MySQL's default makes `=` itself
case-insensitive and CEL's is byte-exact — a store misconfiguration, not an adapter limitation.
`ADAPTER_TEST_MYSQL_COLLATION` replays either leg under another collation to measure what the
default costs.

### Python (SQLAlchemy)
```bash
pdm install
pdm run test           # pytest: translator unit test, get_query contract, adversarial suite
pdm run golden:update  # rewrite golden/expectations.json from what the translator emits
pdm run format         # isort + black
```

`pdm run test` collects three suites. `tests/test_translator.py` is the **translator unit test** and
`tests/test_query.py` / `tests/test_relations.py` are `get_query`'s contract for plans the planner
cannot produce; none of the three starts anything, so `pdm run pytest tests/test_translator.py`
needs no PDP and no database. Only `tests/test_adversarial_conformance.py` needs Docker, and it
starts its own pinned PDP against `conformance/policies/`.

### Ruby (ActiveRecord)
```bash
# Everything runs in Docker. The PDP is pinned by tag AND digest, from
# conformance/CERBOS_VERSION and conformance/CERBOS_IMAGE_DIGEST, which scripts/test.sh reads.
cd activerecord
./scripts/test.sh                                      # all three suites
./scripts/test.sh spec/translator_spec.rb              # offline: no PDP, no database server
./scripts/golden-update.sh                             # rewrite golden/expectations.json
RUBY_VERSION=3.2 ACTIVERECORD_VERSION=7.1 ./scripts/test.sh
./scripts/lint.sh
```

`spec/translator_spec.rb` is the **translator unit test** and `spec/adapter_contract_spec.rb` is
the caller-supplied contract — mapper forms, operator overrides, the per-call null
representation, and the association shapes the adapter refuses to guess at. Neither starts a PDP;
their models are SQLite in memory. Only `spec/adversarial_conformance_spec.rb` needs Docker, and
it starts its own pinned PDP against `conformance/policies/`.

Its golden expectations record the emitted relation **rendered as SQL** — `to_sql` against
SQLite, with literals inlined, so the operands are in the asset rather than behind a `?`. That
makes ActiveRecord's own renderer an input to the bytes, so the file declares
`"activerecord": "8.0"`, `golden-update.sh` refuses to run under another minor series, and the
7.1 leg asserts a pinned divergence list in both directions. The Gemfile pins each CI leg to one
minor series for that reason: a floating `~> 7.1` resolves to the newest 7.x, and the leg named
7.1 would quietly become 7.2.

### Go (Ent, pgx)
```bash
go test ./...             # includes the adversarial suite; starts its own containers
golangci-lint run ./...   # config mirrors github.com/cerbos/cerbos
golangci-lint fmt ./...
```

Both Go modules are standalone: each vendors its own translator under `internal/queryplan` and
depends on nothing else in this repository, so a consumer only ever pulls in the one module. The two
vendored trees are held **byte-identical** and diffed by `validate-corpus.sh` — a semantic fix has to
land in both copies, and anything genuinely per-engine goes in that module's `render.go`, outside the
shared tree. Their unit suites (`translate_test.go`, `render_test.go`) mirror each other for the same
reason and need no Docker: `go test -skip TestAdversarialConformance ./...`. The adversarial harnesses
do need Docker (testcontainers) and read the pinned PDP image from `conformance/CERBOS_VERSION` and
`conformance/CERBOS_IMAGE_DIGEST`.

The commands above are the two *published* modules, and `./...` stops at a nested `go.mod`, so
neither reaches `ent/example/` or `pgx/example/` — each its own module, and deliberately so: a
directory holding a `go.mod` is excluded from its parent's zip, which is what keeps an example's
code, its version pins and (on ent) its generator and driver dependencies out of a consumer's build.
Lint each from its own directory with the adapter's config,
`golangci-lint run --config=../.golangci.yaml ./...`, and run it with
`demo/scripts/run-example.sh <adapter>`. Both happen in that adapter's `example` job, which is also
why each adapter's `.golangci.yaml` carries a `gomoddirectives` exclusion scoped to
`^example/go\.mod$` — the `replace` directive an example needs, without excusing one in the adapter.

The PostgreSQL server both pgx suites use is pinned in `pgx/POSTGRES_IMAGE`, read by
`pgx/adversarial_test.go` and `pgx/example/run.sh`, on the same argument as
`langchain-chromadb/CHROMA_IMAGE` and `mongoose/MONGO_IMAGE`: `validate-corpus.sh` holds one digest
per tag, and nothing holds two tags equal, so a second copy could be left behind on an older server
and stay green.

### Java (Elasticsearch, Spring Data)
```bash
# Run from the REPOSITORY ROOT, not the adapter directory: the Java harnesses read the
# shared corpus at ../conformance/ (seeds.json, catalog.json, check-resources.json, CERBOS_VERSION,
# CERBOS_IMAGE_DIGEST), so the whole
# repo must be mounted or they fail with FileNotFoundException.
# The docker socket mount is for the testcontainers-backed tests (cerbos PDP + DBs).
docker run --rm -v "$(pwd)":/repo -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_RYUK_DISABLED=true --network host \
  -w /repo/elasticsearch-java gradle:8.12-jdk17 gradle build --no-daemon

# Both: rewrite golden/expectations.json from what the translator emits today.
# `gradle test` never regenerates, so a translator change fails CI whatever anyone ran locally.
#   … -w /repo/spring-data        gradle:8.12-jdk17 gradle goldenUpdate --no-daemon
#   … -w /repo/elasticsearch-java gradle:8.12-jdk17 gradle goldenUpdate --no-daemon
```

Both Java adapters have a **translator unit test** that reads its plans from
`conformance/wire-fixtures/` and asserts the emitted filter against that adapter's
`golden/expectations.json` — and, as everywhere, the rest of what an adapter can be asked offline
("What a translator unit test may pin", below) — with no sidecar and no store. On spring-data,
`SpringDataTranslatorTest` needs no database — its persistence unit carries no JDBC connection at
all, and Hibernate is told the dialect rather than discovering it. On elasticsearch-java,
`ElasticsearchTranslatorTest` needs no Elasticsearch, and reads as mostly-throws: each fail-closed
shape is asserted against the message in `elasticsearch-java/adapterctl.json`.

Two suites on elasticsearch-java need Docker, and they need different things:
`ElasticsearchAdversarialConformanceTest` starts a pinned PDP and Elasticsearch;
`ElasticsearchSurfaceTest` starts Elasticsearch alone, to execute an emitted clause against a real
server and to measure the store facts most of that adapter's rejected outcome reasons cite — an
empty array is not indexed, a JSON null is not indexed, an analyzed field is compared per token. A
harness can only ever see the refusal, never the mechanism.

## Testing

Every adapter that had a shared-policy suite now runs a **translator unit test** in its place: it
reads its plans from `conformance/wire-fixtures/` and needs no sidecar and no store (see above, and
"What a translator unit test may pin" below for what it is allowed to assert). Ent and pgx never had
one — their mirrored `translate_test.go` / `render_test.go` suites hand-build their plans and always
did, and porting them is not part of
[#372](https://github.com/cerbos/query-plan-adapters/issues/372).

**`conformance/policies/` is the repository's only policy suite for semantics**, and every
adversarial suite starts a PDP loaded with it. The shared policy suite that used to sit at the
repository root is gone: nothing plans against it, and no workflow gates on it
([ADR 0008](docs/adr/0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md)).
The other policy suites in the repository prove **plumbing**, not semantics, and neither is a place
to put a new shape: `demo/policies/` feeds every example application, and
`spring-data/example/policies/` is that adapter's onboarding artifact. A shape worth proving is a
corpus action.

Some adapters need additional services:
- Mongoose: `npm run mongo` (Docker MongoDB)
- Convex: `npm run convex:up` (Docker Convex backend)
- LangChain/ChromaDB, adversarial leg only: Docker ChromaDB on port 8234 (`npm run chroma`)
- Drizzle and Prisma, PostgreSQL adversarial leg only: Docker (testcontainers starts it)

## Conformance

`conformance/` is the shared adversarial corpus every adapter is proved against: one hostile policy
suite, one set of hostile seed rows, one derived-field table, one action catalog, adapter-local
direct outcomes, canonical check resources, and golden planner wire fixtures.
It exists because the same semantic bug — value-first operand inversion, LIKE metacharacter leaks,
three-valued logic under negation — has historically shipped identically to more than one adapter.

**The invariant: a shape an adapter cannot express must throw, never emit a filter.** A wrong
filter is an authorization bug that returns rows the PDP denies; a throw is a bug report. Every
per-adapter limitation is declared as a `rejected` outcome in that adapter's `adapterctl.json` and
asserted as a throw by its harness.

Each harness plans against a real PDP, executes the translated query against its real store, and
compares the returned ids with per-row `check()` decisions — the PDP is the oracle for both sides,
so there are no hand-written expectations.

```bash
npm run test:adversarial              # TypeScript adapters
npm run test:adversarial:postgres     # drizzle and prisma: the same corpus on real PostgreSQL
npm run test:adversarial:mysql        # drizzle and prisma: the same corpus on real MySQL
pdm run test                          # SQLAlchemy (includes the adversarial suite)
gradle test                           # Java adapters (mount the repo root, see above)
conformance/scripts/validate-corpus.sh          # corpus integrity; runs in every adapter's CI
conformance/scripts/regenerate-wire-fixtures.sh # after bumping conformance/CERBOS_VERSION
conformance/scripts/check-docs.sh               # documentation invariants; runs in the Docs workflow
```

The PDP is pinned by `conformance/CERBOS_VERSION` (the tag) and `conformance/CERBOS_IMAGE_DIGEST`
(the build) and read by every workflow and harness — never hardcode either elsewhere.
`validate-corpus.sh` scans the whole repository and asserts every restatement agrees on **both**
halves; a right tag carrying some other build's digest reads as pinned and is not.

Every other service image (the databases, the search and vector stores) is pinned per harness, in
one constant that adapter's suites share, in the same `repo:tag@sha256:...` form. That is
deliberately not a corpus file: `conformance/**` re-runs every adapter workflow, so a shared file
would make bumping one adapter's server cost every other adapter an irrelevant CI run.
`validate-corpus.sh` enforces the *rule* instead — see "Pinning service images" in `conformance/README.md`, including
what to do when you add a new service.

**Read [conformance/README.md](conformance/README.md) before changing corpus behaviour.** It covers
the oracle recipe, the NULL conventions, catalog cardinality expectations, the corpus's one real to-one
relation ([ADR 0005](docs/adr/0005-the-conformance-corpus-carries-a-real-to-one-relation.md)), how
to add a hostile shape, and how to add or onboard an adapter.

## The demo domain

`demo/` is the repository's second shared corpus, and it proves a different property.
`conformance/` proves **semantics** — that a translated filter returns exactly the rows the PDP
allows — with hostile shapes and five per-adapter classification buckets. `demo/` proves
**plumbing** — that the adapter installs, imports, and composes with its ORM's real query methods
— with realistic shapes and **no per-adapter exceptions at all**
([ADR 0001](docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)).

It exists because every conformance harness imports its adapter from source (`from "."`), which
leaves the published surface — `exports` maps, type declarations, `files` allowlists, peer ranges,
POM scopes — executed nowhere, and because a harness only ever runs one flat filtered query.
Each adapter's `example/` installs the packed artifact
([ADR 0002](docs/adr/0002-examples-install-the-packed-artifact.md)) and exercises five usage
shapes, of which the load-bearing one is the adapter's filter composed with an application-owned
filter.

The Go adapters are the one exception, and ADR 0002 states it: there is no packaging step, so
`ent/example/` and `pgx/example/` resolve their adapter with a `replace` directive and prove **usage
shapes only, not packaging**. Their READMEs say so rather than implying coverage they do not have,
and what each gets in exchange is a shape no Go suite here reaches — on ent, the adapter's predicate
handed to a *generated* ent client, which `ent/adversarial_test.go` never builds; on pgx, the
`WHERE` fragment spliced into a statement the application owns, which is where that adapter's
`$n` placeholders have to be renumbered (`WithPlaceholderOffset` in one direction,
`len(Result.Args)` in the other) and where every suite in `pgx/` instead hands the fragment straight
to a `SELECT` of its own. Being a nested module is also what keeps an example's code, its version
pins and (on ent) its generator and driver dependencies out of a consumer's build: a directory
holding a `go.mod` is excluded from its parent's module zip, which is the mechanism behind "both Go
modules are standalone" above, and each example's README records the packing experiment that
verified it rather than assuming it.

```bash
demo/scripts/run-example.sh <adapter>   # pack, install, run, diff against demo/cases.json
demo/scripts/validate-demo.sh           # corpus integrity; runs in every adapter's example job
```

The demo domain reuses `conformance/CERBOS_VERSION` and `conformance/CERBOS_IMAGE_DIGEST` and gets
**no PDP pin of its own** — one pin in the repository, reused, and `validate-demo.sh` asserts it.

Two rules that are easy to get wrong:

- **A shape needing a carve-out for one adapter is wrong for `demo/`.** The argument belongs in
  `conformance/`, where direct adapter outcomes already express support and rejection.
- **Each example's job must stay inside that adapter's own workflow.** `renovate.json` automerges
  non-major bumps, so an ORM bump arrives as one PR touching both the adapter manifest and the
  example's committed lockfile — the example job on that PR is what blocks the automerge when the
  new ORM breaks real usage. A nightly or standalone workflow silently restores the gap.

**Read [demo/README.md](demo/README.md) before changing the demo domain.** It covers the five
usage shapes, the emitted JSON contract, why the expectations are hardcoded here but banned in
`conformance/`, and what each of `validate-demo.sh`'s four checks stops.

## Code Style

- TypeScript: 2-space indent, camelCase functions, PascalCase types, ESM-friendly
- Python: Black (88 cols, 4-space), isort-controlled imports
- Java: 4-space indent, Java 17+, sealed interfaces, pattern matching
- Tests: co-located as `*.test.ts` in `src/` (TS), `tests/test_*.py` (Python), or `src/test/` (Java)

## Commits & Pull Requests

Conventional Commits: `feat(prisma):`, `fix(mongoose):`, `chore(deps):`. Scope is the adapter name. Keep commits focused, and regenerate build artifacts within the same commit when they change.

For pull requests: give a concise summary, note the affected adapters, link related Cerbos issues, and attach logs for significant behaviour changes. Confirm the relevant build and test commands pass, and call out any services a reviewer needs to reproduce locally. When a change alters what an adapter can translate, say so explicitly and document it as a breaking change — a shape that used to return a filter and now throws is a consumer-visible break, even when the old filter was wrong.

## CI

Each adapter has its own GitHub Actions workflow triggered by changes in its directory or `/conformance/` — plus `/demo/` where that adapter has an example. Matrix tests across Node versions (22, 24, 25) and relevant service versions. Every adapter workflow validates the corpus and runs its adversarial suite **inside the same job as the regular tests** — there is no separate `adversarial` job. Convex is the one exception, and not by choice: its harness imports `convex/_generated`, which only exists once a live backend has been deployed to, so the corpus leg lives in the job that does the deploy and the codegen rather than putting Docker on every Node leg. On the TypeScript adapters the adversarial step is gated to the baseline Node leg (`if: matrix.node-version == '22'`), because the corpus discriminates the translator and the datastore, not the Node runtime; the other matrix dimensions still get their own adversarial run, and those divide into two kinds:

- **The datastore is one.** Drizzle and Prisma each run the corpus once per `ADAPTER_TEST_DB` store on the baseline Node leg (SQLite, PostgreSQL, MySQL) — collation, LIKE escaping, cast targets and parameter typing are translator behaviour, so a store the workflow does not execute is a store the adapter does not cover. MongoDB server version is the mongoose equivalent, and there the store dimension exists **only** on the baseline Node leg: once `npm test` became an offline translator unit test, a second server crossed with a non-baseline Node version ran byte-identical work, so the workflow `exclude`s those legs rather than paying for them.
- **The client engine is not, on its own.** Prisma's v5/v6/v7 dimension is an engine matrix; it crosses with the store dimension, giving 9 adversarial runs per Prisma workflow (3 majors × 3 stores), all on Node 22.

Adding a new adversarial job — or dropping the Node gate so the corpus replays on every Node leg — multiplies runner minutes for no extra coverage. Adding a *store* leg does buy coverage; adding a Node leg does not. `conformance.yaml` additionally replans the golden wire fixtures against the pinned PDP and fails on drift.

Tag-based publishing: `prisma/v*` -> npm, `sqla/v*` -> PyPI, `activerecord/v*` -> RubyGems, `elasticsearch-java/v*` and `spring-data/v*` -> Maven Central; `ent/v*` and `pgx/v*` are Go
module tags resolved directly from the repository.

## Changing how a condition is translated

**Any change to how an operator, condition, or expression shape is translated starts in the
shared corpus, not in one adapter.** The same semantic bug has repeatedly shipped identically to
several adapters because each re-derives the planner's wire contract by hand. A fix proven only
against the adapter you happened to be looking at leaves the identical bug live in every other
adapter.

So when you add, fix, or change the handling of any shape:

1. **Add the shape to `conformance/policies/adversarial.yaml`** as a new action, with seed data
   that discriminates it (see `conformance/README.md`, "Adding a new hostile shape"). If it needs a
   principal attribute or column that does not exist yet, add it to `conformance/seeds.json`.
2. **Add it once to `conformance/catalog.json`** with its oracle cardinality. Run
   `./adapterctl validate --discovery`; every missing adapter outcome is explicitly `unassessed`.
3. **Regenerate the wire fixtures** (`conformance/scripts/regenerate-wire-fixtures.sh`) and confirm
   the diff adds only the new action. An unrelated fixture changing means the corpus edit
   perturbed an existing shape.
4. **Run every adapter's adversarial suite and triage each divergence** into exactly one of: a
   translation bug (fix it), a shape that adapter's query language genuinely cannot express (mark
   it `rejected` in that adapter's manifest with the real mechanism and make it throw), or an
   upstream planner bug (`upstream-blocked`). A rejection also needs the error substring the
   adapter actually raises pinned on the same manifest outcome. Every harness refuses one with a
   missing message, and `adapterctl validate` checks the complete matrix. Pin what the adapter says, then check it
   names the mechanism the `reason` declares; when the two disagree, the reason is usually naming a
   limitation the walk never reaches.
5. **Resolve every direct outcome and run strict validation.** Native harnesses derive action
   accounting from catalog/manifest key equality and assert each catalog cardinality, including
   empty or total oracles. No repeated counts or per-adapter liveness lists are updated.
6. **Update the affected READMEs' `Conformance contract` tables** in the same commit.

### What a translator unit test may pin

**The load-bearing rule is unchanged: for a shape a policy can reach, a per-adapter unit test is
not a substitute for a corpus action.** Only a corpus action proves the emitted filter returns the
rows the PDP actually allows, and only the corpus asks the same question of every other adapter. A
unit test that pins a filter proves the adapter still emits what it emitted yesterday; it says
nothing about whether that filter was ever right.

What a unit test *pins*, though, is broader than the filter. A translator unit test pins whatever
the adapter can be asked **without a store**, and that is a real
list: the emitted filter, the plan kind the planner folds to, the refusal message the adapter
manifest pins *and where in the walk it is raised*, the distribution of those
refusal sites, which half of a split output answers a query, the caller-supplied contracts
(mapper forms, operator overrides, `allowPostFilter`), and the golden asset's own invariants — the
generator it declares, the command that rewrites it, that no library type ever reached it. Some of
those are properties **no corpus action can state at all**: a corpus action asks which rows come
back, and "every refusal in this adapter is raised at one of these sites, in these proportions" is
not a question about rows. Do not delete such an assertion on the grounds that the corpus covers
the shape — it does not cover the property.

Three kinds of material legitimately live only in a unit test, and they are not equal:

1. **A branch CEL itself cannot reach.** A fractional `size()` equality is a type error, so no
   policy can drive it and there is no corpus action to substitute for. Prove the branch cannot be
   planned (compile the shape and quote the type error), then pin it and say so in
   `conformance/README.md`; do not infer unreachability from the adapter's own code. Permanent.
2. **A caller-supplied argument the corpus structurally cannot vary.** Each direct outcome certifies
   one mapping per adapter, so an `OperatorFunction` override, a second mapper
   form, `allowPostFilter`, a per-call `nullAttributeRepresentation`, or `maxMacroDepth` has no
   corpus spelling — the corpus asks what a policy produces, not what a caller passes. Permanent.
3. **A corpus gap wearing a unit test** — policy-reachable, and the corpus simply does not carry it
   yet. This one is a **bridge, not a home**. It is a concession, not a licence: a shape parked here
   is pinned in exactly one adapter and asked of none of the others, which is the condition every
   bug this repository exists to stop was living in. One of the shapes currently parked this way is
   a suspected live over-grant. Each instance must say at the test that it is a corpus gap, name the
   issue tracking the port, and be deleted when the corpus action lands
   ([#414](https://github.com/cerbos/query-plan-adapters/issues/414) is the open port).
   `ElasticsearchQueryPlanAdapterTest` is the worked example: a `KIND 2 — a policy can reach these,
   and the corpus does not carry them yet` banner over the block, and a `Corpus gap.` lead on every
   test under it. Not every adapter meets that bar yet — spring-data labels at the class rather
   than per test — so a shape parked there today is not necessarily findable.

Watch for harnesses that hand-project corpus data into a narrower local shape (a principal
attribute allowlist, a fixed column list). A projection silently drops anything a new action
depends on, and because the same projected input feeds both the plan and the check() oracle, the
two agree and the action passes vacuously. Pass corpus data through verbatim.

Every harness declares the exact `seeds.json` keys, corpus **principal** keys (`{id, roles, attr}`
and the attribute names inside `attr`, with the two value shapes those attributes take) and
`derived-fields.json` fields it consumes and asserts set equality against the corpus, so adding a
seed field or a principal attribute fails every harness loudly instead of being dropped from both
sides at once. Adding one means updating those declarations deliberately — that is the point of the guard, not an obstacle to route around. The derived fields
(`createdBy`, `aDouble`, `createdAt`, `scope`, `labels`) live in `conformance/derived-fields.json`;
never recompute them in a harness.

## Working with Adapters

- Edit only `src/` — never commit `lib/` until tests pass
- There is **one policy suite for semantics**, `conformance/policies/`. A new shape is a corpus action; a second suite of easier shapes beside it is what [ADR 0008](docs/adr/0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md) exists to keep out
- `conformance/` affects all adapters: a change there re-runs every adapter's CI, and adding an action leaves every adapter explicitly unassessed until its manifest outcome is resolved
- `demo/` likewise re-runs every adapter's example job, and adding a usage shape means implementing it in every example — there is no classification bucket to opt out with
- Adding a seed row means adding its `conformance/derived-fields.json` entry in the same commit; adding a seed *field* also means widening every harness's declared key set — both are enforced, not optional
- Adapters share data, not code: the corpus loader each adapter carries (`prisma/src/corpus.ts`, `mongoose/src/corpus.ts`, `drizzle/src/corpus.ts`, `convex/src/corpus.ts`, `langchain-chromadb/src/corpus.ts`, `sqlalchemy/tests/corpus.py`, `activerecord/spec/support/conformance_corpus.rb`, `spring-data/src/test/java/dev/cerbos/queryplan/springdata/Corpus.java`, `elasticsearch-java/src/test/java/dev/cerbos/queryplan/elasticsearch/Corpus.java`, `ent/corpus_test.go`, `pgx/corpus_test.go`, …) is duplicated **deliberately**, so every adapter stays standalone. Do not extract a shared loader, and do not add a drift check between the copies — they are allowed to differ. That is the opposite of the byte-identical rule on the vendored Go *translator* trees, which keeps its exact current scope. See [ADR 0007](docs/adr/0007-adapters-share-data-not-code.md)
- A per-adapter **golden expectation** — the filter one adapter is pinned to emit for one corpus action — lives in that adapter's own `golden/expectations.json`, never under `conformance/`; a throwing action carries no entry, because its message is already pinned in that adapter's `adapterctl.json`. Schema and rationale: `conformance/README.md`, "Golden expectations"
- Write "every adapter" / "every harness" / "every example" wherever prose spans the roster — in docs, test-file comments and JSON `description`s alike. Adapter manifests are the discovered roster, so the phrasing stays true when one is added and nothing else has to count them. Generated certification docs own numeric coverage claims
- Regenerate build artifacts in the same commit as source changes
- Changing what an adapter can translate means updating its direct outcome in `adapterctl.json` and its README contract table in the same commit
- When an adapter cannot express a shape, make it throw with a message naming the real mechanism — never emit a best-effort filter. Pin that message beside the `rejected` outcome so changing it is a deliberate adapter certification edit

## Agent skills

### Issue tracker

GitHub Issues on `cerbos/query-plan-adapters`, via the `gh` CLI. Tag every affected adapter with its
per-adapter label, or `conformance` for corpus-wide work. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, each label string equal to its name. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
