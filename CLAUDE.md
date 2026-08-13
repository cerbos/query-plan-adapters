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
npm test         # Jest + Cerbos sidecar
npm run test:adversarial  # differential suite against the shared conformance corpus
```

On prisma, mongoose and drizzle, `npm test` is the **translator unit test**: it reads its plans from
`conformance/wire-fixtures/`, asserts the emitted filter, and needs no sidecar, no database and no
generated client ([ADR 0006](docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)).
On prisma it is engine-agnostic, so it has no v6/v7 split; the Prisma major is still a dimension of
`npm run typecheck` and of the adversarial legs. On mongoose it never opens a connection, so the
MongoDB server dimension applies to the adversarial leg alone. On drizzle the expected filters are
**golden expectations** — static data in `drizzle/golden/expectations.json`, rewritten by
`npm run golden:update` and reviewed as a diff — which is the format
[#379](https://github.com/cerbos/query-plan-adapters/issues/379) piloted and the remaining adapters
copy; prisma and mongoose keep their inline expectations until they are retrofitted. The schema is
in `conformance/README.md`, "Golden expectations"; the principle is
[ADR 0007](docs/adr/0007-adapters-share-data-not-code.md). The remaining TypeScript adapters have
not been converted yet ([#380](https://github.com/cerbos/query-plan-adapters/issues/380) onwards)
and still run their shared-policy suite behind a sidecar.

Drizzle and Prisma also replay the corpus against a real PostgreSQL server (testcontainers, so
Docker is required): `npm run test:adversarial:postgres`, and `…:postgres:v6` / `…:postgres:v7` on
Prisma. The store is chosen with `ADAPTER_TEST_DB` (`sqlite` by default); an unknown value fails
rather than falling back.

### Python (SQLAlchemy)
```bash
pdm install
pdm run test     # pytest
pdm run format   # isort + black
```

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

### Java (Elasticsearch, Spring Data)
```bash
# Run from the REPOSITORY ROOT, not the adapter directory: the Java harnesses read the
# shared corpus at ../conformance/ (seeds.json, actions.json, CERBOS_VERSION,
# CERBOS_IMAGE_DIGEST), so the whole
# repo must be mounted or they fail with FileNotFoundException.
# The docker socket mount is for the testcontainers-backed tests (cerbos PDP + DBs).
docker run --rm -v "$(pwd)":/repo -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_RYUK_DISABLED=true --network host \
  -w /repo/elasticsearch-java gradle:8.12-jdk17 gradle build --no-daemon
```

## Testing

Most TypeScript tests run behind a Cerbos sidecar:
```bash
cerbos run --set=storage.disk.directory=../policies -- jest src/**.test.ts
```

Cerbos CLI must be installed locally. Shared policies live in `/policies/`. Prisma, mongoose and
drizzle are the exceptions: their `npm test` needs no sidecar (see above), and only their
adversarial suite starts one — against `conformance/policies/`, not `/policies/`. None of those
three directories reads `/policies/` at all any more, which is why none of their workflows lists it
as a trigger path.

Some adapters need additional services:
- Mongoose: `npm run mongo` (Docker MongoDB)
- Convex: `npm run convex:up` (Docker Convex backend)
- LangChain/ChromaDB: Docker ChromaDB on port 8234
- Drizzle and Prisma, PostgreSQL adversarial leg only: Docker (testcontainers starts it)

## Conformance

`conformance/` is the shared adversarial corpus every adapter is proved against: one hostile policy
suite, one set of hostile seed rows, one derived-field table, one classification ledger, and golden
planner wire fixtures.
It exists because the same semantic bug — value-first operand inversion, LIKE metacharacter leaks,
three-valued logic under negation — has historically shipped identically to more than one adapter.

**The invariant: a shape an adapter cannot express must throw, never emit a filter.** A wrong
filter is an authorization bug that returns rows the PDP denies; a throw is a bug report. Every
per-adapter limitation is declared in `conformance/actions.json` and asserted as a throw by that
adapter's harness.

Each harness plans against a real PDP, executes the translated query against its real store, and
compares the returned ids with per-row `check()` decisions — the PDP is the oracle for both sides,
so there are no hand-written expectations.

```bash
npm run test:adversarial              # TypeScript adapters
npm run test:adversarial:postgres     # drizzle and prisma: the same corpus on real PostgreSQL
pdm run test                          # SQLAlchemy (includes the adversarial suite)
gradle test                           # Java adapters (mount the repo root, see above)
conformance/scripts/validate-corpus.sh          # corpus integrity; runs in every adapter's CI
conformance/scripts/regenerate-wire-fixtures.sh # after bumping conformance/CERBOS_VERSION
```

The PDP is pinned by `conformance/CERBOS_VERSION` (the tag) and `conformance/CERBOS_IMAGE_DIGEST`
(the build) and read by every workflow and harness — never hardcode either elsewhere.
`validate-corpus.sh` scans the whole repository and asserts every restatement agrees on **both**
halves; a right tag carrying some other build's digest reads as pinned and is not.

Every other service image (the databases, the search and vector stores) is pinned per harness, in
one constant that adapter's suites share, in the same `repo:tag@sha256:...` form. That is
deliberately not a corpus file: `conformance/**` re-runs all ten adapter workflows, so a shared
file would make bumping one adapter's server cost nine irrelevant CI runs. `validate-corpus.sh`
enforces the *rule* instead — see "Pinning service images" in `conformance/README.md`, including
what to do when you add a new service.

**Read [conformance/README.md](conformance/README.md) before changing corpus behaviour.** It covers
the oracle recipe, the NULL conventions, the degeneracy guard, the corpus's one real to-one
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

```bash
demo/scripts/run-example.sh <adapter>   # pack, install, run, diff against demo/expected.json
demo/scripts/validate-demo.sh           # corpus integrity; runs in every adapter's example job
```

The demo domain reuses `conformance/CERBOS_VERSION` and `conformance/CERBOS_IMAGE_DIGEST` and gets
**no PDP pin of its own** — one pin in the repository, reused, and `validate-demo.sh` asserts it.

Two rules that are easy to get wrong:

- **A shape needing a carve-out for one adapter is wrong for `demo/`.** There is no `actions.json`
  equivalent here and adding one is exactly what ADR 0001 rules out; the argument belongs in
  `conformance/`, where the classification buckets already exist.
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

Each adapter has its own GitHub Actions workflow triggered by changes in its directory, `/policies/`, or `/conformance/`. Matrix tests across Node versions (22, 24, 25) and relevant service versions. All ten adapter workflows validate the corpus and run their adversarial suite **inside the same job as the regular tests** — there is no separate `adversarial` job. On the TypeScript adapters the adversarial step is gated to the baseline Node leg (`if: matrix.node-version == '22'`), because the corpus discriminates the translator and the datastore, not the Node runtime; the other matrix dimensions still get their own adversarial run, and those divide into two kinds:

- **The datastore is one.** Drizzle and Prisma each run the corpus twice on the baseline Node leg, once per `ADAPTER_TEST_DB` store (SQLite, then PostgreSQL) — collation, LIKE escaping and parameter typing are translator behaviour, so a store the workflow does not execute is a store the adapter does not cover. MongoDB server version is the mongoose equivalent, and there the store dimension exists **only** on the baseline Node leg: once `npm test` became an offline translator unit test, a second server crossed with a non-baseline Node version ran byte-identical work, so the workflow `exclude`s those legs rather than paying for them.
- **The client engine is not, on its own.** Prisma's v6/v7 dimension is an engine matrix; it crosses with the store dimension, giving four adversarial runs per Prisma workflow (2 majors × 2 stores), all on Node 22.

Adding a new adversarial job — or dropping the Node gate so the corpus replays on every Node leg — multiplies runner minutes for no extra coverage. Adding a *store* leg does buy coverage; adding a Node leg does not. `conformance.yaml` additionally replans the golden wire fixtures against the pinned PDP and fails on drift.

Tag-based publishing: `prisma/v*` -> npm, `sqla/v*` -> PyPI, `elasticsearch-java/v*` and `spring-data/v*` -> Maven Central; `ent/v*` and `pgx/v*` are Go
module tags resolved directly from the repository.

## Changing how a condition is translated

**Any change to how an operator, condition, or expression shape is translated starts in the
shared corpus, not in one adapter.** The same semantic bug has repeatedly shipped identically to
several adapters because each re-derives the planner's wire contract by hand. A fix proven only
against the adapter you happened to be looking at leaves the identical bug live in the other nine.

So when you add, fix, or change the handling of any shape:

1. **Add the shape to `conformance/policies/adversarial.yaml`** as a new action, with seed data
   that discriminates it (see `conformance/README.md`, "Adding a new hostile shape"). If it needs a
   principal attribute or column that does not exist yet, add it to `conformance/seeds.json`.
2. **Classify it in `conformance/actions.json` for all ten adapters** — but only *after* running
   the harnesses. The classification is an output of the run, not an input: declaring an action
   unsupported before watching it fail is how a translatable shape gets permanently skipped.
3. **Regenerate the wire fixtures** (`conformance/scripts/regenerate-wire-fixtures.sh`) and confirm
   the diff adds only the new action. An unrelated fixture changing means the corpus edit
   perturbed an existing shape.
4. **Run every adapter's adversarial suite and triage each divergence** into exactly one of: a
   translation bug (fix it), a shape that adapter's query language genuinely cannot express (add
   to `adapterUnsupported` with a reason naming the real mechanism, and make it throw), or an
   upstream planner bug (`knownDivergences`). A fail-closed classification also needs the message
   that adapter actually raises pinned next to it — `message` on an `adapterUnsupported` entry,
   `messages.<adapter>` on an `expectedUnsupported` one. Every harness refuses to run with one
   missing, and `validate-corpus.sh` checks the key sets. Pin what the adapter says, then check it
   names the mechanism the `reason` declares; when the two disagree, the reason is usually naming a
   limitation the walk never reaches.
5. **Bump the per-harness tripwires deliberately** — corpus size, oracle/throwing counts, and the
   degeneracy-guard action lists. Add the new action to each guard so it cannot pass vacuously,
   choosing the right list per adapter: the *compared* list where that adapter translates the
   shape, the *liveness-only* list where it throws. Every entry asserts its own side of that split,
   so a guard list copied from another harness fails instead of quietly guarding nothing.
   The exception is an action whose oracle is empty *by construction* (a `nullRepresentationOmitted`
   probe): the guard asserts a non-empty, non-total oracle, so such an action must stay out of both
   lists and carry a different anti-vacuity assertion — one pinning *why* its rejection is required,
   not merely that a rejection happens. See `conformance/README.md`.
6. **Update the affected READMEs' `Conformance contract` tables** in the same commit.

A per-adapter unit test is not a substitute for a corpus action. Unit tests pin the filter an
adapter emits; only the corpus proves that filter returns the rows the PDP actually allows, and
only the corpus asks the same question of every other adapter. The one exception is a branch **CEL
itself cannot reach** — a fractional `size()` equality is a type error, so no policy can drive it
and there is no corpus action to substitute for. Prove the branch cannot be planned (compile the
shape and quote the type error), then pin it with a unit test and say so in
`conformance/README.md`; do not infer unreachability from the adapter's own code.

Watch for harnesses that hand-project corpus data into a narrower local shape (a principal
attribute allowlist, a fixed column list). A projection silently drops anything a new action
depends on, and because the same projected input feeds both the plan and the check() oracle, the
two agree and the action passes vacuously. Pass corpus data through verbatim.

Every harness declares the exact `seeds.json` keys, corpus **principal** keys (`{id, roles, attr}`
and the attribute names inside `attr`, with the two value shapes those attributes take) and
`derived-fields.json` fields it consumes and asserts set equality against the corpus, so adding a
seed field or a principal attribute fails all ten loudly instead of being dropped from both sides at
once. Adding one means updating those declarations
deliberately — that is the point of the guard, not an obstacle to route around. The derived fields
(`createdBy`, `aDouble`, `createdAt`, `scope`, `labels`) live in `conformance/derived-fields.json`;
never recompute them in a harness.

## Working with Adapters

- Edit only `src/` — never commit `lib/` until tests pass
- Shared policies in `/policies/` affect all adapters; edit carefully
- `conformance/` affects all adapters too: a change there re-runs every adapter's CI, and adding an action requires classifying it for all ten
- `demo/` likewise re-runs every adapter's example job, and adding a usage shape means implementing it in all ten examples — there is no classification bucket to opt out with
- Adding a seed row means adding its `conformance/derived-fields.json` entry in the same commit; adding a seed *field* also means widening every harness's declared key set — both are enforced, not optional
- Adapters share data, not code: the corpus loader each adapter carries (`prisma/src/corpus.ts`, `mongoose/src/corpus.ts`, `drizzle/src/corpus.ts`, `ent/corpus_test.go`, `pgx/corpus_test.go`, …) is duplicated **deliberately**, so every adapter stays standalone. Do not extract a shared loader, and do not add a drift check between the copies — they are allowed to differ. That is the opposite of the byte-identical rule on the vendored Go *translator* trees, which keeps its exact current scope. See [ADR 0007](docs/adr/0007-adapters-share-data-not-code.md)
- A per-adapter **golden expectation** — the filter one adapter is pinned to emit for one corpus action — lives in that adapter's own `golden/expectations.json`, never under `conformance/`; a throwing action carries no entry, because its message is already pinned in `conformance/actions.json`. Schema and rationale: `conformance/README.md`, "Golden expectations"
- Regenerate build artifacts in the same commit as source changes
- Changing what an adapter can translate means updating its `conformance/actions.json` entry and its README contract table in the same commit
- When an adapter cannot express a shape, make it throw with a message naming the real mechanism — never emit a best-effort filter. That message is pinned in `conformance/actions.json` and asserted, so changing it is a deliberate corpus edit

## Agent skills

### Issue tracker

GitHub Issues on `cerbos/query-plan-adapters`, via the `gh` CLI. Tag every affected adapter with its
per-adapter label, or `conformance` for corpus-wide work. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, each label string equal to its name. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
