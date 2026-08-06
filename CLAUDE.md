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

Prisma has version-specific tests: `npm run test:v6`, `npm run test:v7`

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
depends on nothing else in this repository, so a consumer only ever pulls in the one module. The
harnesses need Docker (testcontainers) and read the pinned PDP version from
`conformance/CERBOS_VERSION`.

### Java (Elasticsearch, Spring Data)
```bash
# Run from the REPOSITORY ROOT, not the adapter directory: the Java harnesses read the
# shared corpus at ../conformance/ (seeds.json, actions.json, CERBOS_VERSION), so the whole
# repo must be mounted or they fail with FileNotFoundException.
# The docker socket mount is for the testcontainers-backed tests (cerbos PDP + DBs).
docker run --rm -v "$(pwd)":/repo -v /var/run/docker.sock:/var/run/docker.sock \
  -e TESTCONTAINERS_RYUK_DISABLED=true --network host \
  -w /repo/elasticsearch-java gradle:8.12-jdk17 gradle build --no-daemon
```

## Testing

All TypeScript tests run behind a Cerbos sidecar:
```bash
cerbos run --set=storage.disk.directory=../policies -- jest src/**.test.ts
```

Cerbos CLI must be installed locally. Shared policies live in `/policies/`.

Some adapters need additional services:
- Mongoose: `npm run mongo` (Docker MongoDB)
- Convex: `npm run convex:up` (Docker Convex backend)
- LangChain/ChromaDB: Docker ChromaDB on port 8234

## Conformance

`conformance/` is the shared adversarial corpus every adapter is proved against: one hostile policy
suite, one set of hostile seed rows, one classification ledger, and golden planner wire fixtures.
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
pdm run test                          # SQLAlchemy (includes the adversarial suite)
gradle test                           # Java adapters (mount the repo root, see above)
conformance/scripts/validate-corpus.sh          # corpus integrity; runs in every adapter's CI
conformance/scripts/regenerate-wire-fixtures.sh # after bumping conformance/CERBOS_VERSION
```

The PDP version is pinned in `conformance/CERBOS_VERSION` and read by every workflow — never
hardcode it elsewhere; `validate-corpus.sh` asserts the spring-data copies agree.

**Read [conformance/README.md](conformance/README.md) before changing corpus behaviour.** It covers
the oracle recipe, the NULL conventions, the degeneracy guard, how to add a hostile shape, and how
to add or onboard an adapter.

## Code Style

- TypeScript: 2-space indent, camelCase functions, PascalCase types, ESM-friendly
- Python: Black (88 cols, 4-space), isort-controlled imports
- Java: 4-space indent, Java 17+, sealed interfaces, pattern matching
- Tests: co-located as `*.test.ts` in `src/` (TS), `tests/test_*.py` (Python), or `src/test/` (Java)

## Commits & Pull Requests

Conventional Commits: `feat(prisma):`, `fix(mongoose):`, `chore(deps):`. Scope is the adapter name. Keep commits focused, and regenerate build artifacts within the same commit when they change.

For pull requests: give a concise summary, note the affected adapters, link related Cerbos issues, and attach logs for significant behaviour changes. Confirm the relevant build and test commands pass, and call out any services a reviewer needs to reproduce locally. When a change alters what an adapter can translate, say so explicitly and document it as a breaking change — a shape that used to return a filter and now throws is a consumer-visible break, even when the old filter was wrong.

## CI

Each adapter has its own GitHub Actions workflow triggered by changes in its directory, `/policies/`, or `/conformance/`. Matrix tests across Node versions (22, 24, 25) and relevant service versions. All ten adapter workflows validate the corpus and run their adversarial suite **inside the same job as the regular tests** — there is no separate `adversarial` job. On the TypeScript adapters the adversarial step is gated to the baseline Node leg (`if: matrix.node-version == '22'`), because the corpus discriminates the translator and the datastore, not the Node runtime; the other matrix dimensions (Prisma major, MongoDB server version) still get their own adversarial run. Adding a new adversarial job — or dropping that gate so the corpus replays on every Node leg — multiplies runner minutes for no extra coverage. `conformance.yaml` additionally replans the golden wire fixtures against the pinned PDP and fails on drift.

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
   upstream planner bug (`knownDivergences`).
5. **Bump the per-harness tripwires deliberately** — corpus size, oracle/throwing counts, and the
   degeneracy-guard action list. Add the new action to that guard so it cannot pass vacuously.
   The exception is an action whose oracle is empty *by construction* (a `nullRepresentationOmitted`
   probe): the guard asserts a non-empty, non-total oracle, so such an action must stay out of it
   and carry a different anti-vacuity assertion — one pinning *why* its rejection is required, not
   merely that a rejection happens. See `conformance/README.md`.
6. **Update the affected READMEs' `Conformance contract` tables** in the same commit.

A per-adapter unit test is not a substitute for a corpus action. Unit tests pin the filter an
adapter emits; only the corpus proves that filter returns the rows the PDP actually allows, and
only the corpus asks the same question of every other adapter.

Watch for harnesses that hand-project corpus data into a narrower local shape (a principal
attribute allowlist, a fixed column list). A projection silently drops anything a new action
depends on, and because the same projected input feeds both the plan and the check() oracle, the
two agree and the action passes vacuously. Pass corpus data through verbatim.

## Working with Adapters

- Edit only `src/` — never commit `lib/` until tests pass
- Shared policies in `/policies/` affect all adapters; edit carefully
- `conformance/` affects all adapters too: a change there re-runs every adapter's CI, and adding an action requires classifying it for all ten
- Regenerate build artifacts in the same commit as source changes
- Changing what an adapter can translate means updating its `conformance/actions.json` entry and its README contract table in the same commit
- When an adapter cannot express a shape, make it throw with a message naming the real mechanism — never emit a best-effort filter

## Agent skills

### Issue tracker

GitHub Issues on `cerbos/query-plan-adapters`, via the `gh` CLI. Tag every affected adapter with its
per-adapter label, or `conformance` for corpus-wide work. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical roles, each label string equal to its name. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: `CONTEXT.md` + `docs/adr/` at the repo root. See `docs/agents/domain.md`.
