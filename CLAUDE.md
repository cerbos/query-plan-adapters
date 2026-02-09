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
| elasticsearch-java | Java | `cerbos-elasticsearch` | Elasticsearch |

## Commands

Run from the adapter directory:

### TypeScript adapters
```bash
npm install
npm run build    # tsc --build -> lib/
npm test         # Jest + Cerbos sidecar
```

Prisma has version-specific tests: `npm run test:v6`, `npm run test:v7`

### Python (SQLAlchemy)
```bash
pdm install
pdm run test     # pytest
pdm run format   # isort + black
```

### Java (Elasticsearch)
```bash
docker run --rm -v "$(pwd)":/app -w /app gradle:8.12-jdk17 gradle build --no-daemon
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

## Code Style

- TypeScript: 2-space indent, camelCase functions, PascalCase types, ESM-friendly
- Python: Black (88 cols, 4-space), isort-controlled imports
- Java: 4-space indent, Java 17+, sealed interfaces, pattern matching
- Tests: co-located as `*.test.ts` in `src/` (TS), `tests/test_*.py` (Python), or `src/test/` (Java)

## Commits

Conventional Commits: `feat(prisma):`, `fix(mongoose):`, `chore(deps):`. Scope is the adapter name.

## CI

Each adapter has its own GitHub Actions workflow triggered by changes in its directory or `/policies/`. Matrix tests across Node versions (20, 22, 24, 25) and relevant service versions.

Tag-based publishing: `prisma/v*` -> npm, `sqla/v*` -> PyPI, `elasticsearch-java/v*` -> Maven Central.

## Working with Adapters

- Edit only `src/` — never commit `lib/` until tests pass
- Shared policies in `/policies/` affect all adapters; edit carefully
- Regenerate build artifacts in the same commit as source changes
