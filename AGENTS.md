# Repository Guidelines

## Project Structure & Module Organization
The repository hosts language-specific adapters. TypeScript adapters live in `prisma/` and `mongoose/`, while the Python implementation lives in `sqlalchemy/`. Each adapter keeps source under `src/` and publishes compiled artifacts into `lib/` (JavaScript) or the `build` directory implied by the toolchain—edit `src/` only and regenerate outputs via the build scripts. Shared authorization policies reside under `/policies/`, with adapter-specific copies bundled inside each package. Integration fixtures such as Mongo data live in `mongoose/data/`. The root `src/` folder is currently reserved for shared code; coordinate before populating it.

## Build, Test, and Development Commands
Run commands from the adapter directory you are touching.
- Prisma: `npm run build` cleans and compiles CJS/ESM bundles plus type definitions; `npm test` pushes a local Prisma schema, boots a Cerbos sidecar, and executes Jest.
- Mongoose: `npm run build` emits CJS/ESM outputs; `npm test` runs Jest against Cerbos; start MongoDB with `npm run mongo` if integration coverage requires a live database.
- SQLAlchemy: `pdm install` prepares the virtualenv, `pdm run test` executes pytest, and `pdm run format` applies isort + black.

## Coding Style & Naming Conventions
TypeScript sources use 2-space indentation, `camelCase` for functions, and `PascalCase` for exported types (see `prisma/src/index.ts`). Keep modules ESM-friendly and prefer small, pure helpers. Python code is formatted by Black (88 columns, 4-space indents) with isort-controlled imports; run `pdm run format` before committing. Co-locate TypeScript tests as `*.test.ts` beside the module under `src/` and keep fixtures narrowly scoped.

## Testing Guidelines
TypeScript tests rely on Cerbos via the npm scripts, so ensure the Cerbos CLI is installed locally. Avoid checking in `lib/` changes until `npm test` passes. Python tests live in `sqlalchemy/tests/`, follow the `test_*.py` pattern, and run with pytest. Expand policy fixtures when adding new authorization branches and document any external services (e.g., Mongo) needed for reproduction.

## Commit & Pull Request Guidelines
Use Conventional Commits (e.g., `feat:`, `fix:`, `chore(deps):`) as seen in recent history. Keep commits focused and regenerate build artifacts within the same commit when they change. For pull requests, provide a concise summary, note the affected adapters, link related Cerbos issues, and attach logs or screenshots for significant behavior shifts. Confirm the relevant build and test commands complete successfully and call out required services so reviewers can reproduce locally.
