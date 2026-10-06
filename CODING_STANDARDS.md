# Coding standards

Judgement-call rules a reviewer checks a diff against. Mechanical rules belong in a linter or a CI
check, not here.

## Code style

- TypeScript: 2-space indent, camelCase functions, PascalCase types, ESM-friendly. No formatter
  enforces this, so match the surrounding file.
- Java: 4-space indent, Java 17+; model closed sets with sealed interfaces and pattern matching.
- Tests sit beside the code: `*.test.ts` in `src/` (TypeScript), `tests/test_*.py` (Python),
  `src/test/` (Java).

Python style is enforced by `pdm run format` and `pdm run lint` (isort and ruff), which CI runs.

## Commits

- A commit that changes a generated file carries its regeneration: the generator's outputs under
  `conformance/` and any other committed build artifact land in the same commit as their source.

## Pull requests

- Name the affected adapters, link the related Cerbos issues, and attach logs for a significant
  behaviour change.
- Name any service a reviewer needs to reproduce the change locally.
- A change to what an adapter can translate is stated explicitly and documented as a **breaking
  change**: a shape that used to return a filter and now throws is a consumer-visible break, even
  when the old filter was wrong.
