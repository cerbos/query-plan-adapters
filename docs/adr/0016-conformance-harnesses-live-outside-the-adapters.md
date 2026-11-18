# Conformance harnesses live outside the adapters

Accepted. Amends the first consequence of [ADR 0007](0007-adapters-share-data-not-code.md).

## Context

Each adapter carried its conformance harness inside its own folder: the replay suite, the corpus
loader, the store schemas and setup, fuzz mode and `conformance-ledger.json`. Two things went wrong
with that.

- **Test code shipped.** The npm tarballs carried `src/*.test.ts` and `src/corpus.ts`, and
  `@cerbos/orm-prisma` 5.2.0 shipped the Prisma clients generated for the test schemas: 206 files,
  83 of them test material. Each fix was a negation in a `files` list, which the next test file
  slips past.
- **An adapter folder did not say what the adapter is.** A reader had to tell the adapter from the
  harness by file name, and the published surface was "everything in `src/` except a list".

[ADR 0007](0007-adapters-share-data-not-code.md) kept per-adapter expectations out of
`conformance/` because every adapter workflow triggers on `conformance/**`, so a change for one
adapter would re-run all of them.

## Decision

**An adapter folder holds the adapter and its own unit tests. Everything that proves it against the
corpus lives in `conformance/harnesses/<adapter>/` and imports the adapter from its folder.**

```
<adapter>/src/                     the shipped code, nothing else
<adapter>/test/                    the unit suite, the golden-plan reader, fixtures/ (the mapping the
                                   unit suite and the harness share)
conformance/harnesses/<adapter>/   the replay suite, conformance-ledger.json, store setup and
                                   schemas, fuzz mode, the harness's own build and test config
```

- **Imports run one way: harness → adapter.** The harness imports `src/` and `test/`; nothing in
  the adapter imports from `conformance/harnesses/`.
- **The harness runs on the adapter's toolchain and install.** It declares no dependencies of its
  own, so there is one lockfile per adapter and one copy of the ORM, and `instanceof` across the
  boundary holds. The mechanism is the language's own: a `replace` in Go, the adapter's Gemfile in
  Ruby, the adapter's virtualenv in Python, `includeBuild` in Gradle, a `ProjectReference` in .NET.
  TypeScript has none that `tsc` honours for package `exports`, so each TypeScript harness holds a
  committed `node_modules` link to its adapter's.
- **The roster is `conformance/harnesses/*/conformance-ledger.json`.**
- **Each adapter workflow excludes `conformance/harnesses/**` and re-includes its own harness**, so
  one adapter's ledger or harness change runs that adapter's workflow and `conformance.yaml` only.
  This replaces ADR 0007's rule that per-adapter expectations stay out of `conformance/`: the path
  filter now gives the isolation the folder boundary used to.

## Consequences

- A `files` list or a `tsconfig` include names `src/` and needs no negations. A test file cannot
  reach a package by being added.
- The store drivers and testcontainers stay in the adapter's dev dependencies, because the harness
  runs on that install. Each TypeScript `package.json` says so.
- The committed `node_modules` link needs `core.symlinks` on Windows. Every CI runner and every
  maintainer machine is Linux or macOS.
- Image pins (`<adapter>/<SERVICE>_IMAGE`) stay in the adapter folder: the example reads them too.
