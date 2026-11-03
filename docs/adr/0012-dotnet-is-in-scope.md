# .NET is in scope, starting with EF Core

Accepted. Reverses `.out-of-scope/dotnet-entity-framework.md`, which closed
[#12](https://github.com/cerbos/query-plan-adapters/issues/12).

## Context

The out-of-scope record rejected a .NET adapter for two reasons.

**The proof obligation.** Every adapter replays every corpus case against a real store, keeps a
`conformance-ledger.json`, owns a CI workflow and a publish path, and keeps its README's
`Conformance contract` table in sync. .NET would add a toolchain, a CI shape, a publish target
(NuGet) and a harness with no sibling to copy from. The record counted four toolchains; with Ruby
there are five, and .NET is the sixth.

**Holding the line.** The invariant needs maintainers who can tell a faithful translation from a
plausible one. The record called EF Core's `IQueryable` surface, its provider-specific translation
and its history of silent client-side evaluation a demanding surface to hold fail-closed.

Three things have changed since.

- **A harness is smaller than it was.** Since [ADR 0010](0010-conformance-replays-recorded-pdp-decisions.md)
  a harness replays recorded plans and decisions. What is left per adapter is store lifecycle,
  seeding, translation and query execution. The fuzzer
  ([ADR 0011](0011-fuzzing-finds-cases-the-corpus-keeps-them.md)) replays through the same harness,
  so a new adapter gets both nets at once.
- **The client-evaluation objection is dated.** Since EF Core 3.0 an expression the provider cannot
  translate in a `Where` throws `InvalidOperationException` instead of being evaluated in memory.
  Provider-specific translation is still a hazard, and the harness is what proves it per provider.
- **The ecosystem matters.** The maintainers now judge .NET strategically important. The repository
  has a Java presence (spring-data, elasticsearch-java) and no .NET one.

## Decision

**.NET adapters are in scope, under the same proof obligation as every adapter.** Nothing is
waived: a ledger, a harness against real stores for both pinned PDP versions, a CI workflow, a
NuGet publish path and a README contract table.

**The first is EF Core.** It reaches most .NET data access and emits an expression the caller
composes into their own `IQueryable`.

**Every supported provider is a harness leg.** A provider is supported only once the corpus has run
against its real engine. The first legs are SQL Server, PostgreSQL (Npgsql) and SQLite.

**The adapter refuses up front.** EF's query-time translation exception fails closed, but it is not
the adapter's refusal type and it arrives after the caller has built a query. The adapter emits only
expressions every supported provider translates server-side, and throws its refusal type for
anything else. The harness counts a provider translation exception as a divergence, not a refusal.

## Consequences

**SQL Server is new ground for the corpus**, and it brings hazards no current store has:

- the default collation is case-insensitive, so the adapter requires a binary collation on mapped
  string columns, as spring-data requires a byte-exact one;
- `=` ignores trailing spaces (`'a' = 'a '` is true), and so does `LEN`, which affects `size()`;
- `[`, `]` and `^` are `LIKE` metacharacters beside `%` and `_`;
- regular expressions arrive only with SQL Server 2025's `REGEXP_LIKE`, whose semantics would have
  to be proved against RE2 before `matches` translates.

Where the corpus has no case exercising one of these, the case lands in the corpus first.

**The SQL Server image is amd64 only.** On Apple Silicon it runs under emulation, which makes local
harness runs slower there. CI is unaffected.

**Raw SQL for .NET (Dapper) waits.** A second translator over the same SQL Server ground is the
marginal-coverage case `.out-of-scope/typeorm.md` describes, until the EF Core adapter has landed and
demand for raw SQL is shown.
