# .NET / Entity Framework Adapter

This repository does not ship an Entity Framework (or any other .NET) query plan adapter.

## Why this is out of scope

The cost of an adapter here is not the translator — it is the proof obligation that comes
with it. Every adapter in this repository must:

- carry a per-adapter classification of **all 140 actions** in `conformance/actions.json`,
  declaring for each one whether the adapter translates it, cannot express it (and therefore
  throws), or hits an upstream planner divergence;
- run a differential harness that plans against a real PDP, executes the translated query
  against a real store, and compares the returned ids against per-row `check()` decisions;
- own a CI workflow that replays that harness, validates the corpus, and runs a degeneracy
  guard so no action can pass vacuously;
- own a publish path and a README `Conformance contract` table kept in sync with the corpus.

The ten adapters live in four toolchains — TypeScript, Python, Go and Java — and each new
adapter in an existing toolchain can copy a working harness. .NET has none of that. It would be
a fifth toolchain, a fifth CI shape, a fifth publish target (NuGet), and a from-scratch corpus
harness with no sibling to copy from.

There is a second, sharper reason. The repository's central invariant is that **a shape an
adapter cannot express must throw, never emit a filter** — a wrong filter is an authorization
bug that returns rows the PDP denies. Holding that line requires maintainers who can read the
target query language closely enough to know when a translation is merely plausible. EF Core's
`IQueryable` expression-tree surface, its provider-specific translation fallbacks, and its
silent client-side evaluation history make that a demanding surface to hold fail-closed, and
this repository has no .NET maintainership to hold it.

Consumers who need this in .NET are better served calling `PlanResources` and writing the
translation against their own schema, where they own the fail-closed decision explicitly.

## Prior requests

- #12 — ".net Entity Framework Adapter"
