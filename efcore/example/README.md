# EF Core example application

## Running it

From the repository root:

```bash
demo/scripts/run-example.sh efcore
```

Needs Docker with compose (for the PDP), `jq`, `unzip` and the .NET 10 SDK. The runner starts the
pinned PDP over `demo/policies/`, sets `CERBOS_HOST`, runs `run.sh`, and diffs the JSON it prints
against `demo/expected.json`.

## What it demonstrates

This is the EF Core instance of the shared [demo domain](../../demo/README.md). It proves
**plumbing**: the packed `.nupkg` restores, and its predicate composes with `Where`, `OrderBy` and
`Skip`/`Take`. Semantics are proved by the conformance suite against
[`../../conformance/`](../../conformance/).

`Program.cs` seeds in-memory SQLite from `demo/seeds.json`, maps `ownerId` to `OwnerId` and
`public` to `IsPublic`, then emits the five usage shapes:

1. **filtered**: the adapter's predicate is the whole query.
2. **alwaysAllowed**: an unconditional allow returns every row through the same code path.
3. **alwaysDenied**: an action with no rule returns nothing.
4. **paginated**: `OrderBy(d => d.Id).Skip(...).Take(...)` over the predicate, page by page.
5. **composed**: the adapter's predicate and the application's own `Where` from
   `demo/seeds.json`; the query runs even for a denial, to show the application filter cannot
   bring a denied row back.

| File | Role |
| --- | --- |
| `run.sh` | Packs the adapter, builds `Example.csproj` against that package, runs it. The only file the shared runner calls. |
| `Program.cs` | The program. |
| `Example.csproj` | Resolves `Cerbos.QueryPlan.EntityFrameworkCore` from `packages/` at the version `run.sh` packed. |

## Packaging checks

- **The adapter comes from the packed artifact**
  ([ADR 0002](../../docs/adr/0002-examples-install-the-packed-artifact.md)). `run.sh` packs it
  under a version no feed carries (`0.0.0-example.<timestamp>`) and pins `Example.csproj` to exactly
  that version, so restore can only take the fresh `.nupkg`, never a project reference or a cached
  copy.
- **The package must not carry the example.** `run.sh` lists the `.nupkg` and fails if it does.
