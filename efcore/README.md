# Cerbos EF Core Query Plan Adapter

> [!WARNING]
> **A pre-release.** `0.1.0-alpha.1` is the first version, and the interface can change before
> `0.1.0`. The conformance results below prove the *translation*; they cannot prove that your
> attribute map points at the columns your application puts into the Cerbos attributes.

Translates a [Cerbos](https://cerbos.dev) query plan (`PlanResources`) into an
`Expression<Func<TEntity, bool>>` for Entity Framework Core, so the database applies your Cerbos
policies. The result is an ordinary predicate: pass it to `Where` and add your own filters,
ordering, paging and includes as usual.

The adapter is **fail-closed**. A plan shape it cannot translate exactly throws a
`QueryPlanRefusedException` when you call `ToFilter`, before any query reaches EF. It never emits
an approximate predicate, and it never leaves a shape for EF to fail on at query time.

## Install

```bash
dotnet add package Cerbos.QueryPlan.EntityFrameworkCore --prerelease
```

Requirements:

- .NET 10 and EF Core 10
- One of the SQL Server, PostgreSQL (Npgsql) or SQLite providers
- The [Cerbos .NET SDK](https://github.com/cerbos/cerbos-sdk-net) (`Cerbos.Sdk`), which the package
  depends on

## Quick start

```csharp
using Cerbos.QueryPlan.EntityFrameworkCore;
using Cerbos.Sdk.Builder;

var cerbos = CerbosClientBuilder.ForTarget("http://localhost:3593").WithPlaintext().Build();

var plan = cerbos.PlanResources(PlanResourcesRequest.NewInstance()
    .WithPrincipal(Principal.NewInstance("user@example.com", ["USER"]))
    .WithResource(Resource.NewInstance("document"))
    .WithActions(["view"]));

var attributes = new AttributeMap<Document>()
    .Field("request.resource.attr.ownerId", d => d.OwnerId)
    .Field("request.resource.attr.public", d => d.IsPublic);

var filter = QueryPlanAdapter.ToFilter(plan, new QueryPlanOptions<Document>(DatabaseProvider.SqlServer, attributes));

var documents = await db.Documents
    .Where(filter.Predicate)
    .Where(d => d.Region == "emea")
    .OrderBy(d => d.Id)
    .Take(20)
    .ToListAsync();
```

`filter.Kind` is `AlwaysAllowed`, `AlwaysDenied` or `Conditional`. `filter.Predicate` is never
null: it is `true` when the plan always allows and `false` when it always denies, so one code path
serves all three. Skip the query for `AlwaysDenied` if you like.

`ToFilter` also accepts the protobuf `PlanResourcesFilter` directly, for a plan from the REST API or
a cache.

## Attribute mapping

An `AttributeMap<TEntity>` names every plan variable the policies read. A plan reading a variable
the map does not declare throws `UnmappedAttributeException`, a `QueryPlanRefusedException`.

```csharp
var attributes = new AttributeMap<Document>()
    .Field("request.resource.id", d => d.Id)
    .Field("request.resource.attr.status", d => d.Status)
    .Field("request.resource.attr.createdAt", d => d.CreatedAt)
    .Field("request.resource.attr.owner.team", d => d.Owner!.Team)          // a to-one navigation
    .List("request.resource.attr.tagNames", d => d.Tags, t => t.Name, t => t.Position)
    .List("request.resource.attr.reviewers", d => d.Reviewers, r => r
        .Field("id", x => x.UserId)
        .Field("role", x => x.Role));
```

- **`Field`** maps a scalar to a column: a string, a number, a boolean, a `DateTime` or a
  `DateTimeOffset`. A path may pass through to-one navigations, and a null navigation is a missing
  attribute, as it is to Cerbos.
- **`List(name, path, element, position)`** maps a list of scalars stored one per related row.
  `element` is the column holding the value.
- **`List(name, path, fields, position)`** maps a list of objects stored one per related row.
  Inside it, keys are the field names a lambda body reads (`x.role` in
  `R.attr.reviewers.exists(x, x.role == "lead")`), and lists nest.
- **`position`** is optional. It names the column holding each element's zero-based index in the
  list your application sends to `check()`, and only `R.attr.list[0]` needs it; without one, an
  index is refused.

`exists`, `all`, `exists_one`, `filter`, `map`, `in`, `size` and `hasIntersection` over a mapped list
become correlated subqueries. A list is a set of related rows, so the adapter refuses what needs
row order or multiplicity it cannot read: comparing a whole list, `except`/`intersect`/`isSubset`,
list concatenation and two-variable `exists`.

### NULL columns

A NULL column can reach `check()` in two ways, and they decide differently:

| `NullConvention` | Your application sends `check()` | `R.attr.x == null` | `R.attr.x != "a"` |
| --- | --- | --- | --- |
| `Omitted` (default) | no `x` key at all | error: denied | error: denied |
| `Explicit` | `"x": null` | true | true |

Declare it per field (`.Field(name, path, NullConvention.Explicit)`) or for the call
(`new QueryPlanOptions<T>(…) { Nulls = NullConvention.Explicit }`); a field's own wins. Either way,
every operator but equality treats a null as the CEL error it is, and the error survives negation:
`!(R.attr.x > 3)` still denies a row whose `x` is missing.

The predicate means the same with or without `UseRelationalNulls`: it never applies SQL `NOT` to a
value that can be NULL.

## Database requirements

CEL compares strings by code point and exactly. Each provider needs its strings stored so that SQL
does the same; otherwise the database decides differently from the PDP, which is an over-grant.

- **SQL Server.** Store mapped string columns as `varchar` under a UTF-8 binary collation,
  `Latin1_General_100_BIN2_UTF8`:

  ```csharp
  protected override void ConfigureConventions(ModelConfigurationBuilder b) =>
      b.Properties<string>().AreUnicode(false);
  protected override void OnModelCreating(ModelBuilder b) =>
      b.UseCollation("Latin1_General_100_BIN2_UTF8");
  ```

  The default collation is case-insensitive, so `== "finance"` would return `"Finance"`. An
  `nvarchar` column orders by UTF-16 code unit and `LEN` counts code units, where CEL orders and
  counts by code point. The adapter handles the rest of SQL Server itself: `=`, `LEN` and `LIKE`
  ignore trailing spaces, so every string comparison appends a sentinel character, and `[`, `]` and
  `^` are escaped in every `LIKE` pattern.
- **PostgreSQL.** Mapped string columns need the `"C"` collation (or a database initialised with
  `--lc-collate=C`). A linguistic default such as `en_US.utf8` does not order by code point.
  Avoid `citext` and nondeterministic ICU collations.
- **SQLite.** The default `BINARY` collation is exact. Do not declare `NOCASE` on a mapped column.
  The adapter avoids `LIKE`, which is case-insensitive in SQLite, and matches with `substr` and
  `instr` instead.

The conformance harness runs SQL Server with a case-insensitive collation as a sanity check; it
reports 149 over-grants.

### Timestamps

Map a timestamp column as `DateTime` or `DateTimeOffset`. To CEL it is the RFC 3339 string your
application sends; `timestamp(R.attr.createdAt)` compares instants. A plan literal finer than the
provider keeps (PostgreSQL holds microseconds, SQL Server and SQLite 100 ns) is truncated, and the
comparison adjusted so the truncation never admits a row. Comparing the column as a string, without
`timestamp()`, is refused: the store does not keep the exact spelling.

## Supported operators

| Cerbos | Translation |
| --- | --- |
| `&&`, `\|\|`, `!`, `? :` | Boolean logic over the true/false pair, so an erroring operand denies under both polarities |
| `==`, `!=` | Equality with CEL's heterogeneous rule: values of different types are unequal, never an error |
| `<`, `<=`, `>`, `>=` | Numbers, strings (code-point order), booleans, timestamps; across types, an error |
| `in` | A literal list, a mapped list, or a list literal with columns in it |
| `+`, `-`, `*`, `/` | Double arithmetic; `/` only by a finite, non-zero constant |
| `startsWith`, `endsWith`, `contains` | With a constant or a column on either side, escaped |
| `size` | Strings (code points), mapped lists |
| `has` | A mapped field, a navigation's field, a field of a list element |
| `exists`, `all`, `exists_one`, `filter`, `map` | Over a mapped list, nesting; `exists`, `all` and `exists_one` over a list literal |
| `hasIntersection` | A mapped list against a literal, or two lists |
| `timestamp()`, `duration()` | A `DateTime` column or a literal; durations add to and subtract from literal timestamps |
| `string()`, `double()` | `string()` over a string or boolean, `double()` over a number |
| `hierarchy()`, `ancestorOf`, `descendentOf`, `overlaps` | Over a delimited string column or literal |
| `list[i]` | With a constant index and a mapped `position` column |

Refused, each with the reason in [`conformance-ledger.json`](conformance-ledger.json): `matches()`
(CEL's RE2 has no provider equivalent, and SQLite and SQL Server before 2025 have no regex),
`int()`, `%`, `upperAscii()`/`lowerAscii()`, division by a column, NaN and infinities,
`string()` of a number, `except`/`intersect`/`isSubset`, `timeSince` and timestamp arithmetic over a
column.

## Conformance contract

The adapter replays the shared [conformance corpus](../conformance/README.md): for each recorded
plan of Cerbos PDP 0.57.0 and 0.56.0, it translates the plan, runs the query against 43 of the
corpus's 44 seed rows on SQLite, PostgreSQL and SQL Server, and compares the returned ids with the
`check()` decisions the PDP recorded. The ledger lacks `null-list`, so j4 is not loaded. No PDP runs
in the test. Results for the current PDP (0.57.0), where the total is every golden case of that
tier; a case marked as a planner divergence is skipped, and counts toward the total but not as
passed:

| Tier | Passed / total |
| --- | --- |
| core | 30 / 30 |
| extended | 73 / 102 |
| adversarial | 286 / 369 |

Every case that does not pass is listed with its reason in
[`conformance-ledger.json`](conformance-ledger.json). All 102 are `unsupported`: the adapter throws
`QueryPlanRefusedException` rather than emit a predicate, identically on all three stores. None is
`divergent`, and none is a wrong answer.

## Gotchas

### `ToFilter` throws; the query does not

A refusal is a `QueryPlanRefusedException` from `ToFilter`. An `InvalidOperationException` from EF
at query time ("could not be translated") is a bug in this adapter: report it.

### Attribute numbers are doubles

Cerbos sends every attribute number as a double, so map numeric columns as `double` where you can.
An `int`, `long` or `decimal` column is converted to `double` in SQL, which is exact up to 2^53.

### SQL Server stores no NaN or infinity

A plan comparing with `NaN` or `±Inf` is folded at translation time; one that would have to store
or compute one is refused.

## Example application

[`example/`](example) implements the [shared demo domain](../demo) against the packed `.nupkg`
([ADR 0002](../docs/adr/0002-examples-install-the-packed-artifact.md)) on in-memory SQLite. Run it
from the repository root with `demo/scripts/run-example.sh efcore`.

## Development

.NET 10 SDK. The suite reads `../conformance/`, so build in a checkout of the **whole repository**.
SQL Server and PostgreSQL legs need Docker; Testcontainers starts them from the images pinned in
[`SQLSERVER_IMAGE`](SQLSERVER_IMAGE) and [`POSTGRES_IMAGE`](POSTGRES_IMAGE).

```bash
# From efcore/:
dotnet test                                   # SQLite, in memory
ADAPTER_TEST_DB=postgres dotnet test          # PostgreSQL, initialised with --lc-collate=C
ADAPTER_TEST_DB=sqlserver dotnet test         # SQL Server, Latin1_General_100_BIN2_UTF8
```

Or, from the repository root, `conformance/scripts/run-harness.sh efcore [sqlite|postgres|sqlserver]`.

| Variable | Values | Selects |
| --- | --- | --- |
| `ADAPTER_TEST_DB` | `sqlite` (default), `postgres`, `sqlserver` | The store the conformance suite runs on |
| `ADAPTER_TEST_SQLSERVER_COLLATION` | e.g. `SQL_Latin1_General_CP1_CI_AS` | Override the SQL Server database collation |
| `ADAPTER_TEST_POSTGRES_INITDB_ARGS` | e.g. `--lc-collate=en_US.utf8` | Override the PostgreSQL initdb arguments |

`ConformanceTests` is the only suite: every recorded golden plan of both PDPs, replayed against the
store. A case passes when the returned ids equal the recorded `check()` decisions; a ledger
`unsupported` case passes when `ToFilter` throws `QueryPlanRefusedException`.

### Releasing

Push a tag `efcore/v<version>` matching the `<Version>` in the project file.
[`efcore-publish.yaml`](../.github/workflows/efcore-publish.yaml) packs and pushes it to NuGet
through trusted publishing.
