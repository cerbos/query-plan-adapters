# Cerbos + DuckDB Adapter

Translates a Cerbos [`PlanResources`](https://docs.cerbos.dev/cerbos/latest/api/#resources-query-plan)
response into a DuckDB `WHERE` fragment and its `?` parameters, so a query returns exactly the rows
the policy allows. A plan shape DuckDB cannot be asked exactly raises `UnsupportedPlanError`, never
a best-effort filter.

## Install

```bash
pip install cerbos-duckdb
```

Not released yet: until the first `duckdb/v*` tag, install from this directory. It needs Python 3.11
or later and DuckDB 1.4 or later.

## Quick start

```python
import duckdb
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc

from cerbos_duckdb import Entry, ValueType, translate

MAPPING = {
    "request.resource.attr.ownerId": Entry("owner_id", ValueType.STRING),
    "request.resource.attr.public": Entry("is_public", ValueType.BOOL),
}

with CerbosClient("http://localhost:3592") as cerbos:
    plan = cerbos.plan_resources("view", Principal("alice", roles={"user"}), ResourceDesc("document"))

result = translate(plan, "documents", MAPPING)
# ALWAYS_DENIED  -> result.sql == "FALSE"
# ALWAYS_ALLOWED -> result.sql == "TRUE"
# CONDITIONAL    -> a boolean expression over "documents"

con = duckdb.connect("app.duckdb")
rows = con.execute(
    f"SELECT * FROM documents WHERE archived = ? AND {result.sql} ORDER BY id LIMIT ?",
    [False, *result.params, 20],
).fetchall()
```

`translate` accepts the response from either SDK client, `cerbos.sdk.client.CerbosClient` (HTTP) or
`cerbos.sdk.grpc.client.CerbosClient`. Every plan kind returns a fragment, so the query needs no
branch on `result.kind`, and a denied plan stays denied whatever you `AND` it with.

`result.sql` is self-delimiting: it binds the same way wherever it is spliced, beside `AND`, `OR`,
`NOT` or a comparison. It refers to its parameters as positional `?`, in the order of
`result.params`, so run it through `con.execute` with your own `?` placeholders in textual order, as
above; it cannot be mixed with `$name` or `$1` parameters, and the relational API's `filter()` takes
no parameters. The second argument, `"documents"`, is the name or alias that qualifies the resource
row's columns: pass the alias if your `FROM` clause gives one. Table names are quoted as one
identifier, so query an attached catalog's table through an alias or a view.

## Mapping attributes

The mapping is a `dict` from each attribute reference the plan can hold to an `Entry`, or a function
that returns one (or `None`). An unmapped reference is refused.

```python
Entry(column, value_type=ValueType.DEFAULT, null_convention=NullConvention.UNSET,
      list=None, join=None, qualifier=None)
```

- `column` names a column, or a `STRUCT` field path as a tuple: `Entry(("address", "country"))`.
- `value_type` declares what the column holds: `STRING`, `NUMBER`, `BOOL` or `TIMESTAMP`. Every
  scalar attribute a policy reads needs one. DuckDB casts between types implicitly (`'5' = 5` is
  true, an integer is truthy) where CEL compares them unequal or raises, and the plan carries no
  types, so an attribute without a `value_type` is refused. Only an `Entry` that maps a `LIST` or a
  joined row leaves it out.
- `qualifier` reads the column from another table or alias in your query instead of the resource
  row.

### Collections: `LIST` columns

A collection attribute is a DuckDB `LIST` column, of scalars or of `STRUCT`s. `ListOf` describes its
elements: `element` for a list of scalars, `fields` for the fields of a `STRUCT` element.

```python
MAPPING = {
    # tags STRUCT(id VARCHAR, name VARCHAR)[]
    "request.resource.attr.tags": Entry(
        "tags",
        list=ListOf(fields={"id": Entry("id", ValueType.STRING), "name": Entry("name", ValueType.STRING)}),
    ),
    # scores DOUBLE[]
    "request.resource.attr.scores": Entry("scores", list=ListOf(element=Entry(value_type=ValueType.NUMBER))),
}
```

`exists`, `all`, `exists_one`, `filter`, `map`, `in`, `size`, `list[k]`, `hasIntersection`,
`isSubset`, `except`, `intersect` and the Cerbos 0.58 set functions lower to correlated `unnest(...)
WITH ORDINALITY` subqueries. They are exact, including CEL's error semantics, but much slower than
DuckDB's own list functions on a large table, so filter on scalar columns first where you can. A `NULL` list is not an empty one: CEL errors on it, so every operation over a
`NULL` list is UNKNOWN and the row is excluded under both polarities, where `[]` is an empty
collection.

If the collection lives in a child table, give DuckDB a view that aggregates it into a `LIST` and
query the view:

```sql
CREATE VIEW documents_with_tags AS
SELECT d.*, coalesce(list({'id': t.id, 'name': t.name} ORDER BY t.id) FILTER (WHERE t.id IS NOT NULL), [])
         AS tags
FROM documents d LEFT JOIN tags t ON t.document_id = d.id
GROUP BY ALL;
```

The view must build the same list the application sends as the attribute, including its order if a
policy reads `list[k]` or compares whole lists, and any predicate the application's own read of the
child table applies (see [Mapping hazards](#mapping-hazards)).

### To-one relations: `Join`

A nested attribute that is another row, such as `R.attr.parent.status`, is a `Join`: the row of
`table` whose `target_column` equals the resource row's `source_column`.

```python
PARENT = Join("folders", source_column="folder_id", target_column="id")

MAPPING = {
    "request.resource.attr.parent": Entry(join=PARENT),
    "request.resource.attr.parent.status": Entry("status", ValueType.STRING, join=PARENT),
}
```

Declaring a `Join` asserts that `target_column` is unique, so at most one row matches. `via` lists
intermediate tables (innermost first, each a `Hop(table, child_column, join_column)`) for a chain the
policy reads as one attribute, and every operator through the chain requires each hop to exist: a
missing parent is UNKNOWN, as CEL's missing attribute is. A `LIST` column on the joined row is an
`Entry` with both `join` and `list`. Fixed-size `ARRAY` columns work as collections on DuckDB 2.x;
DuckDB 1.x cannot `unnest` them, so the query errors there.

## Null conventions

CEL tells a `null` attribute from an absent one: `R.attr.x != "a"` is true for `null` and an error
for a missing key. A SQL `NULL` cannot say which the application sent, so declare it per attribute:

| `null_convention` | The application sends a `NULL` column as | `x != "a"` on a `NULL` row |
| --- | --- | --- |
| `NullConvention.OMITTED` | no key | excluded |
| `NullConvention.EXPLICIT` | `null` | included |
| `NullConvention.UNSET` | the call's `null_representation` | |

`translate(..., null_representation=NullRepresentation.OMITTED)` is the default for attributes that
declare nothing. Declare the convention your application actually uses: the filter matches the PDP
only for the attributes it sends the way the mapping says.

## Correctness requirements

### Collation

DuckDB compares strings by the column's collation, and `SET default_collation = 'nocase'` or a
column `COLLATE nocase` makes `=`, `starts_with`, `contains`, `IN` and the optimizer's rewrites of
`regexp_matches` fold case. CEL compares bytes. The adapter casts every `STRING` operand to
`VARCHAR` and appends `COLLATE "binary"`, so the filter is exact whatever the column or default
collation, and `ENUM`, `UUID` and `JSON` columns declared `STRING` compare as the text the
application sends. The conformance harness runs a second leg under `default_collation = 'nocase'`.

### Timestamps

Map a `TIMESTAMP` attribute to a `TIMESTAMPTZ` column, a `TIMESTAMP` column holding UTC, or a
`DATE` (read as its midnight UTC). The filter compares `epoch_us(column)`, so the session's
`TimeZone` never shifts a naive column, at the cost of DuckDB's min/max pruning on that column. A
plan literal with sub-microsecond digits is compared on the microsecond grid the column stores.
`TIMESTAMP_NS` columns are not supported: the filter would read them truncated to microseconds,
which can return a row the PDP denies.

Bare temporal attributes are compared as RFC 3339 strings in CEL, which a stored timestamp cannot
reproduce, so `R.attr.createdAt == "…"` without `timestamp()` is refused.

### Numbers

CEL computes every attribute number as a double. The filter casts the operands of `+`, `-`, `*` and
`/` to `DOUBLE`, so a `FLOAT` column does not overflow to infinity and an `INTEGER` product does not
raise. A `DOUBLE` column holding NaN is outside the contract: the PDP cannot receive NaN, and DuckDB
orders it above every number (see "A value the PDP cannot receive" in the conformance README's
mapping hazards).

### Regular expressions

`matches()` with a constant pattern lowers to `regexp_matches`, which is RE2 like CEL's, with the
same partial-match and newline semantics. A pattern RE2 rejects is CEL's error, so it lowers to
UNKNOWN; the adapter checks it against a private single-threaded in-memory DuckDB at translation
time, because DuckDB before 2.0 raises on it even under `TRY()`. Shapes where DuckDB's RE2 and CEL's
part ways are refused: `\C`, repetition counts too large for a 32-bit integer, and `(?<name>…)`
groups. A pattern read from a column is refused, since it cannot be checked.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): every
recorded plan from Cerbos PDP 0.58.0 and 0.57.0 is served by a stub PDP and fetched through both SDK
clients (HTTP and gRPC), translated with one mapping, executed on an in-process DuckDB holding the
corpus dataset, and the returned ids are compared with the decisions the PDP recorded. CI runs it on
DuckDB 1.4, 1.5 and 2.x. For the current PDP, 0.58.0, where the total is every golden case recorded
in that tier:

| Tier | Passed / total |
| --- | --- |
| core | 32 / 32 |
| extended | 109 / 114 |
| adversarial | 530 / 617 |

Every case that does not pass is either refused with `UnsupportedPlanError` (78 cases) or is skipped
because its golden file records a planner divergence (3 extended and 11 adversarial cases), which no
adapter can pass. [`conformance-ledger.json`](conformance-ledger.json) lists each refused case with
the mechanism that rules it out. The largest group is `int()` and `double()`: DuckDB's `CAST` reads
`'1e3'`, `' 12 '` and `'1_000'` as integers where CEL raises, and rounds where CEL truncates.

The harness also checks that every fragment is self-delimiting, that translating the same plan in
another process gives the same SQL, and that every operator the planner cannot emit is refused.

### Refusals

`cerbos_duckdb.UnsupportedPlanError` subclasses `ValueError`. Invalid configuration raises
`ValueError` (an empty table name) or `TypeError` (a mapping value that is not an `Entry`).

## Mapping hazards

The conformance contract proves the *plan* side. The other half is the *mapping*: **the values a
filter reads must be the values the application put into the resource attributes.** The shared
corpus catalogues six ways that breaks.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned** | A `LIST` column, or the view that aggregates one, must hold the elements the application sends: the same `WHERE` its own read applies. For a `Join`, declare that predicate as `subquery_filter` |
| Default scope on the target model | **Caller-owned**, reproducible with `subquery_filter` | A soft-delete column, a tenant column, a `published` flag. A `Join` reads its table bare: add the predicate as a `Restriction` on the `Join` or its `Hop`s |
| Subtype discrimination | **Caller-owned**, reproducible with `subquery_filter` | A `type` column on a shared table: `Restriction("kind", RestrictOp.EQ, "…")` |
| To-one relation used as a collection | **Caller-owned** | A `Join` whose `target_column` has no unique constraint. Add it |
| Composite association key | **Rejected by the type system** | `source_column` and `target_column` are each one column |
| Absent to-one parent | **Reproduced**, and proved by the corpus (`relation/*`) | None: every operator through a `Join` requires its row and its hops, so a missing parent is UNKNOWN under both polarities |

## Example application

[`example/`](example/) installs the built wheel and runs the five shared usage shapes against the
[demo domain](../demo/README.md):

```bash
# from the repository root
demo/scripts/run-example.sh duckdb
```

## Development

| Suite | Checks | Needs |
| --- | --- | --- |
| `tests/test_conformance.py` | returned rows match the recorded decisions, or the ledger's refusal is raised, for every golden case on both PDP tags and both transports; self-delimiting fragments; determinism across processes; unknown operators refused | nothing: DuckDB runs in-process, plans come from `conformance/golden/` |

```bash
pdm install -G :all
pdm run test            # the conformance harness
pdm run format          # isort + ruff format
pdm run lint            # ruff check --fix
```

Without PDM installed, run the same commands through the [pyprojectx](https://pyprojectx.github.io/)
wrapper, `./pw` (`pw.bat` on Windows): `./pw install`, `./pw test`, `./pw format`, `./pw lint`. CI
runs `format` and `lint` on the locked leg and fails if they leave a diff.
`conformance/scripts/run-harness.sh duckdb` runs the harness the way CI does, and with `QPA_FUZZ_DIR`
set it replays a fuzz run instead.
