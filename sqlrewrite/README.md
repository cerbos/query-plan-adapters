# Cerbos SQL rewrite service

A service that takes the SQL statement an application was going to run, plus the Cerbos query plan
for each resource kind that statement reads, and returns the same statement with the plan applied as
a filter. The application keeps writing its own SQL, in any shape it likes, and never builds a filter
itself.

It is the same translator as the [ent](../ent) and [pgx](../pgx) adapters, with an SQL parser in
front of it, served as a Go library, gRPC and HTTP from one handler.

- **PostgreSQL, MySQL and SQLite**, each with its own parser.
- **SELECT only.** Anything else is refused.
- **The caller sends the plan.** The service never calls the PDP and needs no Cerbos credentials.
- **Bound parameters only.** No plan value is ever written into the SQL text.
- **Fail closed.** A statement or plan the service cannot rewrite exactly is refused with a coded
  error, and the caller runs nothing.

## How a statement is rewritten

Every reference to a protected table is replaced with a derived table that filters it, keeping the
caller's alias (or the table name when there is none), so nothing else in the statement changes:

```sql
-- sent
SELECT c.id, co.name FROM contact c JOIN company co ON co.id = c.company_id WHERE c.region = $1

-- returned
SELECT c.id, co.name
FROM (SELECT * FROM contact cerbos_resource WHERE (("cerbos_resource"."owner_id" = $2::text))) c
JOIN company co ON co.id = c.company_id
WHERE c.region = $1
```

The filter is always a wrapper, never an extra `WHERE` condition, so it holds whatever the caller's
statement does around it: joins of either side, CTEs, `UNION`, subqueries in any clause, self-joins,
`GROUP BY`, `ORDER BY` and `LIMIT`. Two references to the same resource kind share one translation.

The statement is parsed, the table references are replaced in the syntax tree, and the tree is
printed back. The filter itself is spliced in as the translator wrote it, so no printer can
re-associate its operators. Before that, a PostgreSQL or MySQL statement must print back to the same
tree it was parsed from, or it is refused. After the rewrite, the output is parsed again and every
read of a protected table must sit inside a filter, or the result is refused.

Printing back from the tree has visible effects: comments are dropped, keywords and identifier
quoting are normalised (MySQL lowercases keywords; SQLite quotes every identifier), and
PostgreSQL and MySQL drop redundant parentheses.

### Parameters

The caller's placeholders keep their positions and values. The filter's values are appended:

- **PostgreSQL**: `$1..$n` stay the caller's; the filter uses `$n+1` on.
- **MySQL, SQLite**: `?` only. `params` in the response is in placeholder order, with the filter's
  values wherever its placeholders landed.

Named parameters (`:name`, `@name`, `$name`) are refused.

## Request and response

`cerbos.sqlrewrite.v1.SQLRewriteService/Rewrite`, defined in
[`protos/cerbos/sqlrewrite/v1/sqlrewrite.proto`](protos/cerbos/sqlrewrite/v1/sqlrewrite.proto).
Over HTTP it is `POST /cerbos.sqlrewrite.v1.SQLRewriteService/Rewrite` with a JSON body (the Connect
protocol); gRPC and gRPC-Web are served on the same port.

```jsonc
{
  "sql": "SELECT id FROM contact WHERE region = $1",
  "params": ["emea"],
  // keyed by the resource kind in the configuration; the value is a PlanResourcesResponse as the
  // PDP returned it
  "plans": { "contact": { "filter": { "kind": "KIND_CONDITIONAL", "condition": { … } } } }
}
```

```jsonc
{
  "sql": "SELECT id FROM (SELECT * FROM contact cerbos_resource WHERE (…)) contact WHERE region = $1",
  "params": ["emea", "alice"],
  "applied": [{ "resource": "contact", "table": "contact", "kind": "PLAN_KIND_CONDITIONAL", "references": 1 }]
}
```

`params` are JSON values: strings, numbers, booleans and null. A number with no fraction below
2^53 is bound as an integer. A timestamp in the plan is returned as an RFC 3339 string (on MySQL,
`2006-01-02 15:04:05.999999` in UTC).

`ALWAYS_ALLOWED` leaves the table as it was. `ALWAYS_DENIED` wraps it in a filter that matches no row,
so the caller never has to switch on the plan kind.

### Refusals

A refusal is `INVALID_ARGUMENT` (`FAILED_PRECONDITION` for `missing_plan`) with a `RewriteRefusal`
detail carrying the code:

| Code | Meaning |
| --- | --- |
| `unsupported_plan` | The plan has a shape the translator cannot express exactly. |
| `unsupported_sql` | Not a single SELECT, a printer round trip changes it, it reads a protected table somewhere a filter cannot go (`IN table`, `TABLESAMPLE`, `query_to_xml()`, a catalog-qualified name), or it uses a reserved identifier (`cerbos_resource`, `cerbos_filter_*`). |
| `unmapped_attribute` | The plan names an attribute the configuration does not map. |
| `missing_plan` | The statement reads a protected table, and no plan was sent for it. |
| `unused_plan` | A plan was sent for a resource the statement never reads, which is usually a caller bug. |
| `ambiguous_reference` | A CTE has the name of a protected table. |
| `invalid_request` | The request itself is malformed: no SQL, no plans, fewer params than placeholders. |

In the library, every refusal is a `*rewrite.RefusalError` that matches `errors.Is(err,
rewrite.ErrRefused)`.

## Configuration

```yaml
server:
  listenAddr: ":3594"

rewrite:
  dialect: postgres            # postgres | mysql | sqlite
  nullRepresentation: omitted  # how a null attribute reaches the PDP: omitted | explicit
  resources:
    contact:                   # the resource kind the plan is for
      table: public.contact
      attributes:
        request.resource.attr.ownerId: { column: owner_id, valueType: string }
        request.resource.attr.createdAt: { column: created_at, valueType: timestamp }
        request.resource.attr.tags:
          relation:
            table: contact_tag
            sourceColumn: id
            targetColumn: contact_id
            field: { column: name, valueType: string }
            subqueryFilter:
              - { column: deleted_at, op: isNull }
  protect:                     # tables that may never be read, with or without a plan
    - public.contact_archive
  readers:                     # extra functions that read a table by name, refused like query_to_xml
    - my_audit_reader
```

The attribute vocabulary is the ent adapter's ([Mapping attributes](../ent/README.md#mapping-attributes)):
`column`, `valueType`, `nullConvention`, `relation` and `scalarRelation` with `field`, `fields`,
`position`, `via` hops and `subqueryFilter` restrictions. Values are `${ENV}`-expanded, and
`--conf.set=key=value` overrides any of them.

A table reference matches a configured table by bare name, ignoring case. Its schema matches when
either side has none or both are equal, so `contact` and `public.contact` are the same table, and
`archive.contact` is not.

```bash
sqlrewrite server --conf.path=config.yaml
sqlrewrite check --conf.path=config.yaml     # validates the configuration and exits
```

## Library

```go
rw, err := rewrite.New(rewrite.Config{Dialect: rewrite.DialectPostgres, Resources: resources})
res, err := rw.Rewrite(ctx, rewrite.Request{SQL: stmt, Params: params, Plans: plans})
rows, err := db.QueryContext(ctx, res.SQL, res.Params...)
```

## Conformance contract

The service is proved against the shared [conformance corpus](../conformance/README.md): for each
golden, `SELECT id FROM adversarial_resource` is rewritten with the plan Cerbos PDP 0.57.0 recorded,
run against the corpus dataset on **SQLite, PostgreSQL and MySQL**, and the returned ids are
compared with the recorded `check()` decisions. The previous PDP's goldens (0.56.0) are replayed
too. The counts are ent's, on all three databases, because the translator is ent's; the skipped
planner divergences are the ones the ent README lists.

| Tier | Passed / total (PDP 0.57.0) |
| --- | --- |
| core | 30 / 30 |
| extended | 97 / 102 |
| adversarial | 334 / 375 |

Every case that does not pass is refused; [`conformance-ledger.json`](conformance-ledger.json) lists
each one with its reason, and one ledger holds for every dialect.

A second suite, `TestSQLShapes`, proves where the filter lands. For every core and extended golden,
fourteen statement shapes (aliases, inner and left joins onto the protected table, a CTE, `IN` and
correlated `EXISTS` subqueries, a scalar subquery, `UNION ALL`, a self-join, `GROUP BY`, a derived
table, `ORDER BY`/`LIMIT`, caller parameters on both sides of the filter) are rewritten and run, and
compared with the same statement run over only the golden's allowed rows. `TestSQLShapeRefusals`
holds the statements each dialect must refuse.

### Known gaps

| Gap | Effect |
| --- | --- |
| SQLite statements are not round-trip checked | The SQLite parser has no tree equality, so a caller statement it prints back differently is not caught. The filter is spliced as text and the protected reads are verified either way. |
| No schema introspection | `SELECT *` from a wrapper returns the table's columns, so a caller's `SELECT *` is unchanged, but the service does not check the configured columns exist; a wrong one fails when the statement runs. |
| No `search_path` | An unqualified name is matched against configured tables regardless of the session's search path. |

## Development

```bash
just test                       # unit suites, no store
just test-conformance           # both conformance suites on SQLite, PostgreSQL and MySQL
just lint
just generate                   # regenerates generated/pb from protos/ with buf
```

- The conformance suites carry the `tests` build tag. They start PostgreSQL and MySQL
  testcontainers unless `SQLREWRITE_TEST_POSTGRES_DSN` or `SQLREWRITE_TEST_MYSQL_DSN` names an
  existing database (its tables are dropped and recreated). `SQLREWRITE_TEST_STORES=sqlite,postgres`
  limits a run to some stores. PostgreSQL must use `--lc-collate=C`, as ent's harness explains.
- `pkg/rewrite/internal/queryplan` is a byte-for-byte copy of [ent's](../ent/internal/queryplan);
  `conformance/scripts/validate-corpus.sh` fails on any difference. Per-dialect SQL belongs in
  `pkg/rewrite/render.go`.
- The mapping for the conformance schema is `pkg/rewrite/testdata/conformance.yaml`, loaded through
  the same configuration path the service uses.
- `example/` is its own module and runs with `demo/scripts/run-example.sh sqlrewrite`: it builds and
  starts the service, and reaches it over HTTP.
- `CGO_ENABLED=0`: the PostgreSQL parser is libpg_query compiled to WebAssembly.

See [conformance/README.md](../conformance/README.md) before changing how a shape is translated.

## License

Apache 2.0
