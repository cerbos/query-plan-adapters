# Domain stress kit

Ten realistic domain models, each with its own schema, seed rows, principals and Cerbos policies,
used to prove that every adapter can be mapped onto a domain it has never seen. The conformance
corpus proves each CEL shape against one synthetic resource; this kit proves the mapping: real
relations, naming quirks, reserved words, self-relations, flattened objects, JSON columns, native
arrays and both NULL conventions.

Nothing here gates CI. The PDP is still the oracle: every plan and every allow decision is
recorded from the release images pinned in `conformance/pdp-versions.json`.

| Domain | Resource kind | What makes it hard to map |
|---|---|---|
| healthcare | `clinical_note` | column named `type`; 3-hop to-one chain; nullable to-one (outpatients); scalar and object child lists; DENY over a list |
| fintech | `payment` | DECIMAL money vs double policy literals; flattened columns sent as one object; field-to-field across a relation |
| logistics | `shipment` | two to-one relations to the same table; optional carrier; list of objects that each join to a hub |
| education | `submission` | to-many two to-one hops below the resource; timestamps compared field-to-field with fractional seconds |
| legal | `legal_document` | explicit nulls (`== null`); negated membership in an ethical-wall list; `now()` |
| retail | `order` | root table named `order`; BIGINT cents beyond 2^32; arithmetic inside `exists`; `all` over an empty list |
| hr | `employee_record` | self-relation followed two hops; natural-key table; `request.resource.id` |
| media | `article` | native `text[]` on PostgreSQL; many-to-many flattened through a join table; regex; `now()` |
| iot | `device` | JSON object column read as a map (`labels.env`, `"tier" in labels`) |
| government | `benefit_case` | explicit nulls; leading-zero strings; nullable DECIMAL; regex |

## Layout

```
domains/<domain>/schema.yaml        tables, columns and the resource projection (hand)
domains/<domain>/data.yaml          rows, principals, actions (hand)
domains/<domain>/policies/          Cerbos policies (hand)
domains/<domain>/generated/         resources.json, rows.json, sql/<dialect>.sql (kit build)
domains/<domain>/golden/<pdp>/<principal>/<action>.json   (kit record)
domains/kit/                        the Go tool
domains/results/<adapter>/<store>.json   harness verdicts
domains/results/<adapter>/friction.md    what each mapping took
```

```bash
cd domains/kit
go run . build             # regenerate generated/ for every domain
go run . record [domain]   # build, then record goldens against both pinned PDPs (Docker)
go run . summary [domain]  # print every golden's plan kind and allowed ids
go run . report            # aggregate domains/results/ into a markdown table, over-grants first
```

### schema.yaml

- `tables.<name>.pk` is a column or a list of columns. Column types are `string`, `int`,
  `bigint`, `float`, `decimal`, `bool`, `timestamp`, `json` or `string[]`; a trailing `?` makes
  the column nullable and `-> <table>` makes it a foreign key to that table's primary key.
- `resource.table` is the root table; its single-column primary key is `request.resource.id`.
- `resource.attributes` projects a root row to `request.resource.attr`:
  - `name: column` reads a column.
  - `{one: fk_column, attributes: {...}}` follows a to-one foreign key to a nested object.
  - `{many: child_table, fk: column, value: column}` is a list of scalars from a child table.
  - `{many: child_table, fk: column, attributes: {...}}` is a list of objects; with
    `via: fk_column` each element is the row that join-table column references.
  - `{attributes: {...}}` with nothing else groups columns of the same row into an object.
- `nulls: omitted` (default) drops a NULL column's attribute; `nulls: explicit` sends `null`.

`generated/resources.json` is the exact resource each `check()` received. `generated/rows.json`
is every table with typed rows, for harnesses that load through their ORM. `generated/sql/` holds
DDL and inserts for `sqlite`, `postgres`, `mysql`, `sqlserver` and `h2`; every identifier is
quoted, so names are lowercase and case-sensitive on H2. SQLite timestamps are ISO-8601 text
with milliseconds and a `Z`; an ORM with another SQLite convention loads `rows.json` itself.
Document stores hold `resources.json` as is: one document per resource, keyed by `id`.

### Golden files

```json
{"domain": "fintech", "pdp": "0.57.0", "principalName": "teller_lon",
 "principal": {"id": "u-teller", "roles": ["teller"], "attr": {...}},
 "action": "view", "resourceKind": "payment",
 "plan": {"kind": "KIND_CONDITIONAL", "condition": {...}}, "planError": null,
 "allowed": ["pay01", "pay02"], "oracle": "discriminating"}
```

`now()` is folded into a literal timestamp in the plan. Seed timestamps sit years away from it, so
a recorded plan stays valid.

## The harness contract

Per adapter, per store, per domain:

1. Load the domain into the store, the way an application using that adapter would hold it.
2. Write the adapter's mapping for the domain by hand, as a user would: ORM models or schema
   where the adapter needs them, plus whatever mapper or override configuration the adapter asks
   for. Rely on the adapter's inference wherever it offers it, and add configuration only where
   it is needed. One mapping per domain serves every principal and action.
3. For every golden under both PDP tags: hand the plan to the adapter (through the Cerbos SDK's
   own types, as the conformance harness does), run the query, and compare the returned ids with
   `allowed`. Record one verdict:
   - `pass`: equal sets.
   - `refused`: the adapter threw its documented refusal type.
   - `over-grant`: the query returned an id that is not in `allowed`. Takes precedence.
   - `under-grant`: a strict subset of `allowed`.
   - `error`: anything else (a crash, a store error, an exception that is not the refusal type).
4. Write `domains/results/<adapter>/<store>.json`:

```json
{"adapter": "prisma", "store": "postgres-v7", "results": [
  {"domain": "fintech", "pdp": "0.57.0", "principal": "teller_lon", "action": "view",
   "verdict": "pass", "returned": ["pay01"], "allowed": ["pay01"], "detail": ""}]}
```
