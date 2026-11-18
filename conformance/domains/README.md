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

Paths are relative to this folder, `conformance/domains/`.

```
<domain>/schema.yaml        tables, columns and the resource projection (hand)
<domain>/data.yaml          rows, principals, actions (hand)
<domain>/policies/          Cerbos policies (hand)
<domain>/generated/         resources.json, rows.json, sql/<dialect>.sql (kit build)
<domain>/golden/<pdp>/<principal>/<action>.json   (kit record)
kit/                        the Go tool
harnesses/<adapter>/        each adapter's mappings and replay, outside the adapter's folder
results/<adapter>/<store>.json   harness verdicts
results/<adapter>/friction.md    what each mapping took
```

```bash
cd kit
go run . build             # regenerate generated/ for every domain
go run . record [domain]   # build, then record goldens against both pinned PDPs (Docker)
go run . summary [domain]  # print every golden's plan kind and allowed ids
go run . report            # aggregate results/ into a markdown table, over-grants first
```

## Running a harness

A harness never lives in the adapter's folder, so nothing it needs is built, tested or published
with the adapter. Each one imports the adapter from the folder it lives in and runs with that
adapter's own toolchain and dependencies, so install the adapter first (`npm ci`, `bundle install`,
`./pw install`). Run each command from `harnesses/<adapter>/`; the store, its URL and a domain
filter come from the environment variables each harness names at its top.

| Adapter | How it reaches the adapter | Run |
|---|---|---|
| drizzle, prisma, mongoose, langchain-chromadb, convex | the adapter's jest, with `@cerbos/<package>` mapped to its `src/` | `npm test` (drizzle and prisma: `npm run test:<store>`; convex: `npm ci && npm run deploy` first) |
| ent, pgx | its own Go module with a `replace` to the adapter | `go test ./...` |
| activerecord, sequel, mongodb-ruby | the adapter's Gemfile (`BUNDLE_GEMFILE`) | `ruby harness.rb [domain ...]` |
| sqlalchemy | the adapter's virtualenv | `../../../../sqlalchemy/.venv/bin/python harness.py <store>` |
| jpa, spring-data, exposed, elasticsearch-java | a Gradle build that includes the adapter's (`includeBuild`) | `../../../../<adapter>/gradlew test` |
| efcore | a console project with a `ProjectReference` to the adapter | `./run.sh <store>` |

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
4. Write `results/<adapter>/<store>.json`:

```json
{"adapter": "prisma", "store": "postgres-v7", "results": [
  {"domain": "fintech", "pdp": "0.57.0", "principal": "teller_lon", "action": "view",
   "verdict": "pass", "returned": ["pay01"], "allowed": ["pay01"], "detail": ""}]}
```
