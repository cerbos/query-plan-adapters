# Drizzle: domain mapping friction

Harness: `conformance/domains/harnesses/drizzle/` (`npm test[:sqlite|:postgres|:mysql]`), run with the adapter's own
jest and dependencies and importing the adapter from `drizzle/src`. Installed drizzle-orm is 0.45.4, so relations are legacy
`relations()`.

**Mapping style:** one style, inferred. Each call passes `table` and `schema` (the tables plus their
`relations()`). The mapper only adds what the schema cannot say. Relations are reached by name, and no relation
is declared in full (`type`/`table`/`sourceColumn`/`targetColumn`). The indexMappers replay is not run.

Each `mappings/<domain>.ts` declares that domain's tables, relations and mapper once, through a small
dialect kit (`kit.ts`). The kit picks `sqliteTable`/`pgTable`/`mysqlTable` and column builders that
match `generated/sql/<dialect>.sql`: SQLite `text` timestamps, PostgreSQL `timestamptz`/`jsonb`/`text[]`, and
MySQL `datetime(6)`/`json`. An application on a single dialect would write the same file with the builders
inlined. TS keys are camelCase forms of the column names.

Stores: the shared SQL is loaded as-is, into one database per domain.

- SQLite: in-memory.
- PostgreSQL: an ICU `en-US` linguistic database, as the adapter's own leg uses.
- MySQL: server default `utf8mb4_0900_ai_ci`, as the adapter's own leg uses. The government seed cannot load
  under it (agencies `DSS`/`dss` collide on the primary key), so that one domain is loaded under
  `utf8mb4_0900_bin`.

Totals out of 368:

| Store | pass | refused | error | under | over |
|---|---|---|---|---|---|
| sqlite | 338 | 8 | 1 | 19 | 2 |
| postgres | 334 | 12 | 1 | 19 | 2 |
| mysql | 334 | 12 | 1 | 19 | 2 |

After the alias fix (latent over-grants 2 and 3 below): the 6 hr self-relation and 2 logistics under-grants
per store pass, except hr 0.56.0 `mgr_e02` `view_compensation`, which is now an over-grant from the 0.56.0
planner dropping `has(R.attr.salaryBand)` (e05 has no salary band). The two probes below return exactly the
matching rows.

## Per domain

| Domain | Mapping size (file / mapper object, non-blank lines) | Configured beyond inference | Could not express | Difficulty |
|---|---|---|---|---|
| healthcare | 85 / 9 | `noteType`→`type`, `deleted`→`isDeleted`, `signedAt` `valueType: "timestamp"`, `careTeam` scalar relation (`collectionValueType` + `field`) | nothing | 2 |
| fintech | 89 / 10 | 3 flattened `counterparty.*` columns, `flags` scalar relation, `createdAt` timestamp | nothing. Field-to-field across a relation (`R.currency == R.account.currency`) works | 2 |
| logistics | 83 / 9 | 2 renames, `handling` scalar relation, `eta` timestamp; `relationName` on the two `hubs` relations | `origin.country != destination.country` (adapter bug: no table alias) | 3 |
| education | 95 / 18 | `submittedAt` and nested `assignment.dueAt` timestamps, `members` scalar relation, **plus a placeholder `field` on `assignment.course.staff`** to get past an adapter error | on PostgreSQL/MySQL, `timestamp(submittedAt) > timestamp(assignment.dueAt)` is refused (SQLite translates it) | 4 |
| legal | 87 / 12 | call-level `explicit`; `matter.team`/`matter.screened` scalar relations nested in `fields`; `retentionUntil` timestamp | nothing | 2 |
| retail | 59 / 4 | `isGift`→`gift`, `placedAt` timestamp; the reserved `order` table needs nothing | nothing. `exists` with `l.quantity * l.unitPriceCents`, `all` over an empty list and BIGINT cents all pass | 1 |
| hr | 60 / 9 | `email`→`workEmail`, 2 timestamps, `skills` scalar relation; the self-relation is declared in `relations()` | the self-relation `manager.id` / `manager.manager.id` (adapter bug: no table alias; an `alias()` table in a full relation declaration is rendered by its alias name without `AS`, giving "no such table") | 4 |
| media | 72 / 17 | `keywords` column `indexable: "pgArray"` on PostgreSQL and `"json"` elsewhere; `contributors.id`→join column; `contributors.staff` through the nested `author` relation; 2 timestamps | `keywords.exists(k, k.startsWith(...))` and the slug regex (both refused) | 3 |
| iot | 84 / 15 + a 20-line per-dialect transform helper | hand-written SQL per dialect: `labels.env`/`labels.restricted` through `json_extract` / `->>` / `->> '$.k'`, and `"k" in labels` through `json_type` / `jsonb_exists` / `json_contains_path` | `has(R.attr.labels.env)` over a transform mapping (plain `Error`) | 5 |
| government | 62 / 8 | call-level `explicit`, 2 timestamps, `flags` scalar relation | the regex (refused); `assignedTo != "sv-bo"` under-grants (adapter bug) | 3 |

**Hardest mapping problem: iot `labels`.** Inference maps a JSON map column without complaint and then
over-grants (below). The only working mapping is custom SQL written per dialect through `transform`. That relies
on the `in` operator reaching the transform with a scalar value for `"k" in R.attr.labels`, and it still cannot
answer `has()`.

## Over-grants

### Found in the domain run: PDP 0.56.0 planner divergence (1 per store, not an adapter defect)

healthcare 0.56.0 `patient_p3` `view`. The 0.56.0 planner drops `has(R.attr.signedAt)`. Plan:
`and(not(exists(R.codes, lambda(and(eq(c.system,"ICD-10"), startsWith(c.code,"F")), c))), and(eq(R.patient.mrn,"MRN-0003"), ne(R.noteType,"psychotherapy"), not(R.deleted)))`.
Row `n13` has `signed_at` NULL, patient p3, type `consult`, no codes and `is_deleted` 0, and is returned
(`["n05","n13"]`, allowed `["n05"]`). `check()` denied it because `signedAt` is absent.

### Latent over-grants in the adapter (each confirmed against SQLite with the domain's own tables)

1. **A JSON map column inferred as a plain column: `"k" in R.attr.m` becomes `m IN ('"k"')`.** This is
   iot with no mapper entry for `labels`, the zero-configuration mapping, and it is what the first domain run
   returned for `admin_acme` `decommission` on both PDPs: `dv07` was over-granted.
   - Plan fragment: `not(in("tier", R.labels))`
   - Emitted SQL: `not "devices"."labels" in (?)` with params `["\"tier\""]`
   - Rows: dv01 `{"env":"prod","tier":"gold"}`, dv06 `{"env":"dev","tier":"silver"}`, dv07 `{"tier":""}`
   - The filter alone returns all 8 devices. dv01, dv06 and dv07 hold a `tier` key, so `check()` denies them.

   The adapter should refuse `in` against a column that is neither a declared list nor a relation. The final iot
   mapping replaces this with a transform.
2. **Two relations to the same table share one unaliased name** (logistics `origin`/`destination`).
   - `ne(R.origin.country, R.destination.country)` (customs_us `view`) emits
     `exists (select 1 from "hubs" where ("hubs"."id" = "shipments"."destination_hub_id" and exists (select 1 from "hubs" where ("hubs"."id" = "shipments"."origin_hub_id" and "hubs"."country" <> "hubs"."country"))))`.
     It compares the inner `hubs.country` with itself, so it is always false. That is an under-grant here (all 5
     allowed rows lost).
   - Probe with the `==` form, `eq(R.origin.country, R.destination.country)`: the same shape with `=` returns all
     9 shipments. Only s03 (US→US) and s06 (DE→DE) have matching countries, so the other 7 are over-granted.
3. **A self-relation subquery is uncorrelated** (hr `manager`).
   - `eq(R.manager.id, "e02")` emits
     `exists (select 1 from "employees" where ("employees"."id" = "employees"."manager_id" and "employees"."id" = ?))`.
     Inside the subquery both sides bind to the inner `employees`, so it no longer depends on the outer row.
   - On the seed no employee is their own manager, so every `manager.*` rule under-grants: 6 cases per store.
   - Probe: give one row a self-reference (`UPDATE employees SET manager_id='e02' WHERE id='e02'`), which is a
     common convention for the top of a hierarchy. `and(eq(R.manager.id,"e02"), lt(R.level,6))` then returns
     e04, e05, e07, e08, e09 and e10, but only e04 and e05 report to e02.
   - With `manager_id='e01'` on e01, `eq(R.manager.id,"e01")` returns all 10 employees.

## Other non-pass verdicts by cause

### Error

- iot 0.57.0 `tech_eu` `update_firmware` (1 per store). `has(R.attr.labels.env)` over a transform mapping throws
  a plain `Error` ("requires a column to be used as a value expression") rather than the refusal type.
  **Adapter bug** (wrong error type; it fails closed).
- Removed by mapping: education `exists` over `assignment.course.staff` threw "Relation mapping for
  'request.resource.attr.assignment.course.staff' does not define a default field". A to-many reached through
  two inferred to-one hops is resolved as if it were read as a value. **Adapter bug.** Declaring the nested chain
  with a placeholder `field: courseStaff.userId` works around it, and that workaround is in the mapping (the
  12 cases now pass).

### Under-grant

- **Adapter bug, hr self-relation** (6 per store: 0.57.0 `ceo_e01`/`mgr_e02` `view`/`view_compensation`, and
  0.56.0 `ceo_e01`/`mgr_e02` `view_compensation`). See latent over-grant 3.
- **Adapter bug, logistics same-table relations** (customs_us `view`, 2). See latent over-grant 2.
- **Adapter bug: the call-level `explicit` does not reach `ne`** (government `supervisor_bo` `approve`, 2).
  `"cases"."assigned_to" <> ?` drops `k01`, whose `assigned_to` is NULL and sent as `null`. Declaring
  `nullAttributeRepresentation: "explicit"` on the `assignedTo` entry emits
  `not ("cases"."assigned_to" is not null and "cases"."assigned_to" = ?)`, which returns `k01` (verified). This is
  the same defect as in the Prisma adapter.
- **Planner divergence on both PDPs** (6): healthcare `nurse_ana` `view` (`!(has(vip) && vip)` planned as
  `not(vip)`); legal `client_globex` `view` (`privilege == null || privilege == ""` planned as
  `eq(privilege, null)`); iot `member_acme` `view` (the DENY `has(labels.restricted) && labels.restricted == "yes"`
  planned as `not(eq(labels.restricted,"yes"))`, so a device without the key is UNKNOWN in SQL but allowed by
  `check()`).
- **PDP 0.56.0 `has()` folding** (11): healthcare `admin_kim` `delete`, `dr_ortiz` `sign`, `researcher_li` `sign`;
  hr `ceo_e01` `view`, `hr_admin` `terminate`, `mgr_e02` `view`, `recruiter` `view`; education `prof_jones`/`ta_lee`
  `grade` (the TA branch is dropped); media `reader_free`/`reader_sub` `read`.

### Refused (`UnsupportedQueryPlanError`)

- Regex outside the finite-literal subset: government `foia_release` (2), media `publish` (2).
- `exists` over a scalar list column (`keywords`): media `lawyer_quinn` `read`/`legal_review` (4).
- PostgreSQL and MySQL only: `timestamp(submittedAt) > timestamp(assignment.dueAt)` across a relation, education
  `flag_late` (4). SQLite translates and passes these.
