# Prisma: domain mapping friction

Harness: `prisma/domains/` (`npm run test:domains[:sqlite|:postgres|:mysql]`; set `PRISMA_VERSION=6` for the
v6 legs). It is kept out of the adapter's CI suites through its own `jest.domains.config.js`. Each domain has a
models-only `domains/schemas/<domain>.prisma`. `domains/generate.mjs` adds the generator and datasource blocks
for each store and major, then generates a client for every domain, plus the `cerbos-prisma-datamodel` datamodel
on v7. On v6 the datamodel is `Prisma.dmmf.datamodel`. Each `domains/mappings/<domain>.ts` holds the model name
and mapper. Every call passes `datamodel`, so the mapper only states what inference cannot know.

Store legs: `sqlite-v7`, `postgres-v7`, `mysql-v7`, `sqlite-v6`, `postgres-v6`, `mysql-v6`. All six give
identical verdicts (305 pass, 32 refused, 14 error, 15 under, 2 over out of 368).

- SQLite: `prisma db push` into a per-domain file, then `rows.json` loaded through `createMany` (Prisma's own
  DateTime convention), and `PRAGMA case_sensitive_like = ON` (README requirement).
- PostgreSQL: the shared `postgres.sql` is run into a per-domain database created with `LC_COLLATE 'C'`
  (README requirement). The Prisma models point at those tables through `@map`/`@@map`.
- MySQL: the shared `mysql.sql` is run into a per-domain database created as `utf8mb4_0900_bin`, then
  `ALTER TABLE … CONVERT TO … utf8mb4_0900_bin` (README requirement). Under the server default
  `utf8mb4_0900_ai_ci` the government seed cannot load: agencies `DSS` and `dss` collide on the primary key.

Prisma field names are camelCase forms of the column names (`authorId @map("author_id")`), and relation
fields take their natural name (`patient`, `ward`, `manager`). The mapper therefore covers only real renames,
projections and the null convention.

## Per domain

| Domain | Mapping size (non-blank lines) | Configured beyond inference | Could not express | Difficulty |
|---|---|---|---|---|
| healthcare | schema 55, mapper 7 | `noteType`→`type`, `deleted`→`isDeleted`, `careTeam` projection (`relation.field: clinicianId`) | nothing (non-passes are planner divergences) | 2 |
| fintech | schema 58, mapper 10 | 3 flattened `counterparty.*` columns, `flags` projection | `R.currency == R.account.currency` (refused: Prisma field references are same-model); `flags.exists(f, f == "structuring")` (adapter bug, below) | 3 |
| logistics | schema 54, mapper 12 | `trackingNumber`, `shipper` renames, `handling` projection; named `@relation("origin")`/`("destination")` in the schema | `origin.country != destination.country` (refused, cross-model); `checkpoints.exists(c, c.hub.code == …)` (adapter bug, below) | 3 |
| education | schema 56, mapper 7 | `members` projection | `assignment.course.staff.exists(...)` (adapter bug, below; declaring `assignment` as `type: "one"` does not help); `timestamp(submittedAt) > timestamp(assignment.dueAt)` (refused) | 4 |
| legal | schema 55, mapper 16 | call-level `explicit`; `matter.team`/`matter.screened` projections nested under `relation.fields` | nothing | 2 |
| retail | schema 42, mapper 7 | `@@map("order")` handles the reserved table name; `isGift`→`gift` | `l.quantity * l.unitPriceCents > 50000` inside `exists` (refused: `mult` of two fields) | 1 |
| hr | schema 39, mapper 8 | `email`→`workEmail`, `skills` projection; a named self-relation in the schema | nothing (two-hop self-relation and `request.resource.id` work) | 2 |
| media | schema 41 (+1 provider line), mapper 15 | `keywords` is `String[]` on PostgreSQL and `Json` on SQLite/MySQL (schema differs per provider); `contributors.id`→`authorId`, `contributors.staff`→`author.staff` | `keywords.exists(k, k.startsWith(...))` (refused); regex `^[a-z0-9]+(-[a-z0-9]+)*$` (refused) | 3 |
| iot | schema 40, mapper 5 | nothing usable | the `labels` JSON map: `labels.env` and `labels.restricted` are refused; `"tier" in labels` is a Prisma validation error (adapter bug, below) | 5 |
| government | schema 40, mapper 8 | call-level `explicit`, `flags` projection | regex `^[A-Z]{2}-[0-9]{4}-[0-9]+$` (refused: more than 256 alternatives); `timestamp(closedOn) > timestamp(openedOn)` on the same model (refused); `assignedTo != "sv-bo"` under-grants (adapter bug, below) | 3 |

**Hardest mapping problem: iot `labels`.** Prisma's `Json` filters cannot read a key as a column or test a
key's presence through this adapter, and the mapper has no transform hook. No mapping can express those policies.

## Non-pass verdicts by cause (same on all six legs)

### Over-grant: PDP 0.56.0 planner divergence (2 per leg, not an adapter defect)

The 0.56.0 planner folds `has(R.attr.x)` to true when `x` is omitted. The adapter translates the plan it is given
correctly.

- healthcare 0.56.0 `patient_p3` `view`. Plan: `and(not(exists(R.codes, …)), and(eq(R.patient.mrn,"MRN-0003"),
  ne(R.noteType,"psychotherapy"), not(R.deleted)))`. `has(R.signedAt)` is gone, though 0.57.0 keeps it. Row
  `n13` has `signed_at` NULL, `patient` p3 (MRN-0003), type `consult`, no codes, not deleted. Where-input:
  `{patient:{is:{mrn:{equals:"MRN-0003"}}}, type:{not:"psychotherapy"}, isDeleted:{equals:false}, NOT:{codes:{some:…}}}`
  returns `n05`, `n13`. `check()` denied `n13` because `signedAt` is absent.
- hr 0.56.0 `mgr_e02` `view_compensation`. Plan: `and(eq(R.manager.id,"e02"), lt(R.level,6))`. `has(R.salaryBand)`
  is gone. Row `e05` has manager e02, level 3 and `salary_band` NULL, and is returned.

### Error: adapter bugs (each fails closed through Prisma's input validation, never through a refusal)

1. **A to-one hop at the head of an `exists` path renders as `some`** (education, 8 per leg).
   `R.attr.assignment.course.staff.exists(s, s.userId == "f-jones" && s.role == "ta")` gives
   `{assignment:{some:{course:{is:{staff:{some:{…}}}}}}}`, and Prisma rejects it with "Unknown argument `some`".
   `assignment` is a required to-one in the datamodel, and declaring `relation: {name: "assignment", type: "one"}`
   does not change the output.
2. **A to-one hop inside a lambda is dropped** (logistics `hubop_lhr` `view`, 2 per leg).
   `R.attr.checkpoints.exists(c, c.hub.code == "LHR")` gives `{checkpoints:{some:{code:{equals:"LHR"}}}}`, with
   the `hub` hop missing, and Prisma rejects it with "Unknown argument `code`". The README documents this shape
   (`c.author.name`) as inferred. Declaring `fields: {hub: {relation: {name: "hub"}}}` does not help. If
   `Checkpoint` had its own `code` column, the filter would silently read the wrong column.
3. **An explicit-null guard lands on a required projected element** (fintech `compliance_eve` `file_sar`, 2 per
   leg). `R.attr.flags.exists(f, f == "structuring")` gives `{flags:{some:{AND:[{flag:{not:null}},{flag:{equals:"structuring"}}]}}}`
   on the required `flag` column, and Prisma rejects it with "Argument `not` is missing". The lambda variable of a
   projection is always given `nullAttributeRepresentation: "explicit"`, and `fields.flag.nullable: false` is not
   consulted.
4. **`in` against an untyped `Json` column is treated as equality** (iot `admin_acme` `decommission`, 2 per leg).
   `!("tier" in R.attr.labels)` gives `{NOT:{labels:"tier"}}`. Prisma rejects it ("Expected JsonFilter"), so this
   fails closed only by luck. If the input were accepted, it would return devices that have a `tier` key, which
   `check()` denies. For a typed `String` column the adapter refuses correctly ("in over a scalar is a CEL error").

### Under-grant

- **Adapter bug: the call-level `nullAttributeRepresentation: "explicit"` does not reach `ne`** (government
  `supervisor_bo` `approve`, 2 per leg). `R.assignedTo != "sv-bo"` gives `{assignedTo:{not:"sv-bo"}}`, which
  drops `k01` (`assigned_to` NULL, sent as explicit `null`, so CEL says true). Declaring
  `nullAttributeRepresentation: "explicit"` on the `assignedTo` entry fixes it (verified). The README says
  undeclared attributes follow the call-level convention.
- **Planner divergence on both PDPs** (not an adapter defect):
  - healthcare `nurse_ana` `view`: `!(has(patient.vip) && patient.vip)` is planned as `not(R.patient.vip)`, so
    `n02` (vip omitted) is denied by the plan and allowed by `check()`.
  - legal `client_globex` `view`: `privilege == null || privilege == ""` is planned as `eq(R.privilege, null)`
    alone, so `d06` (`privilege: ""`) is lost.
- **PDP 0.56.0 `has()` folding** (11 per leg): healthcare `admin_kim` `delete`, `dr_ortiz` `sign`,
  `researcher_li` `sign`; hr `ceo_e01` `view`, `hr_admin` `terminate`, `mgr_e02` `view`, `recruiter` `view`; media
  `reader_free` `read`, `reader_sub` `read`; plus the 0.56.0 halves of `nurse_ana` and `client_globex` above.

### Refused (documented `UnsupportedQueryPlanError`)

- Cross-model field-to-field comparison (Prisma field references are same-model only): fintech
  `approver_mia`/`approver_raj` `view`/`approve` (8), logistics `customs_us` `view` (2).
- `timestamp(a) > timestamp(b)` field-to-field: education `flag_late` (4, cross-model) and government `auditor_cy`
  `audit` (2, same-model). The README's timestamp section reads as if `timestamp()` on both sides is supported.
- `matches()` beyond LIKE: government `foia_release` (2), media `publish` (2).
- `exists` over a scalar list column (`keywords`, `String[]` or `Json`): media `lawyer_quinn` (4).
- `mult` of two element fields inside `exists`: retail `fraud_fin` (4).
- JSON map key read (`labels.env`, `labels.restricted`): iot `member_acme` `view`, `tech_eu` `update_firmware`
  (4).
