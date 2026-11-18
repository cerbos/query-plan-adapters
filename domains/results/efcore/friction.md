# efcore: domain mapping friction

Harness: `efcore/domains/` (a standalone console project, outside the adapter's `.slnx`, so the
adapter's CI never builds it). Run `efcore/domains/run.sh sqlite|postgres|sqlserver [domain...]`.
`Harness.cs` holds the loader and the verdict loop, `DomainContext.cs` a snake_case column
convention (the job `EFCore.NamingConventions` does in an application), and `Mappings/<Domain>.cs`
each domain's entities, `DbContext` and `AttributeMap`. Plans are parsed into the SDK's
`PlanResourcesFilter` with `JsonParser`, as the conformance harness does, and handed to
`QueryPlanAdapter.ToFilter`; the predicate runs as `db.Set<TRoot>().Where(predicate).Select(id)`.

Stores. SQLite runs in memory: `EnsureCreated`, then `generated/rows.json` is loaded through the EF
model, because EF's SQLite `DateTime` convention (`yyyy-MM-dd HH:mm:ss.FFFFFFF`) is not the kit's
ISO text and textual comparison would break. PostgreSQL 17 (initialised under ICU `en-US`, as the
conformance leg is) and SQL Server 2022 (database collation `Latin1_General_100_BIN2_UTF8`) load
the kit's shared `generated/sql/<dialect>.sql`, and the EF model maps onto that existing schema.

Results, identical on all three stores (368 goldens each):

| Store | pass | refused | error | under-grant | over-grant |
|---|---|---|---|---|---|
| sqlite | 341 | 8 | 0 | 17 | 2 |
| postgres | 341 | 8 | 0 | 17 | 2 |
| sqlserver | 341 | 8 | 0 | 17 | 2 |

**The adapter translated every plan it accepted exactly.** I checked every non-pass with a small
CEL evaluator that runs each recorded plan over `generated/resources.json`. It agrees with
`check()` on all 341 passes. On each of the 19 under- and over-grants, the adapter returned exactly
the rows that the plan selects, and the plan disagrees with `check()`. The PDP's planner causes all
19, not the adapter.

## Per domain

Line counts: the whole file (entities + context + map), the `AttributeMap` alone, and the
`OnModelCreating` body. The adapter has no schema inference: every attribute the policies read
is one explicit `Field`/`List` line, so the map is roughly one line per projected attribute.

| Domain | File / map / model lines | Configured beyond the obvious | Could not express | Score |
|---|---|---|---|---|
| healthcare | 103 / 18 / 14 | `noteType` → column `type`, `deleted` → `is_deleted`; PK `note_id`; composite-key care team | nothing | 2 |
| fintech | 123 / 23 / 21 | counterparty as an owned type with three `HasColumnName`s; **`decimal` rejected by `Field`**, mapped as `(double)p.Amount` / `(double?)…OverdraftLimit` | nothing (after the cast) | 3 |
| logistics | 105 / 22 / 15 | two `HasOne` to `hubs` with explicit FKs and `NoAction`; nullable carrier; `(double)DeclaredValue` cast | `c.hub.code`: a to-one object inside a list element (refused) | 4 |
| education | 100 / 19 / 9 | list path through two to-one hops (`s.Assignment.Course.Staff`) works as is | nothing | 2 |
| legal | 103 / 18 / 14 | `Nulls = NullConvention.Explicit` for the call; two scalar lists on the matter | nothing | 2 |
| retail | 85 / 19 / 7 | `ToTable("order")`; EF quotes it on every provider; BIGINT as `long` | nothing | 1 |
| hr | 81 / 15 / 12 | self-relation `HasOne(Manager).WithMany().HasForeignKey(ManagerId)`; `e.Manager!.Manager!.Id`; natural key `locations.code` | nothing | 2 |
| media | 79 / 17 / 14 | many-to-many as an EF skip navigation (`UsingEntity("article_contributors", …)`, shadow FKs); `keywords` as a `List<string>` primitive collection (`text[]` on PostgreSQL, JSON text elsewhere) mapped `List(…, a => a.Keywords, k => k)` | `slug.matches("^[a-z0-9]+(-[a-z0-9]+)*$")` (documented regex refusal) | 3 |
| iot | 96 / 19 / 13 | `labels` as an owned `ToJson("labels")` type with fixed `Env`/`Tier`/`Restricted` properties, each mapped as `labels.<key>` | `"tier" in R.attr.labels` (the whole map) is refused; a free-form key outside the CLR class cannot be mapped at all | 4 |
| government | 81 / 17 / 7 | explicit nulls; `(double?)MonthlyAmount` cast; natural key `agencies.code` | `caseNumber.matches("^[A-Z]{2}-[0-9]{4}-[0-9]+$")` (documented regex refusal) | 2 |

Notes:

- **`decimal` is rejected.** The README's Gotchas says "An `int`, `long` or `decimal` column is
  converted to `double` in SQL". But `ScalarKinds.Of` (Values.cs) has no `decimal`, so
  `Field("…amount", p => p.Amount)` throws `ArgumentException` when the map is built. The fix in
  user code is a cast in the path lambda, `p => (double)p.Amount`. EF translates the cast on all
  three providers, including SQLite, where EF stores `decimal` as TEXT. With the cast, every
  DECIMAL comparison against a double literal passes (fintech 5000.50 / 5000.51, government
  nullable amounts, logistics declared value). This is a mismatch between the code and the
  README. It is not unsafe.
- **No object mapping inside a list element.** At the root, a dotted name such as
  `request.resource.attr.patient.ward.id` maps through navigations. Inside `List(…, fields)`, the
  translator resolves each segment against the element's fields. A field cannot be an object, so
  `c.hub.code` raises `UnmappedAttributeException` ("the element mapping has no field 'hub'").
  `Field("hub.code", x => x.Hub.Code)` is accepted but never matched. This refusal is safe.
- **No map-typed attribute.** `AttributeMap` maps scalars and lists only. The owned JSON type
  covers `labels.env` and `has(labels.restricted)`, with EF reading the JSON via `json_extract`,
  `->>` and `JSON_VALUE`. Key membership on the whole map has no mapping, and the adapter refuses
  it.
- Snake-case naming, table names and composite keys are plain EF configuration. The adapter
  needs nothing from the model beyond the lambdas in the map.

## Every non-pass, by cause (identical on sqlite, postgres, sqlserver)

### Over-grants (2): PDP 0.56.0 planner drops `has()` conjuncts. The adapter is faithful.

1. **hr / 0.56.0 / mgr_e02 / view_compensation**. The policy is `V.direct_report && R.attr.level < P.attr.level && has(R.attr.salaryBand)`.
   - The 0.56.0 plan is `R.attr.manager.id == "e02" && R.attr.level < 6`, with `has(salaryBand)` missing. The 0.57.0 plan keeps it.
   - The rows: e04 `{level: 4, manager.id: e02, salaryBand: "C"}` and e05 `{level: 3, manager.id: e02}`, where `salary_band` is NULL, so the attribute is omitted.
   - The emitted SQL (SQLite):
     ```sql
     SELECT "e"."id" FROM "employees" AS "e"
     LEFT JOIN "employees" AS "e0" ON "e"."manager_id" = "e0"."id"
     WHERE "e0"."id" IS NOT NULL AND "e0"."id" = 'e02' AND CAST("e"."level" AS REAL) < 6.0
     ```
   - It returns e04 and e05. `check()` allowed only e04.
2. **healthcare / 0.56.0 / patient_p3 / view**. The policy is `mrn == P.attr.mrn && noteType != "psychotherapy" && has(R.attr.signedAt) && !deleted`.
   - The 0.56.0 plan has no `has(R.attr.signedAt)`.
   - The rows: n05 is signed, and n13 `{mrn: MRN-0003, noteType: consult}` has `signed_at` NULL.
   - The emitted SQL:
     ```sql
     SELECT "c"."note_id" FROM "clinical_notes" AS "c"
     INNER JOIN "patients" AS "p" ON "c"."patient_id" = "p"."id"
     WHERE NOT EXISTS (SELECT 1 FROM "note_codes" AS "n" WHERE "c"."note_id" = "n"."note_id"
       AND "n"."system" = 'ICD-10' AND substr("n"."code", 0 + 1, 1) = 'F')
       AND "p"."mrn" = 'MRN-0003' AND "c"."type" <> 'psychotherapy' AND NOT ("c"."is_deleted")
     ```
   - It returns n05 and n13. `check()` allowed only n05.

The 0.57.0 plans for both cases pass.

### Under-grants (17): PDP planner divergences. The adapter is faithful.

- **PDP 0.56.0 mishandles `has()`** (13): the planner drops `has(x)` or treats `!has(x)` as false, which collapses some plans to `KIND_ALWAYS_DENIED`. The cases:
  - healthcare: admin_kim delete, dr_ortiz sign, researcher_li sign, nurse_ana view
  - education: prof_jones grade, ta_lee grade
  - hr: ceo_e01 view, hr_admin terminate, mgr_e02 view, recruiter view
  - media: reader_free read, reader_sub read (`!has(embargoUntil) || …` loses its first disjunct)
  - iot: member_acme view
- **PDP 0.57.0 and 0.56.0 drop `has()` under a negation** (4):
  - **healthcare nurse_ana view** (0.57.0; 0.56.0 is counted above). `!(has(R.attr.patient.vip) && R.attr.patient.vip)` is planned as `!R.attr.patient.vip`. n02's patient has `vip` NULL, which is omitted, so the plan errors where `check()` allows.
  - **iot member_acme view**, both PDPs. The DENY rule `has(labels.restricted) && labels.restricted == "yes"` is planned as `!(labels.restricted == "yes")`, so every device without a `restricted` label is denied.
  - **legal client_globex view**, both PDPs. `R.attr.privilege == null || R.attr.privilege == ""` is planned as `privilege == null`, so d06, whose `privilege` is `""`, is lost.

### Refused (8): documented `QueryPlanRefusedException`. No error verdicts.

- Regex not lowerable, an adapter limitation (4): media editor_pat publish (`^[a-z0-9]+(-[a-z0-9]+)*$`) and government foia_dee foia_release (`^[A-Z]{2}-[0-9]{4}-[0-9]+$`), on both PDPs.
- The mapping cannot express it (4):
  - logistics hubop_lhr view: `c.hub.code`, an object inside a list element (`UnmappedAttributeException`).
  - iot admin_acme decommission: `"tier" in R.attr.labels`, key membership on a JSON map (`UnmappedAttributeException`).
  - Both refusals happen on both PDPs.

Hardest mapping problem: iot `labels`. A free-form JSON object has no `AttributeMap` form. The
owned-JSON workaround needs a fixed key set in a CLR class, and key membership on the map stays
refused.
