# ent: domain stress friction

Harness: `conformance/domains/harnesses/ent/` (its own Go module, with a `replace` to `ent/`).
Run it with `DOMAIN_STORES=sqlite go test ./...` there (`sqlite`, `postgres`, `mysql`, or a
comma list; default all three, one after another). Regenerate the clients with `go generate ./`.

- `schema/<domain>/schema.go`: the ent schema, written as an ent user would (field names are the
  column names, the id field carries `StorageKey` where the key column is not `id`, a `From/Ref`
  edge per foreign key, `entsql.Annotation` for each table name).
- `gen/<domain>/`: the client entc generates, with `cerbos_schema.go` from `entcerbos.Extension`.
- `<domain>.go`: the `CerbosSchema.Mapper` overrides plus the generated query that applies the
  predicate (`client.X.Query().Where(...).IDs(ctx)`).
- Stores: PostgreSQL and MySQL load `generated/sql/<dialect>.sql`; SQLite runs the DDL and loads
  `rows.json` itself, writing timestamps in `cerbosent.SQLiteTimestampLayout` (the kit's `.000Z`
  text does not sort against the adapter's 9-digit bound parameters).

Results: 368 goldens per store (184 per PDP), identical on all three stores.

| store | pass | refused | error | under-grant | over-grant |
|---|---|---|---|---|---|
| sqlite | 337 | 12 | 0 | 17 | 2 |
| postgres | 337 | 12 | 0 | 17 | 2 |
| mysql | 337 | 12 | 0 | 17 | 2 |

On PDP 0.57.0 alone: 175 pass, 6 refused, 3 under-grant, 0 over-grant.

## Per domain

Lines are the override file / the ent schema. The overrides are everything beyond inference.

| domain | lines | overrides beyond inference | cannot express | score |
|---|---|---|---|---|
| healthcare | 26 / 101 | `noteType` -> `type`, `deleted` -> `is_deleted`, `careTeam` as a `Declared` relation | nothing | 2 |
| fintech | 31 / 94 | `counterparty.{name,iban,country}` flattened onto `cp_*`, `flags` as `Declared`, `account.customer.branch.code` (natural key: the id field is named `id`, so `code` is not a name inference sees) | nothing | 2 |
| logistics | 33 / 82 | `trackingNumber`, `shipper` renames, `handling` as `Declared`, and the whole `checkpoints` element field set rewritten to add `hub.code`: inferred collection elements skip to-one edges | nothing | 3 |
| education | 24 / 85 | `members` as `Declared`. `assignment.course.staff` (to-many below two to-one hops) and the field-to-field timestamps were inferred | nothing | 1 |
| legal | 27 / 73 | `matter.team` and `matter.screened` as `Declared` relations with a hand-written `Hop` through `matters`; `WithInferredNullConvention(explicit)` | nothing | 3 |
| retail | 24 / 72 | `isGift` -> `gift`. Table `order` is one annotation; ent accepts a type named `Order` | nothing | 1 |
| hr | 25 / 54 | `email` -> `work_email`, `skills` as `Declared`. The self-referential `manager.manager.id` and `request.resource.id` were inferred | nothing | 1 |
| media | 25 / 57 | `section.slug` (natural key). The many-to-many `contributors` was inferred through the join table once `Author` had the inverse edge ent needs to call it M2M | `keywords` (`field.Strings`, a JSON/`text[]` column): no relation form | 3 |
| iot | 22 / 74 | none | `labels.env`, `labels.restricted`, `"tier" in labels` (JSON object column) | 4 |
| government | 26 / 60 | `agency.code` (natural key), `flags` as `Declared`; explicit nulls | nothing | 2 |

Friction common to several domains:

- **Composite-key child tables are unmodellable in ent.** Seven scalar lists (`careTeam`, `flags` x2,
  `handling`, `members`, `team`, `screened`, `skills`) live in tables keyed `(fk, value)`. ent
  needs a single id field (composite ids exist only for M2M edge schemas between two ent types), so
  none of them can be an edge and each needs a fully `Declared` `Relation`. Through a to-one hop
  (`legal`) that means writing the `Hop`'s `ChildColumn`/`JoinColumn` by hand.
- **Natural keys hide their column name.** ent's id field is always named `id`, so an attribute that
  names the key by its column (`branch.code`, `section.slug`, `agency.code`) needs
  `{Edge: ..., Column: <FieldID>}`.
- **Inferred collection elements drop to-one edges** (`elementFields` skips unique edges), so
  `c.hub.code` inside `checkpoints.exists(...)` forced an override of the whole element map.
- ent-side naming: `Client` is a predeclared ent identifier (type renamed `LegalClient`), `case` is a
  Go keyword (type renamed `BenefitCase`), and an `edge.To` with `StorageKey(edge.Table(...))` is
  only M2M if the target declares an inverse edge. None of these changed the mapping.
- MySQL could not hold `government` under the server default `utf8mb4_0900_ai_ci`: agency codes
  `DSS` and `dss` collide on the primary key. The harness gives that domain's database
  `utf8mb4_0900_bin` (what an application with case-distinct natural keys must do). Store
  limitation; every other domain runs under the default collation.

## Hardest mapping problem

JSON and array columns (`iot.labels`, `media.keywords`). The adapter has no `Entry` form for a key
of a JSON object or an element of a JSON/`text[]` column; a `Relation` needs a real table. The only
escape is a database view that explodes the column into rows, which is a schema change, not a
mapping. Worse, inference maps a `field.JSON`/`field.Strings` as a plain scalar column (type
undeclared), so `!("tier" in R.attr.labels)` is translated, not refused: the membership against a
scalar column renders `NULL` (UNKNOWN), and the rows are silently dropped (under-grant, fail
closed) where an unmapped attribute would have raised `ErrUnsupported`.

## Non-pass verdicts by cause

Counts are per store (the same on all three).

### Over-grant (2): PDP 0.56.0 planner divergence, not the adapter

Both are 0.56.0 goldens whose plan drops a `has()` conjunct; the adapter translates the plan
exactly, and the plan itself admits a row `check()` denies. The 0.57.0 plans keep `has()` and pass.
The adapter README already warns: "On Cerbos 0.56.0 and earlier, do not guard with `has()`".

1. healthcare / patient_p3 / view (0.56.0): returned `[n05, n13]`, allowed `[n05]`.
   - Policy: `R.attr.patient.mrn == P.attr.mrn && R.attr.noteType != "psychotherapy" && has(R.attr.signedAt) && !R.attr.deleted`.
   - 0.56.0 plan (conjunct): `and(eq(patient.mrn, "MRN-0003"), ne(noteType, "psychotherapy"), not(deleted))`, no `has(signedAt)`.
   - Row: `clinical_notes n13 {patient_id: p3, type: consult, signed_at: NULL, is_deleted: false}`, patient `p3 {mrn: MRN-0003}`.
   - Emitted (SQLite): `... AND (((SELECT mrn FROM patients AS r2 WHERE r2.id = clinical_notes.patient_id) = 'MRN-0003') AND (clinical_notes.type <> 'psychotherapy') AND (NOT clinical_notes.is_deleted))`, which `n13` satisfies.
2. hr / mgr_e02 / view_compensation (0.56.0): returned `[e04, e05]`, allowed `[e04]`.
   - Policy: `V.direct_report && R.attr.level < P.attr.level && has(R.attr.salaryBand)`.
   - 0.56.0 plan: `and(eq(manager.id, "e02"), lt(level, 6))`, no `has(salaryBand)`.
   - Row: `employees e05 {manager_id: e02, level: 3, salary_band: NULL}`.
   - Emitted: `((SELECT r1.id FROM employees AS r1 WHERE r1.id = employees.manager_id) = 'e02') AND (employees.level < 6)`.

### Under-grant (17)

- PDP 0.56.0 planner folds `has()` (12): education prof_jones/grade, ta_lee/grade; healthcare
  admin_kim/delete, dr_ortiz/sign, nurse_ana/view, researcher_li/sign; hr ceo_e01/view,
  hr_admin/terminate, mgr_e02/view, recruiter/view; media reader_free/read, reader_sub/read. Each
  0.56.0 plan has no `has` operator where the 0.57.0 plan has one to three; all pass on 0.57.0.
- Planner divergence on 0.57.0 too (1): healthcare nurse_ana/view. The policy's
  `!(has(R.attr.patient.vip) && R.attr.patient.vip)` is planned as `not(patient.vip)`; for `n02`,
  whose patient has no `vip`, `check()` allows and the plan errors (NULL). Fails closed.
- Planner divergence on both PDPs (2): legal client_globex/view. The policy's
  `R.attr.privilege == null || R.attr.privilege == ""` is planned as `eq(privilege, null)` only, so
  `d06` (`privilege = ""`) is lost. Fails closed.
- Mapping the adapter cannot express, translated instead of refused (2): iot admin_acme/decommission
  on both PDPs. `!("tier" in R.attr.labels)` over the inferred JSON column renders `NOT NULL`.
  Adapter limitation (inference treats JSON as a scalar column); fails closed.

### Refused, `ErrUnsupported` (12)

- Regex not lowerable, documented adapter limitation (4): government foia_dee/foia_release
  (`^[A-Z]{2}-[0-9]{4}-[0-9]+$`), media editor_pat/publish (`^[a-z0-9]+(-[a-z0-9]+)*$`), both PDPs.
- JSON object keys, mapping the adapter cannot express (4): iot member_acme/view
  (`labels.restricted`), tech_eu/update_firmware (`labels.env`), both PDPs.
- Array column used as a collection, mapping the adapter cannot express (4): media
  lawyer_quinn/read and /legal_review (`keywords.exists(...)`, "mapped to a column but is used as a
  collection"), both PDPs.

No `error` verdicts.
