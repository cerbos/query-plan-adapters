# pgx: domain stress friction

Harness: `pgx/domains/` (its own Go module, so `go test ./...` in `pgx/` never reaches it). Run it
with `cd pgx/domains && go test ./...`. It starts `POSTGRES_IMAGE` (ICU `en-US`, as the conformance
leg does), runs each domain's `generated/sql/postgres.sql`, translates every golden with
`cerbospgx.Translate` and runs `SELECT "<pk>" FROM "<table>" WHERE <Where>` with `Result.Args` on a
`pgxpool.Pool`.

The adapter has no schema inference, so every mapping is a hand-written `MapperMap`, one file per
domain (`<domain>.go`). `domain.go` holds four small helpers every file shares (75 lines): `path()`
lowers a chain of `{table, from, to}` joins into a `Relation` with its `Via` hops, and `col`/`via`/
`list`/`objects` stamp the domain's null convention and value type on each entry. Every column is
declared with its value type (`ValueString`, `ValueNumber`, `ValueBool`, `ValueTimestamp`) and the
domain's null convention.

Results: 368 goldens (184 per PDP).

| store | pass | refused | error | under-grant | over-grant |
|---|---|---|---|---|---|
| postgres | 337 | 14 | 0 | 15 | 2 |

On PDP 0.57.0 alone: 175 pass, 7 refused, 2 under-grant, 0 over-grant.

## Per domain

| domain | lines | configured | cannot express | score |
|---|---|---|---|---|
| healthcare | 28 | 15 entries: 6 columns (`type`, `is_deleted` renames), 2 collections, 7 scalars through 1-3 to-one hops | nothing | 3 |
| fintech | 33 | 20 entries; the counterparty object is three flat columns, branch reached through 3 hops by its natural key `code` | nothing | 3 |
| logistics | 33 | 18 entries; two relations to `hubs` (`origin`, `destination`), and `hub.code` inside the `checkpoints` element as a `ScalarRelation` | nothing | 3 |
| education | 29 | 16 entries; `assignment.course.staff` is a to-many below two to-one hops: a `Relation` with two `Via` hops | nothing | 3 |
| legal | 27 | 15 entries, explicit nulls; `matter.team`/`matter.screened` with a `Via` hop through `matters` | nothing | 3 |
| retail | 27 | 14 entries; table `order` needs nothing (every identifier is quoted) | nothing | 2 |
| hr | 23 | 14 entries; the self-relation two hops up is `path(manager, manager)` | nothing | 2 |
| media | 26 | 13 entries; `contributors` is the many-to-many: element table `authors` with a `Via` hop through `article_contributors` | `keywords` (native `text[]`) | 3 |
| iot | 25 | 12 entries | `labels` (JSONB object): `labels.env`, `labels.restricted`, `"tier" in labels` | 4 |
| government | 26 | 16 entries, explicit nulls | nothing | 2 |

Friction common to several domains:

- **Everything is hand-written.** 153 entries over ten domains, each restating a table, a join
  column pair and a value type the database already knows. A wrong column only fails at query time
  (or, for a column of the right type, silently answers the wrong question).
- **`Hop` direction is easy to get backwards.** `ChildColumn` is the column on the table one step
  further in and `JoinColumn` the one on the hop's table, whichever side holds the foreign key; a
  to-one chain (`patient.ward`) puts the FK on the hop, a child table (`matter.team`) on the element.
  `path()` exists so each mapping states joins in the order they are walked instead.
- **The value type has no exported name.** `cerbospgx.ValueString` and friends are exported, but
  their type is `queryplan.ValueType` in an internal package, so a helper cannot take one as a
  parameter; `domain.go` passes an `Entry` template instead.
- Natural keys, reserved table names, two relations to one table and the self-relation cost nothing
  beyond spelling the join out.

## Hardest mapping problem

`iot.labels` (JSONB object read as a map) and `media.keywords` (`text[]`). No `Entry` reads a key of
a JSON column or an element of an array column, and a `Relation` must name a real table: the only
way to map either is a view that unnests the column into rows, which changes the database. Left
unmapped, every plan that touches them is refused with `ErrUnsupported`, as it should be.

## Non-pass verdicts by cause

### Over-grant (2): PDP 0.56.0 planner divergence, not the adapter

Both 0.56.0 plans drop a `has()` conjunct that 0.57.0 keeps; the adapter translates the plan
exactly and the plan admits a row `check()` denies. The 0.57.0 goldens pass.

1. healthcare / patient_p3 / view (0.56.0): returned `[n05, n13]`, allowed `[n05]`.
   - Policy conjunct `has(R.attr.signedAt)` is missing from the 0.56.0 plan:
     `and(not(exists(codes, ...)), and(eq(patient.mrn, "MRN-0003"), ne(noteType, "psychotherapy"), not(deleted)))`.
   - Row: `n13 {patient_id: p3, type: consult, signed_at: NULL, is_deleted: false}`; `p3.mrn = 'MRN-0003'`.
   - Emitted: `SELECT "note_id" FROM "clinical_notes" WHERE ((NOT (CASE WHEN EXISTS(... "code" LIKE $2 ...) ...)) AND (((SELECT "cerbos_rel_2"."mrn" FROM "patients" AS "cerbos_rel_2" WHERE ("cerbos_rel_2"."id" = "clinical_notes"."patient_id")) = $7::text) AND ("clinical_notes"."type" <> $8::text) AND (NOT "clinical_notes"."is_deleted")))`, args `[ICD-10 F% \ ICD-10 F% \ MRN-0003 psychotherapy]`.
2. hr / mgr_e02 / view_compensation (0.56.0): returned `[e04, e05]`, allowed `[e04]`.
   - Policy conjunct `has(R.attr.salaryBand)` is missing: plan `and(eq(manager.id, "e02"), lt(level, 6))`.
   - Row: `e05 {manager_id: e02, level: 3, salary_band: NULL}`.
   - Emitted: `SELECT "id" FROM "employees" WHERE (((SELECT "cerbos_rel_1"."id" FROM "employees" AS "cerbos_rel_1" WHERE ("cerbos_rel_1"."id" = "employees"."manager_id")) = $1::text) AND ("employees"."level" < $2::double precision))`, args `[e02 6]`.

### Under-grant (15)

- PDP 0.56.0 planner folds `has()` (12): education prof_jones/grade, ta_lee/grade; healthcare
  admin_kim/delete, dr_ortiz/sign, nurse_ana/view, researcher_li/sign; hr ceo_e01/view,
  hr_admin/terminate, mgr_e02/view, recruiter/view; media reader_free/read, reader_sub/read. All pass
  on 0.57.0.
- Planner divergence on 0.57.0 (1): healthcare nurse_ana/view. `!(has(R.attr.patient.vip) &&
  R.attr.patient.vip)` is planned as `not(patient.vip)`, which is NULL for `n02`'s patient (no
  `vip`) where `check()` allows. Fails closed.
- Planner divergence on both PDPs (2): legal client_globex/view. `R.attr.privilege == null ||
  R.attr.privilege == ""` is planned as `eq(privilege, null)` only, losing `d06` (`privilege = ""`).

### Refused, `ErrUnsupported` (14)

- Regex not lowerable, documented adapter limitation (4): government foia_dee/foia_release, media
  editor_pat/publish, both PDPs.
- JSONB object, mapping the adapter cannot express (6): iot member_acme/view, tech_eu/update_firmware,
  admin_acme/decommission, both PDPs.
- `text[]` column, mapping the adapter cannot express (4): media lawyer_quinn/read and
  /legal_review, both PDPs.

No `error` verdicts.
