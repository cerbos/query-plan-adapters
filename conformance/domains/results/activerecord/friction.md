# activerecord: domain stress friction

Harness: `conformance/domains/harnesses/activerecord/harness.rb` (`DOMAINS_DB=sqlite|postgres|mysql|mariadb ruby harness.rb`
from that folder; it loads the adapter's Gemfile), one mapping per domain in `mappings/<domain>.rb`. ActiveRecord 8.0,
Ruby 3.3. Plans reach the adapter through the Cerbos Ruby SDK via the conformance harness's stub PDP
(`activerecord/spec/support/stub_pdp.rb`). PostgreSQL and MySQL/MariaDB load the kit's shared SQL; SQLite loads `rows.json`
through ActiveRecord (its own datetime text format), with JSON columns stored as JSON text.

| Store | pass | refused | under-grant | over-grant | error |
|---|---|---|---|---|---|
| sqlite | 337 | 12 | 17 | 2 | 0 |
| postgres (17, ICU en-US) | 337 | 12 | 17 | 2 | 0 |
| mysql (8.4, utf8mb4_0900_ai_ci) | 337 | 12 | 17 | 2 | 0 |
| mariadb (11.8, uca1400_ai_ci) | 337 | 12 | 17 | 2 | 0 |

All four stores give identical verdicts. Both over-grants are PDP 0.56.0 planner divergences, not adapter bugs.

## Per domain

The adapter has no inference: every plan variable is mapped by hand (`field` or `relation`), so the line counts
include models plus one entry per attribute.

| Domain | Lines | Configured beyond plain `field("col")` | Could not express | Score |
|---|---|---|---|---|
| healthcare | 52 | `self.inheritance_column = nil` (column `type` is STI by default), `primary_key = "note_id"`, `table_name` for `note_care_team`; `noteType`->`type`, `deleted`->`is_deleted`; 3-hop dotted path `patient.ward.hospital.region` | nothing | 2 |
| fintech | 55 | flattened `counterparty.*` mapped to `cp_*` columns; `Branch.primary_key = "code"`, `branch_code` FK | nothing | 2 |
| logistics | 52 | two `belongs_to` to `Hub` with explicit `class_name`/`foreign_key`; `fields: {"hub.code" => field("hub.code")}` inside the checkpoints relation (dotted member key) | nothing | 2 |
| education | 55 | `assignment.course.staff` needs a nested `has_many :through` (`Assignment has_many :staff, through: :course`; `Submission has_many :course_staff, through: :assignment, source: :staff`), because a top-level `relation` must be a collection and cannot start at a `belongs_to` | nothing | 3 |
| legal | 51 | `matter.team` / `matter.screened` as `has_many :through :matter`; call-level `:explicit` | nothing | 3 |
| retail | 44 | `table_name = "order"` (reserved word, quoted fine) | nothing | 1 |
| hr | 42 | self-referential `belongs_to :manager, class_name: "Employee"`, `manager.manager.id`; `Location.primary_key = "code"`; `request.resource.id` | nothing | 2 |
| media | 45 | `has_many :contributors, through: :article_contributors, source: :author`; `Section.primary_key = "slug"` | `keywords` (text[] / JSON array): no relation exists to map it to; `matches()` with a non-finite pattern | 4 |
| iot | 42 | none | `labels` JSON object: no JSON-path mapping, so `labels.env`/`labels.restricted` are unmappable and `"tier" in labels` is a type error (UNKNOWN) | 5 |
| government | 43 | `table_name = "agencies"`, `primary_key = "code"`; call-level `:explicit` | `matches("^[A-Z]{2}-[0-9]{4}-[0-9]+$")` | 2 |

Hardest mapping problem: the iot `labels` JSON object column. The attribute map has no JSON-path form and
operator overrides only run after a variable is mapped, so map key access and key membership cannot be expressed
at all. Second: a to-one chain that ends in a list (`assignment.course.staff`, `matter.team`) has to be rebuilt as
a `has_many :through` on the root model.

## Non-pass verdicts, grouped by cause

Counts are per store (identical on all four).

### Planner divergence, PDP 0.56.0 folds `has()` to true (11 under-grant, 2 over-grant)

0.56.0 drops or folds `has(R.attr.x)` for attributes the plan request omits; `check()` evaluates it. Every
0.56.0 plan below lacks the `has()` its 0.57.0 counterpart carries.

- over-grant healthcare `patient_p3/view`, hr `mgr_e02/view_compensation` (below).
- under-grant healthcare `admin_kim/delete`, `dr_ortiz/sign`, `researcher_li/sign`; education
  `prof_jones/grade`, `ta_lee/grade`; hr `ceo_e01/view`, `hr_admin/terminate`, `mgr_e02/view`, `recruiter/view`;
  media `reader_free/read`, `reader_sub/read`.

#### Over-grant 1: healthcare, PDP 0.56.0, `patient_p3`, `view`

- Policy: `has(R.attr.signedAt)` in the patient rule. 0.57.0 plan keeps it; 0.56.0 plan is
  `and(not(exists(R.codes, ...)), and(eq(R.patient.mrn, "MRN-0003"), ne(R.noteType, "psychotherapy"), not(R.deleted)))`.
- Row: `clinical_notes` `n13` (patient `p3`, MRN-0003, `type` consult, `signed_at` NULL, not deleted, no codes).
  `check()` denies it (no `signedAt`); the plan has no condition on `signedAt`.
- Emitted (PostgreSQL): `... AND (SELECT mrn FROM patients ... ) = 'MRN-0003' AND "clinical_notes"."type" != 'psychotherapy' AND NOT (is_deleted = TRUE)`
  returns `n05, n13`; allowed `n05`. The SQL is the exact translation of the plan.

#### Over-grant 2: hr, PDP 0.56.0, `mgr_e02`, `view_compensation`

- 0.56.0 plan `and(eq(R.manager.id, "e02"), lt(R.level, 6))`; 0.57.0 plan adds `has(R.salaryBand)`.
- Row: `employees` `e05` (manager `e02`, level 3, `salary_band` NULL). `check()` denies it.
- Emitted: `WHERE (SELECT id FROM employees m WHERE m.id = employees.manager_id) = 'e02' AND employees.level < 6`
  returns `e04, e05`; allowed `e04`.

Every adapter in the kit shows these two; the adapter cannot see an attribute the planner removed.

### Planner divergence on both PDPs (2 under-grant per PDP, 4 in all)

- healthcare `nurse_ana/view`: policy `!(has(R.attr.patient.vip) && R.attr.patient.vip)` plans as
  `not(R.patient.vip)`; the `has()` guard is dropped although it sits under a negation. Patient `p1` has no `vip`,
  so the plan errors (UNKNOWN) where `check()` allows `n02`.
- legal `client_globex/view`: policy `R.attr.privilege == null || R.attr.privilege == ""` plans as
  `eq(R.privilege, null)` only; `d06` (`privilege = ""`) is allowed by `check()` and excluded by the plan.

### Refused (6 per PDP, 12 in all)

- Adapter limitation, `matches()` with a pattern whose matches are not a finite literal set: media `editor_pat/publish`
  (`^[a-z0-9]+(-[a-z0-9]+)*$`), government `foia_dee/foia_release` (`^[A-Z]{2}-[0-9]{4}-[0-9]+$`).
  `UnsupportedOperatorError`. An operator override (`~` on PostgreSQL) would serve it; not added.
- Mapping the config cannot express, scalar list stored in a column: media `lawyer_quinn/read`,
  `lawyer_quinn/legal_review`. On PostgreSQL the adapter refuses the `text[]` column outright
  (`UnmappedAttributeError: ... is an array column`); on SQLite/MySQL/MariaDB the JSON column is accepted as a
  field and `exists()` over it is refused (`exists needs a collection`).
- Mapping the config cannot express, JSON object keys: iot `member_acme/view` (`labels.restricted`),
  `tech_eu/update_firmware` (`labels.env`): `UnmappedAttributeError`.

### Mapping the config cannot express, silently UNKNOWN (1 under-grant per PDP, 2 in all)

- iot `admin_acme/decommission`: `!("tier" in R.attr.labels)` over the JSON column is rendered `NOT ((NULL))`
  (a type error) instead of a refusal, so `dv02`, `dv03` are denied. Fail-closed, but it is an under-grant rather
  than the documented refusal.

## Store notes

- MySQL/MariaDB: government keys agencies `DSS` and `dss` apart, which the server default `*_ai_ci` collation
  rejects as a duplicate primary key, so that one domain's database is created `utf8mb4_0900_as_cs` /
  `utf8mb4_uca1400_as_cs`. Every other domain runs on the case-folding default.
- Probe (no golden): mapping a JSON array column as a `field` and calling `size()` on it raises
  `UnsupportedOperatorError: size requires a string operand, got json`, and `in` over it is UNKNOWN. Fail-closed.
