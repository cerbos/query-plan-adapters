# sequel: domain stress friction

Harness: `conformance/domains/harnesses/sequel/harness.rb` (`DOMAINS_DB=sqlite|postgres|mysql ruby harness.rb` from
that folder), one mapping per domain in `mappings/<domain>.rb`. Sequel 5.110, Ruby 3.3,
`Sequel.default_timezone = :utc`, MySQL through trilogy with `fractional_seconds: true` and
`collation_connection = utf8mb4_0900_ai_ci`. Plans reach the adapter through the Cerbos Ruby SDK via the
conformance harness's stub PDP. PostgreSQL and MySQL load the kit's shared SQL; SQLite loads `rows.json` through
Sequel (its own datetime text format).

| Store | pass | refused | under-grant | over-grant | error |
|---|---|---|---|---|---|
| sqlite | 333 | 18 | 15 | 2 | 0 |
| postgres (17, ICU en-US) | 333 | 18 | 15 | 2 | 0 |
| mysql (8.4, utf8mb4_0900_ai_ci) | 333 | 18 | 15 | 2 | 0 |

All three stores give identical verdicts. Both over-grants are PDP 0.56.0 planner divergences, not adapter bugs.

## Per domain

No inference: every plan variable is mapped by hand (`field` or `association`). Sequel has no `has_many :through`
and no `order_field`, but `many_to_many` and `one_to_many` take `left_primary_key` / `primary_key`, which covered
every to-one-then-list chain.

| Domain | Lines | Configured beyond plain `field("col")` | Could not express | Score |
|---|---|---|---|---|
| healthcare | 49 | `noteType`->`type`, `deleted`->`is_deleted`; `one_to_many key: :note_id`; 3-hop dotted path | nothing | 2 |
| fintech | 55 | flattened `counterparty.*` -> `cp_*`; `many_to_one :branch, key: :branch_code` | nothing | 2 |
| logistics | 52 | two `many_to_one` to `Hub` with explicit `key:`; dotted member key `"hub.code"` inside the checkpoints association | nothing | 2 |
| education | 55 | `assignment.course.staff` spelled as `many_to_many :course_staff, join_table: :assignments, left_key: :id, left_primary_key: :assignment_id, right_key: :course_id, right_primary_key: :course_id` (an association mapping must start at a collection, and a nested chain cannot start at a `many_to_one`) | nothing | 4 |
| legal | 53 | `matter.team`/`matter.screened` as `one_to_many key: :matter_id, primary_key: :matter_id` off the document's FK; `null_representation: :explicit` declared on each of the 4 nullable fields (see below) | nothing | 3 |
| retail | 43 | `Sequel::Model(DB[:order])` (reserved word quoted fine) | nothing | 1 |
| hr | 42 | self-referential `many_to_one :manager`, `manager.manager.id`; `request.resource.id` | nothing | 2 |
| media | 41 | `many_to_many :contributors, join_table: :article_contributors` | `keywords` (text[] / JSON array) has no association; `matches()` with a non-finite pattern; `now()` | 4 |
| iot | 44 | none | `labels` JSON object: no JSON-path mapping (`labels.env`, `labels.restricted`, `"tier" in labels`) | 5 |
| government | 45 | `many_to_one :agency, key: :agency_code`; `null_representation: :explicit` on 4 nullable fields | `matches()` with a non-finite pattern | 3 |

Hardest mapping problem: the iot `labels` JSON object (nothing in the attribute map reaches inside a JSON
column). Runner-up: education's two-hop to-one chain ending in a list, which only a `many_to_many` with
`left_primary_key`/`right_primary_key` over the `assignments` table can express.

## Non-pass verdicts, grouped by cause

Counts are per store (identical on all three).

### Planner divergence, PDP 0.56.0 folds `has()` to true (9 under-grant, 2 over-grant)

0.56.0 drops `has(R.attr.x)` that 0.57.0 keeps.

- over-grant healthcare `patient_p3/view`: 0.56.0 plan lacks `has(R.signedAt)`; row `n13` (`signed_at` NULL,
  patient `p3`, MRN-0003, not deleted) is returned. `check()` denies it. Emitted (PostgreSQL):
  `... AND ((SELECT mrn FROM patients AS cerbos_patients_2 WHERE id = clinical_notes.patient_id) = 'MRN-0003') AND (clinical_notes.type != 'psychotherapy') AND (NOT is_deleted)`
  returns `n05, n13`; allowed `n05`.
- over-grant hr `mgr_e02/view_compensation`: 0.56.0 plan `and(eq(R.manager.id, "e02"), lt(R.level, 6))` lacks
  `has(R.salaryBand)`; row `e05` (manager `e02`, level 3, `salary_band` NULL) is returned. Emitted:
  `WHERE ((SELECT id FROM employees AS cerbos_employees_1 WHERE id = employees.manager_id) = 'e02') AND (employees.level < 6.0)`.
- under-grant healthcare `admin_kim/delete`, `dr_ortiz/sign`, `researcher_li/sign`; education
  `prof_jones/grade`, `ta_lee/grade`; hr `ceo_e01/view`, `hr_admin/terminate`, `mgr_e02/view`, `recruiter/view`.

Both over-grants are exact translations of plans that already admit the row; every adapter in the kit has them.

### Planner divergence on both PDPs (2 under-grant per PDP, 4 in all)

- healthcare `nurse_ana/view`: `!(has(R.attr.patient.vip) && R.attr.patient.vip)` plans as `not(R.patient.vip)`;
  `n02` (patient without `vip`) is allowed by `check()` and UNKNOWN in the plan.
- legal `client_globex/view`: `R.attr.privilege == null || R.attr.privilege == ""` plans as
  `eq(R.privilege, null)`; `d06` (`privilege = ""`) is lost.

### Refused (9 per PDP, 18 in all)

- Adapter limitation, `now()` literal: legal `records_eli/destroy`, media `reader_free/read`, `reader_sub/read`.
  The planner folds `now()` to a nanosecond literal and the adapter refuses any timestamp literal with
  sub-microsecond precision (`UnsupportedOperatorError`, listed in its ledger). ActiveRecord passes the same
  plans by moving the bound to the microsecond grid (ceil for `<`, floor for `<=`). Every policy using `now()` is
  therefore unusable with this adapter on every store.
- Adapter limitation, `matches()` with a non-finite pattern: media `editor_pat/publish`, government
  `foia_dee/foia_release`.
- Mapping the config cannot express: media `lawyer_quinn/read`, `lawyer_quinn/legal_review` (`exists` over the
  `keywords` column: `exists needs a collection`); iot `member_acme/view`, `tech_eu/update_firmware`
  (`labels.restricted`/`labels.env` unmapped).

### Mapping the config cannot express, silently UNKNOWN (1 under-grant per PDP, 2 in all)

- iot `admin_acme/decommission`: `!("tier" in R.attr.labels)` is rendered `NOT NULL` over the JSON column.

## Adapter bug fixed around in the mapping: call-level `:explicit` does not reach undeclared fields

With only `null_attribute_representation: :explicit` on the call (what the README's first paragraph tells an
application that sends explicit nulls to pass), government `supervisor_bo/approve` (0.56.0 and 0.57.0) was an
under-grant on every store:

- Plan fragment: `ne(R.assignedTo, "sv-bo")`. Row `k01`: `assigned_to` NULL, which the application sends as
  `assignedTo: null`, so CEL gives `null != "sv-bo"` = true and `check()` allows `k01`.
- Emitted: `("cases"."assigned_to" != 'sv-bo')`, which is NULL for `k01`.

`Translator#explicit_null?` reads only the attribute's own declaration
(`@null_representations[node] == :explicit`), while ActiveRecord's reads the call's default too (#592), so the
`NOT (col IS NOT NULL AND col = c)` expansion never applies to an undeclared column. Every shape this affects
(`ne`, `not(eq)`, `in`, `not(in)` over a NULL column) evaluates to NULL where CEL is definite, so it can only
under-grant. The mappings for legal and government therefore declare `null_representation: :explicit` on each
nullable column (privilege, retention_until, matter.lead_partner_id, matter.client.industry; closed_on,
assigned_to, monthly_amount, applicant.citizenship), as the README's per-attribute section describes; with that
the case passes.

## Probe outside the goldens: `size()` over a JSON column of unknown type over-grants

Not exercised by any golden, but it is the mapping a user would write for media `keywords` on SQLite:

```ruby
# SQLite: CREATE TABLE articles (id varchar PRIMARY KEY, keywords json NOT NULL)
# rows: ('a1', '["legal","x"]'), ('a2', '["other"]'), ('a3', '[]')
attributes = {"request.resource.attr.keywords" => Cerbos::Sequel.field("keywords")}
# plan: gt(size(R.attr.keywords), 0)
```

emits `length(json_extract(replace(... json_quote(articles.keywords) ...), '$')) > 0` and returns `a1, a2, a3`:
Sequel's schema type for a SQLite `json` column is `nil`, the adapter treats it as a string, and `'[]'` has
length 2. CEL's `size([])` is 0, so `a3` is an over-grant. ActiveRecord refuses the same plan
(`size requires a string operand, got json`).

## Store notes

- MySQL: government keys agencies `DSS` and `dss` apart, which `utf8mb4_0900_ai_ci` rejects as a duplicate
  primary key, so that domain's database is created `utf8mb4_0900_as_cs`; the connection stays on `ai_ci`.
