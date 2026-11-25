# duckdb: domain stress friction

Harness: `conformance/domains/harnesses/duckdb/harness.py`
(`../../../../duckdb/.venv/bin/python harness.py [domain ...]`), one module per domain holding its
views and its mapping, plus `_kit.py` for the mapping helpers. Plans reach `translate()` through the
SDK's HTTP `CerbosClient` against the conformance harness's stub PDP (`duckdb/tests/corpus.py`).
DuckDB runs in-process: each domain loads `generated/rows.json` into an in-memory database, then
builds a view per resource that aggregates its child tables into `LIST` columns with
`coalesce(list(... ORDER BY ...), [])`, the shape the adapter's README asks for.

Stores, both DuckDB 2.0.1:

- `duckdb`: defaults, `TIMESTAMPTZ` columns.
- `duckdb-hostile`: `default_collation = 'nocase'`, `TimeZone = 'Pacific/Chatham'` and naive
  `TIMESTAMP` columns holding UTC. Text primary and foreign keys are declared `COLLATE "binary"`
  (see below).

| store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| duckdb / duckdb-hostile | 345 | 6 | 15 | 2 | 0 |

The two over-grants and 12 of the 15 under-grants come from the PDP 0.56.0 planner folding `has()`;
the other 3 under-grants are plans that both PDPs record differently from what `check()` decides.
They are the same 17 cases, with the same causes, as pgx (`results/pgx/friction.md`). Every filter
the adapter emitted matched the plan it was given. On 0.57.0 the adapter passes every case except
the planner divergences and the 3 iot `labels` refusals.

## Per domain

Line counts are the domain module (view SQL plus mapping).

### healthcare (38 lines). Difficulty 1
- `careTeam` is a `LIST(VARCHAR)` and `codes` a `LIST(STRUCT(system, code))`, both aggregated in
  the view. `patient.ward.hospital.region` is a three-step `Join` with two `Hop`s.

### fintech (47 lines). Difficulty 1
- `counterparty.*` is flattened into `cp_*` columns; `flags` and `approvals` are lists. DECIMAL
  amounts compare as DOUBLE. All pass.

### logistics (48 lines). Difficulty 2
- `checkpoints` carries a nested struct per element (`hub.code`), mapped as a field with the tuple
  path `("hub", "code")`. Origin and destination are two `Join`s to `hubs`, each its own subquery.
  All pass.

### education (48 lines). Difficulty 2
- `assignment.course.staff` is a list on a parent two hops away: a second view (`courses_v`)
  aggregates the staff, and the list entry carries a `Join` through `assignments`.

### legal (45 lines). Difficulty 2
- Same pattern as education: `matter.team` and `matter.screened` are lists on `matters_v`, reached
  by a `Join` from `documents_v`.

### retail (44 lines). Difficulty 1
- The table is called `order`; the view `order_v` sidesteps quoting in hand-written SQL, and the
  adapter quotes every identifier anyway. All pass.

### hr (35 lines). Difficulty 1
- `manager.manager.id` is a self-join chain (`employees` twice); the adapter aliases each step.

### media (36 lines). Difficulty 1
- `keywords` is a native `VARCHAR[]` column and maps directly as a list of strings, which the SQL
  adapters without array columns refuse. `contributors` is the many-to-many through
  `article_contributors`, aggregated in the view.

### iot (42 lines). Difficulty 3
- `labels` is a JSON object that policies index as a map. No `Entry` reads a JSON key, so it stays
  unmapped and the 6 plans that touch it are refused. `alerts` is a `LIST(STRUCT)`.

### government (36 lines). Difficulty 1
- `caseNumber` regex is a constant pattern and translates. All pass.

## Hardest mapping problem

Join keys under a case-folding collation. Government holds agencies `DSS` and `dss`. Under
`default_collation = 'nocase'` with plain `VARCHAR` keys, the adapter's correlated subquery
`agencies.code = cases_v.agency_code` matched both rows and DuckDB raised "More than one row
returned by a subquery" (6 errors per PDP pair). A `Join` names columns, not types, so the adapter
cannot collate the key itself without breaking numeric keys; the README now says text keys that can
differ by case need `COLLATE "binary"`, as the hostile leg declares them. In a list reached through a
join the same mismatch would have joined the wrong parent silently. The other adapters' MySQL legs
hit the same rows as a primary-key collision at load time.

## Non-pass verdicts by cause

### Over-grant (2): PDP 0.56.0 planner divergence, not the adapter

- healthcare / patient_p3 / view: the 0.56.0 plan drops `has(R.attr.signedAt)`; returns `n13`.
- hr / mgr_e02 / view_compensation: the 0.56.0 plan drops `has(R.attr.salaryBand)`; returns `e05`.

### Under-grant (15)

- PDP 0.56.0 planner folds `has()` (12): education prof_jones/grade, ta_lee/grade; healthcare
  admin_kim/delete, dr_ortiz/sign, nurse_ana/view, researcher_li/sign; hr ceo_e01/view,
  hr_admin/terminate, mgr_e02/view, recruiter/view; media reader_free/read, reader_sub/read.
- Planner divergence on 0.57.0 (1): healthcare nurse_ana/view (`not(patient.vip)` is NULL where
  `check()` allows).
- Planner divergence on both PDPs (2): legal client_globex/view (`privilege == ""` dropped).

### Refused, `UnsupportedPlanError` (6)

- JSON object read as a map (6): iot member_acme/view, tech_eu/update_firmware,
  admin_acme/decommission, both PDPs.

No `error` verdicts.
