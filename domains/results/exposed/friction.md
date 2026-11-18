# exposed: domain stress friction

Harness: `exposed/src/domainTest/kotlin/dev/cerbos/queryplan/exposed/domains/` (`ADAPTER_DOMAIN_STORE=h2|sqlite|postgres|mysql
./gradlew domainTest`, a separate source set and task). Each domain is one file: Exposed `Table` objects with `references(...)`
plus a `cerbosMapping(root = ...)`. Loading:
- H2, PostgreSQL and MySQL run the shared SQL. H2 runs with `DATABASE_TO_LOWER=TRUE` so Exposed's unquoted names match the
  quoted lowercase DDL.
- SQLite goes through Exposed instead (`SchemaUtils.create` plus `rows.json` inserts), because Exposed's `timestamp()` uses
  its own SQLite text format.

Plans arrive as `PlanResourcesResponse` (protobuf `JsonFormat`) and go to `ExposedQueryPlanAdapter.toFilter`. The query is
`root.select(id).where(op)`. Stores: H2 2.5, SQLite (`case_sensitive_like`), PostgreSQL 16 (ICU en-US) and MySQL 8.4
(`utf8mb4_0900_ai_ci`). Exposed is 1.6.1.

## Results

| store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| h2 | 337 | 14 | 15 | 2 | 0 |
| sqlite | 337 | 14 | 15 | 2 | 0 |
| postgres | 337 | 14 | 15 | 2 | 0 |
| mysql | 335 | 14 | 15 | **4** | 0 |

## HEADLINE: MySQL over-grant from the index-friendly collation pair (legal `lawyer_ali view`, both PDPs)

The plan is the same on both PDPs: `not(in("l-ali", R.matter.screened)) && in("l-ali", R.matter.team) && not(in("l-ali", R.matter.screened))`.
Allowed: d01, d02, d03. MySQL returns d01, d02, d03, **d08, d09**.

Rows: `matter_team` has PK `(matter_id, user_id)` and holds `(m4, 'L-ALI')`; documents d08 and d09 belong to matter m4.
CEL compares bytes, so `"l-ali" in ["L-ALI"]` is false and the PDP denies them.

Emitted membership subquery (excerpt):

```sql
(SELECT COUNT(1) FROM matters cerbos_4 INNER JOIN matter_team cerbos_5 ON cerbos_4.id = cerbos_5.matter_id
 WHERE (cerbos_4.id = documents.matter_id)
   AND (cerbos_5.user_id = 'l-ali' AND cerbos_5.user_id = (CONVERT('l-ali' USING utf8mb4) COLLATE utf8mb4_0900_bin))) > 0
```

`IndexedOnMysql` emits `col = v` (an indexable prefilter under the column's own `ai_ci` collation) AND
`col = (v COLLATE utf8mb4_0900_bin)` (the exact test). Correlated on the composite PK prefix, MySQL 8.4.13 answers from the
prefilter and drops the binary conjunct. Minimal repro, needing no adapter:

```sql
CREATE TABLE parent (id VARCHAR(10) PRIMARY KEY);
CREATE TABLE child (parent_id VARCHAR(10) NOT NULL, user_id VARCHAR(10) NOT NULL, PRIMARY KEY (parent_id, user_id));
INSERT INTO parent VALUES ('m1'), ('m4');
INSERT INTO child VALUES ('m1', 'l-ali'), ('m4', 'L-ALI');
SELECT p.id, (SELECT COUNT(1) FROM child c WHERE c.parent_id = p.id
   AND (c.user_id = 'l-ali' AND c.user_id = (CONVERT('l-ali' USING utf8mb4) COLLATE utf8mb4_0900_bin))) FROM parent p;
-- m1 1, m4 1   <- wrong: m4 must be 0
SELECT COUNT(1) FROM child c WHERE c.parent_id = 'm4'
   AND (c.user_id = 'l-ali' AND c.user_id = (CONVERT('l-ali' USING utf8mb4) COLLATE utf8mb4_0900_bin));
-- 0            <- correct with a constant parent
-- the binary conjunct alone (no prefilter) in the correlated form: m1 1, m4 0 (correct)
```

The cause is the adapter's MySQL-only prefilter/exact pair. It relies on MySQL keeping both conjuncts, and MySQL does not
when the prefilter can be served by an index lookup in a correlated subquery. The pair is applied wherever the target is a
string column, so any `x in R.attr.list` over a child table whose PK or index covers `(fk, value)` can over-grant on a
case- or accent-folding collation. jpa does not over-grant here: it collates the column side,
`convert(col using utf8mb4) collate utf8mb4_0900_bin`, and passes the same case on MySQL. The conformance corpus misses it
because it has no child table indexed on `(fk, value)` holding a case variant. Classification: **adapter bug (MySQL)**.

## Per domain

Exposed infers each relation's keys and cardinality from `references(...)`, including to-many relations nested below
to-one relations. Most mappings are therefore just column names. Lines are the whole file: tables plus mapping.

| domain | lines | configured beyond inference | not expressible | score |
|---|---|---|---|---|
| healthcare | 92 | `OMITTED` on nullable columns; `noteType`→`type`, `deleted`→`is_deleted` | none | 1 |
| fintech | 102 | counterparty as three dotted top-level keys | none | 1 |
| logistics | 93 | `relation(via = originHubId/destinationHubId)`: two keys reach `hubs`, so inference refuses | none | 2 |
| education | 94 | nothing: `assignment.course.staff` is inferred through two to-one relations; timestamp field-to-field translates | none | 1 |
| legal | 93 | per-attribute `EXPLICIT` on nullable columns (README is explicit that the call level is not enough) | none | 1 |
| retail | 83 | `Table("order")` (Exposed quotes it); optional `orderBy` on lines | none | 1 |
| hr | 76 | `relation(via = managerId)` twice for the self-relation | none | 2 |
| media | 77 | `relation(via = authorId)` (articles has two paths to authors) | `keywords` (`text[]`/JSON array) is a column, not a relation; **many-to-many**: contributors map to join-table rows, so `c.staff` (an authors column) cannot be mapped. A nested entry must read the relation's own table, and there is no `through`. This corpus never reaches it, because the plan reading `c.staff` is refused first for its regex. Regex with unbounded repetition | 4 |
| iot | 79 | `labels` kept as an opaque text column | `labels.env`, `labels.restricted`, `"tier" in labels`: a mapping takes a `Column`, never a JSON path expression | 5 |
| government | 79 | per-attribute `EXPLICIT` on nullable columns | regex with >256 alternatives | 1 |

**Hardest problem:** the JSON map column (iot). No mapping reaches inside it on any store. Second is many-to-many
through a join table (media), which has no declaration at all.

## Non-pass verdicts by cause

**Adapter bug, MySQL only: the collation prefilter is dropped in a correlated subquery (over-grant).**
- legal `lawyer_ali view` (0.56.0 and 0.57.0): d08 and d09. Repro above.

**PDP 0.56.0 folds `has()` to true (plan wrong, adapter faithful). Every store, 0.56.0 only.**
- over-grant: healthcare `patient_p3 view` (n13: `has(signedAt)` dropped, `signed_at` NULL); hr `mgr_e02 view_compensation`
  (e05: `has(salaryBand)` dropped, `salary_band` NULL; query `... WHERE (manager subquery) = 'e02' AND level < 6`)
- under-grant: education `prof_jones/ta_lee grade`; healthcare `admin_kim delete`, `dr_ortiz sign`, `researcher_li sign`;
  hr `ceo_e01 view`, `hr_admin terminate`, `mgr_e02 view`, `recruiter view`; media `reader_free read`, `reader_sub read`

**The PDP plan drops a guard on both versions (under-grant, fails closed).**
- healthcare `nurse_ana view` (n02: `!(has(patient.vip) && patient.vip)` planned as `not(patient.vip)`)
- legal `client_globex view` (d06: `privilege == null || privilege == ""` planned as `privilege == null`)

**Documented refusal (`UnsupportedPlanShapeException`).** Regex: media `editor_pat publish`, government `foia_dee foia_release`.

**Mapping the adapter cannot express (`UnmappedAttributeException`).** media `lawyer_quinn read/legal_review` (array column);
iot `admin_acme decommission`, `member_acme view`, `tech_eu update_firmware` (JSON map column).
