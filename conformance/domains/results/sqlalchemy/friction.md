# sqlalchemy: domain stress friction

Harness: `conformance/domains/harnesses/sqlalchemy/harness.py` (`../../../../sqlalchemy/.venv/bin/python harness.py <store>`),
one module per domain
holding its declarative models and both mappings. Plans reach `get_query` through the SDK's HTTP
`CerbosClient` against the conformance harness's stub PDP (`sqlalchemy/tests/corpus.py`). Every store loads
`generated/rows.json` through the models (`create_all` plus Core inserts), so SQLite holds
SQLAlchemy's own datetime format.

Two mappings per domain, as in the conformance harness:

- `<store>.json`: **declared**. Every attribute written out: columns, correlated scalar subqueries
  for to-one chains, hand-written `Relation`s for collections, lambda variables, and
  `relation_overrides()`.
- `<store>+schema.json`: **inferred**. Relationships on the models carry the attribute names, and
  `attr_map` holds only what inference cannot know.

Stores: SQLite (`case_sensitive_like` on), PostgreSQL 17 (ICU en-US collation), MySQL 8.4 (server
default `utf8mb4_0900_ai_ci`). The results are identical on all three stores.

| store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| sqlite / postgresql / mysql (declared) | 327 | 24 | 15 | 2 | 0 |
| sqlite+schema / postgresql+schema / mysql+schema (inferred) | 323 | 28 | 15 | 2 | 0 |

The two over-grants and 9 of the 15 under-grants come from the PDP 0.56.0 planner folding
`has()`. The other 6 under-grants are plans that both PDPs record differently from what `check()`
decides. For both PDP tags, every SQL filter the adapter emitted matched the plan it was given.

## Per domain

The file size counts the models too. "Inferred config" is the `attr_map` and option entries the
inferred mapping needed.

### healthcare (106 lines; inferred config: 3 entries). Difficulty 1
- Inferred: `noteType -> type`, `deleted -> is_deleted`, and `careTeam` as
  `related(ClinicalNote.care_team, NoteCareTeam.clinician_id)`, a list of scalars. The 3-hop
  `patient.ward.hospital.region` chain, the nullable `ward` hop and the `codes` lambda (`c.system`,
  `c.code`) needed nothing.
- Declared: a scalar subquery per to-one leaf, plus `"c"` mapped to the codes `Relation` (the bare
  lambda variable needs its own entry).
- Not expressible: `has(R.attr.signedAt)` on a timestamp column is refused (see cause A).

### fintech (115 lines; 4 entries). Difficulty 1
- `counterparty.{name,iban,country} -> cp_*` (a flattened object) and `flags` as `related(...flag)`.
  `account.customer.branch.code` (3 hops, natural-key `branches.code`), the bare nullable
  `account.customer.pep`, DECIMAL against a double literal, and field-to-field
  `currency == account.currency` across a relation were all inferred. The bare lambda `f` over a
  scalar list resolved on its own.
- All pass.

### logistics (109 lines; 3 entries). Difficulty 2
- `trackingNumber -> tracking_no`, `shipper -> shipper_account`, `handling` as `related(...code)`.
  Two relationships to `hubs` need `foreign_keys=` on the model, which is plain SQLAlchemy. The
  lambda field `c.hub.code`, a to-one hop inside the checkpoint lambda, was inferred.
- Declared needs three `aliased(Hub)` (origin, destination, checkpoint hub).
- All pass.

### education (108 lines; 1 entry). Difficulty 1
- Only `members` as `related(...student_id)`. `assignment.course.staff`, a collection two to-one
  hops below the root, was inferred with its hop guard. Declared needs
  `Relation(..., hop_correlation=[course and assignment hops])`.
- Timestamps compared field to field across the relation pass.
- Not expressible: `has(R.attr.submittedAt)` (cause A).

### legal (126 lines; 2 entries plus null declarations). Difficulty 3
- `matter.team` and `matter.screened` as `related(Document.matter, Matter.<rel>, <Table>.user_id)`.
- Explicit nulls: `null_attribute_representation="explicit"` alone is **not** enough. A call-level
  `explicit` turns `== null` into `IS NULL`, but leaves `!=` three-valued, so `null != "x"`
  (CEL: true) drops the NULL row. Every nullable attribute has to be declared again in
  `attribute_null_representation` (privilege, retentionUntil, matter.leadPartnerId,
  matter.client.industry). The README documents the per-attribute rendering; it is easy to miss
  that the call-level option does not include it. This fails closed.
- The negated ethical wall (`!("x" in matter.screened)`) passes on every store.
- `now()` refusal: the folded literal has nanoseconds (cause B).

### retail (89 lines; 1 entry). Difficulty 1
- Only `isGift -> gift`. The `order` table name, BIGINT cents, `l.quantity * l.unitPriceCents`
  inside `exists`, `all` over lines (including orders with no lines) and `size(lines)` all pass.

### hr (85 lines; 4 entries). Difficulty 2
- `email -> work_email`, `skills` as `related(...)`. Inference refuses the self-referential
  `manager` relationship, so `manager.id -> employees.manager_id` and `manager.manager.id` are
  mapped by hand as a scalar subquery over `aliased(Employee)`. `request.resource.id` and the
  natural-key `locations.code` were inferred.
- Not expressible: `!has(R.attr.terminatedOn)` (cause A).

### media (113 lines; inferred: 1 `collection_columns` entry). Difficulty 4
- Inferred: `keywords` declared in `collection_columns` (`pgArray` on PostgreSQL's native
  `text[]`, `json` elsewhere). That answers `"legal" in keywords`, but
  `keywords.exists(k, k.startsWith("lawsuit"))` is refused: a declared column supports only size,
  index and membership.
- Declared: `keywords` mapped to a hand-written `Relation` over a table-valued function, one per
  dialect: `unnest(keywords)` on PostgreSQL, `json_each(keywords)` on SQLite, and
  `JSON_TABLE(keywords, '$[*]' COLUMNS (value VARCHAR(255) PATH '$'))` on MySQL, with
  `member=<element column>` and `"k"` mapped to that column. This passes on all three stores. It is
  the hardest mapping in the set: it needs dialect knowledge and a raw `literal_column` for MySQL.
- `contributors` is a many-to-many (`secondary=`), and `c.id`/`c.staff` are inferred. Declared
  needs an `aliased(Author)` so it does not collide with the to-one `author`.
- Not expressible: the slug regex `^[a-z0-9]+(-[a-z0-9]+)*$` (cause C), `now()` with nanoseconds
  (B), and `!has(embargoUntil)` (A).

### iot (146 lines; inferred: 3 entries, 2 overrides and a 20-line SQL function). Difficulty 4
- `labels.env` and `labels.restricted` are mapped to `Device.labels[key].as_string()`. That works
  on SQLite, PostgreSQL JSONB and MySQL JSON.
- `"tier" in R.attr.labels` (key membership on a map) has no built-in lowering. Left to inference,
  `labels` resolves to the JSON column, which the adapter treats as a CEL scalar: it emits
  `NOT nullif(1, 1)` (UNKNOWN) **silently**, and every row is denied. The README's table says
  `literal in x` over a JSON column is "refused until declared", but this case is neither refused
  nor declarable, because `collection_columns` holds lists, not maps. The fix: map
  `request.resource.attr.labels` to a marker object, add an `in` override that turns the marker
  into a per-dialect `json_has_key` (`?` on PostgreSQL, `json_type(...) IS NOT NULL` on SQLite,
  `JSON_CONTAINS_PATH` on MySQL), and add a `has` override (`IS NOT NULL`). The `has` override
  is needed because mapping `labels` makes the adapter refuse `has(R.attr.labels.env)` ("prefix is
  itself mapped").
- `in` overrides receive `(attribute, literal)` in that order even for `"tier" in labels`. My
  first attempt guessed the source order and was refused.

### government (89 lines; 1 entry plus null declarations). Difficulty 2
- `flags` as `related(...flag)`. Like legal, every nullable attribute (closedOn, assignedTo,
  monthlyAmount, applicant.citizenship) has to be declared `explicit` per attribute. Under the
  call-level option alone, `assignedTo != "sv-bo"` dropped case k01, whose assignedTo is null.
- Leading-zero FIPS strings in a list and a nullable DECIMAL with `!= null` pass.
- Not expressible: `^[A-Z]{2}-[0-9]{4}-[0-9]+$` (cause C).
- MySQL: `agencies` holds `DSS` and `dss` as two primary keys, which collide under the server's
  case-insensitive default collation. Key columns are therefore `utf8mb4_0900_bin` on MySQL
  (`_types.KEY`, used for every PK/FK column). Data columns keep the ai_ci default, so the
  adapter's own `COLLATE utf8mb4_0900_bin` is still exercised.

## Non-pass verdicts by cause

Counts are per store leg (all three stores agree).

### Over-grants (2 per leg, both mappings): PDP 0.56.0 planner folds `has()` to true
1. **healthcare / 0.56.0 / patient_p3 / view**: returns `n13`; allowed `[n05]`.
   - Policy: `has(R.attr.signedAt)`. The 0.57.0 plan keeps `has(R.signedAt)`, but the 0.56.0 plan
     is `and(not(exists(codes, ...)), and(eq(R.patient.mrn, "MRN-0003"), ne(R.noteType,
     "psychotherapy"), not(R.deleted)))`, with the `has` gone.
   - Row: `n13 {patient_id: p3, type: consult, signed_at: NULL, is_deleted: false}`. `check()`
     omits `signedAt` and denies the row.
   - Emitted (SQLite): `... AND (SELECT patients.mrn FROM patients WHERE patients.id =
     clinical_notes.patient_id) = 'MRN-0003' AND clinical_notes.type != 'psychotherapy' AND
     clinical_notes.is_deleted = 0`.
2. **hr / 0.56.0 / mgr_e02 / view_compensation**: returns `e05`; allowed `[e04]`.
   - The 0.56.0 plan is `and(eq(R.manager.id, "e02"), lt(R.level, 6))`; the 0.57.0 plan adds
     `has(R.salaryBand)`.
   - Row: `e05 {manager_id: e02, level: 3, salary_band: NULL}`.
   - Emitted: `WHERE employees.manager_id = 'e02' AND employees.level < 6`.

The adapter translates both plans exactly. The plan itself is wrong: the 0.56.0 planner assumes
the data layer supplies an attribute the request omitted, as the adapter README's WARNING says.
Any adapter fed these plans over-grants. This is not an adapter bug.

### Under-grants (15 per leg)
- **PDP 0.56.0 folds `has()` (9)**: `!has(x)` becomes `false` and `has(x)` disappears.
  healthcare admin_kim/delete, dr_ortiz/sign, researcher_li/sign; education prof_jones/grade,
  ta_lee/grade; hr ceo_e01/view, hr_admin/terminate (`ALWAYS_DENIED`), mgr_e02/view,
  recruiter/view (`ALWAYS_DENIED`). Planner divergence.
- **The planner drops a `has()` guard inside a negation (4, both PDPs)**: healthcare
  nurse_ana/view `!(has(R.attr.patient.vip) && R.attr.patient.vip)` plans as
  `not(R.patient.vip)`, which denies n02, whose patient p1 has no `vip`. iot member_acme/view, a
  DENY on `has(labels.restricted) && labels.restricted == "yes"`, plans as
  `not(eq(R.labels.restricted, "yes"))`, which denies every device without the key. Planner
  divergence; it fails closed.
- **The planner drops `|| R.attr.privilege == ""` (2, both PDPs)**: legal client_globex/view plans
  only `eq(R.privilege, null)`, so d06 (`privilege: ""`) is missing. Planner divergence.

### Refusals (`UnsupportedPlanError`)
- **A. `has()` on a timestamp attribute (16 per leg, 0.57.0 only)**: adapter bug, an over-strict
  refusal. `Translator.refuse_bare_temporal` (`_translator.py:576`) exempts only `== null` and
  `!= null`, so `has(R.attr.signedAt)` is refused as a "bare temporal" read even though `has()`
  never reads the value: it is `IS NOT NULL` under `omitted`. No override helps, because the check
  runs before translation (`query.py:370`). It hits healthcare (8), education (2), hr (4) and media
  (2, which also hit cause B). The 0.56.0 plans of the same rules translate, because that planner
  folded the `has()` away.
- **B. `now()` folded with nanoseconds (4 per leg)**: documented limitation. The adapter needs
  microsecond-exact literals: legal records_eli/destroy under both PDPs, and media reader_free/read and
  reader_sub/read under 0.56.0 (under 0.57.0 those two hit A first).
- **C. Regex outside the lowered subset (4 per leg)**: documented limitation. media
  editor_pat/publish and government foia_dee/foia_release.
- **D. `exists` over a `collection_columns` column (4 per leg, inferred only)**: documented
  limitation. media lawyer_quinn/read and legal_review. The declared mapping answers them with a
  table-valued `Relation`.

### Errors
None.

### Hardest mapping problem
iot's `"key" in R.attr.labels`. Left unmapped, the adapter returns no rows with no warning. Mapping
it takes a marker object, an `in` override, a `has` override, and a custom SQL construct compiled
three ways. media's `keywords.exists(...)` comes second: it needs a hand-written `Relation` over
`unnest` / `json_each` / `JSON_TABLE`.
