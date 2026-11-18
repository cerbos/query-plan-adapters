# elasticsearch-java: domain stress friction

Harness: `elasticsearch-java/src/domainTest/java/dev/cerbos/queryplan/elasticsearch/domains/`
(`ADAPTER_DOMAIN_ES_URL=http://localhost:49200 ADAPTER_DOMAIN_STORE=elasticsearch-8.19.25 ./gradlew domainTest`, a separate source
set and task).
- **Loading:** each domain's `generated/resources.json` is bulk-indexed as is, one document per resource with `_id` = resource
  id and the `attr` object as the source, plus an `id` keyword. The index uses an explicit, `dynamic: strict` mapping.
- **Translation:** plans are decoded into `PlanResourcesResponse` and passed to `toElasticsearchQuery`.
  `Conditional.query()` goes in `bool.filter`, `AlwaysAllowed` becomes `match_all`, and `AlwaysDenied` runs no search.
- **Stores:** both pinned images, `docker.elastic.co/elasticsearch/elasticsearch:8.19.25` and `:9.5.6`, pulled directly, single
  node, security off, disk watermark off. The two images gave identical verdicts and ids on all 368 goldens.

## Results

| store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| elasticsearch-8.19.25 | 287 | 69 | 11 | 1 | 0 |
| elasticsearch-9.5.6 | 287 | 69 | 11 | 1 | 0 |

The single over-grant is the PDP 0.56.0 `has()` plan (hr `mgr_e02 view_compensation`). The adapter causes none.

## Per domain

The adapter infers nothing from the index, and every compared field needs a `ScalarType`. The harness therefore derives
`fieldMap` (identity: attribute path = document path), `scalarTypes` (keyword→STRING, boolean, date→TIMESTAMP, numeric) and
`nestedPaths` from each domain's index mapping. That derivation is a shared 68-line helper (`EsDomain.fromMapping`), the
code a user would have to write. Per domain, the file is the index mapping (keyword/date/nested choices) plus
`collectionFields` and `explicitNullAttributes`.

| domain | lines | configured beyond the mapping-derived defaults | not expressible | score |
|---|---|---|---|---|
| healthcare | 26 | `codes` as `nested`; `careTeam` collection | the DENY `!codes.exists(...)` (negated exists over a document collection) | 2 |
| fintech | 27 | `approvals` nested; `flags` collection | negated `exists` / negated membership over `approvals`/`flags` | 2 |
| logistics | 27 | `checkpoints` nested (object `hub` inside it); `handling` collection | `origin.country != destination.country` (field-to-field); negated membership | 2 |
| education | 26 | `assignment.course.staff` nested two objects deep | `timestamp(submittedAt) > timestamp(assignment.dueAt)` | 2 |
| legal | 29 | 4 `explicitNullAttributes`; `matter.team`/`matter.screened` collections | `privilege == null` style comparisons under explicit nulls (ES does not index null); negated membership in the ethical-wall list (`!("x" in matter.screened)`) | 4 |
| retail | 25 | `lines` nested | arithmetic inside `exists` (`l.quantity * l.unitPriceCents`); positive `all` and negated `exists` over `lines` | 3 |
| hr | 25 | `skills` collection | negated membership over `skills` | 1 |
| media | 25 | `contributors` nested (flattened m2m stored denormalised, so it maps naturally); `keywords` collection | `all` over contributors; the `now()` literal at nanosecond precision (refused: `date` is millisecond) | 3 |
| iot | 24 | `labels` as an object with `env`/`tier`/`restricted` keywords; `labels.env` etc. map like any sub-field and translate | `"tier" in R.attr.labels` (map key presence): left unmapped, since declaring `labels` as a collection would be a lie | 3 |
| government | 27 | 4 `explicitNullAttributes`; `flags` collection | `!=`/`==` over explicit-null attributes; negated membership; field-to-field timestamps | 3 |

**Hardest problem:** explicit-null domains (legal, government). Elasticsearch stores an explicit `null` as absent, so every
`eq`/`ne`/`in` over a declared nullable attribute is refused. Together with the empty-versus-missing collection rule, which
refuses every negated membership or `exists` over a list, that refuses most legal plans: the ethical wall is exactly a
negated membership.

## Non-pass verdicts by cause

**PDP 0.56.0 folds `has()` to true (plan wrong, adapter faithful). 0.56.0 only.**
- over-grant: hr `mgr_e02 view_compensation`. The plan is `manager.id == "e02" && level < 6`, with `has(salaryBand)` dropped.
  The query `{"bool":{"must":[{"term":{"manager.id":"e02"}},{"range":{"level":{"lt":6}}}]}}` returns e05, whose `salaryBand` is
  absent from its document.
- under-grant: education `prof_jones/ta_lee grade`; healthcare `admin_kim delete`, `dr_ortiz sign`, `researcher_li sign`;
  hr `ceo_e01 view`, `hr_admin terminate`, `mgr_e02 view`, `recruiter view`

**The PDP plan drops a guard on both versions (under-grant, fails closed).** iot `member_acme view`: a DENY
`has(labels.restricted) && labels.restricted == "yes"` is planned as `not(eq(labels.restricted, "yes"))`. The adapter guards it
with `exists`, so devices without the key are excluded, but `check()` allows them.

**Documented refusals (`UnsupportedPlanShapeException`), all 0.56.0 and 0.57.0.**
- negated `exists` / negated membership / positive `all` over a collection (empty versus missing): fintech `approver_mia/raj
  approve, view`; government `foia_dee foia_release`; healthcare `dr_ortiz/nurse_ana/patient_p3/researcher_li view`; hr
  `hr_admin terminate` (0.57.0); legal `client_globex view`, `lawyer_ali/lawyer_bea edit, view`, `paralegal_dan view`,
  `records_eli destroy`; logistics `carrier_dhlx update_status`; media `editor_pat publish`; retail `agent_sue cancel`,
  `shopper_c1 cancel`
- explicit null cannot be told from missing: government `supervisor_bo approve, view`, `worker_ann view`; legal
  `partner_cho edit, view`
- field-to-field comparison needs scripts: education `prof_jones/ta_lee flag_late`, government `auditor_cy audit`,
  logistics `customs_us view`
- arithmetic in a leaf (`mult`): retail `fraud_fin view`, `flag_fraud`
- sub-millisecond `now()` literal: media `reader_free read`, `reader_sub read`

**Mapping the adapter cannot express (`UnmappedAttributeException`).** iot `admin_acme decommission` (`"tier" in labels`).
