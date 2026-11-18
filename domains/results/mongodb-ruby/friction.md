# mongodb-ruby: domain stress friction

Harness: `mongodb-ruby/domains/harness.rb`
(`DOMAINS_MONGODB_URI=mongodb://127.0.0.1:37017 DOMAINS_STORE=mongo-7.0 bundle exec ruby domains/harness.rb`, or
`rake test:domains`), one mapper per domain in `mongodb-ruby/domains/mappings/<domain>.rb`. Official `mongo`
driver 2.x, Ruby 3.3, driver leg only (the Mongoid leg was not run). Each resource of `resources.json` is one
document `{_id: id, **attr}`: nested objects embedded, lists as arrays, `labels` as a subdocument, explicit nulls
stored as `null`, omitted attributes absent, ISO timestamps stored as BSON dates. Plans reach the adapter through
the Cerbos Ruby SDK via the conformance harness's stub PDP.

| Store | pass | refused | under-grant | over-grant | error |
|---|---|---|---|---|---|
| mongo-7.0 (`MONGO_IMAGE`) | 347 | 0 | 19 | 2 | 0 |
| mongo-9.0 (`MONGO_NEXT_IMAGE`) | 347 | 0 | 19 | 2 | 0 |

Both over-grants are PDP 0.56.0 planner divergences, not adapter bugs. Nothing was refused.

## Per domain

The mapper may be a callable, and the documents are shaped like the Cerbos attributes, so one line maps every
attribute to the field of its own name: `->(ref) { {field: ref.delete_prefix(R)} if ref.start_with?(R) }`.
Element fields inside a macro already default to their own name. A fuller mapper declaring every list of objects
as `relation: {type: :many}` and every timestamp as `value_type: :date_time` gave identical verdicts on all 368
goldens, so the minimal one is kept.

| Domain | Lines | Configured beyond the identity mapper | Could not express | Score |
|---|---|---|---|---|
| healthcare | 12 | nothing (`noteType`, nested `patient.ward.hospital`, `codes` elements all by name) | nothing | 1 |
| fintech | 12 | nothing (`counterparty` is a real subdocument here) | nothing | 1 |
| logistics | 12 | nothing (`checkpoints[].hub.code` by name) | nothing | 1 |
| education | 12 | nothing (`assignment.course.staff` is an embedded array) | nothing | 1 |
| legal | 12 | call-level `:explicit` | nothing | 1 |
| retail | 12 | nothing | nothing | 1 |
| hr | 16 | `request.resource.id` -> `_id` | nothing | 1 |
| media | 12 | nothing (native `keywords` array, `matches()` as `$regex`) | nothing | 1 |
| iot | 12 | nothing (`labels.env` is a subdocument path) | `"tier" in R.attr.labels` (map key membership) | 3 |
| government | 12 | call-level `:explicit` | nothing | 1 |

Hardest mapping problem: map key membership on the iot `labels` subdocument. The adapter reads `in` over a field
as array membership, guards it with `{"labels": {"$type": "array"}}`, and so a subdocument is never a member
container; the result is UNKNOWN on every device rather than a refusal. Everything else needed no configuration:
denormalized documents remove the relation, rename and reserved-word problems the SQL adapters have.

## Non-pass verdicts, grouped by cause

Counts are per store (identical on both).

### Planner divergence, PDP 0.56.0 folds `has()` to true (11 under-grant, 2 over-grant)

- over-grant healthcare `patient_p3/view`: the 0.56.0 plan lacks the `has(R.signedAt)` that 0.57.0 keeps.
  Document `n13` has no `signedAt`, patient MRN-0003, `deleted: false`, no codes; `check()` denies it. The filter
  (`$nor` over the codes `$expr`, `patient.mrn: "MRN-0003"`, `noteType $ne "psychotherapy"`, `deleted` false, each
  with its `$ne: null` guard) returns `n05, n13`; allowed `n05`.
- over-grant hr `mgr_e02/view_compensation`: 0.56.0 plan `and(eq(R.manager.id, "e02"), lt(R.level, 6))` lacks
  `has(R.salaryBand)`. Document `e05` (manager `e02`, level 3, no `salaryBand`) matches
  `{"$and": [{"manager.id": {"$ne": null}}, {"manager.id": {"$eq": "e02"}}, {"level": {"$ne": null}}, {"level": {"$lt": 6.0}}]}`;
  returned `e04, e05`, allowed `e04`.
- under-grant healthcare `admin_kim/delete`, `dr_ortiz/sign`, `researcher_li/sign`; education `prof_jones/grade`,
  `ta_lee/grade`; hr `ceo_e01/view`, `hr_admin/terminate`, `mgr_e02/view`, `recruiter/view`; media
  `reader_free/read`, `reader_sub/read`.

Both over-grants are exact translations of plans that already admit the document; every adapter in the kit has
them.

### Planner divergence on both PDPs (3 under-grant per PDP, 6 in all)

The planner drops a `has()` guard that sits under a negation, or drops a disjunct:

- healthcare `nurse_ana/view`: `!(has(R.attr.patient.vip) && R.attr.patient.vip)` plans as `not(R.patient.vip)`;
  `n02` (patient without `vip`) is allowed by `check()`.
- iot `member_acme/view`: DENY `has(R.attr.labels.restricted) && R.attr.labels.restricted == "yes"` plans as
  `not(eq(R.labels.restricted, "yes"))`; devices without `restricted` (`dv01`, `dv02`, `dv03`, `dv05`, `dv07`) are
  allowed by `check()` and UNKNOWN in the plan (the SQL adapters refuse this plan instead, since `labels.*` is
  unmappable there).
- legal `client_globex/view`: `R.attr.privilege == null || R.attr.privilege == ""` plans as
  `eq(R.privilege, null)`; `d06` (`privilege: ""`) is lost.

### Adapter limitation, silently UNKNOWN (1 under-grant per PDP, 2 in all)

- iot `admin_acme/decommission`: `!("tier" in R.attr.labels)` over a map. The filter requires
  `labels` to be an array, so `dv02` and `dv03` are denied. Fail-closed, but no `UnsupportedError` is raised.
