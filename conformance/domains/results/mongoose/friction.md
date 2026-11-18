# mongoose: domain friction

Harness: `conformance/domains/harnesses/mongoose/harness.domains.ts` (`npm test`, with `MONGO_URL`,
`DOMAIN_STORE`, `DOMAIN_ONLY` and `DOMAIN_VARIANT` as env vars). There is one mapping file per
domain, `<domain>.ts`, importing the adapter from `mongoose/src`.
Each domain is one collection with one document per resource, keyed by `_id = resource id`.
To-one chains are single-nested subdocuments, lists are document arrays or native `[String]`,
timestamps are `Date` (Mongoose casts the ISO strings on insert), and money is `Number`.
Omitted-null domains leave the path out. Explicit-null domains (legal, government) store `null`
and pass `nullAttributeRepresentation: "explicit"`. Every call passes `schema: Model`.

| Store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| mongo-7 (`mongo:7.0`) | 349 | 0 | 17 | 2 | 0 |
| mongo-9 (`mongo:9.0`) | 349 | 0 | 17 | 2 | 0 |

The two servers return identical ids for every case.

## Per domain

Lines are the whole file, and the Mongoose `Schema` is most of each. "Mapper" counts the entries
beyond what schema inference provides. Every domain needs `request.resource.id -> _id`, because
inference only resolves `request.resource.attr.*`.

| Domain | Lines | Mapper | What had to be configured | Score |
|---|---|---|---|---|
| healthcare | 45 | 3 | `noteType -> type` (stored under the column name; Mongoose needs `type: { type: String }` for a field named `type`) and `deleted -> isDeleted`. `toDocument` renames the fields. Inference covers the 3-hop chain, the nullable `ward` subdocument, `codes` and `careTeam`. | 2 |
| fintech | 40 | 1 | Nothing. The DECIMAL fields are `Number`, and field-to-field across the relation works through dotted paths. | 1 |
| logistics | 41 | 1 | Nothing. Two subdocuments reuse one hub schema, and the optional `carrier` and the hub inside `checkpoints` elements are inferred. | 1 |
| education | 40 | 1 | Nothing. `timestamp(submittedAt) > timestamp(assignment.dueAt)` translates. A bare field-to-field `dateTime` comparison would throw, but the policy wraps both operands. | 1 |
| legal | 44 | 3 | `matter.team` and `matter.screened` need explicit `{ field: "matter.team" }` entries. **Inference gap:** a native scalar array inside a to-one subdocument resolves to the enclosing `type: "one"` relation (`schema.ts` returns no config for a scalar child, so `lookupFieldReference` falls back to the parent relation), so `"l-ali" in R.attr.matter.team` was refused as key membership on a map. That refusal hit 18 of 36 cases before the explicit entries. | 2 |
| retail | 36 | 1 | Nothing. BIGINT cents are `Number` (exact below 2^53), and arithmetic inside `exists` and `all` over an empty list both translate. | 1 |
| hr | 33 | 1 | Only `R.id -> _id`. The self-relation two hops up is embedded as `manager.manager.id`. | 1 |
| media | 34 | 1 | Nothing. `keywords: [String]` covers both `in` and `exists(k, startsWith)`, regex translates, and the many-to-many contributors are embedded. | 1 |
| iot | 46 | 4 | The `labels` JSON map is the hard one. See below. The final mapping stores a denormalized `labelKeys: [String]` beside `labels: Map<String>`, maps `R.attr.labels -> labelKeys` (for `"tier" in labels`), and maps `labels.env` and `labels.restricted` to the Map's sub-paths. | 4 |
| government | 36 | 1 | Nothing beyond `"explicit"`. Leading-zero FIPS strings stay `String`, and the nullable DECIMAL is a `Number` holding `null`. | 1 |

### iot: the map attribute (hardest mapping problem)

`labels` is a JSON object that the policy reads as a map (`labels.env`, `"tier" in labels`).
Mongoose offers `Map` or `Mixed`, and there are three ways to map it:

1. **Inference alone (`labels: Map`):** `schema.ts` reports a `Map` as an untyped scalar, so
   `"tier" in R.attr.labels` is translated as membership in a native **array**. The `$isArray`
   guard makes it null, so `admin_acme/decommission` silently returned nothing (an under-grant on
   both PDPs) where the README says a `Map` should be refused. This is an adapter bug, but it fails
   closed.
2. **`{ relation: { name: "labels", type: "one" } }`:** `in` is now refused, as documented. But a
   map has no schema children, so the relation has no `fields`, and an attribute under it gets
   **no non-null guard under `"omitted"`**. `R.attr.labels.env != "prod"` translates to
   `{"labels.env":{"$ne":"prod"}}`, which on this dataset returns `dv03` (`labels: {}`) and `dv07`
   (`labels: {"tier":""}`). CEL raises "no such key" on those two documents and `check()` denies
   them. That is a **latent over-grant**. No recorded golden exercises it (the iot policy always
   guards `labels.env` with `has()`), so it does not appear in the results. Repro:
   ```js
   queryPlanToMongoose({ queryPlan: { kind: "KIND_CONDITIONAL", condition:
       { operator: "ne", operands: [{ name: "request.resource.attr.labels.env" }, { value: "prod" }] } },
     schema: Model /* labels: { type: Map, of: String } */,
     mapper: { "request.resource.attr.labels": { relation: { name: "labels", type: "one" } } } })
   // -> { "labels.env": { "$ne": "prod" } }   (inferred mapping gives $ne:null AND $ne:"prod")
   ```
   With this mapping, `member_acme/view` *passes*, but only because of that missing guard.
3. **Final mapping:** `labelKeys` is denormalized, and the two sub-keys are explicit scalar entries
   (guarded). All iot cases pass except the planner divergence below.

### References instead of embedding (retail variant, `DOMAIN_VARIANT=retail:retail_refs`)

The adapter never emits `$lookup` or `populate()` (stated in its README and asserted by
`translator.test.ts`), so a to-one stored as a `ref` is only reachable through the key it holds.
`customer: { type: String, ref }` with `customer.id -> customer` passes 24 of 32 cases. The other
8 (`customer.tier`, `store.region`) are plain `Error: No mapper entry for ...`, not the refusal type,
because the attribute lives in another collection. Embedding is required for any attribute
beyond the foreign key.

## Non-pass verdicts by cause

Every one below is a planner divergence: the PDP's plan does not mean what its `check()` decided.
The adapter evaluated the plan as written. None is an adapter bug, and the same rows fail for
mongoose, langchain-chromadb and convex.

### PDP 0.56.0 folds `has()` to true (documented in each adapter README): 2 over-grants, 11 under-grants

Over-grants (headline, but the cause is the 0.56.0 planner, not the adapter):

- **healthcare / 0.56.0 / patient_p3 / view** returns `n13`, which is not allowed. Policy rule:
  `has(R.attr.signedAt)` in the patient rule. 0.57.0 plan: `and(not(exists(codes,…)), and(eq(R.patient.mrn,"MRN-0003"),
  ne(R.noteType,"psychotherapy"), has(R.signedAt), not(R.deleted)))`. 0.56.0 plan: the same with
  `has(R.signedAt)` removed. Row `n13`: `{"noteType":"consult","deleted":false,"patient":{"mrn":"MRN-0003",…}}`
  with no `signedAt`. Query emitted (mongoose): `patient.mrn == "MRN-0003" && type != "psychotherapy" && !isDeleted && no F-code`,
  which matches `n13`. `check()` denies it because `has(signedAt)` is false.
- **hr / 0.56.0 / mgr_e02 / view_compensation** returns `e05`, which is not allowed. 0.57.0 plan:
  `and(eq(R.manager.id,"e02"), lt(R.level,6), has(R.salaryBand))`. 0.56.0 plan:
  `and(eq(R.manager.id,"e02"), lt(R.level,6))`. Row `e05`: `{"level":3,"manager":{"id":"e02",…}}` with no
  `salaryBand`. Query emitted: `{"$and":[{"manager.id":"e02"},{"level":{"$lt":6}}]}` (Chroma `where` is the same shape).

Under-grants (0.56.0 folds `!has(x)` to false, so the plan becomes ALWAYS_DENIED or loses the branch):
healthcare admin_kim/delete, dr_ortiz/sign, researcher_li/sign; education prof_jones/grade,
ta_lee/grade; hr ceo_e01/view, hr_admin/terminate, mgr_e02/view, recruiter/view; media
reader_free/read, reader_sub/read.

### Planner drops a `has()` guard under a negation, both PDPs: 4 under-grants

- **healthcare / nurse_ana / view** (both PDPs): the policy has `!(has(R.attr.patient.vip) && R.attr.patient.vip)`, and the
  plan has `not(R.patient.vip)`. `n02` has no `vip`: `check()` reads `!(false)` and allows it,
  while the plan reads a missing attribute, which is an error, so it denies.
- **iot / member_acme / view** (both PDPs): the DENY rule is `has(R.attr.labels.restricted) && R.attr.labels.restricted == "yes"`,
  and the plan has `not(eq(R.labels.restricted,"yes"))`. On devices without the key, the plan
  errors and denies, while `check()` reads the guarded DENY as false and allows them. This is the
  same family as cerbos/query-plan-adapters#530.

### Planner drops a disjunct, both PDPs: 2 under-grants

- **legal / client_globex / view** (both PDPs): the policy has `R.attr.privilege == null || R.attr.privilege == ""`, and the
  plan has only `eq(R.privilege, null)`. `d06` (`privilege: ""`) is allowed by `check()`, but no
  correct translation of the plan can return it.
