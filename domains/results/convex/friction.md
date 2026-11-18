# convex: domain friction

Harness: `convex/domains/` is a separate Convex project. `convex.json` points it at `backend/`,
whose schema has one table per domain and which holds the `reset` and `executePlan` functions.
`convex/domains/mappings/<domain>.ts` holds each domain's table validator and mapper, and
`convex/domains/harness.domains.ts` replays the goldens (`npm run test:domains` against a backend on
`CONVEX_URL`, default 127.0.0.1:3210, after `npx convex deploy` from `convex/domains/`). Each
domain is one document per resource: `{ id, ...attr }`, with nested objects and arrays as Convex
objects and arrays, timestamps as the ISO strings the PDP sees, and the iot `labels` as
`v.record(string, string)`. Omitted-null domains use `v.optional`, and explicit-null domains use
`v.union(x, v.null())` with `nullAttributeRepresentation: "explicit"`. Every call passes
`allowPostFilter: true`. The harness translates locally first to classify a refusal by type,
because a refusal thrown inside a Convex query reaches the client only as a message.

| Store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| convex (backend `82d5e9f2`) | 349 | 0 | 17 | 2 | 0 |

How each case was executed: 204 unconditional, 40 `db` (Convex's filter engine alone), 58 `split`
and 66 `post`. Every `split` case was also checked with the post-filter alone over every document,
and none disagreed.

## Per domain

The adapter has no schema inference: every attribute path a policy reads needs an entry. The
`mapperOf` helper writes `{ field: path, nullable }` from one line per path, so the mapper is that
list. The rest of each file is the Convex table validator.

| Domain | Lines | Mapper entries | What had to be configured | Score |
|---|---|---|---|---|
| healthcare | 56 | 16 | Every path, plus `nullable: true` on the optional ones (`sensitivity`, `signedAt`, `patient.vip`, the whole `patient.ward.*` chain). The renames `noteType -> type` and `deleted -> isDeleted` are one tuple each. | 2 |
| fintech | 51 | 17 | Every path. `nullable: false` on always-present fields keeps them on the db filter. Field-to-field (`currency == account.currency`) goes to the post-filter. | 2 |
| logistics | 46 | 15 | Every path. `carrier` and `carrier.scac` are nullable for the optional carrier. | 2 |
| education | 47 | 14 | Every path. The fractional-second timestamp comparison runs in the post-filter. | 2 |
| legal | 43 | 12 | Every path, under `"explicit"`. Nulls are stored values, so `== null` pushes to `q.eq(field, null)`. | 2 |
| retail | 41 | 10 | Every path. `store` is optional. Arithmetic inside `exists` is post-filtered. | 2 |
| hr | 38 | 12 | Every path, plus `R.id -> id`. | 2 |
| media | 39 | 12 | Every path. `keywords` is a native array, and regex runs in the post-filter. | 2 |
| iot | 40 | 11 | `labels` plus explicit nullable entries for `labels.env` and `labels.restricted`. `"tier" in labels` is answered as map-key membership by the post-filter. | 2 |
| government | 46 | 14 | Every path, under `"explicit"`. | 2 |

Hardest mapping problem: choosing `nullable` for each path. Under the default `"omitted"`, an entry
that does not declare it is nullable and goes to the post-filter. So the mapper needs `nullable: false`
on every field the schema marks required, or nothing reaches Convex's filter engine. That doubles
what the `v.optional` validators already say: the adapter does not read the Convex schema.

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
