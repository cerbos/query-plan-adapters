# langchain-chromadb: domain friction

Harness: `langchain-chromadb/domains/harness.domains.ts` (`npm run test:domains`, with
`CHROMA_URL` defaulting to 127.0.0.1:18000). `DOMAIN_POST_FILTER=0` runs the where-only leg. There
is one mapping file per domain in `langchain-chromadb/domains/<domain>.ts`, and the shared helpers
are in `domains/mapping.ts`. Each domain is one collection with one record per resource and a
constant embedding.

**How the domains are flattened:**

- A scalar goes under its dotted path (`patient.ward.hospital.region`), and a NULL or an absent
  to-one writes no key.
- A list goes under its key as JSON text (`valueType: "json"`). A list of strings also writes its
  elements under `<key>.elements` (`listElements`), so membership becomes `$contains`.
- A list of objects (codes, approvals, checkpoints, staff, shares, lines, contributors, alerts) is
  JSON text only.
- The iot `labels` map is JSON text.
- Explicit-null attributes (legal: privilege, retentionUntil, matter.leadPartnerId,
  matter.client.industry; government: closedOn, assignedTo, monthlyAmount, applicant.citizenship)
  are JSON text, because a Chroma key cannot hold null.
- Timestamps are the ISO strings the PDP sees.

The CI leg is `allowPostFilter: true`. The harness applies `postFilter` to the `prefilter`
candidates and checks the post-filter alone over every record; that check never disagreed.

| Store | pass | refused | under | over | error |
|---|---|---|---|---|---|
| chroma (`chromadb/chroma:1.5.9`, `allowPostFilter: true`) | 345 | 4 | 17 | 2 | 0 |
| chroma-where-only (same server, `allowPostFilter` off) | 260 | 101 | 6 | 1 | 0 |

## Per domain

There is no inference: every key needs an entry, chosen by hand from `required`, `numericType`,
`valueType: "boolean"`, `json` and `listElements`.

| Domain | Lines | Entries | What had to be configured; what could not be expressed | Score |
|---|---|---|---|---|
| healthcare | 25 | 15 | Each key's type. `codes` is a JSON key (`exists` over objects is post-filter only), and `careTeam` is a list with elements. The rename `noteType` is not needed, because the metadata key is written by the app. | 3 |
| fintech | 30 | 20 | `amount`, `riskScore` and `overdraftLimit` are `numericType: "float"`: DECIMAL becomes a double, and a whole value (`5000`) is written as integer metadata. `approvals` is JSON. Field-to-field `currency == account.currency` is post-filter only. | 3 |
| logistics | 30 | 17 | Two hubs are flattened under `origin.*` and `destination.*`. `checkpoints` (objects with a nested hub) is JSON, so `checkpoints.exists(c, c.hub.code in ...)` and `size(checkpoints)` are post-filter only. | 3 |
| education | 26 | 16 | `assignment.course.staff` is JSON, so the course-staff `exists` is post-filter only. The timestamp field-to-field comparison is post-filter only. | 3 |
| legal | 27 | 15 | 4 explicit-null attributes become JSON keys, so every `== null`/`!= null` on them is post-filter only, and these keys lose `Where` pushdown entirely. `matter.team` and `matter.screened` are lists with elements, so negated membership is a `$not_contains` filter. | 4 |
| retail | 24 | 14 | `lines` is JSON. **Refused (4):** `lines.exists(l, l.category == "gift_card" && l.quantity * l.unitPriceCents > 50000)`, with `UnsupportedOperatorError: mult over an operand whose CEL type the plan does not carry (a list element…)`. An element of a JSON list has no declared int/double type, so the adapter refuses rather than guess CEL's overload. No mapping can declare an element field's numeric type. | 4 |
| hr | 24 | 14 | `R.id -> id`. The two-hop manager chain is flattened to `manager.id` and `manager.manager.id`. | 2 |
| media | 24 | 14 | `keywords` is a list with elements. `contributors` is JSON. Regex is post-filter only. | 2 |
| iot | 23 | 13 | `labels` is JSON, so `labels.env`, `labels.restricted` and `"tier" in labels` resolve through it in the post-filter (with no `Where` pushdown). `alerts` is JSON. | 3 |
| government | 28 | 16 | 4 explicit-null attributes are JSON keys, as in legal. Leading-zero FIPS stays a string key. | 4 |

Hardest mapping problem: explicit nulls, together with lists of objects. Chroma metadata cannot hold
a null, an empty list or an object, so every such attribute becomes JSON text that only the
in-memory post-filter can read. Without `allowPostFilter`, 101 of 368 cases are refused; the main
reasons are a nested expression (39), a negated `exists` (18), a negated `has` (10), and a
comparison on a JSON key (16).

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

On the where-only leg, `healthcare/0.56.0/patient_p3/view` is refused (its `exists` with
`startsWith` has no `Where` form), so that leg has one over-grant, not two. Its 6 under-grants are the 0.56.0 has()-folding plans that
need no post-filter (healthcare dr_ortiz/sign and researcher_li/sign; hr ceo_e01/view, hr_admin/terminate,
mgr_e02/view and recruiter/view). The other 11 under-grants from the post-filter leg are refused on this leg.
