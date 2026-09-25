# Conformance corpus

Every adapter in this repository is proved against this corpus, end to end:

1. **One policy**, written as cases, one action per case.
2. **One dataset**: seed rows and a fixed principal.
3. **Golden files**, one per case and per PDP version: the plan the PDP returned, and which seed rows
   `check()` allowed.
4. **Per adapter**, a harness that loads the dataset into the adapter's real store, translates each
   recorded plan, runs the query, and asserts the returned ids are exactly the recorded allowed ids.
5. **Per adapter**, a ledger of the cases it cannot pass, and why.

The PDP is the oracle, and its answers are recorded. No adapter starts a PDP, and nobody writes
expected results by hand.

**The invariant: a shape an adapter cannot express must throw, never emit a filter.** A wrong filter
returns rows the PDP denies. A throw is a bug report.

```bash
go -C conformance/generator run .          # rebuild policy, dataset and goldens (Docker)
go -C conformance/generator run . -check   # CI: fail if anything committed is stale
```

## Layout

| Path | What it is | Edited by |
|---|---|---|
| `cases/<area>.yaml` | The cases: id, tier, intent, trap, and the Cerbos condition (or rules). **The source of truth.** | hand |
| `seeds.json`, `derived-fields.json` | The dataset as rows: 41 seed resources and the fixed principal. | hand |
| `pdp-versions.json` | The two pinned PDPs: `current` (N) and `previous` (N-1), each as tag and digest. The only PDP pin in the repository. | `scripts/bump-pdp.sh` |
| `policies/conformance.yaml` | The resource policy built from the cases (resource kind `conformance`). | generator |
| `policies/derived_roles.yaml` | The one derived role the composition cases import. | hand |
| `resources.json` | The dataset as the PDP sees it: the principal, and each seed's `check()` resource. | generator |
| `golden/<tag>/<case id>.json` | For each PDP: the plan, the allowed ids, and the case's intent and trap. | generator |
| `golden/CHANGES.md` | What changed between the previous and current PDP. | generator |
| `generator/` | The Go program that writes everything marked "generator". | hand |

## Case ids

Every case is named `<area>/<operator>/<variant>`, and the id is also the Cerbos action name:

```
string/starts-with/percent-in-needle
comparison/less-or-equal/value-first
relation/equals/one-hop-string
collection/exists/negated
```

- **area** is the construct under test. The areas are:
  - `comparison`, `logic`, `conditional` (ternary)
  - `null`, `membership` (`in`)
  - `string`, `regex`, `size`, `arithmetic`
  - `cast`, `timestamp`, `hierarchy`
  - `collection` (macros over resource lists)
  - `relation` (the to-one `parent` and the `mainCategory` join chain)
  - `principal` (macros over `P.attr.*`), `identifier` (`R.id`)
  - `type-mismatch`, `composition` (several rules for one action)
- **operator** is the defining CEL operator, spelled out: `equals`, `greater-or-equal`, `starts-with`,
  `exists-one`, `has-intersection`, and so on.
- **variant** describes the shape: `value-first`, `negated`, `field-to-field`, `null-element`,
  `empty-collection`. It never holds an issue number or a legacy abbreviation.

Every case also carries:

- **tier**, one of three:
  - `core`: the everyday shapes every adapter should support.
  - `extended`: less common features, such as hierarchy, regex, casts and composition.
  - `adversarial`: a shape that exists to catch one specific mistranslation.
- **intent**: one line saying what the shape is.
- **trap**: one line saying what a naive translation gets wrong.

To write a new adapter, work through the tree tier by tier.

## Golden files

Each golden file is self-contained:

```json
{
  "id": "string/starts-with/percent-in-needle",
  "pdp": "0.55.0",
  "tier": "adversarial",
  "intent": "startsWith with a needle containing a literal % (\"100%\").",
  "trap": "A LIKE translation that does not escape % turns it into a wildcard and matches rows the PDP denies.",
  "rules": [{"effect": "EFFECT_ALLOW", "roles": ["USER"], "condition": {"match": {"expr": "R.attr.aString.startsWith(\"100%\")"}}}],
  "request": {"action": "string/starts-with/percent-in-needle", "resourceKind": "conformance"},
  "plan": {"kind": "KIND_CONDITIONAL", "condition": {"expression": {"operator": "startsWith", "operands": [{"variable": "request.resource.attr.aString"}, {"value": "100%"}]}}},
  "planError": null,
  "allowed": ["a2"],
  "oracle": "discriminating",
  "degenerateReason": null,
  "plannerDivergence": null
}
```

The `plan` is the `filter` object from `PlanResources`, exactly as the PDP returned it. `allowed` lists
the seed ids that `check()` allowed, one call per seed, using that seed's resource from
`resources.json` and the corpus principal.

- **`oracle`** is `empty` or `total` when no seed, or every seed, is allowed. That is only legal when
  the case declares `degenerate` with the reason, for example a planner fold or a type error that
  denies every row. The generator fails on an undeclared degenerate oracle, which usually means a
  discriminating seed is missing. A declaration can be limited to some PDP tags with `pdp: [...]`.
- **`plannerDivergence`** marks a planner bug: the plan and `check()` disagree, so no adapter can
  pass. Harnesses skip the comparison for that PDP tag. It is declared once, on the case.
- **Time.** The literal a plan folds `now() - duration("24h")` into is recorded as
  `"__NOW_MINUS_24H__"`. A harness substitutes the real value before translating. Seed timestamps are
  absolute and far from today, so the recorded decisions stay valid.

## The harness contract

Each adapter implements this once, in its own language. It needs no PDP.

1. **Load the dataset.** Store every row of `seeds.json` and `derived-fields.json` in the adapter's
   real store. `parentSeedId` is a real to-one relation (below). Map the attributes so that each row,
   read back through the mapping, is the resource in `resources.json`. One mapping serves every case;
   there are no per-case options.
2. **For each PDP in `pdp-versions.json`** (current and previous), **for each golden file**:
   - If `plannerDivergence` is set, skip the case.
   - Look the case up in the adapter's ledger. An entry applies unless its `pdp` list excludes this tag.
   - **No entry:** translate the plan, run the query, and assert the returned ids equal `allowed`
     exactly.
   - **`unsupported`:** assert that translating throws the adapter's refusal error type.
   - **`divergent`:** translate and run. Assert the result differs from `allowed`, so a fixed bug
     fails until its entry is removed.
3. **Stale-ledger guard.** Fail if the ledger names a case id that has no golden file.

That is the whole contract. The harness asserts no counts and pins no messages. The dataset
projection is recorded, so a field the harness forgets to store shows up as a wrong result rather than
a vacuous pass.

### The ledger — `<adapter>/conformance-ledger.json`

```json
{
  "adapter": "prisma",
  "cases": {
    "regex/matches/digit-class": {
      "status": "unsupported",
      "reason": "Prisma filters have no regular-expression operator."
    },
    "null/equals/null-literal-on-missing-attribute": {
      "status": "divergent",
      "reason": "…",
      "issue": "https://github.com/cerbos/query-plan-adapters/issues/…"
    }
  }
}
```

- The ledger lists exceptions only. A new case is expected to pass until the harness says otherwise.
- `reason` names the real mechanism, in the store's own terms. `divergent` also requires `issue`.
- An optional `pdp: ["0.55.0"]` limits an entry to one PDP tag, for a case whose plan differs between
  the two.

## The dataset

- `seeds.json` holds the rows: scalars, a `tags` to-many relation, `subCategoryNames` for the
  `mainCategory` chain, and `parentSeedId`. A seed with `subCategoryNames` owns **one** category
  holding every name as a subcategory, so a category can be partly matched by a predicate (i9). `derived-fields.json` adds six more columns per seed.
- **Two NULL conventions, one per attribute.** A NULL column is a *missing attribute*, which CEL
  denies under both polarities. The exceptions are `owner` (which aliases `aOptionalString`),
  `coOwner` (which aliases `scope`), `tagNames`, `aNumberList` and `aBoolList`, which send an
  *explicit null value*. Under CEL, `null != "x"` is true. `resources.json` shows each attribute's
  convention per row. See
  [ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).
- **Every scalar a case reads can be missing.** Seeds `j1`, `j2` and `j3` each leave exactly one of
  `aString` (and `obj.inner`, its alias), `aNumber` and `aBool` NULL, with every other attribute
  present, so each negated case over those attributes meets a missing-attribute row, and a failure
  names the column. A harness maps all three as nullable, on the omitted convention.
- **The real to-one relation.** `parentSeedId` names the seed whose `aBool`, `aNumber`, `aString`
  and `aOptionalString` form this row's `parent`. That seed's own parent forms `parent.inner`, and the
  chain stops there.
  - Materialise a *separate* parent row per resource.
  - An absent level is a missing attribute, so it is UNKNOWN under negation, never false. See
    [ADR 0005](../docs/adr/0005-the-conformance-corpus-carries-a-real-to-one-relation.md).
- **Store configuration is part of conformance.** CEL string comparison is byte-exact, and CEL
  orders strings by code point:
  - MySQL must use `utf8mb4_0900_bin`.
  - SQLite needs `PRAGMA case_sensitive_like = ON`, or string matching lowered to something other than
    `LIKE`.
  - PostgreSQL must order by byte: every PostgreSQL leg initialises its database with
    `--lc-collate=C` rather than inheriting the image's libc order, and
    `ADAPTER_TEST_POSTGRES_INITDB_ARGS` overrides it to reproduce a linguistic collation's
    over-grant ([#489](https://github.com/cerbos/query-plan-adapters/issues/489)).

  The `string/*/case-sensitive`, soft-hyphen and `comparison/*/string-code-point-order` cases
  witness this.

## Mapping hazards

A harness maps each attribute so that the row, read back, is the resource in `resources.json`. Some
mappings can only hold that if the store keeps what the attribute says, and no translation can repair
a value the store has already lost:

- **Precision finer than the column's.** a5's `createdAt` is `2020-03-15T10:30:00.123456Z`. A
  millisecond column (Prisma's `DateTime(3)`, MySQL `DATETIME(3)`) stores `.123`, so
  `timestamp(R.attr.createdAt) <= timestamp("2020-03-15T10:30:00.123Z")` over-grants a5 and `>`
  under-grants it, whatever the literal's precision. The attribute must be the value the store
  returns: map a column at least as precise as the values an application writes to it, or send the
  PDP the stored (truncated) value. The corpus carries no case for this, because the fault is in the
  mapping, not the translation, and every millisecond store would ledger it
  ([#519](https://github.com/cerbos/query-plan-adapters/issues/519)).

## Changing the corpus

1. Edit or add a case in `cases/<area>.yaml`. If it needs a new column or principal attribute, add
   it to `seeds.json` / `derived-fields.json` and teach `generator/resources.go` the projection.
2. Run the generator. A new case must have a discriminating oracle: add a seed that tells a right
   translation from the wrong one it targets.
3. Run every adapter's harness. Each new failure is exactly one of:
   - a translation bug: fix it;
   - a shape the store cannot express: make it throw, and add an `unsupported` ledger entry;
   - a known wrong result tracked by an issue: add a `divergent` ledger entry.

## Bumping the PDP

Run `scripts/bump-pdp.sh` locally, on a branch. With no argument it bumps to the latest Cerbos
release; `scripts/bump-pdp.sh 0.56.0` picks one. It resolves the new tag's digest, moves `current`
to `previous` in `pdp-versions.json`, updates every restatement of the pin (the Compose files, the Go
modules' `cerbos/api/genpb`), drops ledger entries scoped to the old `previous`, runs the generator
and `validate-corpus.sh`. The PR you open from it:

1. Carries the regenerated golden directories and `CHANGES.md`, which is its body and the review
   surface: which plans, allowed sets and plan errors changed.
2. Runs every adapter against both versions. Anything that breaks is fixed or entered in that
   adapter's ledger (with `pdp` when it is specific to one version) before merge.

Entries scoped to the old `previous` tag are deleted in the same PR, since it has stopped being
tested.
