# Query plan adapters

Multi-language adapters that translate a Cerbos query plan into a database-native filter. This
glossary fixes the vocabulary shared across all adapters. Adapter-local design vocabulary lives in
that adapter's own `CONTEXT.md` (currently only `spring-data/CONTEXT.md`).

## Language

### Proving an adapter correct

**Conformance corpus**:
The shared set of hostile cases, the dataset, the two pinned PDPs and their recorded goldens that
every adapter is proved against. Lives in `conformance/`.
_Avoid_: adversarial corpus, test corpus, shared fixtures

**Case**:
One hostile shape: a Cerbos condition (or set of rules) under one action, named
`<area>/<operator>/<variant>`, with a tier, an intent and a trap. Written by hand in
`conformance/cases/<area>.yaml`; the policy is generated from the cases.
_Avoid_: action (except as the Cerbos action name a case is planned under), shape test

**Tier**:
How widely a case is expected to pass: `core` (every adapter should), `extended` (less common
features) or `adversarial` (exists to catch one specific mistranslation). The order in which to
build a new adapter.
_Avoid_: level, priority

**Golden**:
One recorded answer per case per pinned PDP, `conformance/golden/<tag>/<case id>.json`: the plan
the PDP returned and the seed ids `check()` allowed. Written only by the generator; never edited by
hand and never per adapter. `golden/CHANGES.md` is the diff between the two pinned PDPs.
_Avoid_: wire fixture, golden expectation, snapshot, expected rows

**Pinned PDPs**:
`current` (N) and `previous` (N-1) in `conformance/pdp-versions.json`, each a tag and a digest.
Every harness replays both. The only PDP pin in the repository.
_Avoid_: CERBOS_VERSION, the PDP version

**Degenerate oracle**:
A golden whose allowed set is empty or total, so it cannot tell a right translation from a wrong
one. Legal only when the case declares `degenerate` with the reason (a planner fold, a type error
that denies every row); otherwise the generator fails, which usually means a seed is missing.
_Avoid_: vacuous pass, trivial case

**Planner divergence**:
A case where the plan and `check()` disagree, so no adapter can pass: a Cerbos bug, not an adapter
bug. Declared once, as `plannerDivergence` on the case, optionally scoped to PDP tags; every harness
skips it for those tags.
_Avoid_: known divergence, adapter divergence

**Ledger**:
`<adapter>/conformance-ledger.json`: the cases one adapter cannot pass, each `unsupported` (the
adapter throws its refusal type) or `divergent` (a known wrong result, with an issue). Lists
exceptions only, and is an output of running the harness, not an input. The set of directories
holding one is the adapter roster.
_Avoid_: classification, actions.json, skip list

**Conformance harness**:
An adapter's replay of the corpus against its own real store: for each pinned PDP and each golden,
translate the plan, run the query, and compare the ids with `allowed`, or assert what the ledger
says. Built from the adapter's source, not its published package. Needs no PDP. One per adapter.
_Avoid_: adversarial suite, differential test, integration test

**Translator unit test**:
An adapter's offline test of what the corpus cannot ask: branches CEL cannot reach, caller-supplied
arguments the corpus cannot vary, and the refusal type (`CLAUDE.md`, "What a translator unit test
may pin"). It never re-asserts a case's output. Distinct from the conformance harness, which proves
rows.
_Avoid_: filter test, shape test

**Semantics**:
Whether a translated filter returns exactly the rows the PDP would allow. The property the
conformance corpus proves.
_Avoid_: correctness, behaviour

**Plumbing**:
Whether the adapter can be installed, imported, and handed to the ORM's real query methods at all.
Distinct from semantics: a filter can be semantically perfect and still unusable.
_Avoid_: integration, wiring, end-to-end

### Proving an adapter usable

**Example application**:
A runnable program that installs the adapter as a published package, uses it the way a consumer
would, and asserts a fixed set of returned ids. One per adapter. Proves plumbing, not semantics.
_Avoid_: sample app, demo, smoke test, integration app

**Demo domain**:
The single realistic policy suite, seed rows, and expected id sets that every example application
shares. Deliberately separate from the conformance corpus: realistic shapes, not hostile ones, and
no per-adapter exceptions (no ledger). A floor every example must meet, not a ceiling — see
[ADR 0001](docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md).
_Avoid_: example corpus, sample data, demo corpus

**Usage shape**:
A way a consumer calls the ORM with an adapter-produced filter — a plain filtered list, a
paginated page, a count, a sort, a relation traversal. The unit of coverage an example application
adds over a conformance harness.
_Avoid_: query pattern, call pattern, scenario

### Retired language

Named here so the concept is recognisable when it is proposed again, not so it can be used.

**Wire fixture**, **golden expectation**, **golden asset**, **golden suite**:
*Retired* by [ADR 0010](docs/adr/0010-conformance-replays-recorded-pdp-decisions.md). Captured plans
without their `check()` decisions, and the filter each adapter was pinned to emit for them. A
recorded plan is now part of a **golden**, and no adapter pins its filter.
_Avoid_: all four, outside a statement about the past.

**Shared policy suite**:
*Retired.* The Cerbos policy file that used to sit at the repository root, which most adapters read
a different subset of and each proved a different way. Absorbed into the conformance corpus and
deleted — see
[ADR 0008](docs/adr/0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md). It is
the one thing here that never had a name in this glossary, which is much of why it drifted.
_Avoid_: shared policies, the root policies, the friendly corpus, the non-hostile suite — and the
term itself outside a statement about the past. The live term is **conformance corpus**, and a
second, friendlier suite beside it is this idea returning.
