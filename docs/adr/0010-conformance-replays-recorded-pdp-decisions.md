# Conformance replays recorded PDP decisions

Accepted. Supersedes [ADR 0006](0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)
and amends [ADR 0007](0007-adapters-share-data-not-code.md) and
[ADR 0008](0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md).

## Context

The conformance corpus did its job. Every adapter was proved against one hostile policy, one set of
seeds and one oracle, and the bugs it exists to stop (value-first operand inversion, LIKE
metacharacter leaks, three-valued logic under negation) stopped shipping to several adapters at
once. What it cost to run had grown faster than what it proved:

- **Every adapter's harness started a live PDP.** Each harness planned every action and called
  `check()` once per seed in its own CI, so every adapter workflow depended on a pinned Cerbos
  container, a registry and a network. Every PR paid for that, although only a PDP bump could change
  the answers.
- **One shared classification file for every adapter.** `conformance/actions.json` held the roster,
  the conformance list, `adapterUnsupported` and `adapterSupportedExpected` per adapter,
  `expectedUnsupported`, `nullRepresentationOmitted`, `knownDivergences` and `degenerateOracles`,
  each with its pinned refusal message per adapter. One new action touched every adapter's entry,
  and `validate-corpus.sh` needed more than 600 lines to hold the file consistent.
- **Tripwires in every harness.** Each harness asserted corpus size, oracle and throwing counts, a
  liveness-only list, and the exact seed, principal and derived-field keys it consumed. Each one
  guarded a real failure, and together they made any corpus edit a change to every adapter.
- **Hand-maintained golden filters.** Seven adapters pinned the filter they emitted for each action
  in a `golden/expectations.json`, with a generator, a declared ORM version and divergence lists per
  ORM leg. A pinned filter proves the adapter emits what it emitted yesterday, not that the filter
  is right. Only the harness proves that.
- **A monthly PDP release.** Cerbos ships roughly monthly. Each bump meant re-running wire-fixture
  regeneration, the evaluation-mode probes and every adapter's live harness, then reading
  unrelated drift out of all of them.

## Decision

**Record the PDP's answers once. Adapters replay them.**

- **Cases** (`conformance/cases/<area>.yaml`) are the source of truth. Each case is one action named
  `<area>/<operator>/<variant>`, with a tier (`core`, `extended`, `adversarial`), an intent and a
  trap. The generator builds the policy from the cases.
- **Two pinned PDPs**, `current` and `previous` (N and N-1), in `conformance/pdp-versions.json`,
  each as a tag and a digest. This file is the only PDP pin in the repository.
- **The generator** (`conformance/generator`) starts each PDP and records one golden per case per
  tag: the plan exactly as returned, and the seed ids `check()` allowed. It also writes
  `golden/CHANGES.md`, the previous → current diff. It fails on an empty or total oracle the case
  does not declare `degenerate`. CI runs it with `-check`, so a stale golden fails.
- **A per-adapter ledger** (`<adapter>/conformance-ledger.json`) lists only the cases that adapter
  cannot pass: `unsupported` (translating throws the adapter's refusal type) or `divergent` (a
  known wrong result, with an issue), optionally scoped to one PDP tag. Every other case must return
  exactly the recorded ids.
- **Planner bugs are declared once**, as `plannerDivergence` on the case, optionally scoped to PDP
  tags. Every harness skips the case for those tags.
- **Unit tests must not re-assert corpus output.** They pin only what no case can state: branches
  CEL cannot reach, caller-supplied arguments the corpus cannot vary, and the refusal type.

## Consequences

**No PDP in adapter CI.** An adapter workflow needs only its store. The PDP runs in
`conformance.yaml` (the generator's `-check` and the digest check), in `bump-pdp.sh` when someone
bumps the pin locally, and in the example applications, which prove plumbing against a live PDP by design
([ADR 0002](0002-examples-install-the-packed-artifact.md)).

**N and N-1 on every run.** Each harness replays both tags' goldens, so a consumer one release
behind is covered. A case whose answer differs between tags is visible as a `pdp`-scoped ledger
entry or `plannerDivergence`. The first instance came from the port itself: Cerbos 0.54's `check()`
errors on an ordering against NaN, which 0.55 evaluates to false. The plan is identical on both
tags, so the two NaN-ordering cases carry a `plannerDivergence` scoped to 0.54.0 and every harness
returns the 0.55 answer on both.

**`CHANGES.md` is the release review surface.** A bump is `bump-pdp.sh <tag>`, opened as a pull
request with `CHANGES.md` as its body. It lists which plans, allowed sets and plan errors changed.
The reviewer reads one table instead of eleven harness logs.

**Strict evaluation is not an adapter dimension.** The 333 plans captured under
`--strict-evaluation` were byte-identical to the default ones. A second leg per adapter bought no
coverage, so the strict fixtures, the evaluation-mode probes and their script are gone.

**The ledger is an output of the run.** An entry is written after watching the harness fail, and the
harness rejects an entry that names no case. It no longer pins a message: the refusal *type* is the
contract. The port settled one type per adapter: `UnsupportedQueryPlanError` on prisma, drizzle,
mongoose and convex; `UnsupportedOperatorError` on langchain-chromadb; `UnsupportedPlanError` (a
`ValueError`) on sqlalchemy; `Cerbos::ActiveRecord::Error` on activerecord; `ErrUnsupported` on ent
and pgx; `UnsupportedPlanShapeException` on elasticsearch-java; and on spring-data
`UnsupportedPlanShapeException`, or `UnmappedAttributeException` for an unmapped attribute.

**Recording sharpened two contracts.**
- *Negative zero.* Go's `encoding/json` wrote the double `-0.0` as `-0`, which JSON readers in
  other languages decode as the integer zero, losing its sign. The generator now writes `-0.0`, and
  sqlalchemy passes `arithmetic/divide/negative-zero-divisor`, which it used to refuse.
- *Missing attributes.* A missing attribute is the omitted null convention
  ([ADR 0004](0004-the-null-convention-is-a-property-of-the-attribute.md)). Adapters that model it
  declare the attribute omitted in their harness mapping, and then refuse `== null` against it
  rather than emit `IS NULL` (`null/equals/null-literal-on-missing-attribute`). Mongoose passes that
  case instead, because its mapping returns the empty set CEL expects. On activerecord and spring-data this
  turned `relation/not-equals/one-hop-null-literal` from translated to unsupported, because their
  old mapping would over-grant `parent.x == null` on a row with no parent.

**What was lost, deliberately:**
- The sqlalchemy gRPC transport leg and the strict-evaluation leg. The recorded plan is transport
  independent, and the strict plans were identical.
- Synthetic tests that had no corpus case: mongoose's and drizzle's count-threshold tests over the
  `mainCategory` chain, read-back checks, and the parent-chain read-back test. These shapes are
  policy-reachable, so they are corpus gaps
  ([#509](https://github.com/cerbos/query-plan-adapters/issues/509)), not unit-test material.
- Every per-adapter golden filter asset and its ORM-version divergence lists. A rendering change is
  no longer a reviewed diff. It is caught only if it changes which rows come back.
- The per-harness tripwires. A field a harness forgets to store now shows up as a wrong result,
  because the dataset projection (`resources.json`) is recorded.
