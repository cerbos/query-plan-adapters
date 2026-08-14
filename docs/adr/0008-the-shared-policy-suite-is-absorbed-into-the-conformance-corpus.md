# The shared policy suite is absorbed into the conformance corpus

Accepted. Implemented in
[#385](https://github.com/cerbos/query-plan-adapters/issues/385), the contract half of
[#372](https://github.com/cerbos/query-plan-adapters/issues/372).

## Context

This repository proved semantics with two policy suites, and only one of them was a suite.

`conformance/` is the shared corpus: one hostile policy, one set of hostile seed rows, one derived
field table, one classification ledger, golden planner wire fixtures, a stated invariant (a shape an
adapter cannot express must throw, never emit a filter), a written recipe for adding a shape, and
one oracle — per-row `check()` against the pinned PDP — so no expectation is ever written by hand.

The other was a single policy file at the repository root, and it had none of that. No shared seeds,
no shared oracle, no coverage ledger, no wire fixtures. Most adapters read a different subset of it,
and each answered "what should this return" its own way: hand-computed expectations in TypeScript —
in one case the policy reimplemented in the test to produce its own expectation, which is precisely
the practice `conformance/` exists to abolish — hardcoded row counts in Python, hand-built fixtures
in Java, a live `checkResources()` in one adapter. Some of its actions were exercised by nobody at
all, including one carrying a hand-written rationale that had never run. Two adapters had it in
their CI path filter and never opened it.

That divergence was not a series of individual mistakes. It is what happens to a shared artifact
nobody has decided anything about, and nobody had decided anything about this one because it had no
name: `conformance/` had a README, a glossary entry, an invariant and a procedure, and the root
suite had a directory.

The [#373](https://github.com/cerbos/query-plan-adapters/issues/373) triage measured what it
actually covered that the corpus did not, against planner *output* rather than policy source. At the
CEL-operator level the corpus already covered everything the root suite reached, against strictly
more hostile operands. What genuinely survived was small and specific: a real to-one join, a
`string()` cast, the primary key used as an attribute, and a set of hazard classes neither suite
carried.

## The decision

**Absorb and delete, and keep exactly one policy suite for semantics.**

Every surviving shape became a corpus action; every adapter's shared-policy test became a translator
unit test that reads its plans from `conformance/wire-fixtures/`
([ADR 0006](0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)); the file is
deleted, and with it the CI path gate in every workflow that carried one.

The invariant this ADR exists to hold is the second half. A second policy suite for semantics is not
a smaller corpus — it is a second answer to "what should this return", maintained by whoever happens
to be looking at it. The corpus's value is that there is exactly one answer and the PDP computes it.

**Nothing moves on disk.** Deleting the root suite *is* the layout fix. `conformance/` and `demo/`
keep their names and their places. The policy suites that remain outside `conformance/` prove
**plumbing** rather than semantics and are not affected: `demo/policies/` feeds every example
application ([ADR 0001](0001-demo-domain-has-no-per-adapter-exceptions.md)), and
`spring-data/example/policies/` is that adapter's onboarding artifact.

## Alternatives considered

**Grow the root suite into a second full corpus — a "friendly" one beside the hostile one.** This
was on the table and is the option a future reader is most likely to reach for, because the pull is
real: hostile shapes are hard to read, and a suite of ordinary ones is where you would send someone
learning what an adapter does. It was rejected because of what it costs to be a corpus rather than a
pile of policies. A second corpus needs its own seed rows, its own derived fields, its own
classification ledger with a fail-closed entry per adapter, its own degeneracy guard, its own
declared-key guards, its own wire fixtures and drift check, its own loader inside every adapter, and
its own harness and CI leg per adapter — every piece of machinery `conformance/` already carries,
duplicated, in order to prove shapes that are **strictly easier** than ones already proved. An
adapter that gets `contains` right against a needle holding `%`, `_`, `\` and `[` does not need it
proved again against `"test"`. The only thing a friendly corpus could add that the hostile one does
not have is a shape the hostile one does not carry, and the cheap way to add one of those is a new
action in `adversarial.yaml`. Readability is answered elsewhere: `demo/` is the realistic corpus,
and it proves plumbing, which is the property realistic shapes are good at proving.

**A common parent directory — `corpora/{conformance,demo}/`.** Rejected. It buys a nesting level and
spends path churn across every workflow, every harness, every script and every README, during the
largest refactor this repository has run. Two corpora at the root is a legible layout once the third
suite is gone, and the move stays cheap and reversible if it later stops feeling that way.

**Leave the file in place, unreferenced.** Rejected. An unreferenced policy suite is a suite that
gets referenced again — and the state it was already in, with two adapters gating CI on a file they
never opened, is the evidence. A deleted directory cannot accumulate a further methodology.

## Consequences

**A shape worth proving is a corpus action, and there is nowhere else to put one.** That is the
point, and it moves work rather than removing it: the migrations parked some policy-reachable shapes
in per-adapter unit tests, which is a corpus gap wearing a unit test rather than a home for them.
`CLAUDE.md` ("What a translator unit test may pin") names that as a bridge to be deleted when the
action lands, and [#414](https://github.com/cerbos/query-plan-adapters/issues/414) is the port.

**Convex's integration suite goes with the file.** It was the last thing that read the root suite,
and it proved that Convex's real filter engine evaluates a pushed-down filter the way the adapter
assumes. The adversarial harness now makes that argument better: it executes the adapter's own
output inside the same real Convex backend, against the same `check()` oracle, over hostile
operands, and pins per action which half of the output — the filter engine or the adapter's
in-memory post-filter — answered the query, so every builder method that suite enumerated is
executed there, including the unconditional allow (`p-has`, planned to `KIND_ALWAYS_ALLOWED` and run
through the backend for all rows). The one thing that stops being executed end to end is the
backend query function's `ALWAYS_DENIED` early return: the adversarial harness answers that plan
kind before the RPC, where the retired suite let it reach the function. That branch returns an empty
id list without touching the database, and the fold itself is pinned in the translator unit test.

**Two Go adapters stop rebuilding on a file they never opened**, and every other adapter workflow
loses a path filter or a comment explaining why it does not have one.

**Onboarding an adapter has one fewer suite to write.** The floor is the conformance harness, the
translator unit test and the example application; there is no per-adapter shared-policy test to
invent a methodology for.
