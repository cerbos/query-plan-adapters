# Translator unit tests take their plans from wire fixtures

An adapter's translator unit test reads its query plans from `conformance/wire-fixtures/*.json` —
the golden `PlanResources` responses captured against the pinned PDP — and asserts nothing but the
database-native filter the adapter emits for them. It builds no plans of its own, calls no PDP, and
touches no datastore.

## Why

The alternative is what every adapter did before: type the plan into the test. A hand-built plan is
a *belief* about what the planner emits, and it is a belief no test can check, because the same
literal is both the input and the record of the input. Three failure modes follow from that, and
each has happened here:

- **The belief is wrong from the start.** Operand order is the recurring one. `lt(K, V)` and
  `lt(V, K)` are different plans, an adapter that mirrors the operator for one and not the other is
  wrong for exactly half its inputs, and a test written against the half the author had in mind
  passes. Value-first operand inversion is this repository's canonical bug class.
- **The belief goes stale.** The planner has changed shape before. When it does, fixture
  regeneration fails loudly — that is what `conformance.yaml` is for — while hand-written plans go
  on describing a wire contract that no longer exists, and the suites asserting against them stay
  green. The drift check protected exactly the plans nobody hand-built, and none of the plans
  everybody hand-built.
- **The belief drifts between adapters.** Each adapter re-deriving the same wire shape is one more
  chance to derive it differently, which is how one semantic bug has repeatedly shipped
  identically to several of them.

Reading from fixtures inverts all three: there is one recorded plan per corpus action, it is the
PDP's own output, and a planner change fails regeneration instead of silently invalidating every
adapter's tests at once.

The unit test earns its place next to the conformance harness rather than duplicating it. The
harness proves the filter returns the rows `check()` allows **against the seeds it holds**; two
different filters can agree on every seeded row and disagree on the next one, so a rewrite that
quietly changes the emitted SQL passes there. Pinning the filter makes that change a diff a
reviewer reads. It is also the only place some things can be asserted at all: a
`nullAttributeRepresentation` boundary, a mapping hazard the corpus explicitly cannot express as a
policy action, or the behaviour of a plan the planner cannot produce.

## Consequences

**The corpus becomes a dependency of an offline test.** A unit test that reads JSON from a sibling
directory looks like incidental coupling worth removing. It is not: the coupling is the point, and
this ADR exists so a future reader finds that out before deleting it. The coupling is to the corpus
*data* only — the loader that decodes a fixture into that language's plan types is written per
adapter and duplicated deliberately, so the adapter stays standalone
([ADR 0007](0007-adapters-share-data-not-code.md)).

**Every wire fixture must be classified, exactly once, in every adapter that has one of these
tests** — as an expected filter, an expected plan kind, or an expected throw carrying the message
the adapter's `adapterctl.json` pins. That completeness guard is what makes adding a corpus action land
as a failure rather than as silence, and it is the reason the file is worth having rather than a
sampling of shapes somebody found interesting.

**The one value a fixture cannot pin needs an explicit choice.** `regenerate-wire-fixtures.sh`
rewrites the folded `now() - duration("24h")` literal in `ts-window` / `ts-vf` to a placeholder,
because it differs on every capture. Reading it back means substituting a value, and the
substitution has to reproduce what the PDP actually emits — nanosecond precision — or it
contradicts the direct adapter outcome. Pin both sides of that boundary rather than
picking the tidy value.

**Plans the planner cannot produce still belong in a unit test.** Malformed input — an unrecognised
plan kind, a condition with no operator, a ternary of the wrong arity — has no fixture by
construction, and asserting that the adapter fails loudly on it is input validation on a public
function, not a shape the corpus should carry. A shape CEL *can* express is the opposite case: it
belongs in the corpus, where every adapter is asked about it. See "Changing how a condition is
translated" in `CLAUDE.md`.
