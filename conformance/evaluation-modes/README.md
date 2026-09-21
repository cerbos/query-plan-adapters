# Evaluation-mode engine contracts

Run `conformance/scripts/check-evaluation-modes.sh` from any directory. It reads the repository's
Cerbos tag and digest, uses temporary containers on random loopback ports, and tests both explicit
values of `engine.strictEvaluation`.

These are engine probes using dedicated resource kinds in the shared `conformance/policies/`
tree, separate from the adapter action ledger. Every error-mode expression uses known
principal inputs, so Plan must return an unconditional result agreeing with Check; no database
translation is involved. The hostile principal makes a missing-attribute DENY, a type-error DENY,
a variable-dependent DENY, and a derived-role DENY error. Default evaluation allows those actions;
strict evaluation denies them. An unrelated action stays allowed in the same multi-action Check
request. A failed ALLOW without another grant denies in both modes. A valid control principal
keeps the four DENY conditions false and proves their actions remain allowed in both modes.

The invalid-regex directory is intentionally outside the valid policy tree. The literal RE2
lookahead `a(?=b)` must fail compilation with `invalid matches argument`, regardless of evaluation
mode. It cannot be a planner-wire fixture because compilation prevents a PDP from loading it.

`conformance/policies/nonfinite-plan-probe.yaml` retains all five original NaN/infinity corpus expressions
unmodified, including the original `not-nan-ord-le` finite threshold of `0.5`. It pins a separate
0.55 limitation: compile-time folding puts NaN and infinity into plans that cannot be serialized
as protobuf JSON. The Plan API must currently fail with HTTP 500 and the corresponding `invalid NaN value` / `invalid +Inf value` diagnostic; Check
allows the true branch and denies the false branch for both ternary orderings and infinity,
denies both branches of `nan-ord-le`, and allows both branches of the original negated comparison,
in both modes. The last result proves the changed CEL semantics independently of the adapter
corpus.

The adapter corpus keeps its arithmetic plans reachable using the request-constant
`now() == now()` guard. The guard is confined to the adapter corpus; the engine probes exercise every original expression through the
real 0.55 optimizer without that guard. Both fixture directories are generated from real
PlanResources responses, never manufactured from expected adapter trees. These HTTP failures are engine regressions, not adapter refusals. When a PDP upgrade
repairs the Plan response, this explicit regression assertion fails and should be replaced with a successful-plan
assertion after inspecting the new wire output.
