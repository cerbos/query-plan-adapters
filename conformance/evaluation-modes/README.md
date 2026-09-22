# Evaluation-mode engine contracts

Engine probes, separate from the adapter corpus: they test the pinned PDP itself, under both
explicit values of `engine.strictEvaluation`. No adapter and no database is involved.

```bash
conformance/scripts/check-evaluation-modes.sh   # from any directory; needs docker, curl, jq
```

The script reads the PDP tag and digest from `conformance/CERBOS_VERSION` and
`conformance/CERBOS_IMAGE_DIGEST`, and starts temporary containers on random loopback ports. Run it
after a PDP bump (see `conformance/README.md`, "Evaluation modes and the 0.55 baseline").

## What it asserts

**Check and Plan agree on errors** (`policies/evaluation-probe.yaml`,
`policies/evaluation-probe-roles.yaml`, principals in `principals.json`). Every error-mode
expression uses known principal inputs, so Plan must return an unconditional kind that agrees with
Check.

- The `hostile` principal makes four DENY conditions error: a missing attribute, a type error, a
  variable, and a derived role. Default evaluation allows those actions; strict evaluation denies
  them.
- `unrelated` stays allowed in the same multi-action Check request, in both modes.
- `missing-allow` — a failed ALLOW with no other grant — denies in both modes.
- The `valid` control principal keeps the four DENY conditions false, so their actions stay allowed
  in both modes.

**An invalid literal regex fails compilation** (`invalid-regex/`, kept outside the loaded policy
tree). The RE2 lookahead `a(?=b)` must fail `cerbos compile` with `invalid matches argument` in
both modes. It cannot be a wire fixture, because a PDP cannot load it; the corpus reaches the same
plan through a principal-selected pattern (`regex-lookahead`).

**Non-finite plans fail to serialize** (`policies/nonfinite-plan-probe.yaml`). It keeps the five
original NaN/infinity expressions unmodified, including `not-nan-ord-le`'s original finite
threshold of `0.5`. Under 0.55, compile-time folding puts NaN and infinity into plans that cannot be
serialized as protobuf JSON, so Plan must fail with HTTP 500 and `invalid NaN value` (or
`invalid +Inf value` for `nan-ord-inf`). Check, in both modes:

- allows the true branch and denies the false branch of both ternary orderings and of infinity;
- denies both branches of `nan-ord-le`;
- allows both branches of the negated `not-nan-ord-le` — proving the changed CEL NaN semantics
  independently of the adapter corpus.

The adapter corpus keeps its arithmetic plans reachable with the request-constant
`now() == now()` guard; these probes run the original expressions through the real optimizer
without it. The corpus's `wire-fixtures/` and `wire-fixtures-strict/` are always generated from real
`PlanResources` responses, never from expected adapter trees.

These HTTP 500s are engine regressions, not adapter refusals. When a PDP upgrade repairs Plan
serialization, this assertion fails: inspect the new wire output, then replace it with a
successful-plan assertion.
