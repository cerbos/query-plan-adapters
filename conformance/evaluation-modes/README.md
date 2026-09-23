# Evaluation-mode engine contracts

Probes of the pinned PDP itself — no adapter, no database — under both values of
`engine.strictEvaluation`. Run after a PDP bump (see `conformance/README.md`, "Evaluation modes and the
0.55 baseline"):

```bash
conformance/scripts/check-evaluation-modes.sh   # from any directory; needs docker, curl, jq
```

The script reads the PDP pin from `conformance/CERBOS_VERSION` and `conformance/CERBOS_IMAGE_DIGEST`.

## What it asserts

**Check and Plan agree on errors** (`policies/evaluation-probe.yaml`,
`policies/evaluation-probe-roles.yaml`, principals in `principals.json`). All inputs are known, so
Plan must return an unconditional kind matching Check.

- `hostile` makes four DENY conditions error (missing attribute, type error, variable, derived role).
  Default mode allows those actions; strict mode denies them.
- `unrelated` stays allowed in the same request, in both modes.
- `missing-allow`, a failed ALLOW with no other grant, denies in both modes.
- The `valid` control principal keeps the DENYs false, so its actions stay allowed in both modes.

**An invalid literal regex fails compilation** (`invalid-regex/`, outside the loaded policy tree).
`a(?=b)` must fail `cerbos compile` with `invalid matches argument`. The corpus reaches the same plan
through a principal-selected pattern (`regex-lookahead`).

**Non-finite plans fail to serialise** (`policies/nonfinite-plan-probe.yaml`). The five original
NaN/infinity expressions, without the corpus's `now() == now()` guard, fold at compile time into plans
protobuf JSON cannot hold, so Plan must fail with HTTP 500 and `invalid NaN value` (`invalid +Inf value`
for `nan-ord-inf`). Check, in both modes, must:

- allow the true branch and deny the false branch of both ternary orderings and of infinity;
- deny both branches of `nan-ord-le`;
- allow both branches of the negated `not-nan-ord-le` (CEL 0.30's NaN semantics).

These 500s are an engine limitation, not an adapter refusal. When a PDP upgrade fixes them this
assertion fails: inspect the new wire output and replace it with a successful-plan assertion.
