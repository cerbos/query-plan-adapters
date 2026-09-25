# Painless scripts in the Elasticsearch adapter

The Elasticsearch adapter does not generate Painless `script` queries. A plan that needs a
computed operand — arithmetic, `int()`/`double()`/`string()` casts, a ternary, string `size()`, a
field-to-field comparison — throws `UnsupportedPlanShapeException` instead.

## Why this is out of scope

The adapter README has said so since the adapter was reworked: generating scripts "would change
the security and performance profile of every filter". The costs are concrete:

- **A near-miss is an over-grant.** Scripting means reproducing CEL exactly in a second expression
  language. Integer overflow, `%` on negatives, division by zero, `string()` of a double and
  missing fields all have to behave as CEL does, three-valued under `not`, or the filter returns
  rows the PDP denies.
- **A script error is silent partial success.** A Painless runtime exception fails the shard, and
  the search can still return the other shards' hits. Every CEL error would have to be encoded as a
  deny inside the script, never allowed to throw.
- **Coverage is smaller than the ledger suggests.** `doc[...]` has no doc values on `text` fields,
  and a top-level script cannot read `nested` documents, so most collection shapes would stay
  refused anyway.
- **Clusters restrict it.** Many managed clusters limit `script.allowed_types`, so an opt-in
  feature would fail in exactly the deployments least able to diagnose it.
- **Scripts skip the index.** Every scripted clause is evaluated per document.

A caller who needs one of these shapes can index the computed value as its own field and
reference that field in the policy, which keeps the filter a plain query.

What would change the calculation: a shape many real policies need that cannot be moved into an
indexed field, together with a way to prove the script's semantics against the corpus on every
pinned Elasticsearch image.

## Prior requests

- #226 — "elasticsearch-java: support CEL arithmetic, casts, ternary, index, string-size via painless runtime scripts"
