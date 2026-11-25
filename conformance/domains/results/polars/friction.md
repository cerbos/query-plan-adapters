# polars: domain stress friction

Harness: `conformance/domains/harnesses/polars/harness.py`
(`../../../../polars/.venv/bin/python harness.py`), one module per domain holding the frame it
builds and its `attr_map`. Plans reach `get_filter` through the SDK's HTTP `CerbosClient` against
the conformance harness's stub PDP (`polars/tests/corpus.py`).

There is no store. Each domain's `generated/rows.json` is loaded into one frame per table and
joined into the resource's shape the way an application would prepare a frame to filter:
`_frames.one` turns a to-one relation into a struct column (a null key leaves it null), and
`_frames.many` turns a to-many relation into an ordered list column, `[]` where there are no rows.
Timestamps are `Datetime("ns", "UTC")`, DECIMAL columns `Decimal(18, 2)`, the iot JSON column a
struct over the union of its keys, and the media `text[]` a `List(String)`. Every plan is filtered
eagerly and on the streaming engine; the two legs agree on every case.

| leg | pass | refused | under | over | error |
|---|---|---|---|---|---|
| eager | 349 | 0 | 17 | 2 | 0 |
| streaming | 349 | 0 | 17 | 2 | 0 |

The 2 over-grants and 17 under-grants are each shared with every other adapter that translated the
case: the PDP 0.56.0 planner folding `has()`, and plans both PDPs record differently from what
`check()` decides (healthcare nurse_ana/view, legal client_globex/view, iot member_acme/view on
0.57.0). On 0.57.0 every case passes except those three.

## What the mapping takes

The `attr_map` holds only renames of root columns (`noteType -> type`, `deleted -> is_deleted`,
`request.resource.id -> note_id`). Nested objects are built with the policy's field names, because
a struct is the value the policy reads: a dotted attribute under a mapped prefix reads struct fields
by name, and a lambda variable reads the fields of the list's element struct, which `attr_map`
cannot rename.

| domain | difficulty | notes |
|---|---|---|
| healthcare | 1 | the 3-hop `patient.ward.hospital` chain is three nested `one` joins; a missing ward leaves the struct null, so `patient.ward.secure` errors as CEL does |
| fintech | 1 | the flattened counterparty is `pl.struct(name=..., iban=..., country=...)`; DECIMAL against double literals passes |
| logistics | 1 | two joins to `hubs` (origin, destination) and a third inside the checkpoint list |
| education | 1 | the staff list two to-one hops below the root is nested inside the course struct |
| legal | 1 | `null_attribute_representation="explicit"` alone covers every attribute, struct fields included; `retentionUntil != null` on a Datetime column passes |
| retail | 1 | the `order` table name means nothing to a frame; arithmetic inside `exists` and `all` over empty lists pass |
| hr | 1 | the self-relation followed two hops is the employees frame joined to itself twice |
| media | 2 | the many-to-many contributors list needs the join table joined to `authors` first |
| iot | 2 | the JSON labels column becomes a struct over every key seen; `"tier" in labels` reads as `has(labels.tier)` |
| government | 1 | explicit nulls and nullable DECIMAL pass with the call-level setting |

## What the run changed in the adapter

Two shapes were refused before this run and are translated now, both on PR #776:

- `x != null` (and `== null`) on a `Datetime` column: a null check does not depend on how the
  timestamp string is spelled.
- `"key" in m` over a struct column, as `has(m.key)`. This also translates three corpus cases the
  ledger listed as refused.
