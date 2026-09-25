# Shared conformance runner

This repository does not run conformance through one shared runner that drives per-adapter
processes or libraries.

## Why this is out of scope

The corpus already shares everything that benefits from being shared, as data. The generator
plans every case against both pinned PDPs and records the plan and every `check()` decision as a
golden (ADR 0010). What is left in each adapter's harness is the part that is native by nature:
store lifecycle, seeding, translation and query execution. That is the "driver" a shared runner
would have called, and it is already small.

A runner that every harness depends on — imported or over a process protocol — makes the corpus
a component rather than data. It adds a versioned API, a build/CI edge and a blast radius across
four toolchains, which ADR 0007 ("Adapters share data, not code") rejects. The per-adapter corpus
loaders are duplicated deliberately, and a drift check between them is also ruled out.

What would change the calculation: a class of harness bug that recorded goldens, the stale-ledger
guard and the recorded dataset projection all fail to catch, and that shared code would.

## Prior requests

- #435 — "Prototype a fully shared conformance runner"
