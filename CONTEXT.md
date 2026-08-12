# Query plan adapters

Multi-language adapters that translate a Cerbos query plan into a database-native filter. This
glossary fixes the vocabulary shared across all adapters. Adapter-local design vocabulary lives in
that adapter's own `CONTEXT.md` (currently only `spring-data/CONTEXT.md`).

## Language

### Proving an adapter correct

**Conformance corpus**:
The shared set of deliberately hostile policy shapes, seed rows, and per-adapter classifications
that every adapter is proved against. Lives in `conformance/`.
_Avoid_: adversarial corpus, test corpus, shared fixtures

**Conformance harness**:
An adapter's implementation of the conformance corpus against its own store, built from the
adapter's source rather than its published package. One per adapter.
_Avoid_: adversarial suite, differential test, integration test

**Wire fixture**:
One golden `PlanResources` response per corpus action, captured against the pinned PDP and stored
in `conformance/wire-fixtures/`. Pins planner *shape* — operator, operand order, filter kind —
independent of any adapter or database.
_Avoid_: plan fixture, recorded response

**Translator unit test**:
An adapter's offline test that a plan produces the expected database-native filter. Takes its
plans from wire fixtures, runs without a PDP and without a store, and asserts the emitted filter
and nothing else — see
[ADR 0006](docs/adr/0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md). Distinct
from the conformance harness, which proves the rows that filter returns.
_Avoid_: unit test, filter test, shape test

**Golden asset**:
A static file a translator unit test reads rather than constructing in code. Shared golden assets
live in `conformance/` — the wire fixtures, the seeds, the classification ledger; per-adapter
golden expectations live with the adapter that owns them and never under `conformance/`. Adapters
share this data; the code that loads it is duplicated per adapter on purpose — see
[ADR 0007](docs/adr/0007-adapters-share-data-not-code.md).
_Avoid_: golden file, snapshot, test data

**Golden expectation**:
The database-native filter one adapter is pinned to emit for one corpus action. Always
per-adapter, always a filter, never a row set: which rows a filter returns is the PDP `check()`
oracle's answer and is never written down.
_Avoid_: golden rows, golden oracle, expected output

**Golden suite**:
The system-wide pairing of the shared golden assets with every adapter's translator unit test.
Stands to the translator unit test as the conformance corpus stands to the conformance harness:
the shared whole against one adapter's instance of it.
_Avoid_: golden tests, snapshot suite, fixture suite

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
no per-adapter exceptions. A floor every example must meet, not a ceiling — see
[ADR 0001](docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md).
_Avoid_: example corpus, sample data, demo corpus

**Usage shape**:
A way a consumer calls the ORM with an adapter-produced filter — a plain filtered list, a
paginated page, a count, a sort, a relation traversal. The unit of coverage an example application
adds over a conformance harness.
_Avoid_: query pattern, call pattern, scenario
