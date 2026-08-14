# The demo domain is a second corpus with no per-adapter exceptions

Example applications need shared policies and seed data, and the obvious move is to reuse the
conformance corpus. We are not doing that. The demo domain is a separate corpus of realistic
shapes, and unlike the conformance corpus it carries no per-adapter classification: every shape in
it must be expressible by every adapter, so there is no equivalent of `actions.json`.

## Why

The two corpora prove different properties. The conformance corpus proves *semantics* — that a
translated filter returns exactly the rows the PDP allows — and it does that with deliberately
hostile shapes that genuinely divide the adapters. That division is why it needs five
classification buckets and ~107KB of per-adapter declarations.

The demo domain proves *plumbing* — that the published package installs, imports, and composes
with the ORM's real query methods. Nothing about that is adapter-specific. If a demo-domain shape
needs a carve-out for one adapter, the shape is wrong for the demo domain and the argument belongs
in the conformance corpus instead.

Admitting exceptions would give the demo domain the conformance corpus's full maintenance cost
without its purpose.

## Consequences

The shared domain is necessarily thin — roughly the intersection of every adapter's query language, one of
which is a vector store. That is deliberate, and it is why the demo domain is a **floor rather
than a ceiling**: every example must implement the shared shapes, and each example is then free to
add richer adapter-local scenarios that nothing shared asserts. `spring-data/example/` keeps its
photo/album/workspace domain and its edge-case regression script on exactly that basis.
