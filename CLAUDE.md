# CLAUDE.md

Adapters that translate a Cerbos query plan into a database-native filter, each an independent
package in a directory holding a `conformance-ledger.json`.

**The invariant: a shape an adapter cannot express throws the adapter's refusal type.** Every filter
an adapter emits returns exactly the rows the PDP allows. A best-effort filter can *over-grant*,
returning rows the PDP denies: that is an authorization bug, where a throw is a bug report.

## Read before you start

- **Changing how a condition is translated** → [conformance/README.md](conformance/README.md),
  "Changing how a condition is translated". The change starts as a case in the corpus, then reaches
  every adapter.
- **Editing or testing an adapter** → its README's "Development" section: suites, stores and that
  language's gotchas. conformance/README.md, "Running a harness", runs its conformance harness with
  the store started for you.
- **Editing `conformance/`** (cases, dataset, goldens, ledgers, the PDP pin, a harness mapping, a
  service image) → conformance/README.md; its "Layout" names the files only the generator writes.
  [ADR 0010](docs/adr/0010-conformance-replays-recorded-pdp-decisions.md) says why harnesses replay
  recorded decisions.
- **Editing `demo/` or an adapter's `example/`** → [demo/README.md](demo/README.md). A usage shape
  lands in every example at once.
- **Editing `.github/workflows/` or cutting a release** → [docs/ci.md](docs/ci.md).
- **Writing a translator unit test, a commit message or a PR description** →
  [CODING_STANDARDS.md](CODING_STANDARDS.md), which every review checks a diff against.

## Tests

Here the integration test is a conformance case replayed against a real store: it is the proof every
adapter shares. CODING_STANDARDS.md, "What a translator unit test may pin", names what a unit test
may still pin.

- Never write unit tests after you write code.
- Highly prefer E2E or integration tests as the sole testing mechanism. Use them to verify complex
  features work. At the end of those tests, produce a verifiable and repeatable artifact.
- If you must test a system in isolation, first write down all the ways it could fail, then write
  the code.
- A test that breaks under a behavior-preserving refactor is asserting implementation, not behavior.
  Do not add it.
- Never delete or weaken a failing test to make the suite pass. Fix the code, or ask.

## Agent skills

- **Issue tracker**: GitHub Issues via `gh`, with per-adapter scope labels
  (`docs/agents/issue-tracker.md`).
- **Triage labels**: the five canonical roles (`docs/agents/triage-labels.md`).
- **Domain docs**: single-context, `GLOSSARY.md` + `docs/adr/` (`docs/agents/domain.md`).
