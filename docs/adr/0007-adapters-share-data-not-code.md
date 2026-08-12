# Adapters share data, not code

Accepted. Recorded in
[#397](https://github.com/cerbos/query-plan-adapters/issues/397).

## Context

`conformance/` is shared by every adapter: one hostile policy suite, one set of seed rows, one
derived-field table, one classification ledger, and one golden wire fixture per action. The code
that *reads* those files is not shared. Each adapter carries its own loader, doing the same two
jobs — decoding wire fixtures into that language's plan types, and reading `actions.json` into that
adapter's action lists. Two adapters have a translator unit test today
([ADR 0006](0006-translator-unit-tests-take-their-plans-from-wire-fixtures.md)) and each carries an
independent copy; six more adapters, across three languages, are queued to grow the same file
([#379](https://github.com/cerbos/query-plan-adapters/issues/379) onwards).

Read cold, that looks like an oversight, and the repository documents what appears to be the
opposite rule immediately next door: the two Go modules' vendored translator trees under
`internal/queryplan` are held **byte-identical** and diffed by `validate-corpus.sh`, precisely
because a semantic fix has to land in both copies. Nothing currently explains why that rule stops
where it does, so the next reader reasonably concludes the loaders should be consolidated — or at
least pinned together.

The same question arrives from the other direction as the translator unit tests spread. Such a test
pins the filter one adapter emits for one corpus action, and those expectations want to become
static assets rather than literals inside test code. "Expected output as a static file" is one short
step from "expected rows as a static file", which is the thing the corpus most firmly forbids.

## Considered options

### Extract a shared loader module under `conformance/`, imported by each adapter

Rejected — on standalone-ness, not on feasibility.

Every adapter here is an independently publishable package that depends on nothing else in this
repository. That property is why each Go module vendors its own translator: a consumer of
`github.com/cerbos/query-plan-adapters/pgx` pulls in that module and nothing more. A shared
TypeScript module under `conformance/` inverts it — the corpus stops being data every adapter reads
and becomes a component every adapter links against, with an API, a build-order edge, and a blast
radius. Test-only scoping does not save that: the first adapter needing its loader to behave
differently would have to negotiate with the rest, and the option only ever covered one of the
three languages anyway.

For accuracy, and because it is the wrong reason to reach for: as the adapters are configured today
this does not compile either. Both `prisma/tsconfig.json` and `mongoose/tsconfig.json` set
`rootDir: "src"`, and a relative import of a module outside it fails typecheck with
`TS6059: … is not under 'rootDir'`. That is a config change away and is not why the option was
rejected. If the objection were only mechanical, we would change the config.

### Keep the copies, hold them byte-identical, and diff them in `validate-corpus.sh`

Rejected. This is the Go precedent applied unchanged, and the Go precedent deliberately stops short
of exactly this case.

What `validate-corpus.sh` diffs is `internal/queryplan` — the *translator*. The two Go **harness**
loaders, `ent/corpus_test.go` and `pgx/corpus_test.go`, sit outside that check and have already
drifted: different lengths, reordered declarations, differing comments, and a field one carries that
the other does not. Nothing has broken as a result, and the asymmetry is not an accident. A
translator is the shipped artifact: a copy that drifts emits a different filter to a consumer, and
only a corpus action that happens to exercise the fixed shape would notice. A loader is scaffolding
inside one adapter's test tree. It cannot change what that adapter ships, and holding two copies
identical would not make either of them correct — the failure mode that matters is a loader
projecting the corpus into a narrower local shape, which feeds both sides of the comparison and so
passes vacuously whether or not another adapter's copy agrees. What guards against that is
corpus-side and per-adapter: the declared `seeds.json` and `derived-fields.json` key sets, the
classification completeness guard, the degeneracy guards. A diff between copies guards none of it.

The repository has therefore already decided, implicitly, that translators may not drift and harness
loaders may. This ADR makes that explicit rather than extending a rule written for the other case. A
drift check would also cost exactly what the first option was rejected for: it makes every adapter's
loader change every other adapter's problem, reintroducing the coupling through the validation
script instead of through an import.

## Decision

**Adapters share data. Adapters do not share code.**

- `conformance/` holds what every adapter reads: policies, seeds, derived fields, the classification
  ledger, the wire fixtures, and the PDP pin. Data only.
- The code that loads and decodes it is written per adapter, deliberately duplicated, and free to
  differ. Each adapter stays standalone, in the same sense and for the same reason each Go module
  vendors its own translator.
- **Expected filters are static per-adapter assets.** The database-native filter one adapter is
  pinned to emit for one corpus action is data that adapter owns, and it belongs in a file the
  adapter owns rather than inline in test code.
- **Expected rows are never static.** The PDP `check()` remains the oracle for which rows a
  translated filter must return, for every adapter, always. This line does not move.

The two halves are not in tension, because they pin different things. A filter is the adapter's own
output: deterministic, reviewable as a diff, and wrong in a way a reviewer can see. A row set is the
PDP's answer to a policy question, and writing it down freezes an authorization decision into a file
that no longer tracks the policy. A pinned filter records what the translator emitted; only the
oracle establishes that the filter returns the rows the policy allows.

## Consequences

**Per-adapter expectations must not live under `conformance/`.** Eleven workflows trigger on
`conformance/**` — all ten adapter workflows plus `conformance.yaml` — so one adapter re-pinning one
filter would re-run the other ten for nothing. This is the same argument `CLAUDE.md` already makes
for keeping per-harness service image pins out of the corpus, and it holds whatever form the
expectations take.

**The loaders are allowed to differ, including where one is better than the other.** They already
do: mongoose's validates `actions.json` as it parses, prisma's type-asserts it. That is a question
for whichever adapter is wrong, answered in that adapter — not an argument for a shared
implementation, and not an argument for a drift check.

**A corpus data change still lands in every adapter at once, and must.** Nothing here weakens the
guards that make that true: every harness declares the `seeds.json` keys and `derived-fields.json`
fields it consumes and asserts set equality against the corpus; every wire fixture must be
classified in every adapter that has a translator unit test; adding an action means classifying it
for all ten. Duplicated loaders are affordable *because* those guards exist, and they are what a
loader change has to be checked against — including `CLAUDE.md`'s warning about a harness
hand-projecting corpus data into a narrower shape, which is a per-adapter defect that no comparison
between copies would surface.

**The byte-identical rule keeps its exact current scope**: the vendored Go translator trees, and
nothing else. Extending it to any loader — Go, TypeScript, or otherwise — reverses this ADR rather
than elaborating it.

**This ADR records the principle, not a format.** How golden expectations are stored was piloted
against drizzle on [#379](https://github.com/cerbos/query-plan-adapters/issues/379) and is
documented in `conformance/README.md`, under "Golden expectations". Prisma and mongoose keep their
inline expectations until they are retrofitted to it.
