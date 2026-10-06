# CLAUDE.md

Multi-language ORM adapters that translate Cerbos query plan responses into database-native
filters. Each adapter directory is an independent package with its own build and test cycle; the
roster is the set of directories holding a `conformance-ledger.json`.

**The invariant: a shape an adapter cannot express throws the adapter's refusal type.** It never
emits a best-effort filter. A wrong filter is an authorization bug that returns rows the PDP denies;
a throw is a bug report.

## Running tests

No suite starts a PDP: the conformance harness replays the plans and decisions recorded under
`conformance/golden/`.

`conformance/scripts/run-harness.sh <adapter> [store]` runs an adapter's conformance harness with
its store started and torn down: `--list` names each adapter's store legs, and `--all` runs every
adapter one leg at a time. Run legs one at a time; several harnesses at once overload a laptop into
timeouts.

**Before running or changing an adapter's code or tests, read its README's "Development" section**
(drizzle: "Testing"; spring-data: "Build" and "Testing"): its commands, suites, the store each one
needs, and that language's gotchas.

## Changing how a condition is translated

**Any change to how an operator, condition, or expression shape is translated starts in the
shared corpus, not in one adapter.** A fix proven only against the adapter you happened to be
looking at leaves the identical bug live in every other adapter.

1. **Add or edit a case** in `conformance/cases/<area>.yaml` (conformance/README.md, "Changing the
   corpus"). A new column or principal attribute goes in `seeds.json` / `derived-fields.json`, with
   its projection in `generator/resources.go`.
2. **Run the generator** (`go -C conformance/generator run .`, Docker) and read the golden diff. A
   new case needs a discriminating oracle: add a seed that tells a right translation from the wrong
   one it targets. An unrelated golden changing means the edit perturbed an existing shape.
3. **Run every adapter's harness and triage each failure** into exactly one of: a translation bug
   (fix it), a shape that store genuinely cannot express (make it throw and add an `unsupported`
   ledger entry), or a known wrong result tracked by an issue (`divergent`). A plan/`check()`
   disagreement is `plannerDivergence` on the case, not a ledger entry.
4. **The ledger is an output of the run, not an input.** Add an `unsupported` entry only after
   watching the case fail: declaring it first is how a translatable shape gets permanently skipped.
5. **Run `conformance/scripts/validate-corpus.sh`**, which recounts each README's `Conformance
   contract` table from the goldens and the ledger, and update the stale ones.

The generator writes `conformance/policies/conformance.yaml`, `resources.json` and everything under
`golden/`: change them by editing the cases or the dataset and re-running it. CI fails on a
hand-edited or stale one.

## Before editing a shared area

- **Before editing `conformance/`** (cases, dataset, ledgers, the PDP pin, a harness's mapping or
  service images): read [conformance/README.md](conformance/README.md), and
  [ADR 0010](docs/adr/0010-conformance-replays-recorded-pdp-decisions.md) for why it replays
  recorded decisions. A change there re-runs every adapter's CI and every harness.
- **Before editing `demo/` or an adapter's `example/`**: read [demo/README.md](demo/README.md). It
  proves plumbing with no per-adapter exceptions
  ([ADR 0001](docs/adr/0001-demo-domain-has-no-per-adapter-exceptions.md)), so a usage shape lands
  in every example at once.
- **Before touching `.github/workflows/` or cutting a release**: read [docs/ci.md](docs/ci.md).
- **Before writing a translator unit test**: read [CODING_STANDARDS.md](CODING_STANDARDS.md), "What
  a translator unit test may pin". Every review checks a diff against the rest of that file.

## Commits

Conventional Commits scoped by adapter name: `feat(prisma):`, `fix(mongoose):`, `chore(deps):`.

## Agent skills

- **Issue tracker**: GitHub Issues on `cerbos/query-plan-adapters`, via `gh`. Tag every affected
  adapter with its per-adapter label, or `conformance` for corpus-wide work
  (`docs/agents/issue-tracker.md`).
- **Triage labels**: the five canonical roles, mapped in `docs/agents/triage-labels.md`
  (`ready-for-agent` is `ready-for-implementation` here).
- **Domain docs**: single-context, `GLOSSARY.md` + `docs/adr/` (`docs/agents/domain.md`).
