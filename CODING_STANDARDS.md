# Coding standards

Judgement-call rules a reviewer checks a diff against. Mechanical rules belong in a linter or a CI
check.

## Code style

- TypeScript: 2-space indent, camelCase functions, PascalCase types, ESM-friendly. Match the
  surrounding file: no formatter enforces this.
- Java: 4-space indent, Java 17+; model closed sets with sealed interfaces and pattern matching.
- Python: `pdm run format` and `pdm run lint` (isort and ruff) own the style, and CI runs both.
- Tests sit beside the code: `*.test.ts` in `src/` (TypeScript), `tests/test_*.py` (Python),
  `src/test/` (Java).

## Refusals

- A refusal throws the adapter's refusal type with a message naming the real mechanism, in the
  store's own terms. A ledger entry's `reason` names the same mechanism.
- A change to what an adapter can translate updates its `conformance-ledger.json` and its README's
  `Conformance contract` table in the same commit.

## What a translator unit test may pin

**For a shape a policy can reach, the proof is a case.** Only a case proves the emitted filter
returns the rows the PDP allows, and only the corpus asks the same question of every other adapter.
A unit test leaves everything about a case's output to the case: its filter, its refusal message,
and how many cases throw. A pinned filter proves the adapter still emits what it emitted yesterday,
not that it was ever right, and it turns every harmless rewrite into a diff to approve.

What a unit test pins is what the adapter can be asked **without a store** that no case can state.
Three kinds of material live only there, and they are not equal:

1. **A branch CEL itself cannot reach.** An operator CEL does not have (`isSet`) cannot come from
   any policy. Prove the branch cannot be planned by compiling the shape and quoting the planner's
   error before pinning it. Try the `dyn()` spelling first: a type-checker error falls short of
   proof, because `dyn()` defers the check to runtime and the planner drops the wrapper. Plans the
   planner cannot produce at all (an unknown kind, a malformed operand list) belong here too.
   Permanent.
2. **A caller-supplied argument the corpus structurally cannot vary.** Each harness uses *one*
   mapping, so an operator override, a second mapper form, `allowPostFilter`, a per-call
   `nullAttributeRepresentation`, or `maxMacroDepth` has no case spelling. The adapter's refusal
   *type* belongs here too. Permanent.
3. **A corpus gap wearing a unit test**: policy-reachable, and the corpus simply does not carry it
   yet. This one is a **bridge, not a home**: a shape parked here is asked of one adapter and none
   of the others, which is the condition every bug this repository exists to stop was living in.
   Each instance says at the test that it is a corpus gap, names the issue tracking the port
   ([#509](https://github.com/cerbos/query-plan-adapters/issues/509)), and is deleted when the case
   lands. `ElasticsearchQueryPlanAdapterTest` and `SpringDataQueryPlanAdapterTest` are the worked
   examples: a `KIND 3` banner over the block and a `Corpus gap.` lead on every test under it.

## The conformance corpus and its harnesses

- A harness passes corpus data through verbatim: one mapping and one set of options for every case,
  over the whole dataset.
- **Adapters share data, not code.** The corpus loader each adapter carries (`<adapter>/src/corpus.ts`,
  `sqlalchemy/tests/corpus.py`, `activerecord/spec/support/conformance_corpus.rb`, the Java
  `Corpus.java` files, `ent/corpus_test.go`, `pgx/corpus_test.go`) is duplicated **deliberately**,
  so every adapter stays standalone: a loader fix lands in each copy that needs it. Flag a diff that
  extracts a shared loader or adds a drift check between the copies. The vendored Go *translator*
  trees follow the opposite rule: byte-identical. See
  [ADR 0007](docs/adr/0007-adapters-share-data-not-code.md).
- `conformance/cases/` is the repository's only policy source for semantics. `demo/policies/`
  (every example application) and `spring-data/example/policies/` (that adapter's onboarding
  artifact) prove **plumbing**; a new shape proposed in either belongs in a case
  ([ADR 0008](docs/adr/0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md)).

## Prose

- Write "every adapter" / "every harness" / "every example" wherever prose spans the roster, in docs,
  test-file comments and JSON `description`s alike. The roster is the set of directories holding a
  `conformance-ledger.json`, so the phrasing stays true when it changes. Genuine counts of something
  else (cases, seed rows) go in digits.

## Commits

- Conventional Commits, scoped by adapter name (`feat(prisma):`, `fix(mongoose):`), by
  `conformance` for corpus-wide work, and `chore(deps):` for dependencies.
- A commit that changes a generated file carries its regeneration: the generator's outputs under
  `conformance/` and any other committed build artifact land in the same commit as their source.

## Pull requests

- Name the affected adapters, link the related Cerbos issues, and attach logs for a significant
  behaviour change.
- Name any service a reviewer needs to reproduce the change locally.
- A change to what an adapter can translate is stated explicitly and documented as a **breaking
  change**: a shape that used to return a filter and now throws is a consumer-visible break, even
  when the old filter was wrong.
