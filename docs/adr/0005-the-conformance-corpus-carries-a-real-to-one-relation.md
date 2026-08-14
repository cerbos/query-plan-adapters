# The conformance corpus carries a real to-one relation

Accepted. Implementation tracked in
[#374](https://github.com/cerbos/query-plan-adapters/issues/374), under
[#372](https://github.com/cerbos/query-plan-adapters/issues/372).

## Context

Every seed row in `conformance/seeds.json` is flat. Collections (`tags`, `subCategoryNames`) are
lists of scalars on the row, and the one multi-level shape the corpus carries — `mainCategory`,
which reaches sub-categories through a category — is **materialised harness-side** from
`subCategoryNames`, not written into the seed.

That convention has held for everything the corpus needs, with one exception it hides rather than
covers: **the corpus has never had a real to-one join.** The dotted attribute that looks like one,
`obj.inner`, is a flat column wearing a dotted name — every harness maps it to the row's own
`aString`, and no adapter ever emits a join for it. `mainCategory` is a real chain, but its tail is
a *collection*, and the hazards a collection reaches are not the hazards a scalar reaches:

- `conformance/README.md`'s "The absent to-one parent" documents mongoose's `$cond` treating a
  missing field path as falsy, and says outright that **no corpus action reaches it**, because
  "every chained operand the corpus carries is a collection and membership has no
  aggregation-expression form there. Probing it needs a chained **scalar** attribute, which is a
  new seed field."
- The [#373](https://github.com/cerbos/query-plan-adapters/issues/372#issuecomment-5251053077)
  triage of the shared policy suite (since absorbed and deleted —
  [ADR 0008](0008-the-shared-policy-suite-is-absorbed-into-the-conformance-corpus.md)) found the
  to-one join group to be the single largest thing
  that survives absorption: sixteen actions reducing to about six distinct shapes, all of them
  scalar comparisons one or two hops out.

So the corpus needs relational depth it cannot express under the flat-row convention, and it needs
it before any action can use it: every harness asserts **set equality** on the seed keys it
declares, so the moment `seeds.json` gains a key every harness fails at once and neither the corpus nor a
harness can move first.

## The decision

**The corpus carries one real two-level to-one relation, `parent` / `parent.inner`, seeded from a
single new seed key and materialised harness-side.** It lands on its own, with no action using it —
the expand half of an expand–contract.

Three parts to that.

**One seed key, resolved against another row.** `parentSeedId` names the seed whose four scalars
(`aBool`, `aNumber`, `aString`, `aOptionalString`) a row's `parent` carries; that seed's own
`parentSeedId` names the ones `parent.inner` carries, and the chain is cut there. `null` is a row
with no parent. The seed rows stay one line each, which is the property the flat convention was
protecting, and the parent values are the corpus's own hostile strings — LIKE metacharacters,
unicode, the empty string, case traps, NULL optionals — without a second curated set to keep in
step.

**The parent is a copy, not a pointer.** A harness materialises a *fresh* parent (and inner) row
per resource rather than pointing at the named seed's own row. The corpus already seeds distinct
category graphs per resource "so no rows share relations by accident"; the same rule applies here
for a sharper reason — with shared rows, a filter that returned the parent instead of the child
could agree with the oracle.

**Materialised, not written.** The nested structure never appears in `seeds.json`, exactly as
`mainCategory` never does. What a store does with it is the store's business: prisma, drizzle,
sqlalchemy, spring-data, ent and pgx create two owned tables and join; mongoose, convex and
Elasticsearch embed two nested objects; langchain-chromadb flattens both levels onto dotted
metadata keys. How each spells "this level is not there" is its own business — a missing row, a
missing key, or a stored null under the convention that harness already uses for a NULL column.
What every adapter agrees on is the check side: a level that does not exist sends no `parent` (or no
`parent.inner`) attribute, so CEL raises a missing-path error and `check()` denies.

## Why this is worth an ADR

It deliberately breaks a convention (`seeds.json` rows are flat and self-contained; this key is the
only one that resolves against another row), it splits the adapters (a join for six of them, an
embedded object or a flattened key for the rest), and it is hard to reverse once every store carries
the fixture.

## Consequences

**Ten harnesses change atomically, and that is the point of doing it alone.** The seed-key guard
makes the corpus edit and the harness edits inseparable; separating the relation from the actions
that use it means the repo-wide change is a data change, reviewable as one, rather than tangled with
a translation change per adapter.

**A fixture nothing uses can rot, so each harness pins it directly.** Every harness reads both hops
back out of its store — through a real join where it has one — and compares them against the corpus,
rather than counting rows: a count cannot tell an inner row carrying the corpus's values from one
carrying the root's own columns, which is exactly the flat-alias failure this relation exists to
make visible. `validate-corpus.sh` separately checks that every `parentSeedId` names a seed and
that the three depths (no parent, parent without inner, parent with inner) are all non-empty.

**No mapper wiring lands here.** Reaching a scalar *through* a to-one hop is a shape several
translators do not have yet — ent and pgx fail it closed today ("attribute maps to a collection and
cannot be used as a scalar value"), and sqlalchemy has no relation model at all, its collection
semantics being entirely caller-supplied. A mapping written now would be a declaration no assertion
holds to anything, in the same files #375 has to revisit. So every harness stops at schema,
fixture and oracle attribute.

**Two levels, not three.** `parent.inner.inner` is not seeded. Two hops is what discriminates
"joins through every intermediate hop" from "joins off the root"; a third proves nothing new and
doubles the fixture.

## Alternatives considered

**A self-referencing relation on the resource table itself.** Cheaper to seed, and every ORM models
it — but resources would then share relation rows with each other, which is the accident the
per-resource copy exists to prevent.

**Writing the nested object into `seeds.json` literally.** Honest and obvious, and rejected: it
abandons the compact-row convention for the one key that least needs to, and it would mean curating
a second set of hostile values in parallel with the first.

**Aliasing it onto flat columns, the way `obj.inner` is.** That is the status quo this ADR exists to
end. The two are deliberately kept side by side in every harness so a reader can see which of the
two dotted attributes is a join.
