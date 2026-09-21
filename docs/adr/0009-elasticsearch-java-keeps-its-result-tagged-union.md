# elasticsearch-java keeps its `Result` tagged union

Accepted. Decides
[#348](https://github.com/cerbos/query-plan-adapters/issues/348).

## Context

`ElasticsearchQueryPlanAdapter.toElasticsearchQuery(...)` returns `Result`, a sealed interface over
the three plan kinds: `AlwaysAllowed`, `AlwaysDenied` and `Conditional(Map<String, Object> query)`.
[ADR 0003](0003-spring-data-returns-specification-directly.md) deleted spring-data's `Result` of the
same name as a shallow wrapper, and left this one open on purpose: forcing the two Java adapters into
the same shape is plausibly what produced the spring-data wrapper in the first place. The question
was never "does it look like the one we deleted", but whether it fails the same test here.

It does not. The spring-data verdict rested on three facts, and none of them holds for
Elasticsearch:

1. **Spring Data has a free identity for "no restriction", and Elasticsearch does not.**
   `Specification.unrestricted()` is `(root, query, cb) -> null`, which the repository turns into
   *no* `WHERE` clause, and which is an identity for `.and(...)`. Elasticsearch has no clause that
   disappears. `match_all` is a query the cluster evaluates. For an always-denied plan, the only
   thing better than `match_none` is to not search at all. Both collapsed forms cost something, and
   a caller who knows the plan kind can avoid paying.
2. **spring-data's load-bearing facts lived in Javadoc; these live in the type.** That `Result`
   had one method, `toSpecification()`, and two hazards a caller had to read about (null-predicate
   semantics, never cache the `Predicate`). This one has no methods at all. It is a tagged union, and
   the sealing is the whole contract: a caller cannot receive a fourth kind.
3. **spring-data's example never branched on the kind; the consumers here do.** ADR 0003 recorded
   that every spring-data service composed a tenant boundary and executed, whatever the plan said.
   Here, the adapter README searches with no filter, skips the search, or filters, one branch per
   kind. `ElasticsearchAdversarialConformanceTest.adapterFilteredIds` does the same over real corpus
   data (`allIds()`, `List.of()`, `search(query)`). All three kinds are reachable from real policies:
   `p-has` plans `ALWAYS_ALLOWED`, `in-empty` plans `ALWAYS_DENIED`, and the golden asset records
   each action's kind next to its query.

The evidence #348 waited for was the example application, because it is the one consumer-shaped
caller ([#358](https://github.com/cerbos/query-plan-adapters/issues/358)). It cuts both ways, and
both halves are recorded here:

- `elasticsearch-java/example/` **does** collapse the kinds: `AlwaysAllowed` becomes `match_all`,
  `AlwaysDenied` becomes `match_none`, and every shape runs a search. That choice is deliberate and
  specific to the demo's shape 5. The property that shape checks is that the application's own
  predicate cannot resurrect a denied document, and only a search that runs with both halves in place
  demonstrates it. The example's README says it is *not* taking the skip-the-round-trip path the
  adapter README shows.
- It reads the kind off `Result`, not off the SDK's `isAlwaysAllowed()`/`isAlwaysDenied()`, because
  its clause and its reported kind then come from one variant and cannot disagree. This is ADR 0003's
  "the SDK already exposes it upstream" argument turned around. The upstream predicate exists, but
  a caller that must consult the plan *and* the adapter has two sources for one decision. The
  adapter also takes the protobuf `PlanResourcesResponse`, whose kind lives somewhere else again
  (`response.getFilter().getKind()`). `Result` answers it once for both inputs.

## Considered options

### Collapse — return the query map for every kind

`match_all` for always-allowed, `match_none` for always-denied, and the translated clause otherwise,
with callers reading the SDK to skip a denied search.

Rejected. It moves a cost onto every caller to spare a few callers a branch. A composing caller can
already write the collapse in two lines, and the example does. A caller that wants to skip the
search would have to reach past the adapter to the plan, which is the split source of truth described
above. The golden asset would also lose the kind it records per action.

### Keep the union and add a collapsing convenience (`result.toQuery()`)

Rejected. It is the spring-data wrapper's `toSpecification()` over again: a method whose whole
behaviour is choosing one of three values, and whose `AlwaysDenied` arm has to document "prefer not
calling this". The recipe is two lines in the README, where the cost is stated next to it.

### Keep the union as it is

Accepted.

## Decision

`Result` stays a sealed interface with three method-less records, and `toElasticsearchQuery`'s
signature is unchanged. The README states the collapse recipe and what it costs. It also states the
one thing the sealing does *not* give a caller on the adapter's declared floor: pattern matching for
`switch` is standard only from Java 21. On Java 17 a caller matches with an `instanceof` chain, as the
example does, and the compiler does not check that all three kinds are handled.

## Consequences

- No API change and no behaviour change. `conformance/actions.json`, the wire fixtures and the golden
  asset are untouched.
- The seam stays movable while the adapter is `0.1.0` with no Maven Central release. Revisit
  this if a real consumer (rather than a harness or a demo) turns out to collapse every time. That
  would be the evidence ADR 0003 had and this decision does not.
- Exhaustiveness is a compile-time guarantee only for callers on Java 21 or later. On Java 17 it is a
  convention, and a missed kind is a runtime fallthrough in the caller's own code.
