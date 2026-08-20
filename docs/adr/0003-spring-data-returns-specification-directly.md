# spring-data returns `Specification` directly, deleting `Result`

Accepted. Implementation tracked in
[#266](https://github.com/cerbos/query-plan-adapters/issues/266).

## Context

`SpringDataQueryPlanAdapter.toSpecification(...)` returned `Result<T>` — a sealed interface with
three records (`AlwaysAllowed`, `AlwaysDenied`, `Conditional`) and one method, `toSpecification()`.

`Result` failed the deletion test. Its entire behaviour was returning one of three specifications,
and two of its load-bearing facts lived in Javadoc that callers had to read rather than in
behaviour the module owned:

1. An `AlwaysAllowed` result produces a `null` predicate, which Spring Data treats as "no
   restriction" and omits the `WHERE` clause for.
2. The produced `Predicate` must never be cached, because Spring Data fires a separate `COUNT`
   query with its own `Root` under `findAll(spec, Pageable)`, and Hibernate 6 rejects a `Predicate`
   built against a different `Root`.

The wrapper also produced a naming collision. The entry point was named `toSpecification` but
returned a `Result`, which had its own `toSpecification()`. Real call sites read:

```java
SpringDataQueryPlanAdapter.toSpecification(plan, MAPPING).toSpecification()
```

The adapter is `0.1.0-alpha.1` with no published tags and no `maven-publish` configuration, so the
seam is free to move today and locked after 1.0.

Two facts settled the question before the options were weighed:

- **Both hazards are unreachable on the supported path.** A caller can only mishandle a `null`
  predicate or cache a `Predicate` if they obtained a `Predicate` — which only happens if they call
  `Specification.toPredicate` themselves. The documented path (hand the `Specification` to a
  repository method, optionally composing with `.and(...)` first) never exposes one. Only the
  adapter's own tests take the manual path, and it is not a supported audience.
- **The always-denied short-circuit is already available upstream.** The Cerbos SDK exposes
  `planResult.isAlwaysDenied()` before the adapter is called, so the return type does not need to
  carry it a second time. The example app never switched on the result kind anyway — all three
  services compose a tenant boundary and execute.

## Considered options

### Deepen — `Result` absorbs the apply-to-repository knowledge

`Result` would own the repository call, so the caller never handles a raw predicate:

```java
Result<Album> result = SpringDataQueryPlanAdapter.translate(plan, ALBUM_ATTRS);
return result.apply(repository);
```

Rejected. Three reasons:

- **It solves a problem that no longer exists.** Deepening buys the two Javadoc facts moving into
  behaviour. Declaring the manual `toPredicate` path unsupported achieves the same thing at zero
  API cost.
- **It re-exposes most of `JpaSpecificationExecutor`.** The example app composes a tenant boundary
  before executing, and the integration suite runs paginated, sorted, and count queries. `Result`
  would need `and(...)`, `apply(repository, Pageable)`, `apply(repository, Sort)`,
  `applyCount(repository)` and more — mirroring seven repository methods. That is a larger shallow
  module than the one being removed.
- **It keeps the name wrong.** The entry point would go on returning a `Result`, so
  `toSpecification` would have to be renamed to stay honest.

### Delete — return the `Specification` shape directly

Accepted. See below.

## Decision

The six `toSpecification` overloads return `Specification<T>` directly. `Result` is deleted.

```java
Specification<Album> spec = SpringDataQueryPlanAdapter.toSpecification(plan, ALBUM_ATTRS);
return repository.findAll(tenantBoundary.and(spec));
```

Specifics:

- **Always-allowed returns `Specification.unrestricted()`.** Spring Data's own implementation is
  `(root, query, builder) -> null` — byte-for-byte what the adapter already returned — and it is a
  true identity for `.and(...)` and `.or(...)`. This does not merely relocate Javadoc fact 1; it
  dissolves it. The semantics become Spring's, documented on Spring's type. `unrestricted()`
  arrived in spring-data-jpa **3.5.2**, so that becomes the adapter's declared minimum. The README
  currently declares no floor at all, which is its own small bug.
- **Always-denied keeps `cb.disjunction()`** (`1=0`). Unchanged.
- **The caching warning is deleted**, not relocated. It documents a hazard on a path now declared
  unsupported, and keeping it invites callers onto that path.
- **The SELECT-only warning moves to the entry point's Javadoc** and stays in the README. Unlike
  the other two, it documents an enforced runtime throw — the adapter detects a bulk-delete
  invocation context and raises `UnsupportedOperationException` — so a caller can still trigger it.
- **`Result.Conditional` type assertions in tests are dropped.** They weakly restated what the
  differential oracle proves directly: an always-allowed plan returns every row, always-denied
  returns none, and the suite compares against per-row `check()` decisions. Retaining a
  package-private kind accessor purely for tests would resurrect the wrapper under another name.
- **No rename is required.** Deleting the wrapper makes the existing name truthful. The
  `README.md` summary sentence already described this state ("Converts a Cerbos `PlanResources`
  response into a `Specification<T>` you can pass straight to a `JpaSpecificationExecutor`").

## Consequences

- The `SpringDataQueryPlanAdapter.toSpecification(...).toSpecification()` double-call disappears.
- Consumers must be on spring-data-jpa 3.5.2 or later. This is a new constraint, though a mild one:
  Spring deprecated `Specification.where()` in 3.5.0 `forRemoval`, so the ecosystem is being pushed
  past this line regardless.
- Callers who want to skip the database on an always-denied plan check
  `planResult.isAlwaysDenied()` on the SDK response instead of pattern-matching the return value.
  Callers using the raw protobuf overloads check `response.getFilter().getKind()`.
- **No behaviour change.** Nothing about what the adapter can translate moves, so
  the direct outcomes in `spring-data/adapterctl.json`, the wire fixtures, and the README conformance contract table are
  untouched. The differential oracle suite's row-level assertions pass unchanged; the harness
  plumbing does move, because several tests currently cast to `Result.Conditional` to reach the
  spec and that cast disappears.
- This is a breaking API change, taken deliberately while the adapter is `0.1.0-alpha.1` with no
  published tags. The cost is zero today and never lower.
- The `elasticsearch-java` adapter has a same-named `Result` sealed interface, deliberately left
  alone. Elasticsearch has no "null means unfiltered" convention, so its three kinds may genuinely
  differ and forcing the caller to handle each may be defensible depth. Tracked separately rather
  than assumed to be the same defect — forcing symmetry between the two Java adapters is arguably
  what produced the spring-data wrapper in the first place.
