# The Exposed adapter is JDBC-first and returns a sealed result

Accepted.

Four decisions taken before the Kotlin [Exposed](https://github.com/JetBrains/Exposed) adapter had
a line of translator in it, because each of them is expensive to reverse once the artifact is
published: the transport it depends on, the shape of what it returns, the ORM version its jar is
compiled against, and one hook it deliberately does not ship.

## Context

Exposed is a SQL builder, not an ORM with association metadata, so structurally this adapter sits
beside drizzle, ent and pgx: it emits an `Op<Boolean>` that the caller hands to `where { }`,
`andWhere { }` or a DAO `find { }`, and every relation lowers to a correlated subquery over a table
read bare. The semantics port from spring-data, the corpus's declared reference implementation.
What does not port is everything below, because Exposed differs from both references in ways that
are not stylistic.

**Exposed is split by transport.** `Op`, `Expression`, `Column`, `Alias`, `LikePattern`, `case()`,
`castTo`, `exists()` and `wrapAsExpression()` all live in `exposed-core`. The query builders
`select()` and `selectAll()` do not: each transport module — `exposed-jdbc`, `exposed-r2dbc` —
defines its own and returns its own `Query` class. Rendering a table into a subquery without one
needs `ColumnSet.describe(transaction, builder)`, and the only core accessor for the current
transaction, `currentTransaction()`, is marked `@InternalApi`.

**An `Op` is a value.** [ADR 0003](0003-spring-data-returns-specification-directly.md) deleted
spring-data's `Result` wrapper and returned `Specification<T>` directly, on the strength of one fact
about Spring: `Specification.unrestricted()` is Spring's own documented identity for an unfiltered
query, so collapsing the always-allowed case into it dissolved the wrapper's Javadoc rather than
relocating it. Exposed has no equivalent. A `Specification` is a builder Spring Data invokes later,
and the repository method that invokes it is also the thing that would have executed the query;
an `Op` is a finished predicate handed back to the caller, who then decides whether to run a query
at all.

**`exposed-core` is a `compileOnly` dependency**, as `hibernate-core` is on spring-data: the
consumer's Exposed is the one that runs. So a support floor is a promise about other people's
classpaths. Exposed 1.0.0 shipped 8 months before this work; 0.x is out of scope entirely, because
1.0 moved every symbol from `org.jetbrains.exposed.sql.*` to `org.jetbrains.exposed.v1.*` and one
artifact cannot serve both.

**An operator override is an authorization escape hatch.** spring-data ships `OperatorFunction`,
and it is the most-qualified part of that adapter's README: a long list of which paths consult an
override and which reject before any lookup. `conformance/actions.json` classifies each action
against *one* mapping per adapter, so a caller-supplied translation function has no corpus spelling
at all.

## Considered options

### Transport: depend on `exposed-core` alone

Rejected, on the `@InternalApi` opt-in rather than on preference. A core-only adapter would serve
JDBC and R2DBC callers from one artifact with no transport dependency in the POM, which is plainly
the nicer package. It is not reachable through public API: the subquery has to render a table, that
needs the transaction, and the accessor is internal. Opting in would make the adapter's correctness
depend on a symbol JetBrains has reserved the right to change in a patch release — for a library
whose entire job is to fail closed, that is the wrong thing to build the only correlated-subquery
construction on.

### Transport: ship JDBC and R2DBC together

Rejected as sequencing, not as direction. R2DBC is a real audience and it gets nothing until the
follow-up module, which is a stated cost below. But the two transports differ only in how a
subquery is built, and nothing about that is known to be right until a harness has replayed the
corpus through it. Building a second implementation of the one construction this adapter cannot
afford to get wrong, before the first has been proved against the oracle, doubles the surface at
the moment there is the least evidence about it. DAO is JDBC-only as well, so the first module
would be the smaller audience.

### Result: return `Op<Boolean>` directly, as ADR 0003 did

Rejected, and it is worth saying why the earlier ADR does not simply extend here. Returning an `Op`
would mean `Op.TRUE` for an always-allowed plan and `Op.FALSE` for an always-denied one. `Op.TRUE`
is tolerable — an extra `WHERE TRUE` the optimiser drops. `Op.FALSE` is not: the caller has a
predicate that is *statically* unsatisfiable and no way to see it, so every always-denied plan
becomes a round trip to the database for a guaranteed empty result. On spring-data that information
was not lost, because the SDK exposes `planResult.isAlwaysDenied()` before the adapter is called
and the example application never switched on the kind anyway. Here the same escape exists, but the
adapter would be actively discarding a distinction it was just handed, which is the opposite of
what ADR 0003 did: that one deleted a wrapper whose behaviour was already Spring's.

The double-call that made `toSpecification(...).toSpecification()` read badly does not arise,
because the wrapper's collapse method is one word (`toOp()`) and it is the documented path for
callers who always run the query.

### Floor: compile against the latest release and claim it

Rejected as needlessly narrow. It is honest and it is the least work, and it would turn away every
user between 1.0.0 and the current release for no technical reason, 8 months into the 1.x line.

### Floor: compile against the latest release and claim the floor

Rejected as unsound. This is the arrangement that reads as obviously fine and is not.
Binary compatibility runs one way: JetBrains promises that code built against an older 1.x keeps
working on a newer one, and promises nothing in the other direction. A jar compiled against the
latest can therefore fail with `NoSuchMethodError` on the floor even when its source would have
compiled there — and the source compiling is exactly the evidence this option would be resting on.

### Floor: claim it, and do not test it

Rejected. spring-data's floor is re-derived by hand and restated in several places, and that
adapter's own build file describes it as a documented claim rather than a checked one. A floor
nothing executes is a sentence in a README, and the failure mode is a consumer's runtime.

### Overrides: ship `operatorOverrides` in the first release

Rejected, on asymmetry. Adding an optional field to `Options` later breaks nobody; removing or
reshaping a published hook breaks every caller who used it, and this adapter is `0.1.0-alpha.1`
with no published tags, which is the cheapest moment to not ship something.

The substance behind the asymmetry is that no harness in this repository can test an override. The
corpus asks what a policy produces, not what a caller passes, so a shipped hook is a supported way
to emit filters nothing has ever checked — in a library whose invariant is that an inexpressible
shape throws rather than emitting a best-effort filter. The main need the hook serves on spring-data
is also weaker here: typed `Column<T>` removes the coercion problems that motivate several of its
uses, and the likeliest real request — mapping an attribute to a computed expression such as a
lower-cased column — is better served by widening what a mapping entry accepts than by a general
per-operator hook.

## Decision

1. **JDBC first.** The published artifact declares `exposed-core` and `exposed-jdbc` as
   `compileOnly`, and builds its correlated subqueries with the JDBC `Query`
   (`alias.select(expr).where { … }`). **Every subquery is constructed in one place**, `Subqueries`,
   which is the seam an R2DBC module replaces. No `@InternalApi` opt-in anywhere in the adapter.
2. **A sealed result.** `ExposedQueryPlanAdapter.toFilter(plan, options)` returns `QueryPlanFilter`:
   `AlwaysAllowed`, `AlwaysDenied`, or `Conditional(op)`. `toOp()` collapses it to `Op.TRUE`,
   `Op.FALSE` or the translated predicate for callers who always run the query.
3. **Compile against the floor, test against the floor and the latest.** The main source set always
   resolves Exposed 1.0.0, whichever version set the tests run under, so the jar a consumer installs
   is the one the compatibility promise covers. `ADAPTER_TEST_ORM` selects `baseline` (the latest
   release, which the golden asset is rendered under and every store leg executes on) or `floor`
   (1.0.0); an unknown value fails rather than falling back. The floor leg runs the offline suites
   **and** the H2 conformance harness, so the claim is proved against the `check()` oracle rather
   than against compilation.
4. **No operator overrides, and one scalar-leaf seam.** Every comparison of one mapped column
   against one constant is routed through a single `LeafTranslator` function under the normalised
   operator name. Nothing hooks into it. That is what makes overrides a contained change if a
   concrete user case ever argues for them.

## Consequences

- **R2DBC users get nothing from the first release.** This is the real cost of decision 1, and the
  follow-up is a module, not a rewrite: the translator, the algebra and every refusal are shared,
  and only `Subqueries` is reimplemented.
- **A consumer's POM carries a JDBC-shaped dependency they may not want.** It is `compileOnly`, so
  it does not reach them transitively, but the adapter is documented as JDBC-only rather than as
  transport-agnostic.
- **Callers pattern-match, or call `toOp()`.** Both are supported; the README leads with the
  `when` form for the always-denied short circuit and shows `toOp()` for composition.
- **The floor is a checked claim, and it can move.** If the floor leg finds an API the design needs
  that 1.0.0 lacks, or a rendering bug a later release fixed, the response is to raise the floor to
  the lowest release that passes and state that number — never to claim a floor nothing tests.
- **The floor costs one CI leg and a divergence list.** Where 1.0.0 and the baseline render
  identically that list is empty, and the assertion is that it stays empty; where they differ, the
  golden asset's bytes are asserted on the baseline and the pinned divergence list in both
  directions on the floor, exactly as the sqlalchemy, activerecord and spring-data legs do.
- **`goldenUpdate` refuses to run on the floor leg.** The asset declares the Exposed minor that
  rendered it, and regenerating under another one would silently re-pin every statement to a
  different renderer (`conformance/README.md`, "When the generator is an input").
- **A caller who needs a translation this adapter refuses has no in-process escape hatch.** They
  rewrite the policy, map the attribute differently, or answer that request with a per-row
  `check()`. That is a deliberate narrowing of what the reference adapter offers, and the first
  concrete request is what should decide the hook's shape rather than a guess made now.
- **This ADR does not change what any other adapter does.** spring-data keeps `OperatorFunction`,
  and ADR 0003 keeps its exact scope: it is about `Specification`, a builder Spring invokes, and
  nothing here reopens it.
