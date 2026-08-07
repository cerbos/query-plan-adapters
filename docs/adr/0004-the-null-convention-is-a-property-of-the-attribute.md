# The NULL convention is a property of the attribute, not of the call

Accepted. Implementation tracked in
[#308](https://github.com/cerbos/query-plan-adapters/issues/308).

## Context

[#302](https://github.com/cerbos/query-plan-adapters/issues/302) established that the planner emits
the same `eq(attr, null)` node whichever convention the caller uses for a NULL column, so the
adapter cannot recover it from the plan and has to be told. The answer was a **call-level**
option — `nullAttributeRepresentation`, defaulting to `explicit` — and it was enough for the shape
that issue measured: a comparison **against null**.

#308 measured the other side of the same option. Under the `explicit` convention CEL holds a null
*value*, so `null != "x"` is TRUE and `null == "x"` is FALSE — both definite. SQL answers UNKNOWN
to both, and UNKNOWN excludes the row under *both* polarities. Every SQL-backed adapter therefore
returned fewer rows than the PDP allows:

| shape | `check()` allows | the adapter returned |
|---|---|---|
| `R.attr.owner != "x"` | 19 of 20 seeds | 14 |
| `!(R.attr.owner == "x")` | 19 | 14 |
| `!(R.attr.owner in ["x", "one_two"])` | 18 | 13 |
| `R.attr.owner == R.attr.coOwner` | 1 | 0 |
| `!P.attr.manyTeams.exists(t, R.attr.owner == t)` | 16 | 11 |

Reproduced on drizzle (SQLite and PostgreSQL), prisma (SQLite and PostgreSQL, v6 and v7),
sqlalchemy, spring-data, and — which the issue did not predict — **ent and pgx, on all three
dialects each**. The direction is safe, narrower than the decision and never wider, but the id sets
do not match, and matching id sets is the whole property the conformance corpus exists to enforce.

## The decision

**The convention is declared per attribute. The call-level option becomes its default.**

A call-level flag cannot express the corpus, and the corpus is not being perverse: it maps the same
column twice, sending it as an explicit null under `owner` and omitting it under `aOptionalString`,
because real applications do exactly that. Told `explicit`, an adapter has to make `optional-ne`
return the NULL rows and breaks it; told `omitted`, it already refuses the null-comparison shapes.
No single value of one option is correct for both attributes at once.

So each adapter's mapper gained a per-attribute declaration, spelled the way that adapter's mapper
is already spelled:

| adapter | how it is declared |
|---|---|
| prisma, drizzle | `nullAttributeRepresentation` on the mapper entry |
| ent, pgx | `NullConvention` on `Entry` |
| spring-data | `AttributeMapping.field(path, NullAttributeRepresentation)` |
| sqlalchemy | `attribute_null_representation={reference: convention}` |
| elasticsearch-java | `explicitNullAttributes` set |
| mongoose, convex, langchain-chromadb | none — see below |

Declaring it asserts two things at once: that the column can be NULL, **and** how that NULL reaches
`check()`. Leaving it undeclared means the call-level default applies and the column renders the way
it always has — so no existing caller's SQL changes, and the fix is opt-in per column rather than a
silent rewrite of every nullable comparison in every deployment.

### What the declaration changes

Only the **equality family** — `eq`, `ne`, `in`. Those are the operators CEL evaluates to a definite
boolean over a null value, so they are exactly the ones whose SQL must also be definite:

```
eq(col, c)     →  col IS NOT NULL AND col = c
ne(col, c)     →  NOT (col IS NOT NULL AND col = c)
in(col, [cs])  →  col IS NOT NULL AND col IN (cs)          -- no null element in the list
eq(a, b)       →  (a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND a = b)
```

`lt`/`le`/`gt`/`ge` and the string operators are left alone: a null receiver raises a no-overload or
no-such-member error in CEL, which denies under both polarities — precisely what UNKNOWN already
does. Making them definite would break them.

Two consequences worth stating, because both are load-bearing:

**The expansion is dialect-neutral on purpose.** `IS DISTINCT FROM` is spelled three ways across the
stores these adapters run on (`IS`/`IS NOT` on SQLite, `<=>` on MySQL, the standard form on
PostgreSQL), and ent already carries a `NotDistinct` node for one shape. The expansion above needs
no dialect knowledge and is definite everywhere.

**Mixing the two conventions across one comparison has no faithful rendering, so it is refused.**
The declared side needs a *definite* answer for its NULL — CEL holds a null value there, so
`null != "x"` is TRUE. The undeclared side needs *UNKNOWN* for its NULL — that is a missing
attribute, which CEL denies under both polarities. A definite predicate returns rows the PDP
refuses; a plain one drops rows the PDP allows. No single predicate is both, so every SQL adapter
throws rather than picking a direction, and `null-value-f2f-mixed` pins it. Declaring the convention
on both attributes, or on neither, stays expressible.

This is why the both-declared expansion is spelled out rather than using a null-safe equality
operator: the rewrite has to be able to tell "both sides declared" from "one side declared", and the
two cases have different answers.

### Where the declaration does not apply

- **mongoose, convex** store the value the caller sent, so a stored null already compares as a null
  value exactly as CEL does, and a stored null stays distinguishable from an absent field. Every new
  corpus action was aligned before this change — including `null-value-f2f-mixed`, which they are the
  only two adapters to translate rather than refuse. (Mongoose refuses `null-value-pv-not-exists` for
  an unrelated, pre-existing reason: the value-list fold puts a collection macro under a negation.)
- **langchain-chromadb** has no null in its metadata model at all, so `$ne`/`$nin` match documents
  missing the key. All of them are refused, with the messages that limitation already raised.
- **elasticsearch-java** is the interesting one. It cannot represent the convention either — a JSON
  null is not indexed, so an explicitly-null value and a missing field are the same document — and
  it already carried the right error message for exactly that. The message simply never fired,
  because the guard keyed off a null *literal* in the plan rather than off the attribute. So
  Elasticsearch takes the declaration in order to **refuse** rather than to translate: every Query
  DSL spelling of `!= "x"` either requires the field to exist (dropping the row) or matches every
  document missing it, and neither is the decision.

That split is the point. The declaration says what the caller did; each adapter answers with the
best thing its store can honestly do — a correct filter, or a loud refusal where its store cannot
hold the distinction the convention rests on.

## Alternatives considered

**Keep the call-level option and document the limit.** Roughly a third of the work, and the
divergence is in the safe direction. Rejected because it is not implementable as the corpus requires
it to be: an adapter cannot *throw* for these shapes without knowing which attribute is on the
explicit convention — it would have to throw for every `!=` against a constant — so "documented
limit" really means "leave a live divergence and write it down". The corpus's invariant is that a
shape an adapter cannot express must throw, never emit a filter, and a narrower filter is still a
filter.

**Infer nullability from the schema** (drizzle's `column.notNull`, SQLAlchemy's `Column.nullable`).
Rejected on two counts: three of the six adapters cannot see it at all, and where it is visible it
would silently change the SQL emitted for every nullable column in every existing deployment.

**Reuse the existing `nullable` flag** that prisma and mongoose already carry. Rejected because the
two adapters already give that name opposite meanings — in mongoose `nullable: true` declares the
*omitted* convention, in prisma it enables three-valued guards for relation elements — and because
the flag would be describing the schema while the thing that actually needs declaring is what the
caller sent to the PDP. The corpus makes the difference concrete: `aOptionalString` is a nullable
column that is **not** on the explicit convention.

## Consequences

- The `nullAttributeRepresentation` option from #302 is unchanged for existing callers and keeps its
  meaning; it is now also the default for attributes that declare nothing.
- A caller who sends explicit nulls has to declare the attributes that do so, or `!=` against a
  constant keeps under-granting for them. This is stated in each adapter's README; it fails safe.
- Six corpus actions hold the line: `null-value-ne-const`, `null-value-not-eq-const`,
  `null-value-not-in-const`, `null-value-f2f` and `null-value-pv-not-exists` are oracle-compared by
  the six SQL adapters, and `null-value-f2f-mixed` is the fail-closed one every SQL adapter throws
  for. The corpus carries a second explicit-null attribute, `coOwner`, aliasing the `scope` column.
- Mixing the conventions across one comparison now throws where it previously translated. That is a
  consumer-visible break, and a deliberate one: the shape it refuses had no correct answer. It only
  fires for a caller who declares one side of a field-to-field comparison and not the other.
- The issue's own proposal for the field-to-field probe — a second alias of the *same* column — is
  not usable: `owner == ownerAlias` over one column is TRUE for all 20 seeds, and the degeneracy
  guard forbids a total oracle. Aliasing `scope` instead gives a one-row oracle (`e1`, the only seed
  where both columns are NULL), which is thin but non-degenerate and is exactly the row the naive
  translation loses.
