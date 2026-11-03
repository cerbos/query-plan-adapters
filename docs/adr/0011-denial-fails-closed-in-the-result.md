# Denial fails closed in the result, not in the caller's discipline

Accepted. Decides [#494](https://github.com/cerbos/query-plan-adapters/issues/494) and
[#499](https://github.com/cerbos/query-plan-adapters/issues/499). Supersedes the call-level default
in [ADR 0004](0004-the-null-convention-is-a-property-of-the-attribute.md).

## Context

The repository's invariant is that every filter an adapter emits returns exactly the rows the PDP
allows. Two parts of the public APIs let a caller break it without the adapter emitting anything
wrong.

**The result shape (#494).** Several adapters answer an always-denied plan with *no* filter, and
leave a field a caller can read anyway. A caller that skips the switch on `kind` applies the absent
filter, and every store here reads an absent filter as "no restriction":

| adapter | always-denied result before | what a caller that skips the switch got |
|---|---|---|
| mongoose | `{ kind, filters?: undefined }` (a plain interface, not a union) | `Model.find(undefined)`: every document |
| langchain-chromadb | `filters?: undefined` declared on the denied variant | `where: undefined`: every document |
| convex | `filter?: undefined` declared on every variant | `.filter(undefined)` throws, but `path: "split"` let a caller apply `filter` and drop `postFilter`, which over-grants |
| ent | `Predicate: nil` | `s.Where(nil)` leaves WHERE unset: every row |
| pgx | `Where: ""` | a builder that skips an empty WHERE: every row |

The opted-in post-filter results had the same hole one level down. Chroma's post-filter-only
conditional carried `filters?: undefined` next to `postFilter`, and convex's `split` carried a
`filter` that was only the pushable half of the condition. Applying the field named "filter" alone
was an over-grant in both.

Two adapters, the two most used, already had an answer, and they are different answers for
different languages:

- **prisma** (and drizzle) return a discriminated union: `{ kind }` for the unconditional kinds,
  `{ kind: CONDITIONAL, filters }` otherwise. There is no field to read on a denial, so a TypeScript
  caller cannot apply one without narrowing on `kind`, and the compiler says so.
- **sqlalchemy** returns the query itself, `select(...).where(False)` on a denial. There is nothing
  to switch on: the value denies. activerecord (`model.none`) and spring-data (`cb.disjunction()`)
  do the same, and elasticsearch-java's sealed `Result` is the prisma shape in Java
  ([ADR 0009](0009-elasticsearch-java-keeps-its-result-tagged-union.md)).

**The null default (#499).** ADR 0004 made the null convention a property of the attribute and kept
`explicit` as the call-level default for undeclared attributes, so that no existing caller's SQL
changed. `explicit` is the permissive guess. A caller that omits NULL attributes from `check()` and
keeps the default gets `R.attr.x == null` rendered as `IS NULL`, which returns rows the PDP denies
(CEL raises a missing-attribute error there). The opposite mistake, `omitted` for a caller that sends
explicit nulls, either throws (a null operand) or renders a plain comparison that drops NULL rows the
PDP allowed. One wrong guess over-grants and the other fails safe.

## Decision

### 1. The prisma result shape wherever the language has a union, the sqlalchemy value where it does not

**TypeScript adapters return the prisma union.** A filter field exists only on a `CONDITIONAL`
result. The unconditional kinds are `{ kind }` and nothing else, so reading a filter off a result
does not compile until the caller has narrowed on `kind`:

| adapter | change |
|---|---|
| prisma, drizzle | none; this is their shape already |
| mongoose | the plain interface becomes the union |
| langchain-chromadb | `ALWAYS_DENIED` loses `filters?: undefined`; `ALWAYS_ALLOWED` loses `filters: {}`, which Chroma rejected anyway |
| convex | the unconditional kinds lose `filter?` and `postFilter?` (`path?: undefined` stays, as the discriminant) |

**Go adapters, which have no union, return a value that denies, as sqlalchemy does.** `Kind` stays,
so a caller can still skip the round trip, but the filter is never empty on a successful translation:

| adapter | always-denied | always-allowed |
|---|---|---|
| ent | `Predicate` renders `FALSE` | `Predicate` renders `TRUE` |
| pgx | `Where: "FALSE"`, no `Args` | `Where: "TRUE"`, no `Args` |

sqlalchemy, activerecord, spring-data and elasticsearch-java are unchanged.

### 2. A filter field is either complete or named for what it is

`filter` / `filters` appears on a post-filtered result only when it is the whole condition. The
pushable half of a split condition moves to a field named `prefilter`, and `postFilter` evaluates
the **whole** condition, so it is correct on its own and the prefilter only narrows the candidates:

- convex: `path: "db"` carries `filter`. `path: "split"` carries `prefilter` and `postFilter`.
  `path: "post"` carries `postFilter` only.
- langchain-chromadb (opted in): a post-filtered conditional carries `postFilter` and, for a root
  `and` with expressible conjuncts, `prefilter`; never `filters`.

A caller that applies `filter` without reading `path` gets a type error in TypeScript and a
`.filter(undefined)` throw in JavaScript, not a partial filter.

### 3. The call-level null default becomes `omitted`

An undeclared attribute is treated as omitted-when-NULL. A null-literal comparison that the adapter
cannot render under that convention throws its refusal type, with a message naming the call-level
option. Callers that send explicit nulls declare it, per attribute (ADR 0004) or for the whole call.
elasticsearch-java and langchain-chromadb have no call-level option and are unaffected.

## Considered options

### For 1: a deny-everything value in every adapter

`{ $expr: false }` for mongoose, `{ OR: [] }` for prisma, a contradiction over a reserved key for
Chroma, `() => false` for convex. It would also cover JavaScript and `any`-typed callers, which a
type cannot. Rejected: prisma and drizzle are the most used TypeScript adapters and their union is
the shape their callers already write against; a second convention across the TypeScript adapters
costs more than the JavaScript caller it protects, and Chroma has no honest spelling of "nothing".
A JavaScript caller that skips the switch on a prisma-shaped result still gets every row; that
residual is accepted.

### For 1: the prisma union in Go too (keep `Predicate` nil, `Where` empty)

Rejected: Go has no union, so a nil `Predicate` is a field that can be read without a switch, which
is the hazard itself. The sqlalchemy answer is the one that fits the language.

### For 2: return a single `apply(query)` helper for post-filtered plans

The adapter would run the query and filter the documents itself, so nothing can be half-applied.
Rejected: it takes query execution, pagination and the caller's own composition away from the
caller, and pagination over a post-filter needs the caller's page-size policy.

### For 3: keep `explicit`

Rejected: it is the default that over-grants when the guess is wrong.

### For 3: remove the default and require every call to choose

The purest answer, and the one CLAUDE.md's invariant points at most directly. Rejected for now
because it breaks every call in every adapter, including the large majority whose policies never
compare against null, whereas `omitted` breaks only the calls that reach a null comparison, and
breaks them with a message saying what to declare.

## Consequences

- Breaking changes, released as a major (or, below 1.0, a minor) version of each adapter touched:
  mongoose's, langchain-chromadb's and convex's result types; convex's and langchain-chromadb's
  post-filtered result fields; pgx's `Where` and ent's `Predicate` on the unconditional kinds; and
  the null default in every adapter with a call-level option (prisma, drizzle, mongoose, convex,
  sqlalchemy, activerecord, spring-data, ent, pgx).
- A caller that already switches on `kind` and `path` sees no behaviour change from decisions 1
  and 2, only a type change where it read a field the union no longer declares.
- A caller relying on `explicit` without declaring it gets a refusal on a null comparison. On
  mongoose and convex that is every null operand, and on convex an undeclared mapper entry is now
  nullable, so a comparison over it moves to the post-filter. The convex example declares its
  fields `nullable: false` for that reason.
- Under the `omitted` default some adapters also refuse null literals that touch no NULL column (an
  element of a list attribute, a principal-side value), because the call-level convention is all
  they consult there. That is conservative, not wrong; the harnesses pass `explicit` at the call
  level, which is the convention their dataset uses for list elements, and pin the default in a
  unit test. No ledger moved.
- The Go harnesses splice `Where` / apply `Predicate` without switching on `Kind`, and the
  convex and langchain-chromadb harnesses check that a split's `postFilter` alone selects what it
  selects behind its `prefilter`.
