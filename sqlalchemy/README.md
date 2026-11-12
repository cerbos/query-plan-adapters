# Cerbos + SQLAlchemy Adapter

Converts a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [SQLAlchemy](https://docs.sqlalchemy.org/) `Select`, for use with the
[Cerbos Python SDK](https://github.com/cerbos/cerbos-sdk-python).

## Install

```bash
pip install cerbos-sqlalchemy
```

- Python >= 3.10
- SQLAlchemy >= 1.4 (1.4 and 2.x are both tested)
- Cerbos Python SDK (`cerbos`) >= 0.10.4; Cerbos PDP > v0.16
- Either SDK client: the HTTP `CerbosClient` or the gRPC client (see [Transports](#transports))

## Quick start

```python
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base

from cerbos_sqlalchemy import get_query

Base = declarative_base()


class LeaveRequest(Base):
    __tablename__ = "leave_request"

    id = Column(Integer, primary_key=True)
    department = Column(String(225))
    geography = Column(String(225))
    priority = Column(Integer)


with CerbosClient(host="http://localhost:3592") as c:
    principal = Principal(
        "john",
        roles={"employee"},
        attr={"department": "marketing", "geography": "GB"},
    )
    plan = c.plan_resources("view", principal, ResourceDesc("leave_request"))

attr_map = {
    "request.resource.attr.department": LeaveRequest.department,
    "request.resource.attr.geography": LeaveRequest.geography,
    "request.resource.attr.priority": LeaveRequest.priority,
}

# ALWAYS_ALLOWED -> select(LeaveRequest)
# ALWAYS_DENIED  -> select(LeaveRequest).where(False)
# CONDITIONAL    -> select(LeaveRequest).where(<translated condition>)
query = get_query(plan, LeaveRequest, attr_map)

# Compose with your own predicates; a denied plan stays denied.
query = query.where(LeaveRequest.priority < 5).limit(20)

with Session(create_engine("sqlite:///app.db")) as session:
    rows = session.execute(query).scalars().all()
```

`get_query` returns a `Select` for all three plan kinds, so you don't need to branch on the kind.
If you want to skip the round trip on a denial, check
`plan.filter.kind == PlanResourcesFilterKind.ALWAYS_DENIED` (from `cerbos.sdk.model`) first. Any
shape the adapter cannot translate raises instead of returning a broader query.

To debug, print the SQL: `print(query.compile(compile_kwargs={"literal_binds": True}))`.

## Mapping attributes

`attr_map` maps each Cerbos attribute reference to a column. Values can be an ORM attribute
(`LeaveRequest.department`), a Core column (`LeaveRequest.__table__.c.department`), or any SQLAlchemy
column expression — for example a correlated scalar subquery for a value read through a to-one
relation.

### Model styles

`table` accepts a Core `Table`, a legacy `declarative_base()` model, or a SQLAlchemy 2.0
`DeclarativeBase` subclass. An ORM model returns `Select[Tuple[Model]]`; a Core `Table` returns
`Select[Any]`.

### Columns from more than one table

If `attr_map` references columns from other tables, pass `table_mapping` (4th positional
argument) — a list of `(table, join_condition)` pairs:

```python
query = get_query(
    plan,
    Table1,
    {
        "request.resource.attr.foo": Table1.foo,
        "request.resource.attr.bar": Table2.bar,
        "request.resource.attr.bosh": Table3.bosh,
    },
    [
        (Table2, Table1.table2_id == Table2.id),
        (Table3, Table1.table3_id == Table3.id),
    ],
)
```

Tables may be models or Core `Table`s. A column expression that carries its own correlation (a
scalar subquery) needs no `table_mapping` entry.

If your models use `relationship()`, consider `query.with_only_columns(...)` to avoid implicit joins.

### Collections and relations

- **A collection stored in one column** (JSON or PostgreSQL array) — declare it in
  [`collection_columns`](#collection-storage) to get `size()` and constant indexing.
- **A collection in a related table** — map it to a [`Relation`](#collections-in-a-related-table)
  and pass `relation_overrides()` as `operator_override_fns`. Read
  [Mapping hazards](#mapping-hazards) before you do.
- **A collection the PDP already knows** (usually a principal attribute) — translated with no
  configuration; see [Collection macros over known values](#collection-macros-over-known-values).

## Options

| Option | Default | Use it when |
| --- | --- | --- |
| `table_mapping` | `None` | `attr_map` spans more than one table |
| `operator_override_fns` | `None` | you need a dialect-specific operator, or `relation_overrides()` for a collection in a related table |
| `null_attribute_representation` | `"omitted"` | your app sends explicit nulls for NULL columns (`"explicit"`) |
| `attribute_null_representation` | `None` | the NULL convention differs per attribute |
| `collection_columns` | `None` | a policy calls `size()`, indexes, or tests literal membership in a JSON / PostgreSQL array column |

### NULL attribute representation

The planner emits the same `eq(x, null)` node however your application represents a NULL column in
the attributes it sends to `check()`, so you have to tell the adapter which convention you use.

| attributes you send for a NULL column | `check()` on that row | `IS NULL` filter |
| --- | --- | --- |
| `{"x": None}` — explicit null | allow | selects it — aligned |
| `{}` — attribute omitted | **deny** (CEL missing-attribute error) | selects it — **over-grants** |

The default is `"omitted"`, the convention that fails closed when the guess is wrong. If you send
NULL columns as explicit nulls, pass `"explicit"`, which translates `== null` to `IS NULL`:

```python
get_query(plan, Resource, attr_map, null_attribute_representation="explicit")
```

Under `"omitted"`, `x == null` and `x != null` answer exactly what CEL does: a NULL column is a
missing-attribute error, and a present one is never null. The adapter renders them as
`CASE WHEN x IS NOT NULL THEN FALSE END` (`TRUE` for `!=`), which is UNKNOWN for a NULL column and
stays UNKNOWN under any enclosing `not`, so the row is denied under both polarities. This needs `x`
mapped to a SQL expression (a column, or a correlated scalar subquery for a to-one relation) and no
override for that operator; otherwise the comparison raises. Every other null operand raises (a null
element of an `in` list, a null inside `hasIntersection`, ordering against null), and the refusal
fires before `operator_override_fns`. See [#302](https://github.com/cerbos/query-plan-adapters/issues/302)
and [#551](https://github.com/cerbos/query-plan-adapters/issues/551).

#### Declare the convention per attribute

One policy suite can mix both conventions (the same column mapped under two attribute names). Declare
it per attribute; the call-level option covers everything undeclared:

```python
get_query(
    plan,
    Resource,
    attr_map,
    attribute_null_representation={
        "request.resource.attr.owner": "explicit",
        # department is omitted when NULL, so leave it undeclared
    },
)
```

Declaring `"explicit"` asserts that the column can be NULL **and** that a NULL reaches `check()` as
an explicit null. The equality family (`eq`, `ne`, `in`) over that attribute then renders so it is
never SQL UNKNOWN: CEL's `null != "x"` is TRUE, so the NULL row comes back. Ordering and string
operators are unchanged (CEL raises on a null receiver, which denies like UNKNOWN does). An
undeclared attribute follows the call-level option; if that is `"omitted"` and the caller in fact
sends an explicit null, `!=` against a constant under-grants the NULL rows, which fails closed.

**Declare both sides of a field-to-field comparison, or neither** — mixing conventions in one
comparison throws. See [#308](https://github.com/cerbos/query-plan-adapters/issues/308) and
[ADR 0004](../docs/adr/0004-the-null-convention-is-a-property-of-the-attribute.md).

> [!WARNING]
> **On Cerbos 0.56.0 and earlier, do not guard with `has()`: write `R.attr.x != null`.** An
> attribute the plan request omits is unknown to those planners, which assume the data layer
> supplies it, so they fold `has(R.attr.x)` to true: alone it plans as `ALWAYS_ALLOWED`, and
> `has(R.attr.x) && R.attr.y > 0` plans as `R.attr.y > 0`, which never excludes a row missing `x`.
> From 0.57.0 the planner keeps `has(R.attr.x)` in the plan, and the adapter translates it from
> `x`'s null convention or refuses it: under `omitted` a row has `x` when its value is not null, and
> under `explicit` every row has it. The planner still drops a `has(R.attr.x)` that only guards an
> access to `x`, as in `has(R.attr.x) && R.attr.x == "a"`, because that access already denies a row
> without `x`.

### Collection storage

`size(R.attr.tags)` and `R.attr.tags[0]` need to know how the collection is stored. Declare it per
attribute:

```python
from cerbos_sqlalchemy import CollectionColumn, get_query

get_query(
    plan,
    Resource,
    attr_map,
    collection_columns={
        # PostgreSQL JSON/JSONB, a MySQL JSON column, or a SQLite JSON text column
        "request.resource.attr.tags": CollectionColumn(Resource.tags, "json"),
        # PostgreSQL array of text, varchar, boolean, integer or smallint
        "request.resource.attr.labels": CollectionColumn(Resource.labels, "pgArray"),
    },
)
```

The storage names match the drizzle adapter's. The declaration is read in exactly three places. In
the first two it takes precedence over `attr_map` and any override:

- **`size()`** counts elements. Empty is `0`; an SQL NULL column or a non-array JSON value is
  UNKNOWN, so `size(x) == 0` selects empty rows, never missing ones, and `size(x) >= 0` excludes them.
- **`x[i] == literal`** / **`x[i] != literal`** — string, number, boolean or `null` literal, constant
  non-negative index, either operand order, under logical operators. JSON types are kept (`"1"` is not
  `1`; `true` is not `1` even on SQLite); numbers compare as doubles. An absent element is UNKNOWN,
  so it stays excluded under negation. A null *element* is a value: `[null][0] == null` is true.
  PostgreSQL arrays are read through `to_jsonb`, so a non-1 lower bound still reads the element CEL does.

The third applies only to an attribute `attr_map` does **not** map, so a declaration added for
`size()` and indexing never changes how an existing relation answers membership:

- **`literal in x`** / **`hasIntersection(x, [literals])`**, either operand order, for string,
  number, boolean and `null` literals. Each literal matches only an element of its own JSON type, as
  CEL's heterogeneous equality does: `"2" in [2]` is false, and `hasIntersection(x, ["2", 3])`
  matches on the `3` alone. An SQL NULL column or a non-array JSON value is UNKNOWN, so a negated
  membership still excludes it. A column, an expression, or a list/map literal as the element raises.

Everything else over a declared element raises: a negative, fractional or dynamic index, ordering, a
projection like `x[0].name`, and comparison with a list or map literal. Elsewhere the attribute
still resolves through `attr_map`. The column must hold exactly the list you send to Cerbos, null
elements included.

The SQL is picked per dialect at compile time. SQLite (JSON1), PostgreSQL and MySQL 8.0.17+
(`"json"` only) are supported; any other dialect raises `CompileError`.

### Operator overrides

`operator_override_fns` replaces the handler for an operator — for a more idiomatic SQL form, or to
translate a collection held in a related table:

```python
from sqlalchemy.sql.expression import any_

query = get_query(
    plan,
    Table1,
    attr_map={"request.resource.attr.foo": Table1.foo},
    operator_override_fns={
        "in": lambda c, v: c == any_(v),  # PostgreSQL: = ANY instead of IN
    },
)
```

The type is `dict[str, Callable[[GenericColumn, Any], GenericExpression]]`, where `GenericColumn` is
`Column | InstrumentedAttribute` and `GenericExpression` is `BinaryExpression | ColumnOperators`.

- An entry whose value is `None` uses the default handler, including inside nested expressions.
- Without `operator_override_fns`, every `attr_map` entry is validated. With an explicit mapping
  (even `{}` or only `None` entries), only attributes reached outside active overrides are
  validated; an override owns the operands it translates.
- `matches()` never reaches the store's regex engine (none is RE2): the pattern is lowered to
  exact string predicates, or refused (["Regular expressions"](#regular-expressions)). Override it
  only if your database's engine is equivalent.
- An override's intermediate value reaching a default handler raises.

For a collection in a related table, use
[`relation_overrides()`](#collections-in-a-related-table) rather than writing the macros yourself.

### Collection macros over known values

`exists`/`all` over a collection the PDP resolves at plan time — usually a principal attribute — is
translated with no override:

```yaml
condition:
  match:
    expr: P.attr.teams.exists(t, R.attr.team == t)
```

The planner unrolls this into an `or`/`and` chain at 10 elements or fewer, and sends a literal list
above that (cerbos/cerbos#2570, cerbos/cerbos#2817). The adapter applies the same fold with no cap,
so the SQL is equivalent either side of the threshold. A bare `t` becomes the element, `t.name`
drills into it, and each body goes through the normal pipeline (overrides and NULL handling apply).
An empty list: `exists` matches nothing, `all` matches everything. `exists_one` counts the bodies
that hold, `size(list.filter(t, ...))` counts them as a number, and `x in list.map(t, ...)` is an
`==` against each mapped body; CEL raises if any body raises, so each of these is UNKNOWN where
any body is. `except` over a literal list is still refused.

A list the plan builds from attributes (`[R.attr.a, R.attr.b].exists(s, s == "x")`) folds the same
way, with each element substituted into the body. Building the list evaluates every element, so an
element that raises in CEL (a missing attribute, or an expression over one) makes the whole macro
raise even beside a match: the folded filter is UNKNOWN for that row. `in` over a built list is an
`==` per element under the same guard, `x in (cond ? A : B)` is taken per branch, and `x in A + B`
is `x in A || x in B`.

`exists_one`, `filter`, `map` and `except` over a literal list raise, as does a `t.path` the element
doesn't carry, a map or list element, and a two-variable comprehension (`exists(i, v, ...)`), whose
index or map key has no SQL lowering. Macros over a related table (`R.attr.tags.exists(...)`) need
[`relation_overrides()`](#collections-in-a-related-table).

### Collections in a related table

Map the collection attribute to a `Relation`, map each lambda variable's fields to the element
table's columns, and pass `relation_overrides()`:

```python
from cerbos_sqlalchemy import Relation, get_query, relation_overrides

attr_map = {
    "request.resource.attr.tags": Relation(
        [Tag.resource_id == Resource.id],
        correlate=[Resource],
    ),
    # The lambda variable in `R.attr.tags.exists(t, t.name == "public")`.
    "t.name": Tag.name,
    # A list of strings (`"public" in R.attr.tagNames`) needs the member column.
    "request.resource.attr.tagNames": Relation(
        [Tag.resource_id == Resource.id],
        correlate=[Resource],
        member=Tag.name,
    ),
}

query = get_query(
    plan, Resource, attr_map, operator_override_fns=relation_overrides()
)
```

`Relation(correlation, correlate=(), *, member=None, hop_correlation=())`:

- `correlation` joins an element row to the row that owns the collection, through every hop.
- `correlate` lists the entities the subqueries correlate against: the queried table, plus every
  enclosing lambda's table for a nested collection (`c.subCategories` inside `categories`).
  SQLAlchemy auto-correlates only with the immediately enclosing `SELECT`.
- `member` is the element column `in` and `hasIntersection` compare.
- `hop_correlation` names the predicates that cross a to-one parent; see
  [`require_hops`](#require_hops-the-one-hazard-with-a-library-helper).

`relation_overrides()` translates `exists`, `all`, `exists_one`, `size()`, `in`,
`hasIntersection`, `size(filter(...))`, `x in filter(...)`, `x in map(...)` and
`hasIntersection(map(...), [...])` over a `Relation`. Each is three-valued: an element whose body is
SQL NULL is a CEL error, which `exists` absorbs only beside a true element and `all` only beside a
false one, so `!tags.exists(t, t.name == "x")` excludes a row with a nameless tag, as the PDP does.
A plain `EXISTS (... WHERE body)` reads that tag as false and its negation selects the row. `in`,
`size()` and `hasIntersection` over anything that is not a `Relation` fall back to the default
handler. The dict is new on each call, so merge your own overrides into it.

The conformance harness runs exactly these overrides, so the corpus proves them.


### Regular expressions

CEL's `matches()` is RE2, and no store's regex dialect is: MySQL's ICU engine lets `$` match before a
trailing newline, PostgreSQL's differs in classes and flags, and SQLite has none. So the pattern is
never handed to the store. The adapter parses it and lowers it only when what it matches can be said
with exact string predicates:

- a finite set of literals, under anchors or not: `^(ab|b)$` is `IN ('ab', 'b')`, `^h` and `\d` are
  `startsWith`/`contains` over each literal, `(?i)` folds ASCII case (and `k`/`s` to KELVIN SIGN and
  LONG S, as RE2 does);
- every character drawn from a set of at most 64, with a minimum count: `^[ab]+$`, `^\s*$`,
  `^[[:alpha:]]+$`;
- a prefix and a suffix around one run of non-newline characters: `^a.*b$`, `^.{8,}$`.

A pattern RE2 rejects (a backreference, a lookahead, `a**`, a count above 1000) is an evaluation
error in CEL, so it is UNKNOWN for every row and denies under both polarities. Anything else (a
negated class, an anchor inside the pattern, a repetition over an unbounded group) raises
`UnsupportedPlanError`. A missing receiver, or one that is not a string, is UNKNOWN.

### Transports

`get_query` accepts the HTTP client's `PlanResourcesResponse` or the gRPC client's protobuf one.
They behave identically except for a constant zero divisor (`x / -0.0`): only gRPC keeps the sign of
the zero, so only gRPC translates it; over HTTP, whose JSON renders `-0.0` as `-0` and decodes it to
the integer `0`, it raises `UnsupportedPlanError`.

### Async

`get_query` does no I/O. Execute the `Select` however you already do, including `AsyncSession` or
`AsyncConnection`:

```python
async with AsyncSession(async_engine) as session:
    rows = (await session.execute(get_query(plan, Resource, attr_map))).scalars()
```

Plan with the SDK's async client, or run the sync one off the event loop.

## Correctness requirements

### Database collation requirements

CEL string and hierarchy comparisons are byte-exact, and CEL orders strings by code point. The
adapter does not trust the column's collation for that:

- **MySQL:** every comparison, membership test and `LIKE` over a `String` column or a string
  literal runs under `COLLATE utf8mb4_0900_bin`, which is byte-exact and NO PAD, so the default
  `utf8mb4_0900_ai_ci` (on the column or on the connection) cannot make `One` match `one`. String
  columns must use the `utf8mb4` character set, and a comparison over a column whose own collation
  differs cannot use that column's index.
- **PostgreSQL:** equality is exact under any deterministic collation and is left alone. Every
  ordering (`<`, `<=`, `>`, `>=`) over a string runs under `COLLATE "C"`, since a linguistic
  collation such as `en_US.UTF-8` sorts `"One"` after `"a"`
  ([#489](https://github.com/cerbos/query-plan-adapters/issues/489)). Avoid nondeterministic ICU
  collations and `citext`.

The conformance MySQL leg runs under the server default, and the PostgreSQL leg under ICU's
`en-US`, so the corpus proves both on every run.

On SQLite a column collation does not help: SQLite's `LIKE` ignores it and folds ASCII case unless
the connection sets `PRAGMA case_sensitive_like = ON`. Without the pragma, `contains`, `startsWith`,
`endsWith` and hierarchy-prefix predicates **over-grant** (`R.attr.name.startsWith("o")` matches
`One`). The adapter does not own the connection, so set the pragma on every one it opens, as the
conformance harness does:

```python
from sqlalchemy import create_engine, event

engine = create_engine("sqlite:///app.db")


@event.listens_for(engine, "connect")
def _case_sensitive_like(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA case_sensitive_like = ON")
    cursor.close()
```

For an async engine, listen on `async_engine.sync_engine`.

### Timestamps

Timestamp literals must be strict RFC 3339, within CEL's year 0001–9999 range, and exactly
representable at microsecond precision (discarded fractional digits must be zero); the mapped column
and database must preserve microseconds. A temporal attribute read outside `timestamp()` raises:
CEL sees the RFC 3339 string the application sent, which a stored timestamp no longer spells, and
has no overload comparing that string with a timestamp. Compare instants with `timestamp()` on both
operands.

`timestamp(R.attr.x) + duration("86400s")` (or `-`) compared with a timestamp literal is rewritten
to compare the column with the literal shifted the other way, guarded so a row whose sum leaves
CEL's range stays denied. Durations must be exact at microsecond precision. `timeSince()` raises:
the planner leaves it unevaluated, and the plan carries no instant to measure from.

## Supported operators

| Shape | Default | Otherwise |
| --- | --- | --- |
| Comparisons, logical operators, value-first and field-to-field forms, ternaries | translated | — |
| `contains`, `startsWith`, `endsWith` | escaped `LIKE` | — |
| `+`, `-`, `*`, `/` by a constant | translated | — |
| `/` by a column | refused, unless the numerator is that column or a constant zero (a zero divisor then gives NaN, which has no sign) | an override |
| `%` | refused: CEL defines it only over integers, and attribute numbers are doubles | an override |
| `string()` over a text or boolean column | translated (a boolean through a `CASE` that spells `'true'`/`'false'`) | — |
| `string()` over a numeric column | refused: CEL prints Go's shortest `%g` form (`1e+06`, `-0`) | an override matching your database |
| `int()`, `double()` | refused | an override matching your database |
| `size()` over a string column | `LENGTH` (`CHAR_LENGTH` on MySQL, whose `LENGTH` counts bytes) | — |
| `upperAscii()` | a `REPLACE` per ASCII letter (`UPPER` also folds non-ASCII letters on PostgreSQL and MySQL); compared with a string literal, only the literal's own letters, which keeps SQLite before 3.46 inside its parser stack | — |
| `m["key"]` over a map attribute | read as `m.key` when `attr_map` maps that path | — |
| `{"k": v, ...}[R.attr.x] == literal`, `!=` (a map literal read at an attribute key) | the keys whose value answers it, UNKNOWN for a key outside the map | — |
| `timestamp(x) ± duration(...)` against a timestamp literal | the literal shifted instead | — |
| `timeSince()` | refused: the plan carries no evaluation instant | — |
| `size()` over a JSON or PostgreSQL array column | refused until declared | `collection_columns` |
| `x[i] == literal`, `x[i] != literal` over a JSON or PostgreSQL array column | refused until declared | `collection_columns` |
| `literal in x`, `hasIntersection(x, [literals])` over a JSON or PostgreSQL array column | refused until declared | `collection_columns`, for an attribute `attr_map` does not map |
| `exists`, `all` over a literal list (a principal attribute) or a list built from attributes | folded | — |
| `in` over a list built from attributes, a ternary of lists, or a concatenation `A + B` | translated per element, branch or operand | — |
| `exists`, `all`, `exists_one`, `filter`, `map`, `in`, `hasIntersection`, `size()` over a related table | — | `relation_overrides()` over a `Relation` |
| `index` over any other storage | refused | an `index` override |
| `exists_one` over a literal list, `size(filter(...))` and `in` over `map(...)` of one | folded, UNKNOWN where any body raises | — |
| `matches()` whose pattern is a finite set of literals under anchors, every character from a set, or a prefix and suffix around `.*` | translated as exact string predicates | — |
| `matches()` with any other pattern | refused | an override, only if your engine matches RE2 |
| `==`, `!=` between a string, number or boolean column and a list or map literal | false (true for `!=`) where the column is present | — |
| Timestamps, hierarchies | translated | — |

Numeric `size()` and string operations keep CEL's type errors rather than allowing SQL coercion.
Constant NaN ordering is false and its negation true (Cerbos 0.55; under 0.54 it was an evaluation
error and stayed denied under negation). A bare boolean column is accepted as a whole condition.

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): every
recorded plan from Cerbos PDP 0.57.0 and 0.56.0 is served by a stub PDP and fetched through the SDK's
HTTP `CerbosClient`, so it arrives as your application receives it, then translated with one mapping,
executed on SQLite (through a `Connection` and through an `AsyncSession`), PostgreSQL and MySQL
(`utf8mb4_0900_bin`), and the returned ids are compared with the decisions the PDP recorded. The
collections are stored as JSON on every store; the cases that read a collection declared in
`collection_columns` run once more on PostgreSQL with them stored as arrays (`pgArray`). The results
are the same on every store. For the current PDP, 0.57.0, where the total is every golden case
recorded in that tier:

| Tier | Passed / total |
| --- | --- |
| core | 32 / 32 |
| extended | 91 / 110 |
| adversarial | 405 / 539 |

Every case that does not pass is either refused with `UnsupportedPlanError` (81 cases) or is
skipped because its golden file records a planner divergence, which no adapter can pass and the
harness does not compare. Under 0.57.0 those are three extended cases and four adversarial cases:
`arithmetic/add/int-literal-plus-constant` and
`arithmetic/add/int-literal-negated` (the planner drops the int type of the literal in
`R.attr.x + 1`, while `check()` has no double + int overload and denies every row, so write `1.0`),
three
`composition/*` cases whose DENY condition reads `aNumber`, which j2 lacks: the plan's `not(...)` of
it denies j2, while `check()` receives `aNumber` as absent and treats the erroring DENY as not
matching ([#530](https://github.com/cerbos/query-plan-adapters/issues/530)), and
`type-mismatch/in/number-field-in-scalar-principal` and `type-mismatch/in/string-field-in-dyn-string`
(the planner rewrites `in` over a scalar container to `==`, which `check()` has no overload for;
[#596](https://github.com/cerbos/query-plan-adapters/issues/596)). On 0.56.0, `null/has/missing-attribute` and `null/has/composed-with-comparison` are skipped too:
that planner folds `has()` on an omitted attribute to true, while `check()` denies the row; 0.57.0
keeps `has()` in the plan.
[`conformance-ledger.json`](conformance-ledger.json) lists each refused case with the mechanism that
rules it out.

### Refusals

A plan shape the adapter cannot express raises `cerbos_sqlalchemy.UnsupportedPlanError`, never a
best-effort filter. It subclasses `ValueError`, which these refusals raised before it existed; the
refusal for a relation-mapped attribute that no operator override consumes is also a `TypeError`, as
it was before. Invalid configuration (an unknown null representation, an unmapped attribute, a missing
`table_mapping`) keeps raising `ValueError`, `KeyError` or `TypeError`. An operator override that
cannot translate what it was handed should raise `UnsupportedPlanError` too.

## Mapping hazards

The conformance contract proves the *plan* side. The other half is the *mapping*: **the rows a
subquery reads must be the rows the application put into the resource attributes.** The shared
corpus catalogues six ways that breaks, and every adapter records a position on each.

**A `Relation` is only the predicates you give it.** A collection-valued attribute reaches its rows
through the correlation you write, so every hazard below but the last is yours. Which ones apply
depends on how you write it:

- Through a mapped **`relationship()`** (`Model.rel.any(...)`, `Model.rel.has(...)`,
  `select(...).join(Model.rel)`), SQLAlchemy applies the `primaryjoin` and, for a
  single-table-inheritance target, the
  [discriminator](https://docs.sqlalchemy.org/en/20/orm/queryguide/inheritance.html#single-inheritance-mappings).
- Through a **`Relation`**, or a hand-written correlated `select()` over columns (what the
  conformance harness does), none of that applies.

Check both against the SQLAlchemy version you run.

| Hazard | Position | Mechanism to check |
|---|---|---|
| Filtered association | **Caller-owned** | `relationship(primaryjoin=…)` and `relationship(secondaryjoin=…)`. `.any()`/`.has()` apply them; a hand-written `select()` over the target's columns does not, and must repeat the predicate in its own `where()` |
| Default scope on the target model | **Caller-owned** | A soft-delete column (`deleted_at IS NULL`), a tenant column, a `published` flag, or a `with_loader_criteria` on the session. SQLAlchemy applies none of them to a subquery you build yourself |
| Subtype discrimination | **Caller-owned** | `polymorphic_identity` on a single-table-inheritance subclass. `select(Subclass)` carries the discriminator; `select(literal(1)).where(subclass_table.c.x == …)` over the shared table does not, and sees sibling subtypes |
| To-one relation used as a collection | **Caller-owned** | A `relationship(uselist=False)` whose foreign key has no unique constraint. Nothing makes the database enforce the single row the application saw — add the constraint |
| Composite association key | **Caller-owned** | A multi-column foreign key. An override is arbitrary SQLAlchemy, so a composite key *is* expressible — and nothing stops you writing half of it. Conjoin every column pair |
| Absent to-one parent | **Reproduced by `Relation(hop_correlation=…)`**, and proved by the corpus (`relation/all/to-one-chain`, `relation/bare-attribute/negated-one-hop-boolean` and siblings) | `hop_correlation`, which `relation_overrides()` passes to `cerbos_sqlalchemy.require_hops` — see below. Set it on every `Relation` that reaches a COLLECTION through an intermediate to-one hop. A SCALAR read through a to-one hop needs nothing: map it to a correlated scalar subquery and an absent hop is SQL NULL, excluded under both polarities. `attr_map` accepts any column expression for this, and it needs no `table_mapping` ([#375](https://github.com/cerbos/query-plan-adapters/issues/375)) |

### `require_hops`: the one hazard with a library helper

Every intermediate segment of `a.b.c` is a to-one parent (CEL cannot dot through a list). When it is
absent, the application sends no attribute and CEL denies — but a subquery rooted at the resource
row can't tell an absent parent from a childless one, so `all` reads TRUE, `!exists` reads TRUE and
the count reads 0, each admitting denied rows ([#309](https://github.com/cerbos/query-plan-adapters/issues/309)).

```python
# `mainCategory.subCategories`: the collection is reached THROUGH the category hop.
attr_map["request.resource.attr.mainCategory.subCategories"] = Relation(
    [SubCategory.category_id == Category.id, Category.resource_id == Resource.id],
    correlate=[Resource],
    hop_correlation=[Category.resource_id == Resource.id],
)
```

`relation_overrides()` routes every answer over a `Relation` through
`require_hops(expression, hop_correlation, correlate=())`, which wraps it in a `CASE` with **no
`ELSE`**: a missing hop yields NULL, which stays excluded under both polarities. With an empty
`hop_correlation` (a direct relation) it returns the expression unchanged, so `!tags.exists(...)`
over zero tags is still TRUE.

An override of your own over a chain must route **every** operator through it, not only the
macros: a bare `EXISTS` is FALSE for an absent parent and its negation TRUE, which is how
membership, `hasIntersection` ([#315](https://github.com/cerbos/query-plan-adapters/issues/315)) and
`!(size(chain) > 0)` ([#316](https://github.com/cerbos/query-plan-adapters/issues/316)) over-granted.
It must also keep a NULL body NULL, as `relation_overrides()` does; nothing checks either.

## Behaviour changes

- Added ([#527](https://github.com/cerbos/query-plan-adapters/issues/527)): `Relation` and
  `relation_overrides()`, the conformance harness's three-valued relation macros, now shipped. This
  README used to show a hand-written `exists` override that put the body in `.where(body)`, which
  reads a NULL body as false, so `!tags.exists(...)` selected rows the PDP denies. Replace overrides
  copied from it with `relation_overrides()`. A widening.
- Fixed: a string, number or temporal column read where CEL needs a boolean (`not R.attr.aString`, `R.attr.aString && R.attr.aBool`) renders as `NULL`, CEL's type error, which `AND` and `OR` absorb as `&&` and `||` absorb it. PostgreSQL rejected the query, and SQLite and MySQL over-granted by reading the text as a number.
- **Breaking:** `null_attribute_representation` defaults to `"omitted"`, where it defaulted to
  `"explicit"`. A caller that sends explicit nulls and set nothing now gets `x == null` matching no
  row where it rendered `IS NULL`, and a refusal for the other null operands; pass
  `null_attribute_representation="explicit"` to keep the old output
  ([#499](https://github.com/cerbos/query-plan-adapters/issues/499),
  [ADR 0011](../docs/adr/0011-denial-fails-closed-in-the-result.md)).
- [#500](https://github.com/cerbos/query-plan-adapters/issues/500), found by running the corpus on
  PostgreSQL and MySQL:
  - `+`, `-` and `*` read an integer or `Numeric` column as a double, as CEL reads every attribute
    number. PostgreSQL used to multiply in exact `numeric`, so `R.attr.aNumber * 0.1 == 0.3` was true
    for `3` (CEL: `0.30000000000000004`) — an over-grant.
  - `size()` of a string counts characters on MySQL (`CHAR_LENGTH`), where `LENGTH` counted bytes.
  - An ordering comparison between mismatched types inside a ternary no longer fails on PostgreSQL
    with `CASE types text and boolean cannot be matched`.
  - **Widening:** `collection_columns` storage `"json"` renders on MySQL 8.0.17+, where it used to
    raise `CompileError`.
- [#545](https://github.com/cerbos/query-plan-adapters/issues/545): an ordering between a boolean
  column and a boolean literal (`R.attr.aBool < true`) now translates, binding the literal as a
  typed parameter, where SQLAlchemy used to raise `ArgumentError` at translation. CEL orders bools
  `false < true`, as SQL's boolean does. A widening.
- **Breaking:** shapes that used to return a wrong filter now raise `UnsupportedPlanError`:
  - `string()` over a numeric column. CEL prints an attribute double in Go's shortest `%g` form
    (`1e+06`, `2`, `-0`), where `CAST` prints `1000000` or `2.0`, and SQL cannot keep the sign of a
    zero, so `string(R.attr.aDouble) == "1e+06"` under-granted.
  - `/` by a column, unless the numerator is that column or a constant zero. A zero divisor makes
    CEL's quotient an infinity carrying the zero's sign, which SQL cannot read (`-0.0 = 0.0`, and
    SQLite stores `-0.0` as `0.0`); the column used to be assumed `+0.0`, which over-granted.
  - `%` over a column. CEL defines `%` only over integers and attribute numbers are doubles, so it is
    a no-such-overload error; SQL's remainder made `!(R.attr.aNumber % 2 == 1)` true for most rows.
  - A scalar column compared with a list or map literal (`R.attr.aString == ["same"]`), which used to
    bind the list as a parameter and fail, or be coerced, at execution.
- Translation refusals now raise `cerbos_sqlalchemy.UnsupportedPlanError`, a `ValueError` subclass
  (also a `TypeError` where the refusal raised one before), so existing handlers keep catching them.
  Not breaking.
- **Breaking** ([#509](https://github.com/cerbos/query-plan-adapters/issues/509)): a collection macro
  nested over the collection an enclosing macro iterates, whose body reads the enclosing element
  (`tags.exists(t, tags.exists(u, u.name != t.name))`), now raises. `attr_map` binds a lambda
  variable's fields by name and an override never sees the scope, so both subqueries range over the
  one unaliased table and the body would compare each element with itself — under negation, an
  over-grant — for any caller that mapped the inner variable.
- A literal list member the column's type cannot equal is dropped from `in` (`R.attr.aNumber in
  ["5", 2]` compares `2` alone), as `==` already answered it, where it used to be bound into
  `IN (...)` and converted by the store (`'5' = 5` is TRUE on SQLite) — an over-grant, since CEL's
  `5 in ["5"]` is false. A list left with no comparable member is false for a present value.
- **Widening:** literal membership (`in`, `hasIntersection`) over a declared collection that
  `attr_map` does not map now translates, where it used to raise `Attribute does not exist in the
  attribute column map`. An attribute `attr_map` maps resolves membership exactly as before.
- **Breaking** ([#227](https://github.com/cerbos/query-plan-adapters/issues/227)): `size()` over a JSON
  or array column not declared in `collection_columns` now raises; it used to return `LENGTH()` of the
  column's text. `index` over undeclared storage raises a message naming the missing declaration
  instead of `Unrecognised operator: index`.
- [#418](https://github.com/cerbos/query-plan-adapters/issues/418): `string()` over a boolean column now
  translates (to a `CASE` spelling `'true'`/`'false'`, with NULL kept UNKNOWN) where it used to raise.
  `CAST` would render `'1'` on SQLite and MySQL.
- **Breaking** ([#414](https://github.com/cerbos/query-plan-adapters/issues/414)): numeric `size()` and
  string operations preserve CEL type errors instead of SQL coercion; NaN ordering is false and its
  negation true (Cerbos 0.55); membership preserves the declared NULL convention inside lambda bodies
  and against stored collections; bare temporal-attribute comparisons raise (use `timestamp()`).
- [#388](https://github.com/cerbos/query-plan-adapters/issues/388): a bare boolean column is accepted as
  the whole condition (e.g. `R.attr.aBool`), where it used to be refused at the root. A widening.
- A numeric literal outside the int64 range (`R.attr.aDouble < -1e19`) is bound as a float; SQLite
  used to raise `OverflowError` at execution. PostgreSQL is unchanged. A widening.
- [#387](https://github.com/cerbos/query-plan-adapters/issues/387):
  - `hasIntersection` is order-insensitive like `eq`/`ne`/`in`, so a value-first spelling reaches an
    override with the column first. A widening.
  - **Breaking:** an override's intermediate value reaching a default handler raises; it used to
    compare to a bare `False` and filter out every row.
  - **Breaking:** the non-boolean check that refuses `filter()`/`map()` at the root also runs on every
    `and`/`or`/`not` operand, raising the adapter's message instead of SQLAlchemy's WHERE-role error.

## Example application

[`example/`](example/) installs the built wheel and runs the shared
[demo domain](../demo/README.md) against a live PDP, including pagination and the adapter's `Select`
composed with an application-owned `.where()` across all three plan kinds:

```bash
# from the repository root
demo/scripts/run-example.sh sqlalchemy
```

## Development

| Suite | Checks | Needs |
| --- | --- | --- |
| `tests/test_translator.py` | caller options over recorded corpus plans: null representation, overrides, collection storage, transports, model styles | nothing — plans from `conformance/golden/` |
| `tests/test_query.py`, `tests/test_relations.py` | plans the planner cannot produce; options no policy can reach | nothing |
| `tests/test_adversarial_conformance.py` | returned rows match the recorded decisions, or the ledger's refusal is raised: every case on SQLite (sync and async), PostgreSQL and MySQL, and the cases that read a `collection_columns` declaration once more on PostgreSQL with the collections stored as native arrays. CI runs it under SQLAlchemy 1.4 and 2.x | SQLite, and Docker for PostgreSQL and MySQL, pinned in [`POSTGRES_IMAGE`](POSTGRES_IMAGE) and [`MYSQL_IMAGE`](MYSQL_IMAGE) |

```bash
pdm install -G :all
pdm run test            # all three
pdm run format          # isort + ruff format
pdm run lint            # ruff check --fix
```

Without PDM installed, run the same commands through the [pyprojectx](https://pyprojectx.github.io/)
wrapper, `./pw` (`pw.bat` on Windows), which installs PDM, ruff and isort into `.pyprojectx/` on
first use: `./pw install`, `./pw test`, `./pw format`, `./pw lint`, or `./pw pdm <command>`. CI runs
`format` and `lint` on the SQLAlchemy 2.x leg and fails if they leave a diff.

CI links Ubuntu 24.04's SQLite (3.45), usually older than the one a developer's Python bundles, and
before 3.46 a deeply nested expression overflows SQLite's parser stack. `scripts/test-ci-sqlite.sh`
runs every suite that needs no Docker store against that SQLite, from `pdm.lock`, in a container.
