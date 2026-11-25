# Cerbos + Polars Adapter

Converts a [Cerbos](https://cerbos.dev) query plan ([PlanResources API](https://docs.cerbos.dev/cerbos/latest/api/index.html#resources-query-plan))
into a [Polars](https://pola.rs/) expression, for filtering a `DataFrame` or `LazyFrame` with the
[Cerbos Python SDK](https://github.com/cerbos/cerbos-sdk-python).

## Install

```bash
pip install cerbos-polars
```

- Python >= 3.11
- Polars >= 2.2 (the first release whose `list.eval` reads the row's other columns)
- Cerbos Python SDK (`cerbos`) >= 0.16.1, with either client: the HTTP `CerbosClient` or the gRPC one

## Quick start

```python
import polars as pl
from cerbos.sdk.client import CerbosClient
from cerbos.sdk.model import Principal, ResourceDesc

from cerbos_polars import get_filter

documents = pl.scan_parquet("documents.parquet")

with CerbosClient(host="http://localhost:3592") as c:
    principal = Principal("john", roles={"employee"}, attr={"department": "marketing"})
    plan = c.plan_resources("view", principal, ResourceDesc("document"))

# ALWAYS_ALLOWED -> pl.lit(True)
# ALWAYS_DENIED  -> pl.lit(False)
# CONDITIONAL    -> the translated condition
allowed = documents.filter(get_filter(plan, documents))

# Compose with your own predicates; a denied plan stays denied.
page = allowed.filter(pl.col("priority") < 5).head(20).collect()
```

`get_filter(plan, schema, attr_map=None, *, null_attribute_representation="omitted",
attribute_null_representation=None)` returns one `pl.Expr`. `schema` is the frame you will filter
(a `LazyFrame` is read with `collect_schema()`, which runs nothing) or its `pl.Schema`: the
translation reads column types from it, because CEL's operators depend on them (`size()` counts a
string's code points and a list's elements). The expression is built from native Polars operations
only, so it runs on the in-memory and streaming engines and takes part in the lazy optimiser like
any predicate you write yourself. It is never null: a row CEL would deny with an evaluation error is
`false`. Any shape the adapter cannot translate exactly raises `UnsupportedPlanError` instead of
returning a broader filter.

To see what it built, print it: `print(get_filter(plan, documents))`, or
`documents.filter(expr).explain()`.

## Mapping attributes

With no `attr_map`, `request.resource.attr.x` is the column `x`, `request.resource.attr.a.b` is the
field `b` of the struct column `a`, and `request.resource.id` is the column `id`. A list attribute
is a `List` column, a map attribute a `Struct` column, and a lambda variable takes the list's
element, so `R.attr.tags.exists(t, t.name == "x")` reads the field `name` of each struct in `tags`.

`attr_map` maps an attribute reference to a column name or to any expression over the frame, and
wins over the default. A dotted attribute under a mapped prefix reads struct fields of that
expression:

```python
get_filter(
    plan,
    documents,
    {
        "request.resource.attr.owner": "owner_id",
        "request.resource.attr.tagNames": pl.col("tags").list.eval(pl.element().struct.field("name")),
        "request.resource.attr.department": pl.col("org").struct.field("department"),
    },
)
```

An attribute that resolves to no column raises `KeyError`. A mapped expression must give one value
per row: an aggregation such as `pl.col("x").max()` is broadcast to every row, which is a different
attribute from the one your application sends.

**The contract the mapping relies on:** the value your application sends to `check()` for
`R.attr.x` is the value of the column or expression mapped to `x`. Numbers are CEL doubles whatever
the column's numeric type, as they are when your application sends them as JSON. `Categorical` and
`Enum` columns are read as their strings, so they order lexically as CEL does, not by category, and
an `Array` column is read as a list. A `Datetime`
column stands for an RFC 3339 string attribute and is read only through `timestamp()`.

### NULL attribute representation

The planner emits the same `x == null` however your application represents a null column in the
attributes it sends, so tell the adapter which convention it uses.

| attributes you send for a null value | `check()` on that row | what the adapter does |
| --- | --- | --- |
| `{}`, the attribute omitted | CEL errors and denies, under both polarities | `"omitted"`, the default: null is an error |
| `{"x": None}`, an explicit null | `x == null` is true, `x != "a"` is true | `"explicit"`: null is CEL's null value |

The default is the convention that fails closed when the guess is wrong. Declare it per attribute
with `attribute_null_representation`, or for every attribute with `null_attribute_representation`:

```python
get_filter(
    plan,
    documents,
    attribute_null_representation={"request.resource.attr.owner": "explicit"},
)
```

A struct field follows its own declaration, `"omitted"` unless you name it: a field read through a
null struct is an error under either. A null element of a list column is always CEL's null value,
as it is in a JSON array. One comparison may mix the two conventions.

## How a condition becomes an expression

A Polars null is CEL's evaluation error. Comparisons propagate it, `&` and `|` are Kleene, as CEL's
`&&` and `||` are over errors, and `filter` drops a null row, as Cerbos denies a row whose condition
errors. Where CEL also has a null value (an attribute declared `"explicit"`, a null list element),
the adapter tracks which rows are errors beside the value and compares with `eq_missing`, so the two
kinds of null never meet in one column.

- Macros (`exists`, `all`, `exists_one`, `filter`, `map`) are `list.eval` over the list column, with
  Kleene `any`/`all` so an erroring element is decided the way CEL decides it.
- Membership in a list column, `hasIntersection`, `isSubset`, `hasAny`, `hasAll`, `hasOnly`,
  `hasExactly`, `except` and `intersect` are `list.eval` with CEL's equality: `-0.0 == 0.0`, NaN
  equal to nothing, `1 == 1.0`, `null == null`.
- `matches()` runs on Polars' Rust regex engine, after the pattern is re-read by Go's RE2 rules and
  rewritten for it; see [Regular expressions](#regular-expressions).
- `int()` arithmetic is checked in `Int128` and errors outside int64, as CEL does; int division
  truncates toward zero.
- `timestamp()` and `duration()` are nanosecond `Datetime` and `Duration`, checked against CEL's
  year 0001 to 9999 range.

### Regular expressions

CEL matches with Go's `regexp` and Polars with Rust's `regex` crate. They share RE2's matching
model, not its syntax: `\d`, `\w`, `\s` and `\b` are Unicode there and ASCII in Go, `[a&&b]` and
`[[a]]` are set operations there and literals in Go, `\<` is a word boundary there and a literal in
Go, and octal escapes and repeat counts over 1000 are valid in only one of them. The adapter never
hands Polars the pattern as written. It parses it by Go's rules and re-emits every character as a
code point, every class as explicit ranges, every flag applied.

A pattern Go rejects (a backreference, a lookahead, `a**`, a count over 1000, nested counts making
more than 1000 copies) is an evaluation error in CEL, so it denies every row under both polarities.
A Unicode class (`\p{Greek}`) or a case-insensitive non-ASCII letter is refused: each engine carries
its own Unicode tables. A pattern that is not a literal is refused.

## Correctness requirements

- **Polars >= 2.2.** Earlier versions reject a column read inside `list.eval`, so a macro whose body
  reads an attribute fails when the frame is collected, rather than filtering wrongly.
- **Timestamps.** `timestamp()` over a column needs a nanosecond `Datetime` column; a naive one is
  read as UTC. A `Datetime` column read outside `timestamp()` raises: CEL sees the string your
  application sent, which the column no longer spells. `timeSince()` is measured from the moment
  `get_filter` runs.
- **Struct columns.** A struct holds every field on every row. A field your application leaves out
  of the attributes must be null in the struct.

## Supported operators

| Shape | Translated as |
| --- | --- |
| Comparisons, logical operators, ternaries, value-first and field-to-field forms | native comparisons; NaN orders false |
| `contains`, `startsWith`, `endsWith` | `str.contains(literal=True)`, `str.starts_with`, `str.ends_with` |
| `matches()` with a literal pattern | `str.contains` with the pattern rewritten for Rust's engine |
| `size()` of a string or list | `str.len_chars()`, `list.len()` |
| `upperAscii()` | `str.replace_many` over the 26 ASCII letters |
| `+`, `-`, `*`, `/`, `%` | Float64 for doubles; checked Int128 for ints |
| `int()` of a double or string, `double()` of an int, `string()` of a bool, int or string | casts guarded as CEL guards them |
| `in`, `exists`, `all`, `exists_one`, `filter`, `map` over a list column or literal | `list.eval` |
| `hasIntersection`, `isSubset`, `hasAny`, `hasAll`, `hasOnly`, `hasExactly`, `except`, `intersect` | `list.eval` |
| `==` between a list and a list literal | length and each position |
| `x[i]`, `m["key"]`, `{"k": v}[x]`, `has(x.f)` | `list.get`, `struct.field`, a `when` chain, `is_not_null` |
| `timestamp()`, `duration()`, `timeSince()`, `± duration` | nanosecond `Datetime` and `Duration` |
| `hierarchy()`: `ancestorOf`, `descendentOf`, `overlaps` | prefix tests on the delimited string |
| A lambda body reading the element of an enclosing lambda | refused: an inner `list.eval` cannot reach an outer element |
| `string()` of a double, `double()` of a string, `timestamp()` of a string column | refused: Go's formatting and parsing differ from Polars' casts |
| Division by a literal zero | refused: the HTTP SDK reads `-0.0` as the integer `0`, losing the infinity's sign |
| Two-variable comprehensions, macros over a map's keys, `matches()` with a computed pattern | refused |

## Conformance contract

The adapter is replayed against the shared [conformance corpus](../conformance/README.md): every
recorded plan from Cerbos PDP 0.58.0 and 0.57.0 is served by a stub PDP and fetched through the
SDK's HTTP and gRPC clients, translated with one mapping, and used to filter one frame built from
the dataset, eagerly and on the lazy streaming engine. The returned ids are compared with the
decisions the PDP recorded. For the current PDP, 0.58.0, where the total is every golden case
recorded in that tier:

| Tier | Passed / total |
| --- | --- |
| core | 32 / 32 |
| extended | 107 / 114 |
| adversarial | 548 / 617 |

Every case that does not pass is either refused with `UnsupportedPlanError` or skipped because its
golden file records a planner divergence, which no adapter can pass.
[`conformance-ledger.json`](conformance-ledger.json) lists each refused case with the mechanism that
rules it out.

### Refusals

A plan shape the adapter cannot express raises `cerbos_polars.UnsupportedPlanError`, never a
best-effort filter. It subclasses `ValueError`. Invalid configuration (an unknown null
representation, an attribute that resolves to no column) raises `ValueError`, `KeyError` or
`TypeError`.

## Mapping hazards

The conformance contract proves the *plan* side. The other half is the *mapping*: the values in the
frame must be the values your application put into the resource attributes.

| Hazard | Position |
|---|---|
| Filtered association | **Caller-owned.** A list column holds whatever rows you put in it. Filter it before the policy sees it, or map the attribute to an expression that filters it (`pl.col("tags").list.filter(...)`) |
| Default scope on the target | **Caller-owned**, as above |
| Subtype discrimination | **Caller-owned**, as above |
| To-one relation used as a collection | **Not applicable.** A to-one relation is a struct column, one value per row |
| Composite association key | **Not applicable.** The frame holds the related values already joined |
| Absent to-one parent | **Handled.** A null struct is a missing attribute: every field read through it is an error, under both polarities |

## Development

| Suite | Checks | Needs |
| --- | --- | --- |
| `tests/test_conformance.py` | returned rows match the recorded decisions, or the ledger's refusal is raised: every case through the HTTP client on an eager frame and on the lazy streaming engine, and through the gRPC client on an eager frame; plus the determinism and unknown-operator fitness checks | nothing: the frame is built in memory |
| `tests/test_get_filter.py` | caller arguments the corpus cannot vary: schema forms, `attr_map` values, null representations, column dtypes, unknown plan kinds | nothing |
| `tests/typing/check_types.py` | `get_filter`'s signature, through `npx pyright@1.1.411` | Node |

```bash
pdm install -G :all
pdm run test            # the harness
pdm run format          # isort + ruff format
pdm run lint            # ruff check --fix
```

Without PDM installed, run the same commands through the [pyprojectx](https://pyprojectx.github.io/)
wrapper, `./pw` (`pw.bat` on Windows): `./pw install`, `./pw test`, `./pw format`, `./pw lint`.
`conformance/scripts/run-harness.sh polars` runs the harness the way CI does.
