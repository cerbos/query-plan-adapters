# Changelog

## Unreleased

### Fixed

- A principal attribute the planner leaves unresolved (`request.principal.attr.context` compared with a field in a ternary branch) throws `UnsupportedQueryPlanError`. It was rendered as a field reference that Prisma rejected.

### Fixed over-grants

- A negated macro over an `all` whose body nests another macro (`!categories.exists(c, c.subCategories.all(s, s.labels.exists(l, l.name == "gold")))`, `!categories.all(c, c.subCategories.exists(...))`) read an element whose nested macro errors (a label missing `name`) as false and allowed the row. The PDP denies it, and so does the adapter now.
- `!=` against arithmetic over a to-one relation's field (`parent.aNumber - 0.5 != 2.5`) negated the relation filter as a whole, so it allowed every row with no parent. The negation now applies inside the relation, and a row with no parent is denied as the PDP denies it.

### Breaking

- New mapper option `int32`, read from the schema when `datamodel` is passed: it declares a column a Prisma `Int`. A numeric constant outside the 32-bit range against such a column (`R.attr.count < 3000000000`, or `R.attr.count - 45.0 <= 2147483647.0` solved to `count <= 2147483692`) is refused with `UnsupportedQueryPlanError`. PostgreSQL rejected the bound parameter and failed the query; SQLite and MySQL answered it, and with `datamodel` passed they now refuse it too.

## v5.2.1

### Fixed over-grants

- MySQL with `HIGH_NOT_PRECEDENCE` in `sql_mode`: Prisma renders a negated single condition as `NOT x < ?`, which that mode parses as `(NOT x) < ?`, and does the same inside `every` and `isNot`. The adapter now negates a single condition `x` as `AND: [x, x]`, which Prisma renders in parentheses (#758).
- `all` over a boolean list with a bare element in condition position (`flags.all(b, b)`) treated a null element as false. It is a CEL error, so the row is now denied (#759).

### Fixed under-grants

- A negated membership test over a `filter` that reads an outer attribute (`!("public" in tags.filter(t, t.startsWith("p") && R.attr.flag))`) denied rows the PDP allows (#759).

### Other changes

- An operator Cerbos plans do not contain is refused with `UnsupportedQueryPlanError` before any rewrite runs, and so is a collection macro whose second operand is not a lambda (#756).
- The package no longer ships the test suites or the conformance harness (#751).
