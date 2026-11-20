# Changelog

## Unreleased

### Fixed over-grants

- A negated macro over an `all` whose body nests another macro (`!categories.exists(c, c.subCategories.all(s, s.labels.exists(l, l.name == "gold")))`, `!categories.all(c, c.subCategories.exists(...))`) read an element whose nested macro errors (a label missing `name`) as false and allowed the row. The PDP denies it, and so does the adapter now.

## v5.2.1

### Fixed over-grants

- MySQL with `HIGH_NOT_PRECEDENCE` in `sql_mode`: Prisma renders a negated single condition as `NOT x < ?`, which that mode parses as `(NOT x) < ?`, and does the same inside `every` and `isNot`. The adapter now negates a single condition `x` as `AND: [x, x]`, which Prisma renders in parentheses (#758).
- `all` over a boolean list with a bare element in condition position (`flags.all(b, b)`) treated a null element as false. It is a CEL error, so the row is now denied (#759).

### Fixed under-grants

- A negated membership test over a `filter` that reads an outer attribute (`!("public" in tags.filter(t, t.startsWith("p") && R.attr.flag))`) denied rows the PDP allows (#759).

### Other changes

- An operator Cerbos plans do not contain is refused with `UnsupportedQueryPlanError` before any rewrite runs, and so is a collection macro whose second operand is not a lambda (#756).
- The package no longer ships the test suites or the conformance harness (#751).
