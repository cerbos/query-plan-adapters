"""Mapping helpers shared by the domain modules."""

from cerbos_duckdb import Entry, Hop, Join, ListOf, NullConvention, ValueType

STR, NUM, BOOL, TS = (
    ValueType.STRING,
    ValueType.NUMBER,
    ValueType.BOOL,
    ValueType.TIMESTAMP,
)


def path(*steps: tuple[str, str, str]) -> Join:
    """A to-one chain from the resource row: each step is (table, column on the previous row,
    matching column on table)."""
    last = len(steps) - 1
    hops = tuple(
        Hop(steps[i][0], child_column=steps[i + 1][2], join_column=steps[i + 1][1])
        for i in range(last - 1, -1, -1)
    )
    return Join(
        steps[last][0], source_column=steps[0][1], target_column=steps[0][2], via=hops
    )


class Attrs:
    """Builds a domain's mapping, stamping its null convention on every scalar entry."""

    def __init__(self, nulls: NullConvention) -> None:
        self.m: dict[str, Entry] = {}
        self.nulls = nulls

    def col(self, name: str, column: str, vt: ValueType) -> "Attrs":
        self.m["request.resource." + name] = Entry(column, vt, self.nulls)
        return self

    def via(self, name: str, join: Join, column: str, vt: ValueType) -> "Attrs":
        self.m["request.resource.attr." + name] = Entry(
            column, vt, self.nulls, join=join
        )
        return self

    def values(
        self, name: str, column: str, vt: ValueType, join: Join | None = None
    ) -> "Attrs":
        self.m["request.resource.attr." + name] = Entry(
            column, join=join, list=ListOf(element=Entry(value_type=vt))
        )
        return self

    def objects(
        self, name: str, column: str, fields: dict[str, Entry], join: Join | None = None
    ) -> "Attrs":
        self.m["request.resource.attr." + name] = Entry(
            column, join=join, list=ListOf(fields=fields)
        )
        return self


def f(column: str | tuple[str, ...], vt: ValueType) -> Entry:
    return Entry(column, vt)
