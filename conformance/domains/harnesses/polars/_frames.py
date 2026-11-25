"""Builds a domain's tables into Polars frames and nests related rows into struct and list columns."""

from datetime import datetime
from decimal import Decimal

import polars as pl

_DTYPES = {
    "string": pl.String,
    "int": pl.Int64,
    "bigint": pl.Int64,
    "float": pl.Float64,
    "decimal": pl.Decimal(18, 2),
    "bool": pl.Boolean,
    "timestamp": pl.Datetime("ns", "UTC"),
    "string[]": pl.List(pl.String),
}


def _value(value, kind):
    if value is None:
        return None
    if kind == "timestamp":
        return datetime.fromisoformat(value)
    if kind == "decimal":
        return Decimal(str(value))
    return value


def tables(spec: dict) -> dict[str, pl.LazyFrame]:
    """One lazy frame per table in ``generated/rows.json``. A JSON column becomes a struct."""
    out = {}
    for table in spec["tables"]:
        columns = {}
        for column in table["columns"]:
            name, kind = column["name"], column["type"]
            values = [_value(row.get(name), kind) for row in table["rows"]]
            if kind == "json":
                columns[name] = pl.Series(name, values)
            else:
                columns[name] = pl.Series(name, values, dtype=_DTYPES[kind])
        out[table["name"]] = pl.DataFrame(columns).lazy()
    return out


def _expr(value) -> pl.Expr:
    return pl.col(value) if isinstance(value, str) else value


def struct(**fields) -> pl.Expr:
    return pl.struct(**{name: _expr(value) for name, value in fields.items()})


def one(
    frame: pl.LazyFrame, fk: str, target: pl.LazyFrame, key: str, name: str, **fields
) -> pl.LazyFrame:
    """A to-one relation as a struct column; a null or dangling key leaves the struct null."""
    nested = target.select(pl.col(key).alias("__key"), struct(**fields).alias(name))
    return frame.join(
        nested,
        left_on=fk,
        right_on="__key",
        how="left",
        coalesce=True,
        maintain_order="left",
    )


def many(
    frame: pl.LazyFrame,
    pk: str,
    child: pl.LazyFrame,
    fk: str,
    name: str,
    order_by: str,
    value: str | pl.Expr | None = None,
    **fields,
) -> pl.LazyFrame:
    """A to-many relation as a list column, ordered as the application reads it; no rows is []."""
    element = _expr(value) if value is not None else struct(**fields)
    grouped = (
        child.sort(order_by).group_by(fk, maintain_order=True).agg(element.alias(name))
    )
    dtype = grouped.collect_schema()[name]
    joined = frame.join(
        grouped,
        left_on=pk,
        right_on=fk,
        how="left",
        coalesce=True,
        maintain_order="left",
    )
    return joined.with_columns(
        pl.when(pl.col(name).is_null())
        .then(pl.lit([], dtype=dtype))
        .otherwise(pl.col(name))
        .alias(name)
    )
