"""Column types shared by the domain models."""

from sqlalchemy import JSON, BigInteger, Boolean, DateTime, Float, Integer, Numeric, String
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.compiler import compiles

STR = String(255)
INT = Integer
BIGINT = BigInteger
BOOL = Boolean
FLOAT = Float(precision=53)
DECIMAL = Numeric(18, 2)
JSONMAP = JSON(none_as_null=True).with_variant(JSONB(none_as_null=True), "postgresql")
STRLIST = JSON(none_as_null=True).with_variant(ARRAY(String), "postgresql")


class _Timestamp(DateTime):
    pass


@compiles(_Timestamp, "mysql")
def _mysql_timestamp(type_, compiler, **kw):
    return "DATETIME(6)"


TS = _Timestamp(timezone=True)


def scalar(root, column, *joins):
    """A to-one chain read as a correlated scalar subquery."""
    from sqlalchemy import select

    return select(column).where(*joins).correlate(root).scalar_subquery()


KEY = String(255).with_variant(mysql.VARCHAR(255, collation="utf8mb4_0900_bin"), "mysql")
