# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

"""Type-checks `get_filter`'s signature. Run by pyright, not pytest.

`reportUnnecessaryTypeIgnoreComment` is on, so the negative case fails if it stops
erroring.
"""

from typing import assert_type

import polars as pl
from cerbos.sdk.model import PlanResourcesResponse

from cerbos_polars import get_filter


def check(plan: PlanResourcesResponse, frame: pl.LazyFrame) -> None:
    assert_type(get_filter(plan, frame), pl.Expr)
    assert_type(
        get_filter(plan, frame.collect_schema(), {"request.resource.attr.a": "a"}),
        pl.Expr,
    )
    assert_type(
        get_filter(
            plan,
            frame,
            {"request.resource.attr.a": pl.col("a")},
            attribute_null_representation={"request.resource.attr.a": "explicit"},
        ),
        pl.Expr,
    )
    get_filter(plan, frame, null_attribute_representation="missing")  # type: ignore[arg-type]
