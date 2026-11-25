# Copyright 2021-2026 Zenauth Ltd.
# SPDX-License-Identifier: Apache-2.0

import importlib.metadata

from cerbos_polars.errors import UnsupportedPlanError
from cerbos_polars.query import get_filter

__version__ = importlib.metadata.version(__package__ or __name__)

__all__ = ["UnsupportedPlanError", "get_filter"]
