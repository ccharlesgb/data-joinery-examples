"""Focused check for a calendar range crossing leap day."""

from datetime import date
from decimal import Decimal

import polars as pl
import pytest
from data_joinery import Schema
from data_joinery.schemas import SchemaCoercionError
from polars.testing import assert_frame_equal
from tpch_polars.schemas import DimDate, FactSales
from tpch_polars.transforms.models import build_dim_date


def test_date_dimension_includes_leap_day():
    fact = Schema(FactSales).create_dataframe(
        [
            FactSales(
                1,
                1,
                1,
                1,
                1,
                date(2024, 2, 28),
                date(2024, 3, 1),
                date(2024, 3, 1),
                date(2024, 3, 1),
                "O",
                "1-URGENT",
                Decimal("1.00"),
                Decimal("1.00"),
                Decimal("1.00"),
                Decimal("0.00"),
                Decimal("0.00"),
                Decimal("1.0000"),
                Decimal("1.000000"),
                False,
                "N",
                "O",
            )
        ],
        pl.DataFrame,
    )
    expected = Schema(DimDate).create_dataframe(
        [
            DimDate(date(2024, 2, 28), 2024, 1, 2, "February", False),
            DimDate(date(2024, 2, 29), 2024, 1, 2, "February", False),
            DimDate(date(2024, 3, 1), 2024, 1, 3, "March", False),
        ],
        pl.DataFrame,
    )
    assert_frame_equal(build_dim_date(fact.lazy()).collect(), expected)


def test_date_dimension_checks_lazy_input_schema():
    incomplete_fact = pl.LazyFrame({"order_date": [date(2024, 2, 28)]})

    with pytest.raises(SchemaCoercionError, match="Missing field"):
        build_dim_date(incomplete_fact)
