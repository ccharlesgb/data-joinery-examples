"""Focused calendar behavior that is easy to miss in a small mart fixture."""

from datetime import date
from decimal import Decimal

from data_joinery import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual
from tpch_spark.schemas import DimDate, FactSales
from tpch_spark.transforms.models import build_dim_date


def test_date_dimension_includes_leap_day_between_fact_dates():
    spark = (
        SparkSession.builder.master("local[1]").appName("tpch-date-test").getOrCreate()
    )
    try:
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
            DataFrame,
            session=spark,
        )
        expected = Schema(DimDate).create_dataframe(
            [
                DimDate(date(2024, 2, 28), 2024, 1, 2, "February", False),
                DimDate(date(2024, 2, 29), 2024, 1, 2, "February", False),
                DimDate(date(2024, 3, 1), 2024, 1, 3, "March", False),
            ],
            DataFrame,
            session=spark,
        )
        assertDataFrameEqual(build_dim_date(fact), expected)
    finally:
        spark.stop()
