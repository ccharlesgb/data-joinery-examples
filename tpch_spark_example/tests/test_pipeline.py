from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from data_joinery import Schema
from data_joinery.cli import app
from data_joinery.pipeline import PipelineExecutionError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual
from tpch_spark.context import DataDir, OutputDir, PipelineContext
from tpch_spark.pipeline import build_pipeline
from tpch_spark.schemas import (
    Customer,
    DimCustomer,
    DimDate,
    DimPart,
    DimSupplier,
    FactSales,
    LineItem,
    Nation,
    Orders,
    Part,
    PartSupp,
    Region,
    Supplier,
)
from typer.testing import CliRunner

D = Decimal


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tpch-pipeline-test")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def rows():
    return {
        "customer": [
            Customer(
                1, "Ada", "1 Main", 10, "555-0001", D("12.00"), "BUILDING", "customer"
            )
        ],
        "lineitem": [
            LineItem(
                100,
                200,
                300,
                1,
                D("2.00"),
                D("100.00"),
                D("0.10"),
                D("0.05"),
                "N",
                "O",
                date(2026, 1, 2),
                date(2026, 1, 3),
                date(2026, 1, 4),
                "DELIVER",
                "AIR",
                "first",
            ),
            LineItem(
                100,
                200,
                301,
                2,
                D("1.00"),
                D("50.00"),
                D("0.00"),
                D("0.10"),
                "R",
                "F",
                date(2026, 1, 3),
                date(2026, 1, 6),
                date(2026, 1, 5),
                "DELIVER",
                "SHIP",
                "second",
            ),
        ],
        "nation": [Nation(10, "CANADA", 20, "north"), Nation(11, "GERMANY", 21, "eu")],
        "orders": [
            Orders(
                100,
                1,
                "O",
                D("149.50"),
                date(2026, 1, 1),
                "1-URGENT",
                "Clerk#1",
                0,
                "order",
            )
        ],
        "part": [
            Part(
                200,
                "Widget",
                "Maker",
                "Brand#1",
                "STANDARD",
                2,
                "BOX",
                D("75.00"),
                "part",
            )
        ],
        "partsupp": [
            PartSupp(200, 300, 5, D("10.00"), "a"),
            PartSupp(200, 301, 6, D("11.00"), "b"),
        ],
        "region": [Region(20, "AMERICA", "a"), Region(21, "EUROPE", "e")],
        "supplier": [
            Supplier(300, "Supplier A", "A St", 10, "555-0300", D("20.00"), "a"),
            Supplier(301, "Supplier B", "B St", 11, "555-0301", D("21.00"), "b"),
        ],
    }


def frame(spark, items):
    return Schema(type(items[0])).create_dataframe(items, DataFrame, session=spark)


def write_sources(spark, rows, data_dir):
    data_dir.mkdir()
    for name, items in rows.items():
        source = frame(spark, items)
        if name in {"lineitem", "orders"}:
            # Emulate a Parquet landing zone that still has VARCHAR columns.
            source = source.select(
                *(F.col(c).cast("string").alias(c) for c in source.columns)
            )
        source.write.parquet(str(data_dir / f"{name}.parquet"))


def test_tpch_pipeline_casts_sources_and_builds_mart(spark, rows, tmp_path):
    data_dir = tmp_path / "data"
    write_sources(spark, rows, data_dir)
    context = PipelineContext(
        spark,
        DataDir(str(data_dir)),
        OutputDir(str(tmp_path / "mart")),
    )

    result = build_pipeline().run(context)
    for name, items in rows.items():
        assertDataFrameEqual(
            result.get_output(f"read_{name}", DataFrame), frame(spark, items)
        )

    fact = spark.read.parquet(str(tmp_path / "mart" / "fact_sales"))
    expected_fact = frame(
        spark,
        [
            FactSales(
                100,
                1,
                1,
                300,
                200,
                date(2026, 1, 1),
                date(2026, 1, 2),
                date(2026, 1, 3),
                date(2026, 1, 4),
                "O",
                "1-URGENT",
                D("149.50"),
                D("2.00"),
                D("100.00"),
                D("0.10"),
                D("0.05"),
                D("90.0000"),
                D("94.500000"),
                True,
                "N",
                "O",
            ),
            FactSales(
                100,
                2,
                1,
                301,
                200,
                date(2026, 1, 1),
                date(2026, 1, 3),
                date(2026, 1, 6),
                date(2026, 1, 5),
                "O",
                "1-URGENT",
                D("149.50"),
                D("1.00"),
                D("50.00"),
                D("0.00"),
                D("0.10"),
                D("50.0000"),
                D("55.000000"),
                False,
                "R",
                "F",
            ),
        ],
    )
    assertDataFrameEqual(fact, expected_fact)
    assertDataFrameEqual(
        spark.read.parquet(str(tmp_path / "mart" / "dim_customers")),
        frame(
            spark,
            [
                DimCustomer(
                    1,
                    "Ada",
                    "1 Main",
                    "555-0001",
                    D("12.00"),
                    "BUILDING",
                    "CANADA",
                    "AMERICA",
                )
            ],
        ),
    )
    assertDataFrameEqual(
        spark.read.parquet(str(tmp_path / "mart" / "dim_suppliers")),
        frame(
            spark,
            [
                DimSupplier(
                    300,
                    "Supplier A",
                    "A St",
                    "555-0300",
                    D("20.00"),
                    "CANADA",
                    "AMERICA",
                ),
                DimSupplier(
                    301,
                    "Supplier B",
                    "B St",
                    "555-0301",
                    D("21.00"),
                    "GERMANY",
                    "EUROPE",
                ),
            ],
        ),
    )
    assertDataFrameEqual(
        spark.read.parquet(str(tmp_path / "mart" / "dim_part")),
        frame(
            spark,
            [
                DimPart(
                    200,
                    300,
                    "Widget",
                    "Maker",
                    "Brand#1",
                    "STANDARD",
                    2,
                    "BOX",
                    D("75.00"),
                    5,
                    D("10.00"),
                ),
                DimPart(
                    200,
                    301,
                    "Widget",
                    "Maker",
                    "Brand#1",
                    "STANDARD",
                    2,
                    "BOX",
                    D("75.00"),
                    6,
                    D("11.00"),
                ),
            ],
        ),
    )
    expected_dates = frame(
        spark,
        [
            DimDate(
                date(2026, 1, day),
                2026,
                1,
                1,
                "January",
                date(2026, 1, day).weekday() >= 5,
            )
            for day in range(1, 7)
        ],
    )
    assertDataFrameEqual(
        spark.read.parquet(str(tmp_path / "mart" / "dim_date")), expected_dates
    )


def test_reader_rejects_invalid_flags(spark, rows, tmp_path):
    data_dir = tmp_path / "data"
    write_sources(spark, rows, data_dir)
    lineitem_path = data_dir / "lineitem.parquet"
    invalid = spark.read.parquet(str(lineitem_path)).withColumn(
        "l_returnflag", F.lit("LONG")
    )
    invalid.write.mode("overwrite").parquet(str(tmp_path / "invalid.parquet"))
    # Replace just this source after Spark has completed the write.
    import shutil

    shutil.rmtree(lineitem_path)
    shutil.move(str(tmp_path / "invalid.parquet"), str(lineitem_path))
    context = PipelineContext(
        spark,
        DataDir(str(data_dir)),
        OutputDir(str(tmp_path / "mart")),
    )
    with pytest.raises(PipelineExecutionError, match="non-single-character flags"):
        build_pipeline().run(context)


def test_cli_runs_without_writing_when_side_effects_are_skipped(
    spark, rows, tmp_path, monkeypatch
):
    data_dir = tmp_path / "data"
    write_sources(spark, rows, data_dir)
    output_dir = tmp_path / "mart"
    monkeypatch.chdir(Path(__file__).resolve().parents[1])

    result = CliRunner().invoke(
        app,
        [
            "run",
            "--context",
            f"data_dir={data_dir}",
            "--context",
            f"output_dir={output_dir}",
            "--skip-side-effects",
        ],
    )

    assert result.exit_code == 0, result.output
    assert not output_dir.exists()
