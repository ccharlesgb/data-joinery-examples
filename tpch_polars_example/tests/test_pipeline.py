"""End-to-end checks for the lazy Polars TPC-H mart."""

from datetime import date
from decimal import Decimal as D
from pathlib import Path

import polars as pl
import pytest
from data_joinery import Schema
from data_joinery.cli import app
from data_joinery.pipeline import PipelineExecutionError
from polars.testing import assert_frame_equal
from tpch_polars.context import DataDir, OutputDir, PipelineContext
from tpch_polars.pipeline import build_pipeline
from tpch_polars.schemas import (
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
            )
        ],
        "nation": [Nation(10, "CANADA", 20, "north")],
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
        "partsupp": [PartSupp(200, 300, 5, D("10.00"), "a")],
        "region": [Region(20, "AMERICA", "a")],
        "supplier": [
            Supplier(300, "Supplier A", "A St", 10, "555-0300", D("20.00"), "a")
        ],
    }


def frame(items):
    return Schema(type(items[0])).create_dataframe(items, pl.DataFrame)


def write_sources(rows, data_dir):
    data_dir.mkdir()
    for name, items in rows.items():
        source = frame(items)
        if name in {"lineitem", "orders"}:
            source = source.select(pl.all().cast(pl.String))
        source.write_parquet(data_dir / f"{name}.parquet")


def test_pipeline_builds_mart_with_lazy_frames(rows, tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    write_sources(rows, data_dir)
    output_dir = tmp_path / "mart"
    collect_all = pl.collect_all
    collect_calls = []

    def record_collect_all(sinks, *, optimizations):
        collect_calls.append((len(sinks), optimizations.comm_subplan_elim))
        return collect_all(sinks, optimizations=optimizations)

    monkeypatch.setattr(pl, "collect_all", record_collect_all)
    result = build_pipeline().run(
        PipelineContext(DataDir(str(data_dir)), OutputDir(str(output_dir)))
    )
    assert collect_calls == [(5, False)]

    for name, items in rows.items():
        assert_frame_equal(
            result.get_output(f"read_{name}", pl.LazyFrame).collect(), frame(items)
        )

    expected = {
        "fact_sales": [
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
            )
        ],
        "dim_customers": [
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
        "dim_suppliers": [
            DimSupplier(
                300, "Supplier A", "A St", "555-0300", D("20.00"), "CANADA", "AMERICA"
            )
        ],
        "dim_part": [
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
            )
        ],
        "dim_date": [
            DimDate(
                date(2026, 1, day),
                2026,
                1,
                1,
                "January",
                date(2026, 1, day).weekday() >= 5,
            )
            for day in range(1, 5)
        ],
    }
    for name, items in expected.items():
        assert_frame_equal(
            pl.read_parquet(output_dir / f"{name}.parquet"), frame(items)
        )


def test_reader_rejects_invalid_flags(rows, tmp_path):
    rows["lineitem"][0].l_returnflag = "LONG"
    data_dir = tmp_path / "data"
    write_sources(rows, data_dir)
    context = PipelineContext(DataDir(str(data_dir)), OutputDir(str(tmp_path / "mart")))
    with pytest.raises(PipelineExecutionError, match="non-single-character flags"):
        build_pipeline().run(context)


def test_cli_runs_without_writing_when_side_effects_are_skipped(
    rows, tmp_path, monkeypatch
):
    data_dir = tmp_path / "data"
    write_sources(rows, data_dir)
    output_dir = tmp_path / "mart"
    monkeypatch.chdir(Path(__file__).resolve().parents[1])

    result = CliRunner().invoke(
        app,
        [
            "run",
            "tpch_polars.main",
            "--context",
            f"data_dir={data_dir}",
            "--context",
            f"output_dir={output_dir}",
            "--skip-side-effects",
        ],
    )

    assert result.exit_code == 0, result.output
    assert not output_dir.exists()
