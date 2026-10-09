"""Parquet sinks for deferred Polars mart tables."""

from pathlib import Path
from typing import Annotated

import polars as pl
from data_joinery import Context, Strict, side_effect

from ..context import OutputDir
from ..schemas import DimCustomer, DimDate, DimPart, DimSupplier, FactSales


def _sink(frame: pl.LazyFrame, output_dir: OutputDir, name: str) -> None:
    path = Path(output_dir) / f"{name}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.sink_parquet(path)


@side_effect
def write_fact_sales(
    fact_sales: Annotated[pl.LazyFrame, Strict(FactSales)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    _sink(fact_sales, output_dir, "fact_sales")


@side_effect
def write_dim_customers(
    dim_customers: Annotated[pl.LazyFrame, Strict(DimCustomer)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    _sink(dim_customers, output_dir, "dim_customers")


@side_effect
def write_dim_suppliers(
    dim_suppliers: Annotated[pl.LazyFrame, Strict(DimSupplier)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    _sink(dim_suppliers, output_dir, "dim_suppliers")


@side_effect
def write_dim_part(
    dim_part: Annotated[pl.LazyFrame, Strict(DimPart)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    _sink(dim_part, output_dir, "dim_part")


@side_effect
def write_dim_date(
    dim_date: Annotated[pl.LazyFrame, Strict(DimDate)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    _sink(dim_date, output_dir, "dim_date")
