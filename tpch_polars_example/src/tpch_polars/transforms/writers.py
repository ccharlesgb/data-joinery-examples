"""Parquet sinks for deferred Polars mart tables."""

from pathlib import Path
from typing import Annotated

import polars as pl
from data_joinery import Context, Strict, side_effect, transform

from ..context import OutputDir
from ..schemas import DimCustomer, DimDate, DimPart, DimSupplier, FactSales


def _sink(frame: pl.LazyFrame, output_dir: OutputDir, name: str) -> pl.LazyFrame:
    path = Path(output_dir) / f"{name}.parquet"
    return frame.sink_parquet(path, lazy=True)


@transform
def write_fact_sales(
    fact_sales: Annotated[pl.LazyFrame, Strict(FactSales)],
    output_dir: Annotated[OutputDir, Context()],
) -> pl.LazyFrame:
    return _sink(fact_sales, output_dir, "fact_sales")


@transform
def write_dim_customers(
    dim_customers: Annotated[pl.LazyFrame, Strict(DimCustomer)],
    output_dir: Annotated[OutputDir, Context()],
) -> pl.LazyFrame:
    return _sink(dim_customers, output_dir, "dim_customers")


@transform
def write_dim_suppliers(
    dim_suppliers: Annotated[pl.LazyFrame, Strict(DimSupplier)],
    output_dir: Annotated[OutputDir, Context()],
) -> pl.LazyFrame:
    return _sink(dim_suppliers, output_dir, "dim_suppliers")


@transform
def write_dim_part(
    dim_part: Annotated[pl.LazyFrame, Strict(DimPart)],
    output_dir: Annotated[OutputDir, Context()],
) -> pl.LazyFrame:
    return _sink(dim_part, output_dir, "dim_part")


@transform
def write_dim_date(
    dim_date: Annotated[pl.LazyFrame, Strict(DimDate)],
    output_dir: Annotated[OutputDir, Context()],
) -> pl.LazyFrame:
    return _sink(dim_date, output_dir, "dim_date")


@side_effect
def collect_mart_sinks(
    fact_sales: pl.LazyFrame,
    dim_customers: pl.LazyFrame,
    dim_suppliers: pl.LazyFrame,
    dim_part: pl.LazyFrame,
    dim_date: pl.LazyFrame,
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    pl.collect_all([fact_sales, dim_customers, dim_suppliers, dim_part, dim_date])
