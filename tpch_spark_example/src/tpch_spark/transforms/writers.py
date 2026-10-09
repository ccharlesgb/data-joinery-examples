"""Parquet sinks for the analytical mart."""

from typing import Annotated

from data_joinery import Context, Strict, side_effect
from pyspark.sql import DataFrame

from ..context import OutputDir
from ..schemas import DimCustomer, DimDate, DimPart, DimSupplier, FactSales


@side_effect
def write_fact_sales(
    fact_sales: Annotated[DataFrame, Strict(FactSales)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    fact_sales.write.mode("overwrite").parquet(f"{output_dir}/fact_sales")


@side_effect
def write_dim_customers(
    dim_customers: Annotated[DataFrame, Strict(DimCustomer)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    dim_customers.write.mode("overwrite").parquet(f"{output_dir}/dim_customers")


@side_effect
def write_dim_suppliers(
    dim_suppliers: Annotated[DataFrame, Strict(DimSupplier)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    dim_suppliers.write.mode("overwrite").parquet(f"{output_dir}/dim_suppliers")


@side_effect
def write_dim_part(
    dim_part: Annotated[DataFrame, Strict(DimPart)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    dim_part.write.mode("overwrite").parquet(f"{output_dir}/dim_part")


@side_effect
def write_dim_date(
    dim_date: Annotated[DataFrame, Strict(DimDate)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    dim_date.write.mode("overwrite").parquet(f"{output_dir}/dim_date")
