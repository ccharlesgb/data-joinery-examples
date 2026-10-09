"""Read, cast, and validate source Parquet tables for the mart."""

from typing import Annotated

from data_joinery import Context, Schema, Strict, transform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..context import DataDir
from ..schemas import (
    SOURCE_MODELS,
    Customer,
    LineItem,
    Nation,
    Orders,
    Part,
    PartSupp,
    Region,
    Supplier,
)


def _read(spark: SparkSession, data_dir: DataDir, name: str) -> DataFrame:
    source = spark.read.parquet(f"{data_dir}/{name}.parquet")
    typed = Schema(SOURCE_MODELS[name]).coerce_dataframe(source, mode="project_cast")
    # Spark's nullable schema does not guarantee valid values after a cast.
    invalid = F.lit(False)
    for column in typed.columns:
        invalid = invalid | F.col(column).isNull()
    for flag in {"l_returnflag", "l_linestatus", "o_orderstatus"} & set(typed.columns):
        invalid = invalid | (F.length(F.col(flag)) != 1)
    if typed.filter(invalid).limit(1).count():
        raise ValueError(
            f"{name} contains null, invalid cast, or non-single-character flags"
        )
    return typed


@transform
def read_customer(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(Customer)]:
    return _read(spark, data_dir, "customer")


@transform
def read_lineitem(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(LineItem)]:
    return _read(spark, data_dir, "lineitem")


@transform
def read_nation(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(Nation)]:
    return _read(spark, data_dir, "nation")


@transform
def read_orders(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(Orders)]:
    return _read(spark, data_dir, "orders")


@transform
def read_part(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(Part)]:
    return _read(spark, data_dir, "part")


@transform
def read_partsupp(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(PartSupp)]:
    return _read(spark, data_dir, "partsupp")


@transform
def read_region(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(Region)]:
    return _read(spark, data_dir, "region")


@transform
def read_supplier(
    spark: Annotated[SparkSession, Context()],
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[DataFrame, Strict(Supplier)]:
    return _read(spark, data_dir, "supplier")
