"""Scan, cast, and validate source Parquet tables with lazy Polars plans."""

from pathlib import Path
from typing import Annotated

import polars as pl
from data_joinery import Context, Schema, Strict, transform

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


def _read(data_dir: DataDir, name: str) -> pl.LazyFrame:
    path = Path(data_dir) / f"{name}.parquet"
    source = pl.scan_parquet(str(path / "*.parquet") if path.is_dir() else str(path))
    model = SOURCE_MODELS[name]
    expected = Schema(model).native_schema(pl.Schema)
    source_schema = source.collect_schema()
    missing = set(expected) - set(source_schema)
    if missing:
        raise ValueError(f"{name} is missing columns: {sorted(missing)}")
    typed = source.select(
        (
            pl.col(column).str.to_date(strict=False)
            if dtype == pl.Date and source_schema[column] == pl.String
            else pl.col(column).cast(dtype, strict=False)
        ).alias(column)
        for column, dtype in expected.items()
    )
    invalid = pl.any_horizontal(pl.col(column).is_null() for column in expected)
    for flag in {"l_returnflag", "l_linestatus", "o_orderstatus"} & set(expected):
        invalid = invalid | (pl.col(flag).str.len_chars() != 1)
    if typed.filter(invalid).limit(1).collect(engine="streaming").height:
        raise ValueError(
            f"{name} contains null, invalid cast, or non-single-character flags"
        )
    return typed


@transform
def read_customer(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(Customer)]:
    return _read(data_dir, "customer")


@transform
def read_lineitem(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(LineItem)]:
    return _read(data_dir, "lineitem")


@transform
def read_nation(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(Nation)]:
    return _read(data_dir, "nation")


@transform
def read_orders(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(Orders)]:
    return _read(data_dir, "orders")


@transform
def read_part(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(Part)]:
    return _read(data_dir, "part")


@transform
def read_partsupp(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(PartSupp)]:
    return _read(data_dir, "partsupp")


@transform
def read_region(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(Region)]:
    return _read(data_dir, "region")


@transform
def read_supplier(
    data_dir: Annotated[DataDir, Context()],
) -> Annotated[pl.LazyFrame, Strict(Supplier)]:
    return _read(data_dir, "supplier")
