"""Build TPC-H dimensional tables as deferred Polars queries."""

from typing import Annotated

import polars as pl
from data_joinery import Strict, transform

from ..schemas import (
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


@transform
def build_dim_customers(
    customer: Annotated[pl.LazyFrame, Strict(Customer)],
    nation: Annotated[pl.LazyFrame, Strict(Nation)],
    region: Annotated[pl.LazyFrame, Strict(Region)],
) -> Annotated[pl.LazyFrame, Strict(DimCustomer)]:
    result = (
        customer.join(nation, left_on="c_nationkey", right_on="n_nationkey")
        .join(region, left_on="n_regionkey", right_on="r_regionkey")
        .select(
            pl.col("c_custkey").alias("customer_key"),
            pl.col("c_name").alias("customer_name"),
            pl.col("c_address").alias("address"),
            pl.col("c_phone").alias("phone"),
            pl.col("c_acctbal").alias("account_balance"),
            pl.col("c_mktsegment").alias("market_segment"),
            pl.col("n_name").alias("nation"),
            pl.col("r_name").alias("region"),
        )
    )
    return result


@transform
def build_dim_suppliers(
    supplier: Annotated[pl.LazyFrame, Strict(Supplier)],
    nation: Annotated[pl.LazyFrame, Strict(Nation)],
    region: Annotated[pl.LazyFrame, Strict(Region)],
) -> Annotated[pl.LazyFrame, Strict(DimSupplier)]:
    result = (
        supplier.join(nation, left_on="s_nationkey", right_on="n_nationkey")
        .join(region, left_on="n_regionkey", right_on="r_regionkey")
        .select(
            pl.col("s_suppkey").alias("supplier_key"),
            pl.col("s_name").alias("supplier_name"),
            pl.col("s_address").alias("address"),
            pl.col("s_phone").alias("phone"),
            pl.col("s_acctbal").alias("account_balance"),
            pl.col("n_name").alias("nation"),
            pl.col("r_name").alias("region"),
        )
    )
    return result


@transform
def build_dim_part(
    part: Annotated[pl.LazyFrame, Strict(Part)],
    partsupp: Annotated[pl.LazyFrame, Strict(PartSupp)],
) -> Annotated[pl.LazyFrame, Strict(DimPart)]:
    result = part.join(partsupp, left_on="p_partkey", right_on="ps_partkey").select(
        pl.col("p_partkey").alias("part_key"),
        pl.col("ps_suppkey").alias("supplier_key"),
        pl.col("p_name").alias("part_name"),
        pl.col("p_mfgr").alias("manufacturer"),
        pl.col("p_brand").alias("brand"),
        pl.col("p_type").alias("part_type"),
        pl.col("p_size").alias("size"),
        pl.col("p_container").alias("container"),
        pl.col("p_retailprice").alias("retail_price"),
        pl.col("ps_availqty").alias("available_quantity"),
        pl.col("ps_supplycost").alias("supply_cost"),
    )
    return result


@transform
def build_fact_sales(
    lineitem: Annotated[pl.LazyFrame, Strict(LineItem)],
    orders: Annotated[pl.LazyFrame, Strict(Orders)],
) -> Annotated[pl.LazyFrame, Strict(FactSales)]:
    lines = lineitem.join(orders, left_on="l_orderkey", right_on="o_orderkey")
    one = pl.lit(1).cast(pl.Decimal(15, 2))
    revenue = pl.col("l_extendedprice") * (one - pl.col("l_discount"))
    total_cost = revenue * (one + pl.col("l_tax"))
    result = lines.select(
        pl.col("l_orderkey").alias("order_key"),
        pl.col("l_linenumber").alias("line_number"),
        pl.col("o_custkey").alias("customer_key"),
        pl.col("l_suppkey").alias("supplier_key"),
        pl.col("l_partkey").alias("part_key"),
        pl.col("o_orderdate").alias("order_date"),
        pl.col("l_shipdate").alias("ship_date"),
        pl.col("l_commitdate").alias("commit_date"),
        pl.col("l_receiptdate").alias("receipt_date"),
        pl.col("o_orderstatus").alias("order_status"),
        pl.col("o_orderpriority").alias("order_priority"),
        pl.col("o_totalprice").alias("order_total_price"),
        pl.col("l_quantity").alias("quantity"),
        pl.col("l_extendedprice").alias("extended_price"),
        pl.col("l_discount").alias("discount"),
        pl.col("l_tax").alias("tax"),
        revenue.cast(pl.Decimal(20, 4)).alias("revenue"),
        total_cost.cast(pl.Decimal(22, 6)).alias("total_cost"),
        (pl.col("l_receiptdate") > pl.col("l_commitdate")).alias("is_late_shipment"),
        pl.col("l_returnflag").alias("return_flag"),
        pl.col("l_linestatus").alias("line_status"),
    )
    return result


@transform
def build_dim_date(
    fact_sales: Annotated[pl.LazyFrame, Strict(FactSales)],
) -> Annotated[pl.LazyFrame, Strict(DimDate)]:
    dates = ["order_date", "ship_date", "commit_date", "receipt_date"]
    bounds = fact_sales.select(
        pl.min_horizontal(dates).min().alias("start"),
        pl.max_horizontal(dates).max().alias("end"),
    )
    days = bounds.select(
        pl.date_ranges("start", "end", interval="1d").explode().alias("date_key")
    )
    result = days.select(
        pl.col("date_key"),
        pl.col("date_key").dt.year().cast(pl.Int32).alias("calendar_year"),
        pl.col("date_key").dt.quarter().cast(pl.Int32).alias("quarter"),
        pl.col("date_key").dt.month().cast(pl.Int32).alias("month_number"),
        pl.col("date_key").dt.strftime("%B").alias("month_name"),
        (pl.col("date_key").dt.weekday() >= 6).alias("is_weekend"),
    )
    return result
