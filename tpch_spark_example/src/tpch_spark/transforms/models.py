"""Build the TPC-H dimensional model at explicit business grains."""

from typing import Annotated

from data_joinery import Strict, transform
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

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
    customer: Annotated[DataFrame, Strict(Customer)],
    nation: Annotated[DataFrame, Strict(Nation)],
    region: Annotated[DataFrame, Strict(Region)],
) -> Annotated[DataFrame, Strict(DimCustomer)]:
    return (
        customer.alias("c")
        .join(nation.alias("n"), F.col("c.c_nationkey") == F.col("n.n_nationkey"))
        .join(region.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"))
        .select(
            F.col("c.c_custkey").alias("customer_key"),
            F.col("c.c_name").alias("customer_name"),
            F.col("c.c_address").alias("address"),
            F.col("c.c_phone").alias("phone"),
            F.col("c.c_acctbal").alias("account_balance"),
            F.col("c.c_mktsegment").alias("market_segment"),
            F.col("n.n_name").alias("nation"),
            F.col("r.r_name").alias("region"),
        )
    )


@transform
def build_dim_suppliers(
    supplier: Annotated[DataFrame, Strict(Supplier)],
    nation: Annotated[DataFrame, Strict(Nation)],
    region: Annotated[DataFrame, Strict(Region)],
) -> Annotated[DataFrame, Strict(DimSupplier)]:
    return (
        supplier.alias("s")
        .join(nation.alias("n"), F.col("s.s_nationkey") == F.col("n.n_nationkey"))
        .join(region.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"))
        .select(
            F.col("s.s_suppkey").alias("supplier_key"),
            F.col("s.s_name").alias("supplier_name"),
            F.col("s.s_address").alias("address"),
            F.col("s.s_phone").alias("phone"),
            F.col("s.s_acctbal").alias("account_balance"),
            F.col("n.n_name").alias("nation"),
            F.col("r.r_name").alias("region"),
        )
    )


@transform
def build_dim_part(
    part: Annotated[DataFrame, Strict(Part)],
    partsupp: Annotated[DataFrame, Strict(PartSupp)],
) -> Annotated[DataFrame, Strict(DimPart)]:
    """One row per part and supplier; the composite key preserves supply attributes."""
    return (
        part.alias("p")
        .join(partsupp.alias("ps"), F.col("p.p_partkey") == F.col("ps.ps_partkey"))
        .select(
            F.col("p.p_partkey").alias("part_key"),
            F.col("ps.ps_suppkey").alias("supplier_key"),
            F.col("p.p_name").alias("part_name"),
            F.col("p.p_mfgr").alias("manufacturer"),
            F.col("p.p_brand").alias("brand"),
            F.col("p.p_type").alias("part_type"),
            F.col("p.p_size").alias("size"),
            F.col("p.p_container").alias("container"),
            F.col("p.p_retailprice").alias("retail_price"),
            F.col("ps.ps_availqty").alias("available_quantity"),
            F.col("ps.ps_supplycost").alias("supply_cost"),
        )
    )


@transform
def build_fact_sales(
    lineitem: Annotated[DataFrame, Strict(LineItem)],
    orders: Annotated[DataFrame, Strict(Orders)],
) -> Annotated[DataFrame, Strict(FactSales)]:
    """One row per order line, with order attributes and precomputed measures."""
    lines = lineitem.alias("l").join(
        orders.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey")
    )
    one = F.lit(1).cast(DecimalType(15, 2))
    revenue = F.col("l.l_extendedprice") * (one - F.col("l.l_discount"))
    total_cost = revenue * (one + F.col("l.l_tax"))
    # Both the fact writer and date dimension consume this expensive join.
    return lines.select(
        F.col("l.l_orderkey").alias("order_key"),
        F.col("l.l_linenumber").alias("line_number"),
        F.col("o.o_custkey").alias("customer_key"),
        F.col("l.l_suppkey").alias("supplier_key"),
        F.col("l.l_partkey").alias("part_key"),
        F.col("o.o_orderdate").alias("order_date"),
        F.col("l.l_shipdate").alias("ship_date"),
        F.col("l.l_commitdate").alias("commit_date"),
        F.col("l.l_receiptdate").alias("receipt_date"),
        F.col("o.o_orderstatus").alias("order_status"),
        F.col("o.o_orderpriority").alias("order_priority"),
        F.col("o.o_totalprice").alias("order_total_price"),
        F.col("l.l_quantity").alias("quantity"),
        F.col("l.l_extendedprice").alias("extended_price"),
        F.col("l.l_discount").alias("discount"),
        F.col("l.l_tax").alias("tax"),
        revenue.cast(DecimalType(20, 4)).alias("revenue"),
        total_cost.cast(DecimalType(22, 6)).alias("total_cost"),
        (F.col("l.l_receiptdate") > F.col("l.l_commitdate")).alias("is_late_shipment"),
        F.col("l.l_returnflag").alias("return_flag"),
        F.col("l.l_linestatus").alias("line_status"),
    ).persist(StorageLevel.DISK_ONLY)


@transform
def build_dim_date(
    fact_sales: Annotated[DataFrame, Strict(FactSales)],
) -> Annotated[DataFrame, Strict(DimDate)]:
    """Cover every day from the earliest to latest date referenced by a fact."""
    bounds = fact_sales.select(
        F.least("order_date", "ship_date", "commit_date", "receipt_date").alias(
            "first_date"
        ),
        F.greatest("order_date", "ship_date", "commit_date", "receipt_date").alias(
            "last_date"
        ),
    ).agg(F.min("first_date").alias("start"), F.max("last_date").alias("end"))
    dates = bounds.select(F.explode(F.sequence("start", "end")).alias("date_key"))
    return dates.select(
        "date_key",
        F.year("date_key").alias("calendar_year"),
        F.quarter("date_key").alias("quarter"),
        F.month("date_key").alias("month_number"),
        F.date_format("date_key", "MMMM").alias("month_name"),
        F.dayofweek("date_key").isin(1, 7).alias("is_weekend"),
    )
