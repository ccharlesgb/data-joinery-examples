"""Read TPC-H sources and build two analytical fact tables."""

from typing import Annotated

from data_joinery import Context, Project, ProjectCast, transform
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .context import DataDir, OutputDir
from .schemas import OrderSummary, SalesLine


def _read(spark: SparkSession, data_dir: DataDir, table: str) -> DataFrame:
    return spark.read.parquet(f"{data_dir}/{table}.parquet")


@transform
def read_customer(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "customer")


@transform
def read_lineitem(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "lineitem")


@transform
def read_nation(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "nation")


@transform
def read_orders(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "orders")


@transform
def read_part(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "part")


@transform
def read_partsupp(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "partsupp")


@transform
def read_region(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "region")


@transform
def read_supplier(
    spark: Annotated[SparkSession, Context()], data_dir: Annotated[DataDir, Context()]
) -> DataFrame:
    return _read(spark, data_dir, "supplier")


@transform
def build_sales_lines(
    lineitem: DataFrame,
    orders: DataFrame,
    customer: DataFrame,
    supplier: DataFrame,
    part: DataFrame,
    partsupp: DataFrame,
    nation: DataFrame,
    region: DataFrame,
) -> Annotated[DataFrame, ProjectCast(SalesLine)]:
    """One row per order line, with customer and supplier geography."""
    customer_geo = (
        customer.alias("c")
        .join(nation.alias("cn"), F.col("c.c_nationkey") == F.col("cn.n_nationkey"))
        .join(region.alias("cr"), F.col("cn.n_regionkey") == F.col("cr.r_regionkey"))
        .select(
            F.col("c.c_custkey").alias("customer_key"),
            F.col("c.c_name").alias("customer_name"),
            F.col("c.c_mktsegment").alias("customer_segment"),
            F.col("cn.n_name").alias("customer_nation"),
            F.col("cr.r_name").alias("customer_region"),
        )
    )
    supplier_geo = (
        supplier.alias("s")
        .join(nation.alias("sn"), F.col("s.s_nationkey") == F.col("sn.n_nationkey"))
        .join(region.alias("sr"), F.col("sn.n_regionkey") == F.col("sr.r_regionkey"))
        .select(
            F.col("s.s_suppkey").alias("supplier_key"),
            F.col("s.s_name").alias("supplier_name"),
            F.col("sn.n_name").alias("supplier_nation"),
            F.col("sr.r_name").alias("supplier_region"),
        )
    )
    lines = (
        lineitem.alias("l")
        .join(orders.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey"))
        .join(
            customer_geo.alias("cg"), F.col("o.o_custkey") == F.col("cg.customer_key")
        )
        .join(
            supplier_geo.alias("sg"), F.col("l.l_suppkey") == F.col("sg.supplier_key")
        )
        .join(part.alias("p"), F.col("l.l_partkey") == F.col("p.p_partkey"))
        .join(
            partsupp.alias("ps"),
            (F.col("l.l_partkey") == F.col("ps.ps_partkey"))
            & (F.col("l.l_suppkey") == F.col("ps.ps_suppkey")),
        )
    )
    net = F.col("l.l_extendedprice") * (F.lit(1) - F.col("l.l_discount"))
    return lines.select(
        F.col("l.l_orderkey").alias("order_key"),
        F.col("l.l_linenumber").alias("line_number"),
        F.col("o.o_orderdate").alias("order_date"),
        F.col("l.l_shipdate").alias("ship_date"),
        F.col("cg.customer_key"),
        F.col("cg.customer_name"),
        F.col("cg.customer_segment"),
        F.col("cg.customer_nation"),
        F.col("cg.customer_region"),
        F.col("sg.supplier_key"),
        F.col("sg.supplier_name"),
        F.col("sg.supplier_nation"),
        F.col("sg.supplier_region"),
        F.col("p.p_partkey").alias("part_key"),
        F.col("p.p_name").alias("part_name"),
        F.col("p.p_brand").alias("part_brand"),
        F.col("p.p_type").alias("part_type"),
        F.col("l.l_quantity").alias("quantity"),
        F.col("l.l_extendedprice").alias("extended_price"),
        F.col("l.l_discount").alias("discount"),
        F.col("l.l_tax").alias("tax"),
        net.alias("net_revenue"),
        (net * (F.lit(1) + F.col("l.l_tax"))).alias("charge"),
        F.col("l.l_returnflag").alias("return_flag"),
        F.col("l.l_linestatus").alias("line_status"),
    )


@transform
def build_order_summary(
    sales_lines: Annotated[DataFrame, Project(SalesLine)],
    orders: DataFrame,
) -> Annotated[DataFrame, ProjectCast(OrderSummary)]:
    """One row per order, with line measures and order attributes."""
    grouped = sales_lines.groupBy(
        "order_key",
        "order_date",
        "customer_key",
        "customer_name",
        "customer_segment",
        "customer_nation",
        "customer_region",
    ).agg(
        F.count("line_number").alias("line_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("net_revenue").alias("net_revenue"),
        F.sum("charge").alias("total_charge"),
    )
    return grouped.join(orders, grouped.order_key == orders.o_orderkey).select(
        "order_key",
        "order_date",
        "customer_key",
        "customer_name",
        "customer_segment",
        "customer_nation",
        "customer_region",
        F.col("o_orderstatus").alias("order_status"),
        F.col("o_orderpriority").alias("order_priority"),
        F.col("o_totalprice").alias("order_total_price"),
        "line_count",
        "total_quantity",
        "net_revenue",
        "total_charge",
    )


@transform
def write_sales_lines(
    sales_lines: Annotated[DataFrame, Project(SalesLine)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    sales_lines.write.mode("overwrite").parquet(f"{output_dir}/sales_lines")


@transform
def write_order_summary(
    order_summary: Annotated[DataFrame, Project(OrderSummary)],
    output_dir: Annotated[OutputDir, Context()],
) -> None:
    order_summary.write.mode("overwrite").parquet(f"{output_dir}/order_summary")
