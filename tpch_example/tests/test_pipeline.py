from datetime import date
from decimal import Decimal

from pyspark.sql import Row, SparkSession
from tpch.context import DataDir, OutputDir, PipelineContext
from tpch.pipeline import build_pipeline


def test_tpch_mart_joins_sources_and_calculates_measures(tmp_path):
    spark = (
        SparkSession.builder.master("local[1]").appName("tpch-mart-test").getOrCreate()
    )
    try:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        tables = {
            "customer": [
                Row(c_custkey=1, c_name="Ada", c_mktsegment="BUILDING", c_nationkey=10)
            ],
            "lineitem": [
                Row(
                    l_orderkey=100,
                    l_linenumber=1,
                    l_partkey=200,
                    l_suppkey=300,
                    l_shipdate=date(2026, 1, 2),
                    l_quantity=Decimal("2.00"),
                    l_extendedprice=Decimal("100.00"),
                    l_discount=Decimal("0.10"),
                    l_tax=Decimal("0.05"),
                    l_returnflag="N",
                    l_linestatus="O",
                )
            ],
            "nation": [Row(n_nationkey=10, n_name="CANADA", n_regionkey=20)],
            "orders": [
                Row(
                    o_orderkey=100,
                    o_custkey=1,
                    o_orderdate=date(2026, 1, 1),
                    o_orderstatus="O",
                    o_orderpriority="1-URGENT",
                    o_totalprice=Decimal("94.50"),
                )
            ],
            "part": [
                Row(
                    p_partkey=200, p_name="Widget", p_brand="Brand#1", p_type="STANDARD"
                )
            ],
            "partsupp": [Row(ps_partkey=200, ps_suppkey=300)],
            "region": [Row(r_regionkey=20, r_name="AMERICA")],
            "supplier": [Row(s_suppkey=300, s_name="Supplier", s_nationkey=10)],
        }
        for table, rows in tables.items():
            spark.createDataFrame(rows).write.parquet(
                str(data_dir / f"{table}.parquet")
            )

        output_dir = tmp_path / "out"
        build_pipeline().run(
            PipelineContext(
                spark=spark,
                data_dir=DataDir(str(data_dir)),
                output_dir=OutputDir(str(output_dir)),
            )
        )
        line = spark.read.parquet(str(output_dir / "sales_lines")).collect()
        summary = spark.read.parquet(str(output_dir / "order_summary")).collect()
        assert len(line) == len(summary) == 1
        assert line[0].customer_nation == "CANADA"
        assert line[0].supplier_region == "AMERICA"
        assert line[0].net_revenue == Decimal("90.0000")
        assert line[0].charge == Decimal("94.500000")
        assert summary[0].line_count == 1
        assert summary[0].net_revenue == line[0].net_revenue
    finally:
        spark.stop()
