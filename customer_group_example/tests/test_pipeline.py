from customer_group.context import CustomersPath, OutputPath, PipelineContext
from customer_group.pipeline import build_pipeline
from customer_group.schemas import Customer, CustomerGroup
from data_joinery import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual


def test_pipeline_enriches_customers_and_passes_rows_to_writer(
    spark: SparkSession, tmp_path
):
    source = Schema(Customer).create_dataframe(
        [
            Customer("parent", "Parent"),
            Customer("child", "Child", "parent"),
            Customer("orphan", "Orphan", "missing"),
        ],
        DataFrame,
        session=spark,
    )
    expected = Schema(CustomerGroup).create_dataframe(
        [
            CustomerGroup("parent", "Parent"),
            CustomerGroup("child", "Child", "parent", "Parent"),
            CustomerGroup("orphan", "Orphan", "missing"),
        ],
        DataFrame,
        session=spark,
    )
    source_path = tmp_path / "customers"
    output_path = tmp_path / "output"
    source.write.parquet(str(source_path))

    result = build_pipeline().run(
        PipelineContext(
            spark=spark,
            customers_path=CustomersPath(str(source_path)),
            output_path=OutputPath(str(output_path)),
        )
    )

    actual = result.get_input("write_output", "customer_groups", DataFrame)
    assertDataFrameEqual(actual, expected)
    assertDataFrameEqual(spark.read.parquet(str(output_path)), expected)
