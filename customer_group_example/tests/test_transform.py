from customer_group.context import CustomersPath, OutputPath
from customer_group.schemas import Customer, CustomerGroup
from customer_group.transform import denormalise_group_id, read_customers, write_output
from data_joinery import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual


def test_read_customers_reads_parquet(spark: SparkSession, tmp_path):
    expected = Schema(Customer).create_dataframe(
        [Customer("parent", "Parent"), Customer("child", "Child", "parent")],
        DataFrame,
        session=spark,
    )
    path = tmp_path / "customers"
    expected.write.parquet(str(path))

    actual = read_customers(spark, CustomersPath(str(path)))

    assertDataFrameEqual(actual, expected)


def test_denormalise_group_id_keeps_roots_and_enriches_children(spark: SparkSession):
    customers = Schema(Customer).create_dataframe(
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

    assertDataFrameEqual(denormalise_group_id(customers), expected)


def test_write_output_writes_parquet(spark: SparkSession, tmp_path):
    expected = Schema(CustomerGroup).create_dataframe(
        [CustomerGroup("child", "Child", "parent", "Parent")],
        DataFrame,
        session=spark,
    )
    path = tmp_path / "output"

    write_output(expected, OutputPath(str(path)))

    assertDataFrameEqual(spark.read.parquet(str(path)), expected)
