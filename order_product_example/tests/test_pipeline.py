from datetime import UTC, datetime

from data_joinery import Schema
from order_product.context import (
    CustomersPath,
    OrdersPath,
    OutputPath,
    PipelineContext,
    RunDate,
)
from order_product.pipeline import build_pipeline
from order_product.schemas import Customer, Order, OrderWithCustomerDimension
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual


def test_pipeline_filters_joins_and_writes_both_sources(spark: SparkSession, tmp_path):
    order_time = datetime(2026, 1, 1, 8, 30, tzinfo=UTC)
    other_time = datetime(2026, 1, 2, 9, 45, tzinfo=UTC)
    orders = Schema(Order).create_dataframe(
        [
            Order(1, "customer-1", order_time, 100, 2),
            Order(2, "customer-2", order_time, 200, 1),
            Order(3, "customer-1", other_time, 300, 1),
        ],
        DataFrame,
        session=spark,
    )
    customers = Schema(Customer).create_dataframe(
        [Customer("customer-1", "Ada")], DataFrame, session=spark
    )
    expected = Schema(OrderWithCustomerDimension).create_dataframe(
        [
            OrderWithCustomerDimension(
                order_id=1,
                customer_id="customer-1",
                order_timestamp=order_time,
                product_id=100,
                quantity=2,
                name="Ada",
            )
        ],
        DataFrame,
        session=spark,
    )
    orders_path = tmp_path / "orders"
    customers_path = tmp_path / "customers"
    output_path = tmp_path / "output"
    orders.write.parquet(str(orders_path))
    customers.write.parquet(str(customers_path))

    result = build_pipeline().run(
        PipelineContext(
            spark=spark,
            orders_path=OrdersPath(str(orders_path)),
            customers_path=CustomersPath(str(customers_path)),
            output_path=OutputPath(str(output_path)),
            run_date=RunDate(2026, 1, 1),
        )
    )

    assertDataFrameEqual(
        result.get_output("join_orders_with_customers", DataFrame), expected
    )
    assertDataFrameEqual(spark.read.parquet(str(output_path)), expected)
