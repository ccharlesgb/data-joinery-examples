from pyspark.sql import DataFrame, SparkSession

from order_product.context import PipelineContext

from .context import CustomersPath, OrdersPath, OutputPath, RunDate
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.appName("order-product-pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        context = PipelineContext(
            spark=spark,
            orders_path=OrdersPath(str(DATA_DIR / "orders.parquet")),
            customers_path=CustomersPath(str(DATA_DIR / "customers.parquet")),
            output_path=OutputPath(str(OUTPUT_DIR / "output.parquet")),
            run_date=RunDate(2026, 1, 1),
        )
        result = build_pipeline().run(context)
        result.get_output("join_orders_with_customers", DataFrame).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
