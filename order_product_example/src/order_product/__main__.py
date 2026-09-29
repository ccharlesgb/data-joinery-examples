from pyspark.sql import SparkSession

from order_product.context import PipelineContext

from .context import CustomersPath, OrdersPath, OutputPath, RunDate
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline

spark = (
    SparkSession.builder.appName("order-product-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()
context = PipelineContext(
    spark=spark,
    orders_path=OrdersPath(str(DATA_DIR / "orders.parquet")),
    customers_path=CustomersPath(str(DATA_DIR / "customers.parquet")),
    output_path=OutputPath(str(OUTPUT_DIR / "output.parquet")),
    run_date=RunDate(2026, 1, 1),
)
outputs = pipeline.run(context)
outputs["join_orders_with_customers"].show()

spark.stop()
