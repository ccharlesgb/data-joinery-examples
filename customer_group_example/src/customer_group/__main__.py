from pyspark.sql import SparkSession

from .context import CustomersPath, OutputPath, PipelineContext
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline

spark = (
    SparkSession.builder.appName("customer-group-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()
context = PipelineContext(
    spark=spark,
    customers_path=CustomersPath(str(DATA_DIR / "customers.parquet")),
    output_path=OutputPath(str(OUTPUT_DIR / "output.parquet")),
)
outputs = pipeline.run(context)
outputs["denormalise_group_id"].show()

spark.stop()
