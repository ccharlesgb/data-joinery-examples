from pyspark.sql import DataFrame, SparkSession

from .context import CustomersPath, OutputPath, PipelineContext
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.appName("customer-group-pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        context = PipelineContext(
            spark=spark,
            customers_path=CustomersPath(str(DATA_DIR / "customers.parquet")),
            output_path=OutputPath(str(OUTPUT_DIR / "output.parquet")),
        )
        result = build_pipeline().run(context)
        result.get_output("denormalise_group_id", DataFrame).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
