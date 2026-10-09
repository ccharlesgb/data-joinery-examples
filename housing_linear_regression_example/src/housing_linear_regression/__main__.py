from pyspark.sql import SparkSession

from housing_linear_regression.context import PipelineContext

from .context import HousingPath
from .pipeline import DATA_DIR, build_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.appName("housing-linear-regression-pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        context = PipelineContext(spark, HousingPath(str(DATA_DIR / "housing.parquet")))
        build_pipeline().run(context)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
