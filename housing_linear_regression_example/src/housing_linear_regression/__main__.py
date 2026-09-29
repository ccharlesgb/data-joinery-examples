from pyspark.sql import SparkSession

from housing_linear_regression.context import PipelineContext

from .context import HousingPath
from .pipeline import DATA_DIR, build_pipeline

spark = (
    SparkSession.builder.appName("housing-linear-regression-pipeline")
    .master("local[*]")
    .getOrCreate()
)

pipeline = build_pipeline()
context = PipelineContext(spark, HousingPath(str(DATA_DIR / "housing.parquet")))
pipeline.run(context)

spark.stop()
