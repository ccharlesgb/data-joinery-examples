from pyspark.sql import SparkSession

from .context import AdvertEventsPath, AdvertPipelineContext
from .pipeline import DATA_DIR, build_pipeline

spark = (
    SparkSession.builder.appName("advert-view-linear-regression-pipeline")
    .master("local[*]")
    .getOrCreate()
)

try:
    pipeline = build_pipeline()
    context = AdvertPipelineContext(
        spark=spark,
        advert_events_path=AdvertEventsPath(str(DATA_DIR / "advert_events.parquet")),
    )
    pipeline.run(context)
finally:
    spark.stop()
