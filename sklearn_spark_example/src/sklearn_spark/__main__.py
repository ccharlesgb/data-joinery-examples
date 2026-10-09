from pyspark.sql import SparkSession

from .context import AdvertEventsPath, AdvertPipelineContext
from .pipeline import DATA_DIR, build_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.appName("advert-view-linear-regression-pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        context = AdvertPipelineContext(
            spark=spark,
            advert_events_path=AdvertEventsPath(
                str(DATA_DIR / "advert_events.parquet")
            ),
        )
        build_pipeline().run(context)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
