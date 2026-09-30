import argparse

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from .context import CheckpointPath, SnowplowDeltaContext, SnowplowDeltaPath
from .pipeline import build_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print one-minute Snowplow event counts from a Delta stream."
    )
    parser.add_argument("delta_path", help="Path to the Snowplow Delta event table")
    parser.add_argument(
        "checkpoint_path", help="Durable checkpoint path for this streaming query"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    builder = (
        SparkSession.builder.appName("snowplow-delta-event-counts")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    query: StreamingQuery | None = None

    try:
        outputs = build_pipeline().run(
            SnowplowDeltaContext(
                spark=spark,
                delta_path=SnowplowDeltaPath(args.delta_path),
                checkpoint_path=CheckpointPath(args.checkpoint_path),
            )
        )
        query = outputs["start_printing_event_counts"]
        if query is not None:
            query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
