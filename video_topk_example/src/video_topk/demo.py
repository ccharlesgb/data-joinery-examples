"""Generate local view events and demonstrate rankings across stream restarts."""

from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from data_joinery import Schema
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from .context import (
    CheckpointPath,
    InputPath,
    OutputPath,
    PipelineContext,
    TopKSettings,
    create_spark,
)
from .schemas import VideoView


def demo_time(minute: int, second: int = 0) -> datetime:
    return datetime(2026, 1, 1, 12, minute, second, tzinfo=UTC)


def append_views(spark: SparkSession, path: str, rows: list[VideoView]) -> None:
    frame = Schema(VideoView).create_dataframe(rows, DataFrame, session=spark)
    frame.coalesce(1).write.format("delta").mode("append").save(path)


def seed_views(spark: SparkSession, path: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        append_views(
            spark,
            path,
            [
                VideoView(demo_time(1), "music", "acoustic-session"),
                VideoView(demo_time(1, 10), "music", "acoustic-session"),
                VideoView(demo_time(1, 20), "music", "synth-live"),
                VideoView(demo_time(1, 30), "gaming", "speedrun"),
                VideoView(demo_time(1, 40), "gaming", "speedrun"),
                VideoView(demo_time(1, 50), "gaming", "build-guide"),
            ],
        )


def main() -> None:
    from .pipeline import build_pipeline

    spark = create_spark()
    try:
        # Isolated demo data: running this again never overwrites previous runs.
        with TemporaryDirectory(prefix="video-topk-") as directory:
            root = Path(directory)
            context = PipelineContext(
                spark=spark,
                input_path=InputPath(str(root / "views")),
                output_path=OutputPath(str(root / "leaderboards")),
                checkpoint_path=CheckpointPath(str(root / "checkpoint")),
                settings=TopKSettings(),
            )
            seed_views(spark, context.input_path)
            batches = [
                ("Initial rankings", []),
                (
                    "Synth-live takes the lead; counts survive a restart",
                    [
                        VideoView(demo_time(2, i), "music", "synth-live")
                        for i in range(3)
                    ],
                ),
                (
                    "An out-of-order view is still accepted",
                    [VideoView(demo_time(1, 15), "gaming", "build-guide")],
                ),
                (
                    "Newer views advance the watermark and finalize the first window",
                    [VideoView(demo_time(8), "gaming", "new-release")],
                ),
            ]
            for label, events in batches:
                if events:
                    append_views(spark, context.input_path, events)
                build_pipeline().run(context)
                print(label)
                spark.read.format("delta").load(context.output_path).orderBy(
                    "window_start", "category"
                ).show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
