from typing import Annotated

from data_joinery import Context, Project, Schema, Strict, side_effect, transform
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from .context import CheckpointPath, InputPath, OutputPath, TopKSettings
from .processor import VideoTopKProcessor
from .schemas import Leaderboard, VideoView


@transform
def read_views(
    spark: Annotated[SparkSession, Context()],
    path: Annotated[InputPath, Context()],
) -> Annotated[DataFrame, Project(VideoView)]:
    return (
        spark.readStream.format("delta")
        .option("withEventTimeOrder", "true")
        .load(str(path))
    )


@transform
def update_leaderboards(
    views: Annotated[DataFrame, Strict(VideoView)],
    settings: Annotated[TopKSettings, Context()],
) -> Annotated[DataFrame, Strict(Leaderboard)]:
    valid = views.filter(
        F.col("event_time").isNotNull()
        & F.col("category").isNotNull()
        & (F.length("category") > 0)
        & F.col("video_id").isNotNull()
        & (F.length("video_id") > 0)
    )
    windowed = (
        valid.withWatermark("event_time", f"{settings.lateness_minutes} minutes")
        .withColumn(
            "window", F.window("event_time", f"{settings.window_minutes} minutes")
        )
        .select(
            "*",
            F.unix_micros("event_time").alias("event_time_us"),
            F.unix_millis("window.start").alias("window_start_ms"),
            F.unix_millis("window.end").alias("window_end_ms"),
        )
        .drop("window")
    )
    return windowed.groupBy(
        "window_start_ms", "window_end_ms", "category"
    ).transformWithState(
        statefulProcessor=VideoTopKProcessor(settings.k),
        outputStructType=Schema(Leaderboard).native_schema(StructType),
        outputMode="Update",
        timeMode="EventTime",
    )


@side_effect
def merge_leaderboards(
    leaderboards: Annotated[DataFrame, Strict(Leaderboard)],
    path: Annotated[OutputPath, Context()],
) -> None:
    # foreachBatch may replay a batch. Replacing a complete snapshot by its stable
    # window/category key makes retries idempotent, including rank displacement.
    target = DeltaTable.forPath(leaderboards.sparkSession, str(path))
    (
        target.alias("target")
        .merge(
            leaderboards.alias("source"),
            "target.window_start = source.window_start AND "
            "target.window_end = source.window_end AND target.category = source.category",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


@side_effect
def write_leaderboards(
    leaderboards: Annotated[DataFrame, Strict(Leaderboard)],
    path: Annotated[OutputPath, Context()],
    checkpoint: Annotated[CheckpointPath, Context()],
    settings: Annotated[TopKSettings, Context()],
) -> None:
    # Create an empty table without changing an existing target.
    (
        DeltaTable.createIfNotExists(leaderboards.sparkSession)
        .location(str(path))
        .addColumns(Schema(Leaderboard).native_schema(StructType))
        .execute()
    )

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        merge_leaderboards(batch, path)

    writer = (
        leaderboards.writeStream.outputMode("update")
        .option("checkpointLocation", checkpoint)
        .foreachBatch(write_batch)
    )
    writer = (
        writer.trigger(availableNow=True)
        if settings.available_now
        else writer.trigger(processingTime=settings.trigger_interval)
    )
    query = writer.start()
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()
