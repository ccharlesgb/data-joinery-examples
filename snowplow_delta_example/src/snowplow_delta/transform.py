from typing import Annotated

from data_joinery import Context, Project, Strict, transform
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from .context import CheckpointPath, SnowplowDeltaPath
from .schemas import SnowplowEvent, WindowedEventCount

WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "1 minute"
QUERY_NAME = "snowplow-event-counts"


@transform
def read_snowplow_delta_stream(
    spark: Annotated[SparkSession, Context()],
    path: Annotated[SnowplowDeltaPath, Context()],
) -> Annotated[DataFrame, Project(SnowplowEvent)]:
    """Read Snowplow enriched events from a Delta streaming source."""
    return spark.readStream.format("delta").load(path)


@transform
def count_events_by_name(
    events: Annotated[DataFrame, Project(SnowplowEvent)],
) -> Annotated[DataFrame, Strict(WindowedEventCount)]:
    """Count events by self-describing event name and one-minute event-time window."""
    return (
        events.withWatermark("collector_tstamp", WATERMARK_DELAY)
        .groupBy(
            F.window("collector_tstamp", WINDOW_DURATION).alias("window"),
            F.col("event_name"),
        )
        .agg(F.count(F.lit(1)).cast("long").alias("event_count"))
        .select("window", "event_name", "event_count")
    )


def print_event_count_microbatch(batch: DataFrame, batch_id: int) -> None:
    """Collect the small aggregated microbatch and print each updated count."""
    rows: list[Row] = (
        batch.select(
            F.date_format("window.start", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                "window_start"
            ),
            F.date_format("window.end", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("window_end"),
            "event_name",
            "event_count",
        )
        .orderBy("window_start", "event_name")
        .collect()
    )
    print(f"Microbatch {batch_id}")
    if not rows:
        print("  no updated event counts")
        return

    for row in rows:
        event_name = row.event_name if row.event_name is not None else "<null>"
        print(
            f"  [{row.window_start}, {row.window_end}) {event_name}: {row.event_count}"
        )


@transform
def start_printing_event_counts(
    event_counts: Annotated[DataFrame, Strict(WindowedEventCount)],
    checkpoint_path: Annotated[CheckpointPath, Context()],
) -> StreamingQuery:
    """Start a checkpointed query that prints every microbatch of count updates."""
    return (
        event_counts.writeStream.queryName(QUERY_NAME)
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(print_event_count_microbatch)
        .start()
    )
