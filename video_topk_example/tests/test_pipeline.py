import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from data_joinery import Schema
from pyspark.sql import DataFrame, Row
from pyspark.sql.streaming import StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import ValueState
from pyspark.testing import assertDataFrameEqual
from video_topk.context import (
    CheckpointPath,
    InputPath,
    OutputPath,
    PipelineContext,
    TopKSettings,
    create_spark,
)
from video_topk.demo import append_views, demo_time
from video_topk.pipeline import build_pipeline
from video_topk.processor import VideoTopKProcessor
from video_topk.schemas import Leaderboard, RankedVideo, VideoView
from video_topk.transform import merge_leaderboards


class UncheckpointedCache(ValueState):
    """Keep the first run's checkpoint in the original, counts-only format."""

    def __init__(self) -> None:
        pass

    def get(self) -> None:
        return None

    def update(self, newValue: tuple[int, dict[str, int]]) -> None:
        pass

    def clear(self) -> None:
        pass


class CountsOnlyProcessor(VideoTopKProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.counts = handle.getMapState(
            "video_counts", "video_id STRING", "view_count LONG"
        )
        self.top_k = UncheckpointedCache()


def test_live_rankings_restart_lateness_and_finalization(tmp_path):
    # Delta jars must be present when the JVM starts. Isolate this test from
    # the other examples' session-scoped, non-Delta Spark fixtures.
    subprocess.run([sys.executable, __file__, str(tmp_path)], check=True, timeout=240)


def exercise_streaming_pipeline(spark, tmp_path):
    context = PipelineContext(
        spark=spark,
        input_path=InputPath(str(tmp_path / "views")),
        output_path=OutputPath(str(tmp_path / "leaderboards")),
        checkpoint_path=CheckpointPath(str(tmp_path / "checkpoint")),
        settings=TopKSettings(k=2),
    )
    pipeline = build_pipeline()
    expected_schema = Schema(Leaderboard)

    def assert_output(expected):
        assertDataFrameEqual(
            spark.read.format("delta").load(context.output_path),
            expected_schema.create_dataframe(expected, DataFrame, session=spark),
        )

    music = Leaderboard(
        demo_time(0),
        demo_time(5),
        "music",
        [
            RankedVideo(1, "a", 2),
            RankedVideo(2, "b", 1),
        ],
        False,
    )
    gaming = Leaderboard(
        demo_time(0),
        demo_time(5),
        "gaming",
        [
            RankedVideo(1, "a", 1),
        ],
        False,
    )
    append_views(
        spark,
        context.input_path,
        [
            VideoView(demo_time(1), "music", "a"),
            VideoView(demo_time(1, 10), "music", "a"),
            VideoView(demo_time(1, 20), "music", "b"),
            VideoView(demo_time(1, 30), "music", "c"),
            VideoView(demo_time(1, 40), "gaming", "a"),
        ],
    )
    # Start with only the original MapState registered in Spark. The next run
    # adds ValueState to the same checkpoint and rebuilds the missing caches.
    with patch("video_topk.transform.VideoTopKProcessor", CountsOnlyProcessor):
        pipeline.run(context)
    assert_output([music, gaming])

    # An initially excluded video enters the TOPK after checkpoint recovery;
    # ties use video_id and the displaced entry disappears from the array.
    append_views(
        spark,
        context.input_path,
        [
            VideoView(demo_time(2), "music", "c"),
            VideoView(demo_time(2, 1), "music", "c"),
        ],
    )
    pipeline.run(context)
    music = replace(music, top_videos=[RankedVideo(1, "c", 3), RankedVideo(2, "a", 2)])
    assert_output([music, gaming])

    # Out-of-order input is newer than the current watermark (12:00:01).
    append_views(
        spark, context.input_path, [VideoView(demo_time(1, 15), "gaming", "a")]
    )
    pipeline.run(context)
    gaming = replace(gaming, top_videos=[RankedVideo(1, "a", 2)])
    assert_output([music, gaming])

    # Tumbling windows are [start, end): 12:05 belongs to the next one.
    next_window = Leaderboard(
        demo_time(5), demo_time(10), "music", [RankedVideo(1, "d", 1)], False
    )
    append_views(spark, context.input_path, [VideoView(demo_time(5), "music", "d")])
    pipeline.run(context)
    assert_output([music, gaming, next_window])

    # Too late for the watermark, even though the first window is still open.
    append_views(spark, context.input_path, [VideoView(demo_time(1), "music", "b")])
    pipeline.run(context)
    assert_output([music, gaming, next_window])

    # A newer event advances watermark past 12:05; timers emit final snapshots.
    append_views(spark, context.input_path, [VideoView(demo_time(8), "music", "d")])
    pipeline.run(context)
    music = replace(music, is_final=True)
    gaming = replace(gaming, is_final=True)
    next_window = replace(next_window, top_videos=[RankedVideo(1, "d", 2)])
    assert_output([music, gaming, next_window])

    # A finalized window cannot be recreated or revised by very late input.
    append_views(spark, context.input_path, [VideoView(demo_time(2), "music", "b")])
    pipeline.run(context)
    assert_output([music, gaming, next_window])

    # A foreachBatch replay replaces the same key without creating duplicates.
    replay = expected_schema.create_dataframe([music, gaming], DataFrame, session=spark)
    merge_leaderboards(replay, context.output_path)
    merge_leaderboards(replay, context.output_path)
    assert_output([music, gaming, next_window])

    # The persistent processing-time trigger also updates while the query stays
    # alive, and stopping it releases the writer's awaitTermination call.
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            pipeline.run,
            replace(
                context,
                settings=replace(
                    context.settings, available_now=False, trigger_interval="1 second"
                ),
            ),
        )
        query = None
        try:
            deadline = time.monotonic() + 30
            while not spark.streams.active:
                if future.done():
                    future.result()
                    raise AssertionError("Persistent query exited before starting")
                if time.monotonic() > deadline:
                    raise AssertionError("Persistent query did not start")
                time.sleep(0.1)
            query = spark.streams.active[0]
            append_views(
                spark, context.input_path, [VideoView(demo_time(9), "music", "d")]
            )
            query.processAllAvailable()
            next_window = replace(next_window, top_videos=[RankedVideo(1, "d", 3)])
            assert_output([music, gaming, next_window])
        finally:
            if query is not None:
                query.stop()
            for active in spark.streams.active:
                active.stop()
        future.result(timeout=30)

    # Both state variables are removed for finalized windows. The open window's
    # cache is persisted with the correct capacity and complete leader counts.
    for state_name in ("video_counts", "top_k_cache"):
        state = (
            spark.read.format("statestore")
            .option("stateVarName", state_name)
            .load(context.checkpoint_path)
        )
        assert state.filter(
            "key.window_start_ms < unix_millis(TIMESTAMP '2026-01-01 12:05:00')"
        ).isEmpty()

    cache = (
        spark.read.format("statestore")
        .option("stateVarName", "top_k_cache")
        .load(context.checkpoint_path)
    )
    assert cache.select("key.category", "value.capacity", "value.counts").collect() == [
        Row(category="music", capacity=2, counts={"d": 3})
    ]


if __name__ == "__main__":
    session = create_spark()
    session.sparkContext.setLogLevel("ERROR")
    try:
        exercise_streaming_pipeline(session, Path(sys.argv[1]))
    finally:
        session.stop()
