"""Exact per-window counts, updated leaderboards, and event-time eviction."""

from collections.abc import Iterator
from datetime import UTC, datetime
from heapq import nsmallest
from typing import NamedTuple, override

from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import (
    ExpiredTimerInfo,
    MapState,
    TimerValues,
    ValueState,
)

MILLISECONDS_PER_SECOND = 1_000
MICROSECONDS_PER_MILLISECOND = 1_000

type GroupingKey = tuple[int, int, str]
type VideoCountEntry = tuple[tuple[str], tuple[int]]


class WindowKey(NamedTuple):
    """Fields in the order used by update_leaderboards' groupBy."""

    start_ms: int
    end_ms: int
    category: str


def leaderboard_order(entry: tuple[str, int]) -> tuple[int, str]:
    """Rank by descending count, breaking ties by ascending video ID."""
    video_id, view_count = entry
    return -view_count, video_id


class VideoTopKProcessor(StatefulProcessor):
    """Use integer epoch keys so timer callbacks are independent of host timezone."""

    handle: StatefulProcessorHandle
    counts: MapState
    top_k: ValueState

    def __init__(self, k: int) -> None:
        self.k = k

    @override
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.counts = handle.getMapState(
            "video_counts", "video_id STRING", "view_count LONG"
        )
        self.top_k = handle.getValueState(
            "top_k_cache", "capacity LONG, counts MAP<STRING, LONG>"
        )

    def _load_top_k(self) -> dict[str, int]:
        cached = self.top_k.get()
        if cached is not None:
            capacity, leaders = cached
            if capacity == self.k:
                return dict(leaders)

        # Bootstrap older checkpoints, or rebuild after a K change. This is the
        # only full count scan; normal updates and snapshots use the bounded cache.
        candidates = (
            (video_id, view_count)
            for ((video_id,), (view_count,)) in self.counts.iterator()
        )
        return dict(nsmallest(self.k, candidates, key=leaderboard_order))

    def _update_top_k(
        self, leaders: dict[str, int], video_id: str, view_count: int
    ) -> None:
        # Counts only increase, so an unchanged outsider cannot displace a leader.
        if video_id in leaders or len(leaders) < self.k:
            leaders[video_id] = view_count
            return

        worst = max(leaders.items(), key=leaderboard_order)
        candidate = (video_id, view_count)
        if leaderboard_order(candidate) < leaderboard_order(worst):
            worst_video_id, _ = worst
            del leaders[worst_video_id]
            leaders[video_id] = view_count

    def _snapshot(
        self, window: WindowKey, leaders: dict[str, int], *, is_final: bool
    ) -> Row:
        best = sorted(leaders.items(), key=leaderboard_order)
        return Row(
            window_start=datetime.fromtimestamp(
                window.start_ms / MILLISECONDS_PER_SECOND, tz=UTC
            ),
            window_end=datetime.fromtimestamp(
                window.end_ms / MILLISECONDS_PER_SECOND, tz=UTC
            ),
            category=window.category,
            top_videos=[
                Row(rank=rank, video_id=video_id, view_count=view_count)
                for rank, (video_id, view_count) in enumerate(best, start=1)
            ],
            is_final=is_final,
        )

    @override
    # Spark's base also accepts pandas iterators; this processor implements only
    # the Row-based transformWithState API, as required by Spark's callback contract.
    def handleInputRows(  # pyrefly: ignore[bad-override]
        self, key: GroupingKey, rows: Iterator[Row], timerValues: TimerValues
    ) -> Iterator[Row]:
        window = WindowKey(*key)
        watermark_ms = timerValues.getCurrentWatermarkInMs()
        watermark_us = watermark_ms * MICROSECONDS_PER_MILLISECOND
        leaders: dict[str, int] | None = None
        for row in rows:
            # The explicit check also prevents a closed window being recreated.
            if window.end_ms <= watermark_ms or row.event_time_us <= watermark_us:
                continue
            if leaders is None:
                leaders = self._load_top_k()
            video = (row.video_id,)
            previous = self.counts.getValue(video)
            (previous_count,) = previous if previous is not None else (0,)
            view_count = previous_count + 1
            self.counts.updateValue(video, (view_count,))
            self._update_top_k(leaders, row.video_id, view_count)
        if leaders is not None:
            self.top_k.update((self.k, leaders))
            self.handle.registerTimer(window.end_ms)
            yield self._snapshot(window, leaders, is_final=False)

    @override
    def handleExpiredTimer(
        self,
        key: GroupingKey,
        timerValues: TimerValues,
        expiredTimerInfo: ExpiredTimerInfo,
    ) -> Iterator[Row]:
        window = WindowKey(*key)
        if self.counts.exists():
            snapshot = self._snapshot(window, self._load_top_k(), is_final=True)
            self.counts.clear()
            self.top_k.clear()
            yield snapshot
