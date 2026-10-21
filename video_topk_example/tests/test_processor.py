from collections import Counter
from collections.abc import Iterator
from copy import deepcopy
from datetime import UTC, datetime
from random import Random
from unittest.mock import Mock

import pytest
from pyspark.sql import Row
from pyspark.sql.streaming import StatefulProcessorHandle
from pyspark.sql.streaming.stateful_processor import (
    ExpiredTimerInfo,
    MapState,
    TimerValues,
    ValueState,
)
from video_topk.processor import VideoCountEntry, VideoTopKProcessor

WINDOW = (0, 300_000, "music")


class InMemoryMapState(MapState):
    """The subset of Spark's map state used by the processor, without a JVM."""

    def __init__(self) -> None:
        self.entries: dict[tuple[str], tuple[int]] = {}
        self.scans = 0

    def getValue(self, key: tuple[str]) -> tuple[int] | None:
        return self.entries.get(key)

    def updateValue(self, key: tuple[str], value: tuple[int]) -> None:
        self.entries[key] = value

    def iterator(self) -> Iterator[VideoCountEntry]:
        self.scans += 1
        return iter(self.entries.items())

    def exists(self) -> bool:
        return bool(self.entries)

    def clear(self) -> None:
        self.entries.clear()


class InMemoryValueState(ValueState):
    """Copy values at the state boundary, just as Spark serialization does."""

    def __init__(self) -> None:
        self.value: tuple[int, dict[str, int]] | None = None

    def get(self) -> tuple[int, dict[str, int]] | None:
        return deepcopy(self.value)

    def update(self, newValue: tuple[int, dict[str, int]]) -> None:
        self.value = deepcopy(newValue)

    def clear(self) -> None:
        self.value = None


@pytest.fixture
def counts() -> InMemoryMapState:
    return InMemoryMapState()


@pytest.fixture
def top_k() -> InMemoryValueState:
    return InMemoryValueState()


@pytest.fixture
def handle(counts: InMemoryMapState, top_k: InMemoryValueState) -> Mock:
    handle = Mock(spec=StatefulProcessorHandle)
    handle.getMapState.return_value = counts
    handle.getValueState.return_value = top_k
    return handle


@pytest.fixture
def processor(handle: Mock) -> VideoTopKProcessor:
    processor = VideoTopKProcessor(k=2)
    processor.init(handle)
    return processor


def expected_snapshot(videos: list[Row], *, is_final: bool = False) -> Row:
    return Row(
        window_start=datetime(1970, 1, 1, tzinfo=UTC),
        window_end=datetime(1970, 1, 1, 0, 5, tzinfo=UTC),
        category="music",
        top_videos=videos,
        is_final=is_final,
    )


@pytest.mark.parametrize(
    "video_ids",
    [("c", "b", "a", "b", "a"), ("a", "b", "a", "b", "c")],
)
def test_ranking_is_deterministic_and_limited_to_k(
    processor: VideoTopKProcessor, video_ids: tuple[str, ...]
) -> None:
    rows = (Row(video_id=video_id, event_time_us=2_000_000) for video_id in video_ids)

    actual = list(
        processor.handleInputRows(WINDOW, rows, TimerValues(currentWatermarkInMs=1_000))
    )

    assert actual == [
        expected_snapshot(
            [
                Row(rank=1, video_id="a", view_count=2),
                Row(rank=2, video_id="b", view_count=2),
            ]
        )
    ]


def test_counts_accumulate_for_videos_outside_top_k(
    processor: VideoTopKProcessor, counts: InMemoryMapState
) -> None:
    timers = TimerValues(currentWatermarkInMs=1_000)
    initial = (
        Row(video_id=video_id, event_time_us=2_000_000)
        for video_id in ("a", "a", "b", "b", "c")
    )
    list(processor.handleInputRows(WINDOW, initial, timers))
    assert counts.entries == {("a",): (2,), ("b",): (2,), ("c",): (1,)}

    returning = iter([Row(video_id="c", event_time_us=3_000_000) for _ in range(2)])
    actual = list(processor.handleInputRows(WINDOW, returning, timers))

    assert actual == [
        expected_snapshot(
            [
                Row(rank=1, video_id="c", view_count=3),
                Row(rank=2, video_id="a", view_count=2),
            ]
        )
    ]


@pytest.mark.parametrize("event_time_us", [999_999, 1_000_000])
def test_events_at_or_before_watermark_do_not_create_state(
    processor: VideoTopKProcessor,
    counts: InMemoryMapState,
    handle: Mock,
    top_k: InMemoryValueState,
    event_time_us: int,
) -> None:
    actual = list(
        processor.handleInputRows(
            WINDOW,
            iter([Row(video_id="a", event_time_us=event_time_us)]),
            TimerValues(currentWatermarkInMs=1_000),
        )
    )

    assert actual == []
    assert counts.entries == {}
    assert top_k.get() is None
    handle.registerTimer.assert_not_called()


def test_event_one_microsecond_after_watermark_is_accepted(
    processor: VideoTopKProcessor, handle: Mock
) -> None:
    actual = list(
        processor.handleInputRows(
            WINDOW,
            iter([Row(video_id="a", event_time_us=1_000_001)]),
            TimerValues(currentWatermarkInMs=1_000),
        )
    )

    assert actual == [expected_snapshot([Row(rank=1, video_id="a", view_count=1)])]
    handle.registerTimer.assert_called_once_with(300_000)


@pytest.mark.parametrize("watermark_ms", [300_000, 300_001])
def test_closed_window_cannot_be_recreated(
    processor: VideoTopKProcessor,
    counts: InMemoryMapState,
    handle: Mock,
    top_k: InMemoryValueState,
    watermark_ms: int,
) -> None:
    # A newer event isolates the window guard from the event lateness guard.
    actual = list(
        processor.handleInputRows(
            WINDOW,
            iter([Row(video_id="a", event_time_us=400_000_000)]),
            TimerValues(currentWatermarkInMs=watermark_ms),
        )
    )

    assert actual == []
    assert counts.entries == {}
    assert top_k.get() is None
    handle.registerTimer.assert_not_called()


def test_expiration_emits_final_snapshot_and_clears_state(
    processor: VideoTopKProcessor,
    counts: InMemoryMapState,
    top_k: InMemoryValueState,
) -> None:
    counts.entries = {("b",): (1,), ("a",): (2,)}

    actual = list(
        processor.handleExpiredTimer(
            WINDOW,
            TimerValues(currentWatermarkInMs=300_001),
            ExpiredTimerInfo(expiryTimeInMs=300_000),
        )
    )

    assert actual == [
        expected_snapshot(
            [
                Row(rank=1, video_id="a", view_count=2),
                Row(rank=2, video_id="b", view_count=1),
            ],
            is_final=True,
        )
    ]
    assert counts.entries == {}
    assert top_k.get() is None


def test_repeated_expiration_emits_nothing(
    processor: VideoTopKProcessor, counts: InMemoryMapState
) -> None:
    counts.entries = {("a",): (1,)}
    timers = TimerValues(currentWatermarkInMs=300_001)
    expired = ExpiredTimerInfo(expiryTimeInMs=300_000)
    list(processor.handleExpiredTimer(WINDOW, timers, expired))

    assert list(processor.handleExpiredTimer(WINDOW, timers, expired)) == []


def test_empty_input_emits_nothing_and_registers_no_timer(
    processor: VideoTopKProcessor, handle: Mock
) -> None:
    actual = list(
        processor.handleInputRows(
            WINDOW, iter([]), TimerValues(currentWatermarkInMs=1_000)
        )
    )

    assert actual == []
    handle.registerTimer.assert_not_called()


def test_cache_survives_processor_restart_without_scanning_counts(
    processor: VideoTopKProcessor,
    handle: Mock,
    counts: InMemoryMapState,
    top_k: InMemoryValueState,
) -> None:
    timers = TimerValues(currentWatermarkInMs=1_000)
    initial = (
        Row(video_id=video_id, event_time_us=2_000_000)
        for video_id in ("a", "a", "b", "b", "c")
    )
    list(processor.handleInputRows(WINDOW, initial, timers))
    assert top_k.get() == (2, {"a": 2, "b": 2})

    restarted = VideoTopKProcessor(k=2)
    restarted.init(handle)
    counts.scans = 0
    list(
        restarted.handleInputRows(
            WINDOW,
            iter([Row(video_id="c", event_time_us=3_000_000) for _ in range(2)]),
            timers,
        )
    )
    assert top_k.get() == (2, {"a": 2, "c": 3})

    actual = list(
        restarted.handleExpiredTimer(
            WINDOW,
            TimerValues(currentWatermarkInMs=300_001),
            ExpiredTimerInfo(expiryTimeInMs=300_000),
        )
    )
    assert actual == [
        expected_snapshot(
            [
                Row(rank=1, video_id="c", view_count=3),
                Row(rank=2, video_id="a", view_count=2),
            ],
            is_final=True,
        )
    ]
    assert counts.scans == 0
    assert counts.entries == {}
    assert top_k.get() is None


def test_missing_cache_is_rebuilt_once_before_incrementing_existing_counts(
    processor: VideoTopKProcessor,
    counts: InMemoryMapState,
    top_k: InMemoryValueState,
) -> None:
    counts.entries = {("a",): (5,), ("b",): (4,), ("c",): (3,)}
    timers = TimerValues(currentWatermarkInMs=1_000)

    for _ in range(2):
        actual = list(
            processor.handleInputRows(
                WINDOW, iter([Row(video_id="c", event_time_us=2_000_000)]), timers
            )
        )

    assert actual == [
        expected_snapshot(
            [
                Row(rank=1, video_id="a", view_count=5),
                Row(rank=2, video_id="c", view_count=5),
            ]
        )
    ]
    assert top_k.get() == (2, {"a": 5, "c": 5})
    assert counts.scans == 1


@pytest.mark.parametrize("k", [1, 3])
def test_changing_k_rebuilds_cache_from_full_counts(
    handle: Mock,
    counts: InMemoryMapState,
    top_k: InMemoryValueState,
    k: int,
) -> None:
    counts.entries = {("a",): (5,), ("b",): (4,), ("c",): (3,)}
    top_k.update((2, {"a": 5, "b": 4}))
    restarted = VideoTopKProcessor(k)
    restarted.init(handle)

    list(
        restarted.handleInputRows(
            WINDOW,
            iter([Row(video_id="c", event_time_us=2_000_000)]),
            TimerValues(currentWatermarkInMs=1_000),
        )
    )

    expected = {"a": 5} if k == 1 else {"a": 5, "b": 4, "c": 4}
    assert top_k.get() == (k, expected)
    assert counts.scans == 1


@pytest.mark.parametrize("k", [1, 2, 5, 20])
def test_incremental_rankings_match_full_counts_across_batches(
    handle: Mock, k: int
) -> None:
    processor = VideoTopKProcessor(k)
    processor.init(handle)
    random = Random(42)
    totals: Counter[str] = Counter()
    timers = TimerValues(currentWatermarkInMs=1_000)

    for _ in range(30):
        video_ids = [f"video-{random.randrange(15):02}" for _ in range(7)]
        totals.update(video_ids)
        rows = (
            Row(video_id=video_id, event_time_us=2_000_000) for video_id in video_ids
        )
        actual = list(processor.handleInputRows(WINDOW, rows, timers))
        expected = sorted(totals.items(), key=lambda item: (-item[1], item[0]))[:k]

        assert actual == [
            expected_snapshot(
                [
                    Row(rank=rank, video_id=video_id, view_count=view_count)
                    for rank, (video_id, view_count) in enumerate(expected, start=1)
                ]
            )
        ]
