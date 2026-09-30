from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from data_joinery import Schema
from pyspark.sql import DataFrame, SparkSession
from snowplow_delta.schemas import SnowplowEvent
from snowplow_delta.transform import (
    count_events_by_name,
    print_event_count_microbatch,
)


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("snowplow-delta-transform-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_count_events_by_name_uses_one_minute_windows(spark: SparkSession):
    events = Schema(SnowplowEvent).create_dataframe(
        [
            SnowplowEvent(datetime(2026, 9, 29, 12, 0, 5, tzinfo=UTC), "page_view"),
            SnowplowEvent(datetime(2026, 9, 29, 12, 0, 45, tzinfo=UTC), "page_view"),
            SnowplowEvent(datetime(2026, 9, 29, 12, 0, 50, tzinfo=UTC), "add_to_cart"),
            SnowplowEvent(datetime(2026, 9, 29, 12, 1, 1, tzinfo=UTC), "page_view"),
            SnowplowEvent(datetime(2026, 9, 29, 12, 1, 15, tzinfo=UTC), None),
        ],
        DataFrame,
        session=spark,
    )

    counts = count_events_by_name(events).collect()
    actual = {
        (row.window.start.minute, row.event_name): row.event_count for row in counts
    }

    assert actual == {
        (0, "page_view"): 2,
        (0, "add_to_cart"): 1,
        (1, "page_view"): 1,
        (1, None): 1,
    }


def test_print_event_count_microbatch_collects_and_prints_counts(
    spark: SparkSession, capsys: pytest.CaptureFixture[str]
):
    events = Schema(SnowplowEvent).create_dataframe(
        [
            SnowplowEvent(datetime(2026, 9, 29, 12, 0, 5, tzinfo=UTC), "page_view"),
            SnowplowEvent(datetime(2026, 9, 29, 12, 0, 15, tzinfo=UTC), "page_view"),
        ],
        DataFrame,
        session=spark,
    )

    print_event_count_microbatch(count_events_by_name(events), batch_id=7)

    output = capsys.readouterr().out
    assert "Microbatch 7" in output
    assert "[2026-09-29T12:00:00Z, 2026-09-29T12:01:00Z) page_view: 2" in output
