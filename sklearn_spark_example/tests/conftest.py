from collections.abc import Generator
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from sklearn_spark.schemas import AdvertEvents


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("advert-linear-regression-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def advert_events_rows() -> list[AdvertEvents]:
    event_time = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    advert_dimensions = [
        ("00000000-0000-0000-0000-000000000001", "m1a1a1", "ford", "focus", 1, 2),
        ("00000000-0000-0000-0000-000000000002", "m1a1a1", "ford", "fiesta", 2, 3),
        ("00000000-0000-0000-0000-000000000003", "v6b1a1", "honda", "civic", 3, 4),
        ("00000000-0000-0000-0000-000000000004", "v6b1a1", "honda", "accord", 4, 5),
    ]
    events: list[AdvertEvents] = []
    event_number = 0
    for (
        advert_id,
        postcode,
        make,
        model,
        search_views,
        advert_views,
    ) in advert_dimensions:
        for event_type, count in (
            ("SEARCH_VIEW", search_views),
            ("ADVERT_VIEW", advert_views),
        ):
            for _ in range(count):
                event_number += 1
                events.append(
                    AdvertEvents(
                        event_timestamp=event_time,
                        event_id=f"10000000-0000-0000-0000-{event_number:012d}",
                        advert_id=advert_id,
                        postcode=postcode,
                        vehicle_make=make,
                        vehicle_model=model,
                        event_type=event_type,
                    )
                )
    return events
