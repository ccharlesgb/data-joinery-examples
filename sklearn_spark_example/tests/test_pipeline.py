from collections.abc import Generator
from datetime import UTC, datetime
from typing import Annotated

import polars as pl
import pytest
from data_joinery import Context, ProjectCast, Schema, transform
from pyspark.sql import DataFrame, SparkSession
from sklearn_spark.context import AdvertEventsPath, AdvertPipelineContext
from sklearn_spark.pipeline import build_pipeline
from sklearn_spark.schemas import (
    AdvertEvents,
    AdvertPredictions,
    PredictionMetrics,
)
from sklearn_spark.transform import create_advert_features


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession]:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("advert-linear-regression-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def advert_events() -> list[AdvertEvents]:
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


def test_create_advert_features_counts_each_event_type(spark: SparkSession):
    events = Schema(AdvertEvents).create_dataframe(
        advert_events(), DataFrame, session=spark
    )

    features = create_advert_features(events)
    counts = {
        row.advert_id: (
            row.number_of_search_views,
            row.number_of_advert_views,
        )
        for row in features.collect()
    }

    assert counts["00000000-0000-0000-0000-000000000001"] == (1, 2)
    assert counts["00000000-0000-0000-0000-000000000004"] == (4, 5)


def test_pipeline_end_to_end_with_reader_override(
    spark: SparkSession, capsys: pytest.CaptureFixture[str]
):
    @transform
    def read_test_advert_events(
        spark: Annotated[SparkSession, Context()],
    ) -> Annotated[DataFrame, ProjectCast(AdvertEvents)]:
        return Schema(AdvertEvents).create_dataframe(
            advert_events(), DataFrame, session=spark
        )

    outputs = build_pipeline().run(
        AdvertPipelineContext(
            spark=spark,
            advert_events_path=AdvertEventsPath("unused-in-test"),
        ),
        transform_overrides={"read_advert_events": read_test_advert_events},
    )

    predictions = outputs["predict_training_set"]
    metrics = outputs["calculate_prediction_metrics"]

    assert isinstance(predictions, pl.DataFrame)
    assert predictions.height == 4
    assert predictions.columns == [
        field.name for field in AdvertPredictions.__dataclass_fields__.values()
    ]
    assert isinstance(metrics, PredictionMetrics)
    assert metrics.mean_absolute_error == pytest.approx(0.0, abs=1e-10)
    assert metrics.mean_squared_error == pytest.approx(0.0, abs=1e-10)
    assert metrics.r2_score == pytest.approx(1.0)
    assert str(metrics) in capsys.readouterr().out
