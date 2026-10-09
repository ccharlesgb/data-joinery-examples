from dataclasses import asdict
from datetime import date

import polars as pl
import pytest
from data_joinery import Schema
from polars.testing import assert_frame_equal
from pyspark.sql import DataFrame, SparkSession
from sklearn_spark.context import AdvertEventsPath, AdvertPipelineContext
from sklearn_spark.pipeline import build_pipeline
from sklearn_spark.schemas import (
    AdvertEvents,
    AdvertPredictions,
    PredictionMetrics,
)


def test_pipeline_reads_trains_predicts_and_reports(
    spark: SparkSession,
    tmp_path,
    advert_events_rows: list[AdvertEvents],
    capsys: pytest.CaptureFixture[str],
):
    events = Schema(AdvertEvents).create_dataframe(
        advert_events_rows, DataFrame, session=spark
    )
    source_path = tmp_path / "advert_events"
    events.write.parquet(str(source_path))

    result = build_pipeline().run(
        AdvertPipelineContext(
            spark=spark,
            advert_events_path=AdvertEventsPath(str(source_path)),
        ),
    )

    predictions = result.get_output("predict_training_set", pl.DataFrame)
    metrics = result.get_input(
        "print_prediction_metrics", "prediction_metrics", PredictionMetrics
    )
    expected = Schema(AdvertPredictions).create_dataframe(
        [
            AdvertPredictions(
                date(2026, 9, 1),
                "00000000-0000-0000-0000-000000000001",
                "m1a1a1",
                "ford",
                "focus",
                1,
                2,
                1.0,
                2.0,
            ),
            AdvertPredictions(
                date(2026, 9, 1),
                "00000000-0000-0000-0000-000000000002",
                "m1a1a1",
                "ford",
                "fiesta",
                2,
                3,
                2.0,
                3.0,
            ),
            AdvertPredictions(
                date(2026, 9, 1),
                "00000000-0000-0000-0000-000000000003",
                "v6b1a1",
                "honda",
                "civic",
                3,
                4,
                3.0,
                4.0,
            ),
            AdvertPredictions(
                date(2026, 9, 1),
                "00000000-0000-0000-0000-000000000004",
                "v6b1a1",
                "honda",
                "accord",
                4,
                5,
                4.0,
                5.0,
            ),
        ],
        pl.DataFrame,
    )

    assert_frame_equal(
        predictions.sort("advert_id"),
        expected.sort("advert_id"),
        check_exact=False,
        abs_tol=1e-9,
    )
    assert asdict(metrics) == pytest.approx(
        asdict(PredictionMetrics(0.0, 0.0, 1.0)), abs=1e-10
    )
    assert str(metrics) in capsys.readouterr().out
