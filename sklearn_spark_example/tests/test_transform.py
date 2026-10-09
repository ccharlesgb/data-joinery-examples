from datetime import date

import polars as pl
import pytest
from data_joinery import Schema
from numpy.testing import assert_allclose
from polars.testing import assert_frame_equal
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual
from sklearn_spark.context import AdvertEventsPath
from sklearn_spark.schemas import (
    AdvertEvents,
    AdvertFeatures,
    AdvertPredictions,
    PredictionMetrics,
)
from sklearn_spark.transform import (
    CATEGORICAL_FEATURE_COLUMNS,
    calculate_prediction_metrics,
    collect_advert_features,
    create_advert_features,
    fit_model,
    predict_training_set,
    print_prediction_metrics,
    read_advert_events,
)


def feature_rows() -> list[AdvertFeatures]:
    snapshot_date = date(2026, 9, 1)
    return [
        AdvertFeatures(snapshot_date, "advert-1", "m1a1a1", "ford", "focus", 1, 2),
        AdvertFeatures(snapshot_date, "advert-2", "m1a1a1", "ford", "fiesta", 2, 3),
        AdvertFeatures(snapshot_date, "advert-3", "v6b1a1", "honda", "civic", 3, 4),
        AdvertFeatures(snapshot_date, "advert-4", "v6b1a1", "honda", "accord", 4, 5),
    ]


def test_read_advert_events_reads_parquet(
    spark: SparkSession, tmp_path, advert_events_rows: list[AdvertEvents]
):
    expected = Schema(AdvertEvents).create_dataframe(
        advert_events_rows, DataFrame, session=spark
    )
    path = tmp_path / "events"
    expected.write.parquet(str(path))

    assertDataFrameEqual(
        read_advert_events(spark, AdvertEventsPath(str(path))), expected
    )


def test_create_advert_features_counts_each_event_type(
    spark: SparkSession, advert_events_rows: list[AdvertEvents]
):
    events = Schema(AdvertEvents).create_dataframe(
        advert_events_rows, DataFrame, session=spark
    )
    expected_rows = [
        AdvertFeatures(
            row.snapshot_date,
            f"00000000-0000-0000-0000-{index:012d}",
            row.postcode,
            row.vehicle_make,
            row.vehicle_model,
            row.number_of_search_views,
            row.number_of_advert_views,
        )
        for index, row in enumerate(feature_rows(), start=1)
    ]
    expected = Schema(AdvertFeatures).create_dataframe(
        expected_rows, DataFrame, session=spark
    )

    assertDataFrameEqual(create_advert_features(events), expected)


def test_collect_advert_features_converts_spark_to_polars(spark: SparkSession):
    rows = feature_rows()
    source = Schema(AdvertFeatures).create_dataframe(rows, DataFrame, session=spark)
    expected = Schema(AdvertFeatures).create_dataframe(rows, pl.DataFrame)

    assert_frame_equal(
        collect_advert_features(source).sort("advert_id"),
        expected.sort("advert_id"),
    )


def test_fit_model_learns_training_targets():
    features = Schema(AdvertFeatures).create_dataframe(feature_rows(), pl.DataFrame)

    model = fit_model(features)
    actual = model.predict(features.select(CATEGORICAL_FEATURE_COLUMNS))

    assert_allclose(actual, [[1, 2], [2, 3], [3, 4], [4, 5]], atol=1e-9)


def test_predict_training_set_adds_both_predictions():
    rows = feature_rows()
    features = Schema(AdvertFeatures).create_dataframe(rows, pl.DataFrame)
    model = fit_model(features)
    expected = Schema(AdvertPredictions).create_dataframe(
        [
            AdvertPredictions(
                row.snapshot_date,
                row.advert_id,
                row.postcode,
                row.vehicle_make,
                row.vehicle_model,
                row.number_of_search_views,
                row.number_of_advert_views,
                float(row.number_of_search_views),
                float(row.number_of_advert_views),
            )
            for row in rows
        ],
        pl.DataFrame,
    )

    assert_frame_equal(
        predict_training_set(features, model).sort("advert_id"),
        expected.sort("advert_id"),
        check_exact=False,
        abs_tol=1e-9,
    )


def test_calculate_prediction_metrics_reports_errors():
    predictions = Schema(AdvertPredictions).create_dataframe(
        [
            AdvertPredictions(
                row.snapshot_date,
                row.advert_id,
                row.postcode,
                row.vehicle_make,
                row.vehicle_model,
                row.number_of_search_views,
                row.number_of_advert_views,
                2.5,
                3.5,
            )
            for row in feature_rows()
        ],
        pl.DataFrame,
    )

    assert calculate_prediction_metrics(predictions) == PredictionMetrics(
        1.0, 1.25, 0.0
    )


def test_print_prediction_metrics_displays_values(capsys: pytest.CaptureFixture[str]):
    metrics = PredictionMetrics(1.0, 1.25, 0.0)

    print_prediction_metrics(metrics)

    assert capsys.readouterr().out == f"{metrics}\n"
