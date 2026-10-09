from datetime import UTC, date, datetime

from data_joinery import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual
from snapshot_diff.context import OutputPath, RunDate, SnapshottedDimensionPath
from snapshot_diff.schemas import DimensionTransitions, SnapshottedDimension
from snapshot_diff.transform import (
    compute_dimension_transitions,
    get_current_snapshot,
    get_previous_snapshot,
    write_output,
)


def test_get_current_snapshot_selects_run_date(spark: SparkSession, tmp_path):
    timestamp = datetime(2026, 1, 2, tzinfo=UTC)
    previous = SnapshottedDimension(date(2026, 1, 1), "one", "a", "b", timestamp)
    current = SnapshottedDimension(date(2026, 1, 2), "one", "c", "d", timestamp)
    source = Schema(SnapshottedDimension).create_dataframe(
        [previous, current], DataFrame, session=spark
    )
    expected = Schema(SnapshottedDimension).create_dataframe(
        [current], DataFrame, session=spark
    )
    path = tmp_path / "snapshots"
    source.write.parquet(str(path))

    actual = get_current_snapshot(
        spark, SnapshottedDimensionPath(str(path)), RunDate(2026, 1, 2)
    )

    assertDataFrameEqual(actual, expected)


def test_get_previous_snapshot_selects_prior_date(spark: SparkSession, tmp_path):
    timestamp = datetime(2026, 1, 2, tzinfo=UTC)
    previous = SnapshottedDimension(date(2026, 1, 1), "one", "a", "b", timestamp)
    current = SnapshottedDimension(date(2026, 1, 2), "one", "c", "d", timestamp)
    source = Schema(SnapshottedDimension).create_dataframe(
        [previous, current], DataFrame, session=spark
    )
    expected = Schema(SnapshottedDimension).create_dataframe(
        [previous], DataFrame, session=spark
    )
    path = tmp_path / "snapshots"
    source.write.parquet(str(path))

    actual = get_previous_snapshot(
        spark, SnapshottedDimensionPath(str(path)), RunDate(2026, 1, 2)
    )

    assertDataFrameEqual(actual, expected)


def test_compute_dimension_transitions_pairs_matching_entities(spark: SparkSession):
    timestamp = datetime(2026, 1, 2, tzinfo=UTC)
    previous = Schema(SnapshottedDimension).create_dataframe(
        [
            SnapshottedDimension(date(2026, 1, 1), "one", "old", "same", timestamp),
            SnapshottedDimension(date(2026, 1, 1), "gone", "x", "y", timestamp),
        ],
        DataFrame,
        session=spark,
    )
    current = Schema(SnapshottedDimension).create_dataframe(
        [
            SnapshottedDimension(date(2026, 1, 2), "one", "new", "same", timestamp),
            SnapshottedDimension(date(2026, 1, 2), "new", "x", "y", timestamp),
        ],
        DataFrame,
        session=spark,
    )
    expected = Schema(DimensionTransitions).create_dataframe(
        [
            DimensionTransitions(
                date(2026, 1, 2), "one", "old", "same", "new", "same", timestamp
            )
        ],
        DataFrame,
        session=spark,
    )

    assertDataFrameEqual(compute_dimension_transitions(previous, current), expected)


def test_write_output_writes_parquet(spark: SparkSession, tmp_path):
    expected = Schema(DimensionTransitions).create_dataframe(
        [
            DimensionTransitions(
                date(2026, 1, 2),
                "one",
                "old",
                "same",
                "new",
                "same",
                datetime(2026, 1, 2, tzinfo=UTC),
            )
        ],
        DataFrame,
        session=spark,
    )
    path = tmp_path / "output"

    write_output(expected, OutputPath(str(path)))

    assertDataFrameEqual(spark.read.parquet(str(path)), expected)
