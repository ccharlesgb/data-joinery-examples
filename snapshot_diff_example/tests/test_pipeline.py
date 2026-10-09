from datetime import UTC, date, datetime

from data_joinery import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual
from snapshot_diff.context import (
    OutputPath,
    PipelineContext,
    RunDate,
    SnapshottedDimensionPath,
)
from snapshot_diff.pipeline import build_pipeline
from snapshot_diff.schemas import DimensionTransitions, SnapshottedDimension


def test_pipeline_pairs_previous_and_current_snapshots(spark: SparkSession, tmp_path):
    previous_date = date(2026, 1, 1)
    current_date = date(2026, 1, 2)
    updated_at = datetime(2026, 1, 2, tzinfo=UTC)
    source = Schema(SnapshottedDimension).create_dataframe(
        [
            SnapshottedDimension(previous_date, "changed", "old", "same", updated_at),
            SnapshottedDimension(current_date, "changed", "new", "same", updated_at),
            SnapshottedDimension(previous_date, "unchanged", "a", "b", updated_at),
            SnapshottedDimension(current_date, "unchanged", "a", "b", updated_at),
            SnapshottedDimension(current_date, "new", "x", "y", updated_at),
        ],
        DataFrame,
        session=spark,
    )
    expected = Schema(DimensionTransitions).create_dataframe(
        [
            DimensionTransitions(
                current_date, "changed", "old", "same", "new", "same", updated_at
            ),
            DimensionTransitions(
                current_date, "unchanged", "a", "b", "a", "b", updated_at
            ),
        ],
        DataFrame,
        session=spark,
    )
    source_path = tmp_path / "snapshots"
    output_path = tmp_path / "output"
    source.write.parquet(str(source_path))

    result = build_pipeline().run(
        PipelineContext(
            spark=spark,
            snapshotted_dimension_path=SnapshottedDimensionPath(str(source_path)),
            output_path=OutputPath(str(output_path)),
            run_date=RunDate(2026, 1, 2),
        )
    )

    assertDataFrameEqual(
        result.get_output("compute_dimension_transitions", DataFrame), expected
    )
    assertDataFrameEqual(spark.read.parquet(str(output_path)), expected)
