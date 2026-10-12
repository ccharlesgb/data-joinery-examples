from __future__ import annotations

import atexit
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from data_joinery.backends.spark import SparkContext
from pyspark.sql import SparkSession


class SnapshottedDimensionPath(str):
    """
    Marker class for the snapshotted dimension path context.
    """


class OutputPath(str):
    """
    Marker class for the output path context.
    """


class RunDate(date):
    """
    Marker class for the run date context.
    """


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    snapshotted_dimension_path: SnapshottedDimensionPath
    output_path: OutputPath
    run_date: RunDate

    @classmethod
    def from_kwargs(
        cls,
        *,
        snapshotted_dimension_path: str | None = None,
        output_path: str | None = None,
        run_date: str = "2026-01-01",
    ) -> PipelineContext:
        example_dir = Path(__file__).resolve().parents[2]
        parsed_run_date = RunDate.fromisoformat(run_date)
        spark = (
            SparkSession.builder.appName("snapshot-diff-pipeline")
            .master("local[*]")
            .getOrCreate()
        )
        atexit.register(spark.stop)
        return cls(
            spark=spark,
            snapshotted_dimension_path=SnapshottedDimensionPath(
                snapshotted_dimension_path
                if snapshotted_dimension_path is not None
                else str(example_dir / "data" / "snapshotted_dimension.parquet")
            ),
            output_path=OutputPath(
                output_path
                if output_path is not None
                else str(example_dir / "__output" / "output.parquet")
            ),
            run_date=parsed_run_date,
        )
