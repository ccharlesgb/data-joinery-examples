from dataclasses import dataclass
from datetime import date

from data_joinery import SparkContext


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
