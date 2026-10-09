"""Paths and Spark session supplied when the mart runs."""

from dataclasses import dataclass

from data_joinery.backends.spark import SparkContext


class DataDir(str):
    """Directory containing the eight generated TPC-H Parquet files."""


class OutputDir(str):
    """Directory for the materialized analytical tables."""


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    data_dir: DataDir
    output_dir: OutputDir
