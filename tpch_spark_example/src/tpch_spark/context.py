"""Runtime paths and Spark session for the TPC-H pipeline."""

from __future__ import annotations

import atexit
from dataclasses import dataclass
from pathlib import Path

from data_joinery.backends.spark import SparkContext
from pyspark.sql import SparkSession


class DataDir(str):
    """Directory containing source Parquet tables."""


class OutputDir(str):
    """Directory containing the analytical mart tables."""


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    data_dir: DataDir
    output_dir: OutputDir

    @classmethod
    def from_kwargs(
        cls, *, data_dir: str | None = None, output_dir: str | None = None
    ) -> PipelineContext:
        example_dir = Path(__file__).resolve().parents[2]
        spark = (
            SparkSession.builder.appName("tpch-spark-analytical-mart")
            .master("local[10]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.files.maxPartitionBytes", "64m")
            .getOrCreate()
        )
        # The Data Joinery CLI does not close services created by the context.
        atexit.register(spark.stop)
        return cls(
            spark=spark,
            data_dir=DataDir(
                data_dir
                if data_dir is not None
                else str(example_dir.parent / "datasets" / "tpch")
            ),
            output_dir=OutputDir(
                output_dir if output_dir is not None else str(example_dir / "__output")
            ),
        )
