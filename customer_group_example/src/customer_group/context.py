from __future__ import annotations

import atexit
from dataclasses import dataclass
from pathlib import Path

from data_joinery.backends.spark import SparkContext
from pyspark.sql import SparkSession


class CustomersPath(str):
    """
    Marker class for the customers path context.
    """


class OutputPath(str):
    """
    Marker class for the output path context.
    """


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    customers_path: CustomersPath
    output_path: OutputPath

    @classmethod
    def from_kwargs(
        cls, *, customers_path: str | None = None, output_path: str | None = None
    ) -> PipelineContext:
        example_dir = Path(__file__).resolve().parents[2]
        spark = (
            SparkSession.builder.appName("customer-group-pipeline")
            .master("local[*]")
            .getOrCreate()
        )
        atexit.register(spark.stop)
        return cls(
            spark=spark,
            customers_path=CustomersPath(
                customers_path
                if customers_path is not None
                else str(example_dir / "data" / "customers.parquet")
            ),
            output_path=OutputPath(
                output_path
                if output_path is not None
                else str(example_dir / "__output" / "output.parquet")
            ),
        )
