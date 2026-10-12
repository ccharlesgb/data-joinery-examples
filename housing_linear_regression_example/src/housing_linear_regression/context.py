from __future__ import annotations

import atexit
from dataclasses import dataclass
from pathlib import Path

from data_joinery.backends.spark import SparkContext
from pyspark.sql import SparkSession


class HousingPath(str):
    """Marker class for the housing input path context."""


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    housing_path: HousingPath

    @classmethod
    def from_kwargs(cls, *, housing_path: str | None = None) -> PipelineContext:
        example_dir = Path(__file__).resolve().parents[2]
        spark = (
            SparkSession.builder.appName("housing-linear-regression-pipeline")
            .master("local[*]")
            .getOrCreate()
        )
        atexit.register(spark.stop)
        return cls(
            spark=spark,
            housing_path=HousingPath(
                housing_path
                if housing_path is not None
                else str(example_dir / "data" / "housing.parquet")
            ),
        )
