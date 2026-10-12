from __future__ import annotations

import atexit
from dataclasses import dataclass
from pathlib import Path

from data_joinery.backends.spark import SparkContext
from pyspark.sql import SparkSession


class AdvertEventsPath(str):
    """Pipeline-context type for the input AdvertEvents parquet path."""


@dataclass(frozen=True)
class AdvertPipelineContext(SparkContext):
    advert_events_path: AdvertEventsPath

    @classmethod
    def from_kwargs(
        cls, *, advert_events_path: str | None = None
    ) -> AdvertPipelineContext:
        example_dir = Path(__file__).resolve().parents[2]
        spark = (
            SparkSession.builder.appName("advert-view-linear-regression-pipeline")
            .master("local[*]")
            .getOrCreate()
        )
        atexit.register(spark.stop)
        return cls(
            spark=spark,
            advert_events_path=AdvertEventsPath(
                advert_events_path
                if advert_events_path is not None
                else str(example_dir / "data" / "advert_events.parquet")
            ),
        )
