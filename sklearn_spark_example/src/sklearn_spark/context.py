from dataclasses import dataclass

from pyspark.sql import SparkSession


class AdvertEventsPath(str):
    """Pipeline-context type for the input AdvertEvents parquet path."""


@dataclass(frozen=True)
class AdvertPipelineContext:
    spark: SparkSession
    advert_events_path: AdvertEventsPath
