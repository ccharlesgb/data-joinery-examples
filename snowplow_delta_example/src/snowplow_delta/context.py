from dataclasses import dataclass

from data_joinery.backends.spark import SparkContext


class SnowplowDeltaPath(str):
    """Location of the Delta table written by the Snowplow Lake Loader."""


class CheckpointPath(str):
    """Durable checkpoint location for the Structured Streaming query."""


@dataclass(frozen=True)
class SnowplowDeltaContext(SparkContext):
    delta_path: SnowplowDeltaPath
    checkpoint_path: CheckpointPath
