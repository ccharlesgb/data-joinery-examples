from __future__ import annotations

import atexit
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from data_joinery.backends.spark import SparkContext
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class InputPath(str):
    """Append-only Delta table of video-view events."""


class OutputPath(str):
    """Delta table of current and finalized leaderboards."""


class CheckpointPath(str):
    """Durable streaming offsets, counts, and event-time timers."""


@dataclass(frozen=True)
class TopKSettings:
    k: int = 10
    window_minutes: int = 5
    lateness_minutes: int = 2
    available_now: bool = True
    trigger_interval: str = "5 seconds"

    def __post_init__(self) -> None:
        if self.k < 1 or self.window_minutes < 1 or self.lateness_minutes < 0:
            raise ValueError(
                "k and window_minutes must be positive; lateness_minutes must be nonnegative"
            )


def create_spark() -> SparkSession:
    # SparkContext uses this environment variable when serializing Python UDFs.
    # The stateful runner must use the environment containing this package.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("video-topk")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


@dataclass(frozen=True)
class PipelineContext(SparkContext):
    input_path: InputPath
    output_path: OutputPath
    checkpoint_path: CheckpointPath
    settings: TopKSettings

    @classmethod
    def from_kwargs(
        cls,
        *,
        input_path: str | None = None,
        output_path: str | None = None,
        checkpoint_path: str | None = None,
        k: str = "10",
        window_minutes: str = "5",
        lateness_minutes: str = "2",
        available_now: str = "true",
        trigger_interval: str = "5 seconds",
    ) -> PipelineContext:
        if available_now.lower() not in {"true", "false"}:
            raise ValueError("available_now must be true or false")
        settings = TopKSettings(
            k=int(k),
            window_minutes=int(window_minutes),
            lateness_minutes=int(lateness_minutes),
            available_now=available_now.lower() == "true",
            trigger_interval=trigger_interval,
        )
        root = Path(__file__).resolve().parents[2] / "__output"
        spark = create_spark()
        atexit.register(spark.stop)
        source = InputPath(
            input_path if input_path is not None else str(root / "views")
        )
        if input_path is None:
            # Only the self-contained demo source is initialized automatically.
            from .demo import seed_views

            seed_views(spark, source)
        return cls(
            spark=spark,
            input_path=source,
            output_path=OutputPath(
                output_path if output_path is not None else str(root / "leaderboards")
            ),
            checkpoint_path=CheckpointPath(
                checkpoint_path
                if checkpoint_path is not None
                else str(root / "checkpoint")
            ),
            settings=settings,
        )
