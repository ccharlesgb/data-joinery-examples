"""Materialize the TPC-H analytical mart from locally generated Parquet."""

from pathlib import Path

from pyspark.sql import SparkSession

from .context import DataDir, OutputDir, PipelineContext
from .pipeline import build_pipeline


def main() -> None:
    example_dir = Path(__file__).resolve().parents[2]
    spark = SparkSession.builder.appName("tpch-analytical-mart").getOrCreate()
    try:
        build_pipeline().run(
            PipelineContext(
                spark=spark,
                data_dir=DataDir(str(example_dir / "data")),
                output_dir=OutputDir(str(example_dir / "__output")),
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
