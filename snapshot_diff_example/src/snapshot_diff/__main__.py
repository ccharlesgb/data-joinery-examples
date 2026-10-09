from pyspark.sql import DataFrame, SparkSession

from .context import OutputPath, PipelineContext, RunDate, SnapshottedDimensionPath
from .pipeline import DATA_DIR, OUTPUT_DIR, build_pipeline


def main() -> None:
    spark = (
        SparkSession.builder.appName("snapshot-diff-pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        context = PipelineContext(
            spark=spark,
            snapshotted_dimension_path=SnapshottedDimensionPath(
                str(DATA_DIR / "snapshotted_dimension.parquet")
            ),
            output_path=OutputPath(str(OUTPUT_DIR / "output.parquet")),
            run_date=RunDate(2026, 1, 1),
        )
        result = build_pipeline().run(context)
        result.get_output("compute_dimension_transitions", DataFrame).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
